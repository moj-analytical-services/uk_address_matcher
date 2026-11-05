from __future__ import annotations

import logging
import re
import time
from collections.abc import Callable
from copy import copy
from functools import partial
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Optional, Union

from uk_address_matcher.cleaning.chunking_strategies import (
    _add_canonical_road_blocking_keys,
    derive_inverted_index,
    derive_roadlike_places,
    derive_term_frequencies_table,
    prepare_data_for_matching,
)
from uk_address_matcher.cleaning.materialisation import _parquet_preserves_type
from uk_address_matcher.cleaning.steps.inverted_index import (
    MESSY_INVERTED_INDEX_LOOKUP_STRATEGIES,
)
from uk_address_matcher.cleaning.steps.roadlike_places import add_road_blocking_features
from uk_address_matcher.helpers.canonical_inputs import (
    normalise_and_validate_raw_canonical,
)
from uk_address_matcher.linking_model.address_record import AddressRecord
from uk_address_matcher.linking_model.matching.runner import _run_matching
from uk_address_matcher.linking_model.matching.stages.base_stage import MatchingStage
from uk_address_matcher.linking_model.matching.stages.splink import SplinkStage
from uk_address_matcher.linking_model.matching.utils import format_elapsed, safe_divide
from uk_address_matcher.logging.chunking import report_stage
from uk_address_matcher.logging.progress import (
    ShowProgress,
    _ProgressBar,
    resolve_progress_mode,
)
from uk_address_matcher.post_linkage.match_result import MatchResult
from uk_address_matcher.prepare_canonical import load_prepared_canonical_data
from uk_address_matcher.sql_pipeline.helpers import (
    _drop_table_and_registered_aliases,
    _quote_identifier,
    _register_input_relation_once,
    _uid,
)

if TYPE_CHECKING:
    import duckdb

    from uk_address_matcher._typing import StageDiagnostics
    from uk_address_matcher.sql_pipeline.runner import DebugOptions

logger = logging.getLogger("uk_address_matcher")


def _default_stages() -> list[MatchingStage]:
    """Return the default stage sequence: exact match then Splink."""
    from uk_address_matcher.linking_model.matching.stages import ExactMatchStage
    from uk_address_matcher.linking_model.matching.stages.splink import SplinkStage

    return [ExactMatchStage(), SplinkStage()]


class AddressMatcher:
    """Primary entry point for address matching.

    Accepts either a raw `DuckDBPyRelation` (cleaned on the fly) or a
    `str` / `Path` pointing to a folder created by
    `prepare_canonical_folder` for canonical addresses.
    Messy addresses can be a DuckDB relation or a list of
    `AddressRecord` / dicts.

    Stages default to `[ExactMatchStage(), SplinkStage()]`. Pass your own
    list to customise matching behaviour — the existing stage dataclasses
    (`ExactMatchStage`, `UniqueTrigramStage`, `SplinkStage`) already
    expose all the knobs you need.

    Args:
        canonical_addresses: Canonical dataset to match against. Can be a
            `DuckDBPyRelation` or a path to a prepared canonical folder.
        canonical_address_filter: Optional DuckDB SQL expression used to
            filter canonical addresses after load (for prepared folders)
            or directly on the provided canonical relation.
        addresses_to_match: Messy addresses to resolve. Can be a
            `DuckDBPyRelation`, a list of `AddressRecord`, or a list of dicts
            with `address_concat`, `postcode`, and `unique_id` fields.
        con: DuckDB connection to use for all operations.
        stages: Optional list of `MatchingStage` instances defining the
            matching pipeline. Defaults to exact match followed by Splink.
        cleaning_num_chunks: Number of chunks to use for cleaning and term
            frequency derivation when canonical input is a raw relation. Also
            used for messy-address cleaning. Must be a positive integer.
        matching_num_chunks: Number of hash buckets on messy `unique_id` to
            clean and match sequentially against the full canonical dataset.
            Defaults to 1 (no matching chunks). Empty buckets are skipped.
            Bucket sizes are approximate; repeated IDs stay together.
            Chunked results use temporary Parquet parts until `MatchResult.close()`
            and support input-column joins, but do not retain per-chunk Splink
            inspection tables.
        show_progress: ``"auto"`` renders live updates in a supported
            interactive terminal and otherwise logs stage boundaries.
            ``"stages"`` logs only stage boundaries; ``"off"`` suppresses
            progress output.
        debug_options: Optional `DebugOptions` to control debug output and logging.

    Examples:
        Simple matching:

            import duckdb
            from uk_address_matcher import AddressMatcher

            con = duckdb.connect()
            canonical = con.read_parquet("./canonical.parquet")
            messy = con.read_parquet("./messy.parquet")

            matcher = AddressMatcher(
                canonical_addresses=canonical,
                addresses_to_match=messy,
                con=con,
            )
            result = matcher.match()

        Custom stages:

            from uk_address_matcher import (
                AddressMatcher, ExactMatchStage, UniqueTrigramStage, SplinkStage,
            )

            matcher = AddressMatcher(
                canonical_addresses=canonical,
                addresses_to_match=messy,
                con=con,
                stages=[
                    ExactMatchStage(),
                    UniqueTrigramStage(),
                    SplinkStage(),
                ],
            )
            result = matcher.match()

        Pre-prepared canonical data:

            matcher = AddressMatcher(
                canonical_addresses="./prepared_addressbase",
                addresses_to_match=messy,
                con=con,
            )
            result = matcher.match()
    """

    def __init__(
        self,
        canonical_addresses: Union[duckdb.DuckDBPyRelation, str, Path],
        addresses_to_match: Union[
            duckdb.DuckDBPyRelation,
            list[AddressRecord],
            list[dict],
        ],
        *,
        canonical_address_filter: str | None = None,
        con: duckdb.DuckDBPyConnection,
        stages: Optional[list[MatchingStage]] = None,
        debug_options: Optional[DebugOptions] = None,
        cleaning_num_chunks: int = 10,
        show_progress: ShowProgress = "auto",
        matching_num_chunks: int = 1,
    ):
        self.con = con
        self.stages = stages if stages is not None else _default_stages()
        self.debug_options = debug_options
        self.show_progress = resolve_progress_mode(show_progress)
        self.canonical_address_filter = canonical_address_filter
        if not isinstance(cleaning_num_chunks, int):
            raise TypeError("cleaning_num_chunks must be an integer.")
        if cleaning_num_chunks < 1:
            raise ValueError("cleaning_num_chunks must be >= 1.")
        self.cleaning_num_chunks = cleaning_num_chunks
        if not isinstance(matching_num_chunks, int) or isinstance(
            matching_num_chunks, bool
        ):
            raise TypeError("matching_num_chunks must be an integer.")
        if matching_num_chunks < 1:
            raise ValueError("matching_num_chunks must be >= 1.")
        self.matching_num_chunks = matching_num_chunks
        if self.canonical_address_filter is not None and not isinstance(
            self.canonical_address_filter, str
        ):
            raise TypeError("canonical_address_filter must be a SQL string or None.")

        if isinstance(canonical_addresses, (str, Path)):
            self._raw_canonical = canonical_addresses
        else:
            canonical_relation = _register_input_relation_once(
                canonical_addresses,
                con=self.con,
                role="canonical",
            )
            if self.canonical_address_filter is not None:
                canonical_relation = canonical_relation.filter(
                    self.canonical_address_filter
                )
            self._raw_canonical = canonical_relation

        coerced_messy = self._coerce_addresses_to_match(addresses_to_match)
        self._raw_messy = _register_input_relation_once(
            coerced_messy,
            con=self.con,
            role="messy",
        )

        # Internal state — populated during match()
        self._canonical_clean: duckdb.DuckDBPyRelation | None = None
        self._canonical_count: int | None = None
        self._roadlike_places: duckdb.DuckDBPyRelation | None = None
        self._tf_table: duckdb.DuckDBPyRelation | None = None
        self._inverted_index_table_name: str | None = None
        self._messy_clean: duckdb.DuckDBPyRelation | None = None

    def _register_inverted_index(
        self,
        inverted_index: duckdb.DuckDBPyRelation,
    ) -> None:
        """Register the index, keeping chunked prepared data disk-backed."""
        if self._inverted_index_table_name is not None:
            _drop_table_and_registered_aliases(self.con, self._inverted_index_table_name)

        source_name = getattr(inverted_index, "alias", None)
        source_relation = _register_input_relation_once(
            inverted_index,
            con=self.con,
            role="inverted_index_source",
        )

        table_name = f"__ukam__inverted_index_{_uid()}"
        # A view avoids keeping the full prepared index resident across matching chunks.
        storage = "TABLE"
        if self.matching_num_chunks > 1 and isinstance(self._raw_canonical, (str, Path)):
            storage = "TEMP VIEW"
        self.con.execute(
            f"CREATE {storage} "
            + table_name
            + " AS SELECT * FROM ("
            + source_relation.sql_query()
            + ")"
        )
        # The derived source table has been copied; it has no further use.
        if isinstance(source_name, str) and source_name.startswith(
            "__ukam_derived_inverted_index_"
        ):
            _drop_table_and_registered_aliases(self.con, source_name)
        self._inverted_index_table_name = table_name

    @property
    def _inverted_index(self) -> duckdb.DuckDBPyRelation | None:
        """Return the registered inverted index relation, if available."""
        if self._inverted_index_table_name is None:
            return None
        return self.con.table(self._inverted_index_table_name)

    def _resolve_canonical_data(self) -> None:
        """Loads or cleans canonical data depending on the input type."""

        self._canonical_count = None
        self._roadlike_places = None

        if isinstance(self._raw_canonical, (str, Path)):
            logger.debug("Loading prepared canonical data from '%s'", self._raw_canonical)
            prepared = load_prepared_canonical_data(
                self._raw_canonical,
                self.con,
                canonical_address_filter=self.canonical_address_filter,
            )
            self._canonical_clean = prepared.addresses
            self._roadlike_places = prepared.roadlike_places
            self._tf_table = prepared.term_frequencies
            self._register_inverted_index(prepared.inverted_index)
            if (
                self._roadlike_places is not None
                and "road_1_norm" not in self._canonical_clean.columns
            ):
                self._canonical_clean = _add_canonical_road_blocking_keys(
                    self._canonical_clean,
                    self.con,
                    num_of_chunks=self.cleaning_num_chunks,
                    roadlike_places=self._roadlike_places,
                )

        else:
            canonical_for_preparation = normalise_and_validate_raw_canonical(
                self._raw_canonical
            )

            # Data is either raw or only pre-cleaned.  In both cases we need
            # term frequencies and the inverted index.  `prepare_data_for_matching`
            # handles pre-cleaned input correctly (it checks internally).
            logger.debug("Deriving term frequencies from canonical data")
            self._tf_table = derive_term_frequencies_table(
                canonical_for_preparation,
                con=self.con,
                num_of_chunks=self.cleaning_num_chunks,
                debug_options=self.debug_options,
                show_progress=self.show_progress,
            )

            logger.debug("Cleaning canonical data")
            self._canonical_clean = prepare_data_for_matching(
                canonical_for_preparation,
                con=self.con,
                num_of_chunks=self.cleaning_num_chunks,
                term_frequency_lookup=self._tf_table,
                derive_distinguishing_wrt_adjacent_records=True,
                dataset_role="canonical",
                debug_options=self.debug_options,
                show_progress=self.show_progress,
            )
            if any(isinstance(stage, SplinkStage) for stage in self.stages):
                self._roadlike_places = derive_roadlike_places(
                    self._canonical_clean,
                    self.con,
                    debug_options=self.debug_options,
                    show_progress=self.show_progress,
                )
                self._canonical_clean = _add_canonical_road_blocking_keys(
                    self._canonical_clean,
                    self.con,
                    num_of_chunks=self.cleaning_num_chunks,
                    roadlike_places=self._roadlike_places,
                )
            logger.debug("Building inverted index from canonical data")
            inverted_index = derive_inverted_index(
                self._canonical_clean,
                con=self.con,
                debug_options=self.debug_options,
                show_progress=self.show_progress,
            )
            self._register_inverted_index(inverted_index)

        for stage in self.stages:
            if isinstance(stage, SplinkStage):
                stage.roadlike_places = self._roadlike_places

    def _resolve_messy_data(self, *, show_progress: ShowProgress | None = None) -> None:
        """Cleans messy data, reusing the canonical term frequencies and index."""

        logger.debug("Cleaning messy data")
        if self._canonical_clean is not None and self._canonical_count is None:
            self._canonical_count = self._canonical_clean.count("*").fetchone()[0]
        self._messy_clean = prepare_data_for_matching(
            self._raw_messy,
            con=self.con,
            num_of_chunks=self.cleaning_num_chunks,
            # If nothing was loaded from disk, these will be None — but that's fine,
            term_frequency_lookup=self._tf_table,
            inverted_index=self._inverted_index,
            _inverted_index_strategies=MESSY_INVERTED_INDEX_LOOKUP_STRATEGIES,
            inverted_index_n=self._canonical_count,
            dataset_role="messy",
            debug_options=self.debug_options,
            show_progress=self.show_progress if show_progress is None else show_progress,
        )
        if self._roadlike_places is not None:
            logger.debug("Deriving messy road blocking keys from canonical catalogue")
            self._messy_clean = add_road_blocking_features(
                self.con,
                self._messy_clean,
                roadlike_places=self._roadlike_places,
            )
        else:
            logger.debug("No road catalogue available; skipping road parsing")

    def _coerce_addresses_to_match(
        self,
        addresses_to_match: Union[
            duckdb.DuckDBPyRelation,
            list[AddressRecord],
            list[dict],
        ],
    ) -> duckdb.DuckDBPyRelation:
        """Coerce addresses_to_match into a DuckDB relation."""

        if isinstance(addresses_to_match, list):
            if not addresses_to_match:
                raise ValueError("addresses_to_match cannot be empty.")
            if all(isinstance(record, AddressRecord) for record in addresses_to_match):
                return AddressRecord.to_duckdb_relation(addresses_to_match, self.con)
            if all(isinstance(record, dict) for record in addresses_to_match):
                records = [
                    AddressRecord.from_dict(record) for record in addresses_to_match
                ]
                return AddressRecord.to_duckdb_relation(records, self.con)
            raise TypeError(
                "addresses_to_match must be a DuckDB relation, a list of AddressRecord, "
                "or a list of dicts."
            )

        return addresses_to_match

    def _match_chunk(
        self,
        *,
        row_offset: int = 0,
        progress_callback: Callable[[str, int, int, float | None], None] | None = None,
    ) -> tuple[duckdb.DuckDBPyRelation, StageDiagnostics]:
        if row_offset:
            self._messy_clean = self._messy_clean.project(
                f"* REPLACE (ukam_address_id::BIGINT + {row_offset} AS ukam_address_id)"
            )
        return _run_matching(
            con=self.con,
            df_messy_clean=self._messy_clean,
            df_canonical_clean=self._canonical_clean,
            stages=self.stages,
            debug_options=self.debug_options,
            _progress_callback=progress_callback,
        )

    def _match_chunks(
        self, started_at: float
    ) -> tuple[duckdb.DuckDBPyRelation, StageDiagnostics, TemporaryDirectory]:
        raw_messy = self._raw_messy
        chunks = self.con.sql(f"""
            SELECT hash(unique_id) % {self.matching_num_chunks} AS bucket,
                count(*) AS rows
            FROM ({raw_messy.sql_query()}) AS messy
            GROUP BY bucket ORDER BY bucket
        """).fetchall()
        if not chunks:
            raise ValueError(
                "Supplied address table has no records. Please provide a non-empty table."
            )
        total_rows = sum(rows for _, rows in chunks)
        processed_rows = 0
        retained_tables = {name for (name,) in self.con.execute("SHOW TABLES").fetchall()}
        output_directory = TemporaryDirectory(prefix="ukam-match-chunks-")
        canonical_cache_directory = TemporaryDirectory(prefix="ukam-canonical-cache-")
        encoded_types = {}
        stage_totals = {}
        chunks_started_at = time.perf_counter()
        progress = _ProgressBar(
            label="Matching",
            total=total_rows,
            total_units=len(chunks),
            enabled=self.show_progress == "auto",
            width=16,
        )

        try:
            for stage_index, stage in enumerate(self.stages):
                if isinstance(stage, SplinkStage):
                    stage._canonical_cache_path = str(
                        Path(canonical_cache_directory.name) / f"{stage_index}.parquet"
                    )
            for chunk_number, (bucket, rows) in enumerate(chunks, start=1):
                chunk_started_at = time.perf_counter()
                self._raw_messy = raw_messy.filter(
                    f"hash(unique_id) % {self.matching_num_chunks} = {bucket}"
                )
                try:
                    progress.label = f"Chunk {chunk_number}/{len(chunks)} cleaning"
                    progress.update(processed_rows, completed_units=chunk_number - 1)
                    self._resolve_messy_data(show_progress="off")
                    result, diagnostics = self._match_chunk(
                        row_offset=processed_rows,
                        progress_callback=partial(
                            report_stage,
                            progress_mode=self.show_progress,
                            progress=progress,
                            chunk_number=chunk_number,
                            total_chunks=len(chunks),
                            processed_rows=processed_rows,
                            total_rows=total_rows,
                            started_at=started_at,
                            chunks_started_at=chunks_started_at,
                        ),
                    )
                    projection = []
                    for column, kind in zip(result.columns, result.types, strict=True):
                        quoted = _quote_identifier(column)
                        if _parquet_preserves_type(kind):
                            projection.append(quoted)
                        else:
                            encoded_types[column] = kind
                            projection.append(f"to_json({quoted})::VARCHAR AS {quoted}")
                    result = result.project(", ".join(projection))
                    result.write_parquet(
                        str(Path(output_directory.name) / f"{chunk_number}.parquet"),
                        compression="snappy",
                    )
                    for diagnostic in diagnostics:
                        stage_name = diagnostic["stage"]
                        if stage_name not in stage_totals:
                            stage_totals[stage_name] = diagnostic.copy()
                        else:
                            for field in (
                                "unmatched_before",
                                "matched_this_stage",
                                "remaining_after",
                                "elapsed_seconds",
                            ):
                                stage_totals[stage_name][field] += diagnostic[field]
                finally:
                    result = None
                    self._messy_clean = None
                    self._raw_messy = raw_messy
                    for stage in self.stages:
                        if isinstance(stage, SplinkStage):
                            stage._release_splink_tables()
                            stage.linker = None
                            stage.predictions_table = None
                            stage.improved_predictions_table = None
                            stage.best_matches_table = None
                    self._cleanup_intermediate_tables(None, retained_tables)
                processed_rows += rows
                now = time.perf_counter()
                elapsed = now - started_at
                remaining = (
                    (now - chunks_started_at)
                    * (total_rows - processed_rows)
                    / processed_rows
                )
                progress.label = f"Matching chunk {chunk_number}/{len(chunks)}"
                progress.update(
                    processed_rows,
                    completed_units=chunk_number,
                    remaining_seconds=remaining,
                )
                if self.show_progress != "off" and not progress.enabled:
                    filled = int(16 * processed_rows / total_rows)
                    bar = "#" * filled + "-" * (16 - filled)
                    logger.info(
                        "Matching chunk %d/%d completed in %s: [%s] %.0f%%; "
                        "%s/%s rows processed; elapsed %s; estimated total %s "
                        "(%s remaining).",
                        chunk_number,
                        len(chunks),
                        format_elapsed(now - chunk_started_at),
                        bar,
                        100 * processed_rows / total_rows,
                        f"{processed_rows:,}",
                        f"{total_rows:,}",
                        format_elapsed(elapsed),
                        format_elapsed(elapsed + remaining),
                        format_elapsed(remaining),
                    )
            result = self.con.read_parquet(
                str(Path(output_directory.name) / "*.parquet"),
                union_by_name=True,
                hive_partitioning=False,
            )
            if encoded_types:
                restored = ", ".join(
                    f"CAST({_quote_identifier(column)}::JSON AS {kind}) "
                    f"AS {_quote_identifier(column)}"
                    for column, kind in encoded_types.items()
                )
                result = result.project(f"* REPLACE ({restored})")
        except BaseException:
            output_directory.cleanup()
            raise
        finally:
            progress.close()
            self._raw_messy = raw_messy
            for stage in self.stages:
                if isinstance(stage, SplinkStage):
                    stage._canonical_cache_path = None
            canonical_cache_directory.cleanup()
        for diagnostic in stage_totals.values():
            diagnostic["matched_pct_of_unmatched"] = round(
                safe_divide(
                    diagnostic["matched_this_stage"], diagnostic["unmatched_before"]
                ),
                6,
            )
            diagnostic["matched_pct_of_input"] = round(
                safe_divide(diagnostic["matched_this_stage"], total_rows), 6
            )
        self._messy_clean = result
        if self.show_progress != "off":
            logger.info(
                "Chunked matching completed: %s rows across %d chunks in %s.",
                f"{total_rows:,}",
                len(chunks),
                format_elapsed(time.perf_counter() - started_at),
            )
        return result, list(stage_totals.values()), output_directory

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @classmethod
    def available_stages(cls) -> list[type[MatchingStage]]:
        """All registered ``MatchingStage`` subclasses.

        Delegates to ``MatchingStage.available_stages()`` which walks the
        subclass tree dynamically, so newly added stages are picked up
        automatically without maintaining a hard-coded list.
        """
        return MatchingStage.available_stages()

    def match(self) -> MatchResult:
        """Runs the full matching pipeline.

        Each stage is executed in sequence. Earlier stages consume easy
        matches; later stages handle the remainder.

        Returns:
            A `MatchResult` wrapper around the final DuckDB relation, including
            `unique_id`, `resolved_canonical_id`, `match_reason`, and any
            additional columns produced by the stages.
        """

        if self.show_progress != "off":
            stage_list = ", ".join(stage.__class__.__name__ for stage in self.stages)
            total_rows = self._raw_messy.count("*").fetchone()[0]
            logger.info(
                "Address matching: %s messy rows, %d requested chunk%s, "
                "full canonical dataset; stages: %s.",
                f"{total_rows:,}",
                self.matching_num_chunks,
                "" if self.matching_num_chunks == 1 else "s",
                stage_list,
            )

        existing_tables = {name for (name,) in self.con.execute("SHOW TABLES").fetchall()}
        for stage in self.stages:
            if isinstance(stage, SplinkStage):
                stage.linker = None
                stage.predictions_table = None
                stage.improved_predictions_table = None
                stage.best_matches_table = None
                stage._owned_splink_frames = ()

        completed = False
        output_directory = None
        try:
            started_at = time.perf_counter()
            self._resolve_canonical_data()
            if self.matching_num_chunks > 1:
                result, stage_diagnostics, output_directory = self._match_chunks(
                    started_at
                )
            else:
                self._resolve_messy_data()
            # The index is no longer needed once messy cleaning has finished.
            if self._inverted_index_table_name is not None:
                _drop_table_and_registered_aliases(
                    self.con, self._inverted_index_table_name
                )
                self._inverted_index_table_name = None

            if self.matching_num_chunks == 1:
                result, stage_diagnostics = self._match_chunk()
            completed = True
        finally:
            if not completed:
                for stage in self.stages:
                    if isinstance(stage, SplinkStage):
                        stage._release_splink_tables()
                self._cleanup_intermediate_tables(None, existing_tables)
                self._inverted_index_table_name = None
                if output_directory is not None:
                    output_directory.cleanup()

        splink_stage = next(
            (stage for stage in self.stages if isinstance(stage, SplinkStage)),
            None,
        )
        if self.matching_num_chunks > 1:
            splink_stage = None

        owned_tables = self._cleanup_intermediate_tables(result, existing_tables)
        # Snapshot inspection state too: callers may reuse these stage objects.
        if splink_stage is not None:
            splink_stage = copy(splink_stage)
        owned_splink_frames = tuple(
            frame
            for stage in self.stages
            if isinstance(stage, SplinkStage)
            for frame in stage._owned_splink_frames
        )

        return MatchResult(
            result,
            con=self.con,
            _splink_stage=splink_stage,
            _canonical_relation=self._canonical_clean,
            _messy_relation=self._messy_clean,
            _stage_diagnostics=stage_diagnostics,
            _owned_table_names=owned_tables,
            _owned_splink_frames=owned_splink_frames,
            _temporary_directory=output_directory,
        )

    def _cleanup_intermediate_tables(
        self,
        result: duckdb.DuckDBPyRelation | None,
        existing_tables: set[str],
    ) -> tuple[str, ...]:
        """Clean only this run's objects, returning those retained for inspection."""
        retained = []
        table_names = [name for (name,) in self.con.execute("SHOW TABLES").fetchall()]
        for table_name in table_names:
            if table_name in existing_tables:
                continue
            # These pre-existing fixed-name caches have a separate lifecycle.
            if table_name in {"__ukam_derived_term_frequencies", "__ukam_index_meta"}:
                continue
            is_pipeline_input = re.fullmatch(r"root_[a-z0-9]{4}", table_name) is not None
            if not is_pipeline_input and not table_name.startswith(
                ("__ukam", "__splink__")
            ):
                continue
            if result is None or table_name.startswith(
                ("__ukam__tmp_", "__ukam_results_", "__ukam_stage_matches_")
            ):
                _drop_table_and_registered_aliases(self.con, table_name)
            elif not table_name.startswith("__splink__"):
                retained.append(table_name)
        return tuple(retained)
