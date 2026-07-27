from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Optional, Union

from uk_address_matcher._experimental import _current_lookup_strategies
from uk_address_matcher.cleaning.chunking_strategies import (
    derive_inverted_index,
    derive_term_frequencies_table,
    prepare_data_for_matching,
)
from uk_address_matcher.helpers.canonical_inputs import (
    normalise_and_validate_raw_canonical,
)
from uk_address_matcher.linking_model.address_record import AddressRecord
from uk_address_matcher.linking_model.matching.runner import _run_matching
from uk_address_matcher.linking_model.matching.stages.base_stage import MatchingStage
from uk_address_matcher.linking_model.matching.stages.splink import SplinkStage
from uk_address_matcher.logging.progress import ShowProgress, resolve_progress_mode
from uk_address_matcher.post_linkage.match_result import MatchResult
from uk_address_matcher.prepare_canonical import load_prepared_canonical_data
from uk_address_matcher.sql_pipeline.helpers import (
    _drop_table_and_registered_aliases,
    _register_input_relation_once,
    _uid,
)

if TYPE_CHECKING:
    import duckdb

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
        show_progress: ``True`` uses automatic live progress when supported;
            ``False`` suppresses progress output. ``"auto"`` renders live
            updates only in a supported interactive terminal and otherwise logs
            stage boundaries. ``"stages"`` logs only stage boundaries; ``"off"``
            suppresses progress output.
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
        show_progress: ShowProgress = True,
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
        self._tf_table: duckdb.DuckDBPyRelation | None = None
        self._inverted_index_table_name: str | None = None
        self._messy_clean: duckdb.DuckDBPyRelation | None = None

    def _register_inverted_index(
        self,
        inverted_index: duckdb.DuckDBPyRelation,
    ) -> None:
        """Materialise and register inverted index on this matcher's connection."""
        if self._inverted_index_table_name is not None:
            _drop_table_and_registered_aliases(self.con, self._inverted_index_table_name)

        source_relation = _register_input_relation_once(
            inverted_index,
            con=self.con,
            role="inverted_index_source",
        )

        table_name = f"__ukam__inverted_index_{_uid()}"
        self.con.execute(
            "CREATE TABLE "
            + table_name
            + " AS SELECT * FROM ("
            + source_relation.sql_query()
            + ")"
        )
        self._inverted_index_table_name = table_name

    @property
    def _inverted_index(self) -> duckdb.DuckDBPyRelation | None:
        """Return the registered inverted index relation, if available."""
        if self._inverted_index_table_name is None:
            return None
        return self.con.table(self._inverted_index_table_name)

    def _resolve_canonical_data(self) -> None:
        """Loads or cleans canonical data depending on the input type."""

        if isinstance(self._raw_canonical, (str, Path)):
            logger.debug("Loading prepared canonical data from '%s'", self._raw_canonical)
            prepared = load_prepared_canonical_data(
                self._raw_canonical,
                self.con,
                canonical_address_filter=self.canonical_address_filter,
            )
            self._canonical_clean = prepared.addresses
            self._tf_table = prepared.term_frequencies
            self._register_inverted_index(prepared.inverted_index)

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
                dataset_role="canonical",
                debug_options=self.debug_options,
                show_progress=self.show_progress,
            )

            logger.debug("Building inverted index from canonical data")
            inverted_index = derive_inverted_index(
                self._canonical_clean,
                con=self.con,
                debug_options=self.debug_options,
                show_progress=self.show_progress,
            )
            self._register_inverted_index(inverted_index)

    def _resolve_messy_data(self) -> None:
        """Cleans messy data, reusing the canonical term frequencies and index."""

        logger.debug("Cleaning messy data")
        inverted_index_n: int | None = None
        if self._canonical_clean is not None:
            inverted_index_n = self._canonical_clean.count("*").fetchone()[0]
        self._messy_clean = prepare_data_for_matching(
            self._raw_messy,
            con=self.con,
            num_of_chunks=self.cleaning_num_chunks,
            # If nothing was loaded from disk, these will be None — but that's fine,
            term_frequency_lookup=self._tf_table,
            inverted_index=self._inverted_index,
            _inverted_index_strategies=_current_lookup_strategies(),
            inverted_index_n=inverted_index_n,
            dataset_role="messy",
            debug_options=self.debug_options,
            show_progress=self.show_progress,
        )

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

        stage_list = "\n".join(f"    - {s.__class__.__name__}" for s in self.stages)
        logger.info("Running address matcher with stages:\n%s", stage_list)

        self._resolve_canonical_data()
        self._resolve_messy_data()

        result, stage_diagnostics = _run_matching(
            con=self.con,
            df_messy_clean=self._messy_clean,
            df_canonical_clean=self._canonical_clean,
            stages=self.stages,
            debug_options=self.debug_options,
        )

        splink_stage = next(
            (stage for stage in self.stages if isinstance(stage, SplinkStage)),
            None,
        )

        self._cleanup_intermediate_tables(result)

        return MatchResult(
            result,
            con=self.con,
            _splink_stage=splink_stage,
            _canonical_relation=self._canonical_clean,
            _messy_relation=self._messy_clean,
            _stage_diagnostics=stage_diagnostics,
        )

    def _cleanup_intermediate_tables(self, result: duckdb.DuckDBPyRelation) -> None:
        """A simple cleaning utility to drop transient tables created during
        matching, while keeping the final result and canonical/messy tables."""
        keep_names = {
            getattr(result, "alias", None),
            getattr(self._canonical_clean, "alias", None),
            getattr(self._messy_clean, "alias", None),
            self._inverted_index_table_name,
        }
        keep_names = {name for name in keep_names if isinstance(name, str) and name}

        transient_prefixes = ("__ukam__tmp_",)

        table_names = [name for (name,) in self.con.execute("SHOW TABLES").fetchall()]
        for table_name in table_names:
            if table_name in keep_names:
                continue
            if not table_name.startswith(transient_prefixes):
                continue

            _drop_table_and_registered_aliases(self.con, table_name)
