from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Optional, Union

from uk_address_matcher.cleaning.chunking_strategies import (
    prepare_data_for_matching,
)
from uk_address_matcher.linking_model.address_record import AddressRecord
from uk_address_matcher.linking_model.matching.runner import _run_matching
from uk_address_matcher.linking_model.matching.stages.base_stage import MatchingStage
from uk_address_matcher.post_linkage.match_result import MatchResult

if TYPE_CHECKING:
    import duckdb

    from uk_address_matcher.sql_pipeline.runner import DebugOptions

logger = logging.getLogger("uk_address_matcher")


def _ensure_splink_udfs(con: duckdb.DuckDBPyConnection) -> None:
    """Installs and loads the splink_udfs community extension if needed."""
    loaded = con.execute(
        "SELECT * FROM duckdb_extensions() "
        "WHERE extension_name = 'splink_udfs' AND loaded"
    ).fetchone()
    if loaded is None:
        con.execute("INSTALL splink_udfs FROM community")
        con.execute("LOAD splink_udfs")


def _is_fully_prepared(rel: duckdb.DuckDBPyRelation) -> bool:
    """True when a relation has been through `prepare_data_for_matching`.

    The full pipeline adds term-frequency columns (e.g. `tf_numeric_token_1`)
    and `exploding_unique_ids`, which distinguish it from the lighter output
    of `clean_data_pre_term_frequencies`.
    """
    cols = set(rel.columns)
    return "tf_numeric_token_1" in cols and "exploding_unique_ids" in cols


def _is_pre_cleaned(rel: duckdb.DuckDBPyRelation) -> bool:
    """True when a relation has been through `clean_data_pre_term_frequencies`.

    That stage adds `ukam_address_id` but does not yet add term-frequency
    adjustments or trigram blocking columns.
    """
    return "ukam_address_id" in rel.columns and not _is_fully_prepared(rel)


def _default_stages() -> list[MatchingStage]:
    """Return the default stage sequence: exact match then Splink."""
    from uk_address_matcher.linking_model.matching.stages import ExactMatchStage
    from uk_address_matcher.linking_model.matching.stages.splink import SplinkStage

    return [ExactMatchStage(), SplinkStage()]


class AddressMatcher:
    """Primary entry point for address matching.

    Accepts either a raw `DuckDBPyRelation` (cleaned on the fly) or a
    `str` / `Path` pointing to a folder created by
    `prepare_canonical_folder`.

    Stages default to `[ExactMatchStage(), SplinkStage()]`. Pass your own
    list to customise matching behaviour — the existing stage dataclasses
    (`ExactMatchStage`, `UniqueTrigramStage`, `SplinkStage`) already
    expose all the knobs you need.

    Args:
        canonical_addresses: Canonical dataset to match against. Can be a
            `DuckDBPyRelation` or a path to a prepared canonical folder.
        addresses_to_match: Messy addresses to resolve. Must be a
            `DuckDBPyRelation`.
        con: DuckDB connection to use for all operations.
        stages: Optional list of `MatchingStage` instances defining the
            matching pipeline. Defaults to exact match followed by Splink.
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
                    SplinkStage(
                        final_match_weight_threshold=20,
                        final_distinguishability_threshold=5.0,
                    ),
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
        addresses_to_match: duckdb.DuckDBPyRelation,
        *,
        con: duckdb.DuckDBPyConnection,
        stages: Optional[list[MatchingStage]] = None,
        debug_options: Optional[DebugOptions] = None,
    ):
        self.con = con
        _ensure_splink_udfs(self.con)
        self.stages = stages if stages is not None else _default_stages()
        self.debug_options = debug_options

        self._raw_canonical = canonical_addresses
        self._raw_messy = addresses_to_match

        # Internal state — populated during match()
        self._canonical_clean: duckdb.DuckDBPyRelation | None = None
        self._tf_table: duckdb.DuckDBPyRelation | None = None
        self._inverted_index: duckdb.DuckDBPyRelation | None = None
        self._messy_clean: duckdb.DuckDBPyRelation | None = None

    def _resolve_canonical_data(self) -> None:
        """Loads or cleans canonical data depending on the input type."""
        from uk_address_matcher.cleaning.chunking_strategies import (
            derive_inverted_index,
            derive_term_frequencies_table,
            prepare_data_for_matching,
        )
        from uk_address_matcher.prepare_canonical import load_prepared_canonical_data

        if isinstance(self._raw_canonical, (str, Path)):
            logger.debug(
                "Loading prepared canonical data from '%s'", self._raw_canonical
            )
            prepared = load_prepared_canonical_data(self._raw_canonical, self.con)
            self._canonical_clean = prepared.addresses
            self._tf_table = prepared.term_frequencies
            self._inverted_index = prepared.inverted_index

        elif _is_fully_prepared(self._raw_canonical):
            logger.debug("Canonical data already fully prepared; skipping.")
            self._canonical_clean = self._raw_canonical

        else:
            # Data is either raw or only pre-cleaned.  In both cases we need
            # term frequencies and the inverted index.  `prepare_data_for_matching`
            # handles pre-cleaned input correctly (it checks internally).
            logger.debug("Deriving term frequencies from canonical data")
            self._tf_table = derive_term_frequencies_table(
                self._raw_canonical, con=self.con
            )

            logger.debug("Cleaning canonical data")
            self._canonical_clean = prepare_data_for_matching(
                self._raw_canonical,
                con=self.con,
                term_frequency_lookup=self._tf_table,
            )

            logger.debug("Building inverted index from canonical data")
            self._inverted_index = derive_inverted_index(
                self._canonical_clean, con=self.con
            )

    def _resolve_messy_data(self) -> None:
        """Cleans messy data, reusing the canonical term frequencies and index."""
        from uk_address_matcher.cleaning.chunking_strategies import (
            prepare_data_for_matching,
        )

        if _is_fully_prepared(self._raw_messy):
            logger.debug("Messy data already fully prepared; skipping.")
            self._messy_clean = self._raw_messy
        else:
            logger.debug("Cleaning messy data")
            self._messy_clean = prepare_data_for_matching(
                self._raw_messy,
                con=self.con,
                # If nothing was loaded from disk, these will be None — but that's fine,
                term_frequency_lookup=self._tf_table,
                inverted_index=self._inverted_index,
            )

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

        result = _run_matching(
            con=self.con,
            df_messy_clean=self._messy_clean,
            df_canonical_clean=self._canonical_clean,
            stages=self.stages,
            debug_options=self.debug_options,
        )

        splink_stage = self._find_splink_stage()
        splink_tables = {}
        splink_linker = None
        if splink_stage is not None:
            splink_linker = splink_stage.linker
            if splink_stage.predictions_table is not None:
                splink_tables["predictions"] = splink_stage.predictions_table

        return MatchResult(
            result,
            con=self.con,
            metadata={"splink_tables": splink_tables},
            _splink_linker=splink_linker,
        )

    def match_one(
        self,
        address: Union[AddressRecord, dict],
        *,
        top_n: int = 5,
    ) -> duckdb.DuckDBPyRelation:
        """Matches a single address against the canonical dataset.

        Useful for testing, debugging, or interactive exploration.

        Args:
            address: An `AddressRecord` or a dict with keys
                `address_concat`, `postcode`, and optionally `unique_id`.
            top_n: Number of top candidates to return.

        Returns:
            A `DuckDBPyRelation` with the top matching candidates.

        Examples:
                from uk_address_matcher import AddressMatcher, AddressRecord

                result = matcher.match_one(
                    AddressRecord(
                        address_concat="10 downing street westminster london",
                        postcode="SW1A 2AA",
                    )
                )

                # Or with a plain dict:
                result = matcher.match_one({
                    "address_concat": "10 downing street westminster london",
                    "postcode": "SW1A 2AA",
                })
        """

        if isinstance(address, dict):
            record = AddressRecord.from_dict(address)
        elif isinstance(address, AddressRecord):
            record = address
        else:
            raise TypeError(
                f"Expected `AddressRecord` or dict, got {type(address).__name__}"
            )

        single_rel = record.as_duckdb_relation(self.con)

        if self._canonical_clean is None:
            self._resolve_canonical_data()

        messy_clean = prepare_data_for_matching(
            single_rel,
            con=self.con,
            term_frequency_lookup=self._tf_table,
            inverted_index=self._inverted_index,
        )

        result = _run_matching(
            con=self.con,
            df_messy_clean=messy_clean,
            df_canonical_clean=self._canonical_clean,
            stages=self.stages,
            debug_options=self.debug_options,
        )

        return result.limit(top_n)

    def _find_splink_stage(self):
        """Return the first SplinkStage instance from the stage list, or None."""
        from uk_address_matcher.linking_model.matching.stages.splink import SplinkStage

        for stage in self.stages:
            if isinstance(stage, SplinkStage):
                return stage
        return None
