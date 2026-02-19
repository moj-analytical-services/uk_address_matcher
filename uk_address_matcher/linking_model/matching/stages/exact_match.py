from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, Optional

from uk_address_matcher.linking_model.matching.input_filters import (
    _restrict_canonical_to_messy_postcodes,
)
from uk_address_matcher.linking_model.matching.stages.base_stage import MatchingStage
from uk_address_matcher.sql_pipeline.match_reasons import MatchReason
from uk_address_matcher.sql_pipeline.steps import CTEStep, pipeline_stage

if TYPE_CHECKING:
    import duckdb

    from uk_address_matcher.sql_pipeline.runner import DebugOptions

MessyInputName = Literal["messy_addresses", "unmatched_records"]


@dataclass(frozen=True)
class ExactMatchStage(MatchingStage):
    """Exact hash-join matching on clean_full_address + postcode."""

    def find_matches(
        self,
        con: duckdb.DuckDBPyConnection,
        stage_name: str,
        df_unmatched: duckdb.DuckDBPyRelation,
        df_canonical: duckdb.DuckDBPyRelation,
        debug_options: Optional[DebugOptions] = None,
        explain: bool = False,
    ) -> Optional[duckdb.DuckDBPyRelation]:
        from uk_address_matcher.linking_model.matching.stages._sql_helpers import (
            run_sql_pipeline,
        )

        return run_sql_pipeline(
            con=con,
            pipeline_stages=[
                _restrict_canonical_to_messy_postcodes("exact"),
                _exact_matches("messy_addresses"),
            ],
            stage_name=stage_name,
            df_unmatched=df_unmatched,
            df_canonical=df_canonical,
            debug_options=debug_options,
            explain=explain,
        )

    def __repr__(self) -> str:
        return (
            "Exact match linking stage:\n"
            "    from uk_address_matcher import ExactMatchStage\n"
        )


@dataclass(frozen=True)
class ExactMatchWithoutPostcodeStage(MatchingStage):
    """Exact hash-join matching on uniquely occurring clean_full_address values without postcode."""

    def find_matches(
        self,
        con: duckdb.DuckDBPyConnection,
        stage_name: str,
        df_unmatched: duckdb.DuckDBPyRelation,
        df_canonical: duckdb.DuckDBPyRelation,
        debug_options: Optional[DebugOptions] = None,
        explain: bool = False,
    ) -> Optional[duckdb.DuckDBPyRelation]:
        from uk_address_matcher.linking_model.matching.stages._sql_helpers import (
            run_sql_pipeline,
        )

        return run_sql_pipeline(
            con=con,
            pipeline_stages=[_exact_matches_without_postcode("messy_addresses")],
            stage_name=stage_name,
            df_unmatched=df_unmatched,
            df_canonical=df_canonical,
            debug_options=debug_options,
            explain=explain,
        )

    def __repr__(self) -> str:
        return (
            "Exact match without postcode linking stage:\n"
            "    from uk_address_matcher import ExactMatchWithoutPostcodeStage\n"
        )


@pipeline_stage(
    name="exact_matches",
    description="Match using exact hash-join on clean_full_address + postcode",
    tags=["phase_1", "matching"],
    depends_on=["restrict_canonical_to_messy_postcodes"],
)
def _exact_matches(
    messy_input_name: MessyInputName = "messy_addresses",
) -> list[CTEStep]:
    """Find exact matches using hash-join on clean_full_address + postcode.

    Parameters
    ----------
    messy_input_name:
        The placeholder name for the messy input table. Defaults to "messy_addresses" for
        the initial pass. Can be set to "unmatched_records" when running after filtering.
    """
    exact_value = MatchReason.EXACT.value
    enum_values = str(MatchReason.enum_values())
    matches_sql = f"""
        SELECT
            messy.ukam_address_id AS ukam_address_id,
            canon.ukam_address_id AS canonical_ukam_address_id,
            canon.canonical_unique_id AS resolved_canonical_id,
            '{exact_value}'::ENUM {enum_values} as match_reason
        FROM {{{messy_input_name}}} AS messy
        INNER JOIN {{canonical_addresses_restricted}} AS canon
            ON messy.clean_full_address = canon.clean_full_address
            AND messy.postcode = canon.postcode
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY messy.ukam_address_id
            ORDER BY canon.ukam_address_id
        ) = 1
    """

    return [
        CTEStep("exact_matches", matches_sql),
    ]


@pipeline_stage(
    name="exact_matches_without_postcode",
    description=(
        "Match using exact clean_full_address joins, but only when the canonical "
        "clean_full_address maps to one unique canonical id"
    ),
    tags=["phase_1", "matching"],
)
def _exact_matches_without_postcode(
    messy_input_name: MessyInputName = "messy_addresses",
) -> list[CTEStep]:
    """Find exact matches on clean_full_address regardless of postcode."""

    exact_value = MatchReason.EXACT_WITHOUT_POSTCODE.value
    enum_values = str(MatchReason.enum_values())

    matches_sql = f"""
        WITH canonical_unique_clean_full_addresses AS (
            SELECT
                clean_full_address
            FROM {{canonical_addresses}}
            WHERE clean_full_address IS NOT NULL
            GROUP BY clean_full_address
            HAVING COUNT(DISTINCT unique_id) = 1
        )
        SELECT
            messy.ukam_address_id AS ukam_address_id,
            canonical.ukam_address_id AS canonical_ukam_address_id,
            canonical.unique_id AS resolved_canonical_id,
            '{exact_value}'::ENUM {enum_values} as match_reason
        FROM {{{messy_input_name}}} AS messy
        INNER JOIN {{canonical_addresses}} AS canonical
            ON messy.clean_full_address = canonical.clean_full_address
        INNER JOIN canonical_unique_clean_full_addresses AS unique_clean
            ON canonical.clean_full_address = unique_clean.clean_full_address
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY messy.ukam_address_id
            ORDER BY canonical.ukam_address_id
        ) = 1
    """

    return [
        CTEStep("exact_matches_without_postcode", matches_sql),
    ]
