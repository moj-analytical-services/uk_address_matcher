from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, Optional

from uk_address_matcher.linking_model.matching.stages.base_stage import MatchingStage
from uk_address_matcher.sql_pipeline.match_reasons import MatchReason
from uk_address_matcher.sql_pipeline.steps import CTEStep, pipeline_stage

if TYPE_CHECKING:
    import duckdb

    from uk_address_matcher.sql_pipeline.runner import DebugOptions

FuzzyInputName = Literal["fuzzy_addresses", "unmatched_records"]


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
    ) -> Optional[duckdb.DuckDBPyRelation]:
        from uk_address_matcher.linking_model.matching.input_filters import (
            _restrict_canonical_to_fuzzy_postcodes,
        )
        from uk_address_matcher.linking_model.matching.stages._sql_helpers import (
            run_sql_pipeline,
        )

        return run_sql_pipeline(
            con=con,
            pipeline_stages=[
                _restrict_canonical_to_fuzzy_postcodes("exact"),
                _annotate_exact_matches,
            ],
            stage_name=stage_name,
            df_unmatched=df_unmatched,
            df_canonical=df_canonical,
            debug_options=debug_options,
        )


# ---------------------------------------------------------------------------
# SQL implementation
# ---------------------------------------------------------------------------


@pipeline_stage(
    name="annotate_exact_matches",
    description=(
        "Annotate fuzzy addresses with exact hash-join matches on "
        "clean_full_address + postcode"
    ),
    tags=["phase_1", "exact_matching"],
    depends_on=["restrict_canonical_to_fuzzy_postcodes"],
)
def _annotate_exact_matches(
    fuzzy_input_name: FuzzyInputName = "fuzzy_addresses",
) -> list[CTEStep]:
    """Annotate fuzzy addresses with exact matches."""
    match_condition = """
        fuzzy.clean_full_address = canon.clean_full_address
        AND fuzzy.postcode = canon.postcode
    """

    exact_value = MatchReason.EXACT.value
    enum_values = str(MatchReason.enum_values())
    annotated_sql = f"""
        SELECT
            fuzzy.ukam_address_id AS ukam_address_id,
            matched_canon.ukam_address_id AS canonical_ukam_address_id,
            matched_canon.canonical_unique_id AS resolved_canonical_id,
            '{exact_value}'::ENUM {enum_values} as match_reason
        FROM {{{fuzzy_input_name}}} AS fuzzy
        INNER JOIN LATERAL (
            SELECT
                canon.ukam_address_id as ukam_address_id,
                canon.canonical_unique_id as canonical_unique_id
            FROM {{canonical_addresses_restricted}} AS canon
            WHERE {match_condition}
            LIMIT 1
        ) AS matched_canon ON true
    """

    return [
        CTEStep("annotated_exact_matches", annotated_sql),
    ]
