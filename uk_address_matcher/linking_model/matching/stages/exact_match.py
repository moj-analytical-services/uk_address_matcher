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

MessyInputName = Literal["__ukam__tmp_messy_addresses", "unmatched_records"]


@dataclass(frozen=True, repr=False)
class ExactMatchStage(MatchingStage):
    """Deterministic exact matching on `clean_full_address` and `postcode`.

    This is usually the first stage in a pipeline. It accepts the easy,
    unambiguous cases before any probabilistic matching is attempted.

    A match is emitted when the cleaned messy address and the cleaned canonical
    address are identical and the postcode is also identical. A cleaned address
    match on its own is not enough: differing postcodes will not match.

    Example:
        ``"10 Demo Road Townton"`` matches
        ``"10 Demo Road, Townton"`` only when cleaning normalises punctuation
        and whitespace and both records have the same postcode.
    """

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
                _restrict_canonical_to_messy_postcodes(
                    "exact",
                    include_structural_fields=False,
                ),
                _exact_matches("__ukam__tmp_messy_addresses"),
            ],
            stage_name=stage_name,
            df_unmatched=df_unmatched,
            df_canonical=df_canonical,
            debug_options=debug_options,
            explain=explain,
        )


@pipeline_stage(
    name="exact_matches",
    description="Match using exact hash-join on clean_full_address + postcode",
    tags=["phase_1", "matching"],
    depends_on=["restrict_canonical_to_messy_postcodes"],
)
def _exact_matches(
    messy_input_name: MessyInputName = "__ukam__tmp_messy_addresses",
) -> list[CTEStep]:
    """Find exact matches using hash-join on clean_full_address + postcode.

    Parameters
    ----------
    messy_input_name:
        The placeholder name for the messy input table. Defaults to
        "__ukam__tmp_messy_addresses" for the initial pass. Can be set
        to "unmatched_records" when running after filtering.
    """
    match_condition = """
        messy.clean_full_address = canon.clean_full_address
        AND messy.postcode = canon.postcode
    """

    exact_value = MatchReason.EXACT.value
    enum_values = str(MatchReason.enum_values())
    matches_sql = f"""
        SELECT
            messy.ukam_address_id AS ukam_address_id,
            matched_canon.ukam_address_id AS canonical_ukam_address_id,
            matched_canon.canonical_unique_id AS resolved_canonical_id,
            '{exact_value}'::ENUM {enum_values} as match_reason
        FROM {{{messy_input_name}}} AS messy
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
        CTEStep("exact_matches", matches_sql),
    ]
