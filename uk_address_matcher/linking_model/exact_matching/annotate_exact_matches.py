from __future__ import annotations

from typing import Literal

from uk_address_matcher.sql_pipeline.match_reasons import MatchReason
from uk_address_matcher.sql_pipeline.steps import CTEStep, pipeline_stage

FuzzyInputName = Literal["fuzzy_addresses", "unmatched_records"]


@pipeline_stage(
    name="annotate_exact_matches",
    description=(
        "Annotate fuzzy addresses with exact hash-join matches on "
        "original_address_concat + postcode"
    ),
    tags=["phase_1", "exact_matching"],
    depends_on=["restrict_canonical_to_fuzzy_postcodes"],
)
def _annotate_exact_matches(
    fuzzy_input_name: FuzzyInputName = "fuzzy_addresses",
) -> list[CTEStep]:
    """Annotate fuzzy addresses with exact matches.

    Parameters
    ----------
    fuzzy_input_name:
        The placeholder name for the fuzzy input table. Defaults to "fuzzy_addresses" for
        the initial pass. Can be set to "unmatched_records" when running after filtering.
    """
    match_condition = """
        fuzzy.original_address_concat = canon.original_address_concat
        AND fuzzy.postcode = canon.postcode
    """

    # TODO(ThomasHepworth): For now, we are deduplicating on exact matches, where a
    # a single address appears multiple times in the canonical dataset. This should be
    # reviewed later to see if we can improve handling of these cases.
    exact_value = MatchReason.EXACT.value
    enum_values = str(MatchReason.enum_values())
    annotated_sql = f"""
        SELECT
            fuzzy.unique_id AS unique_id,
            COALESCE(
                canon.canonical_unique_id,
                fuzzy.resolved_canonical_id
            ) AS resolved_canonical_id,
            fuzzy.* EXCLUDE (match_reason, unique_id, resolved_canonical_id),
            CASE
                WHEN canon.canonical_unique_id IS NOT NULL THEN '{exact_value}'::ENUM {enum_values}
                ELSE fuzzy.match_reason
            END AS match_reason
        FROM {{{fuzzy_input_name}}} AS fuzzy
        LEFT JOIN (
            SELECT *
            FROM (
                SELECT c.*,
                       ROW_NUMBER() OVER (
                           PARTITION BY original_address_concat, postcode
                           ORDER BY canonical_unique_id
                       ) AS rn
                FROM {{canonical_addresses_restricted}} AS c
            ) d
            WHERE rn = 1
        ) AS canon
          ON
            {match_condition}
    """
    return [
        CTEStep("annotated_exact_matches", annotated_sql),
    ]


__all__ = ["_annotate_exact_matches"]
