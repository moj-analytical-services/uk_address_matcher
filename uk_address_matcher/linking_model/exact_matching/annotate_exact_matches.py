from __future__ import annotations

from typing import Literal

from uk_address_matcher.sql_pipeline.match_reasons import MatchReason
from uk_address_matcher.sql_pipeline.steps import CTEStep, pipeline_stage

FuzzyInputName = Literal["fuzzy_addresses", "unmatched_records"]


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
    """Annotate fuzzy addresses with exact matches.

    Parameters
    ----------
    fuzzy_input_name:
        The placeholder name for the fuzzy input table. Defaults to "fuzzy_addresses" for
        the initial pass. Can be set to "unmatched_records" when running after filtering.
    """
    match_condition = """
        fuzzy.clean_full_address = canon.clean_full_address
        AND fuzzy.postcode = canon.postcode
    """

    # TODO(ThomasHepworth): For now, we are deduplicating on exact matches, where a
    # a single address appears multiple times in the canonical dataset. This should be
    # reviewed later to see if we can improve handling of these cases.
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
            -- If we get multiple matches, we just take the first one
            -- Usually an indication that the canonical dataset has duplicates
            LIMIT 1
        ) AS matched_canon ON true
    """

    return [
        CTEStep("annotated_exact_matches", annotated_sql),
    ]


__all__ = ["_annotate_exact_matches"]
