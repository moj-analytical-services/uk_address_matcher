from __future__ import annotations

from typing import Literal

from uk_address_matcher.sql_pipeline.match_reasons import MatchReason
from uk_address_matcher.sql_pipeline.steps import CTEStep, pipeline_stage

PostcodeStrategy = Literal["exact", "drop_last_char"]
POSTCODE_STRATEGIES: tuple[PostcodeStrategy, PostcodeStrategy] = (
    "exact",
    "drop_last_char",
)


@pipeline_stage(
    name="filter_unmatched_matches",
    description="Filter records that haven't been matched yet",
    tags=["phase_1", "exact_matching"],
    stage_output="unmatched_records",
)
def _filter_unmatched_exact_matches() -> list[CTEStep]:
    unmatched_value = MatchReason.UNMATCHED.value
    sql = f"""
        SELECT
            f.*
        FROM {{input}} AS f
        WHERE f.match_reason = '{unmatched_value}'
    """
    return [CTEStep("unmatched_records", sql)]


@pipeline_stage(
    name="restrict_canonical_to_fuzzy_postcodes",
    description="Restrict canonical addresses to postcodes observed in the fuzzy input.",
    tags=["phase_1", "exact_matching", "utility"],
    stage_output="canonical_addresses_restricted",
)
def _restrict_canonical_to_fuzzy_postcodes(
    postcode_strategy: PostcodeStrategy,
) -> list[CTEStep]:
    """Filter canonical addresses to those matching fuzzy input postcodes."""
    if postcode_strategy not in POSTCODE_STRATEGIES:
        valid_strategies = ", ".join(f"'{s}'" for s in POSTCODE_STRATEGIES)
        raise ValueError(
            f"postcode_strategy must be one of: {valid_strategies}. Got '{postcode_strategy}'."
        )

    def _postcode_prefix(expr: str) -> str:
        return (
            f"CASE WHEN {expr} IS NULL OR LENGTH({expr}) <= 1 THEN NULL "
            f"ELSE LEFT({expr}, LENGTH({expr}) - 1) END"
        )

    canonical_select_fields = [
        "canon.clean_full_address",
        "canon.postcode",
        "canon.unique_id AS canonical_unique_id",
        "canon.ukam_address_id AS ukam_address_id",
        "canon.address_tokens",
        "canon.numeric_tokens",
        "canon.has_flat_indicator",
        "canon.flat_positional",
        "canon.flat_letter",
        "canon.flat_number",
        "canon.has_business_unit",
        "canon.business_unit_type",
        "canon.business_unit_id",
        "canon.non_traditional_address_type",
    ]

    if postcode_strategy == "exact":
        fuzzy_key_expr = "postcode"
        canonical_key_expr = "canon.postcode"

    else:
        # TODO(ThomasHepworth): extend to support outcode-only filtering.
        fuzzy_key_expr = _postcode_prefix("postcode")
        canonical_key_expr = _postcode_prefix("canon.postcode")
        canonical_select_fields.append(
            "LEFT(canon.postcode, LENGTH(canon.postcode) - 1) AS postcode_group"
        )

    canonical_select_fields_str = ",\n            ".join(canonical_select_fields)

    fuzzy_subquery = f"""
        SELECT DISTINCT
            {fuzzy_key_expr} AS postcode_key
        FROM {{fuzzy_addresses}}
        WHERE {fuzzy_key_expr} IS NOT NULL
    """

    sql = f"""
        SELECT
            {canonical_select_fields_str}
        FROM {{canonical_addresses}} AS canon
        JOIN (
        {fuzzy_subquery}
        ) AS fuzzy
          ON {canonical_key_expr} = fuzzy.postcode_key
        WHERE canon.unique_id IS NOT NULL
    """
    return [CTEStep("canonical_addresses_restricted", sql)]


__all__ = [
    "_filter_unmatched_exact_matches",
    "_restrict_canonical_to_fuzzy_postcodes",
]
