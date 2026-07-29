from __future__ import annotations

from typing import Literal

from uk_address_matcher.sql_pipeline.steps import CTEStep, pipeline_stage

PostcodeStrategy = Literal["exact", "drop_last_char"]
POSTCODE_STRATEGIES: tuple[PostcodeStrategy, PostcodeStrategy] = (
    "exact",
    "drop_last_char",
)


def _numeric_tokens_from_scalar_columns_sql(alias: str) -> str:
    """Build the reduced numeric-token array used by matching stages."""

    return f"""
        list_filter(
            list_value(
                {alias}.numeric_token_1,
                {alias}.numeric_token_2,
                {alias}.numeric_token_3
            ),
            token -> token IS NOT NULL
        )
    """.strip()


@pipeline_stage(
    name="restrict_canonical_to_messy_postcodes",
    description="Restrict canonical addresses to postcodes observed in the messy input.",
    tags=["phase_1", "matching", "utility"],
    stage_output="canonical_addresses_restricted",
)
def _restrict_canonical_to_messy_postcodes(
    postcode_strategy: PostcodeStrategy,
) -> list[CTEStep]:
    """Filter canonical addresses to those matching messy input postcodes."""
    if postcode_strategy not in POSTCODE_STRATEGIES:
        valid_strategies = ", ".join(f"'{s}'" for s in POSTCODE_STRATEGIES)
        raise ValueError(
            "postcode_strategy must be one of: "
            f"{valid_strategies}. Got '{postcode_strategy}'."
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
        (f"{_numeric_tokens_from_scalar_columns_sql('canon')} AS numeric_tokens"),
        "canon.has_flat_indicator",
        "canon.flat_positional",
        "canon.sub_premise_location",
        "canon.flat_letter",
        "canon.flat_number",
        "canon.has_business_unit",
        "canon.business_unit_type",
        "canon.business_unit_id",
    ]

    if postcode_strategy == "exact":
        messy_key_expr = "postcode"
        canonical_key_expr = "canon.postcode"

    else:
        messy_key_expr = _postcode_prefix("postcode")
        canonical_key_expr = _postcode_prefix("canon.postcode")
        canonical_select_fields.append(
            "LEFT(canon.postcode, LENGTH(canon.postcode) - 1) AS postcode_group"
        )

    canonical_select_fields_str = ",\n            ".join(canonical_select_fields)

    messy_subquery = f"""
        SELECT DISTINCT
            {messy_key_expr} AS postcode_key
        FROM {{__ukam__tmp_messy_addresses}}
        WHERE {messy_key_expr} IS NOT NULL
    """

    sql = f"""
        SELECT
            {canonical_select_fields_str}
        FROM {{__ukam__tmp_canonical_addresses}} AS canon
        JOIN (
        {messy_subquery}
        ) AS messy
          ON {canonical_key_expr} = messy.postcode_key
        WHERE canon.unique_id IS NOT NULL
    """
    return [CTEStep("canonical_addresses_restricted", sql)]


__all__ = ["_restrict_canonical_to_messy_postcodes"]
