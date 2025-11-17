from __future__ import annotations

from typing import Optional

import duckdb

from uk_address_matcher.sql_pipeline.match_reasons import MatchReason
from uk_address_matcher.sql_pipeline.runner import (
    DebugOptions,
    InputBinding,
    create_sql_pipeline,
)
from uk_address_matcher.sql_pipeline.steps import CTEStep, pipeline_stage


@pipeline_stage(
    name="prepare_splink_candidates",
    description="Filter Splink matches to the top-ranked candidate per fuzzy address.",
    tags=["post_linkage", "splink"],
    stage_output="splink_top__ukam",
    checkpoint=True,
)
def _prepare_splink_candidates(
    *,
    match_weight_threshold: float,
    distinguishability_threshold: Optional[float],
) -> list[CTEStep]:
    """Filter Splink matches and retain the best candidate for each fuzzy ID."""

    enum_values = MatchReason.enum_values()
    enum_literal = ", ".join(f"'{value}'" for value in enum_values)
    splink_label = MatchReason.SPLINK.value.replace("'", "''")

    distinguishability_filter = ""
    if distinguishability_threshold is not None:
        distinguishability_filter = (
            "AND distinguishability IS NOT NULL "
            f"AND distinguishability >= {distinguishability_threshold}"
        )

    # _r = record from addresses to match (fuzzy)
    # _l = record from addresses to search within (canonical)
    top_sql = f"""
        SELECT
            unique_id_r AS unique_id,
            ukam_address_id_r as ukam_address_id,
            unique_id_l AS resolved_canonical_id,
            ukam_address_id_l as canonical_ukam_address_id,
            address_concat_r as original_address_concat,
            postcode_r as postcode,
            match_weight,
            distinguishability,
            distinguishability_category,
            '{splink_label}'::ENUM({enum_literal}) AS match_reason
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY unique_id_r
                    ORDER BY match_weight DESC, distinguishability DESC NULLS LAST, unique_id_l
                ) AS match_rank
            FROM {{splink_matches__ukam}}
            WHERE match_weight >= {match_weight_threshold}
            {distinguishability_filter}
        ) AS ranked
        WHERE match_rank = 1
    """

    return [CTEStep("splink_top", top_sql)]


@pipeline_stage(
    name="combine_exact_and_splink_matches",
    description="Merge deterministic and Splink matches with canonical context.",
    tags=["post_linkage", "matching"],
    stage_output="match_candidates",
)
def _combine_exact_and_splink_matches(*, include_unmatched: bool) -> list[CTEStep]:
    """Join exact and Splink matches with canonical address details."""

    canonical_sql = """
        SELECT
            ukam_address_id,
            original_address_concat AS original_address_concat_canonical,
            postcode AS postcode_canonical
        FROM {canonical_addresses__ukam}
    """

    common_fields = """
        unique_id,
        resolved_canonical_id,
        ukam_address_id,
        canonical_ukam_address_id,
        original_address_concat,
        postcode,
        match_reason
    """

    # When include_unmatched=True, we need to exclude unmatched records that got a Splink match
    # When include_unmatched=False, we exclude all unmatched records
    if include_unmatched:
        exact_filter = """
            WHERE match_reason IS NOT NULL
            OR (match_reason IS NULL AND ukam_address_id NOT IN
                (SELECT ukam_address_id FROM {splink_top__ukam}))
        """
    else:
        exact_filter = "WHERE match_reason IS NOT NULL"

    # Union exact matches and Splink matches
    union_sql = f"""
        SELECT
            {common_fields},
            NULL AS match_weight,
            NULL AS distinguishability,
            NULL AS distinguishability_category
        FROM {{exact_matches__ukam}}
        {exact_filter}

        UNION ALL

        SELECT
            {common_fields},
            match_weight,
            distinguishability,
            distinguishability_category
        FROM {{splink_top__ukam}}
        -- Ensures we don't duplicate exact matches if they also appear in Splink
        WHERE ukam_address_id NOT IN (
            SELECT ukam_address_id FROM {{exact_matches__ukam}} WHERE match_reason IS NOT NULL
        )
    """

    # Join with canonical addresses to get canonical address details
    final_sql = """
        SELECT
            combined.unique_id,
            combined.resolved_canonical_id,
            combined.original_address_concat,
            canon.original_address_concat_canonical,
            combined.postcode,
            canon.postcode_canonical,
            combined.match_weight,
            combined.distinguishability,
            combined.distinguishability_category,
            combined.match_reason,
            combined.ukam_address_id,
            combined.canonical_ukam_address_id
        FROM {combined_matches__ukam} AS combined
        LEFT JOIN {canonical_projection__ukam} AS canon
            ON canon.ukam_address_id = combined.canonical_ukam_address_id
        ORDER BY combined.unique_id
    """

    return [
        CTEStep("canonical_projection__ukam", canonical_sql),
        CTEStep("combined_matches__ukam", union_sql),
        CTEStep("match_candidates__ukam", final_sql),
    ]


def select_top_match_candidates(
    *,
    con: duckdb.DuckDBPyConnection,
    df_exact_matches: duckdb.DuckDBPyRelation,
    df_splink_matches: duckdb.DuckDBPyRelation,
    df_canonical: duckdb.DuckDBPyRelation,
    match_weight_threshold: float = 10.0,
    distinguishability_threshold: Optional[float] = 5.0,
    include_unmatched: bool = False,
    debug_options: Optional[DebugOptions] = None,
) -> duckdb.DuckDBPyRelation:
    """Combine deterministic and Splink matches using the SQL pipeline framework.

    Args:
        con: DuckDB connection
        df_exact_matches: Exact match results
        df_splink_matches: Splink match candidates
        df_canonical: Canonical addresses
        match_weight_threshold: Minimum match weight for Splink matches
        distinguishability_threshold: Minimum distinguishability for Splink matches
        include_unmatched: If True, include unmatched records from exact_matches
        debug_options: Debug options for pipeline execution
    """

    pipeline = create_sql_pipeline(
        con,
        [
            InputBinding("exact_matches__ukam", df_exact_matches),
            InputBinding("splink_matches__ukam", df_splink_matches),
            InputBinding("canonical_addresses__ukam", df_canonical),
        ],
        [
            _prepare_splink_candidates(
                match_weight_threshold=match_weight_threshold,
                distinguishability_threshold=distinguishability_threshold,
            ),
            _combine_exact_and_splink_matches(include_unmatched=include_unmatched),
        ],
        pipeline_name="Match candidate selection",
        pipeline_description="Filter Splink matches and merge with deterministic outputs.",
    )

    if debug_options is not None and debug_options.debug_mode:
        pipeline.show_plan()

    return pipeline.run(options=debug_options)


__all__ = ["select_top_match_candidates"]
