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
    stage_output="splink_top",
)
def _prepare_splink_candidates(
    *,
    match_weight_threshold: float,
    distinguishability_threshold: Optional[float],
    enum_literal: str,
    splink_label: str,
) -> list[CTEStep]:
    """Filter Splink matches and retain the best candidate for each fuzzy ID."""

    distinguishability_filter = ""
    if distinguishability_threshold is not None:
        distinguishability_filter = (
            "AND distinguishability IS NOT NULL "
            f"AND distinguishability >= {distinguishability_threshold}"
        )

    ranked_sql = f"""
        SELECT
            unique_id_r,
            unique_id_l,
            match_weight,
            distinguishability,
            ROW_NUMBER() OVER (
                PARTITION BY unique_id_r
                ORDER BY match_weight DESC, distinguishability DESC NULLS LAST, unique_id_l
            ) AS match_rank
        FROM {{splink_matches}}
        WHERE match_weight >= {match_weight_threshold}
        {distinguishability_filter}
    """

    top_sql = f"""
        SELECT
            unique_id_r AS unique_id,
            unique_id_l AS resolved_canonical_id,
            match_weight,
            distinguishability,
            '{splink_label}'::ENUM({enum_literal}) AS match_reason
        FROM {{splink_ranked}}
        WHERE match_rank = 1
    """

    return [
        CTEStep("splink_ranked", ranked_sql),
        CTEStep("splink_top", top_sql),
    ]


@pipeline_stage(
    name="combine_exact_and_splink_matches",
    description="Merge deterministic and Splink matches with canonical context.",
    tags=["post_linkage", "matching"],
    stage_output="match_candidates",
)
def _combine_exact_and_splink_matches(
    *,
    enum_literal: str,
    unmatched_label: str,
) -> list[CTEStep]:
    """Join exact and Splink matches with canonical address details."""

    canonical_sql = """
        SELECT
            unique_id AS resolved_canonical_id,
            original_address_concat AS original_address_concat_canonical,
            postcode AS postcode_canonical
        FROM {canonical_addresses}
    """

    final_sql = f"""
        SELECT
            em.unique_id,
            em.original_address_concat,
            canon.original_address_concat_canonical,
            em.postcode,
            canon.postcode_canonical,
            COALESCE(
                st.match_reason,
                em.match_reason,
                '{unmatched_label}'::ENUM({enum_literal})
            ) AS match_reason,
            COALESCE(st.resolved_canonical_id, em.resolved_canonical_id) AS resolved_canonical_id,
            st.match_weight,
            st.distinguishability
        FROM {{exact_matches}} AS em
        LEFT JOIN {{input}} AS st
            ON st.unique_id = em.unique_id
        LEFT JOIN {{canonical_projection}} AS canon
            ON canon.resolved_canonical_id = COALESCE(st.resolved_canonical_id, em.resolved_canonical_id)
        ORDER BY em.unique_id
    """

    return [
        CTEStep("canonical_projection", canonical_sql),
        CTEStep("match_candidates", final_sql),
    ]


def select_top_match_candidates(
    *,
    con: duckdb.DuckDBPyConnection,
    df_exact_matches: duckdb.DuckDBPyRelation,
    df_splink_matches: duckdb.DuckDBPyRelation,
    df_canonical: duckdb.DuckDBPyRelation,
    match_weight_threshold: float = 10.0,
    distinguishability_threshold: Optional[float] = 5.0,
    debug_options: Optional[DebugOptions] = None,
) -> duckdb.DuckDBPyRelation:
    """Combine deterministic and Splink matches using the SQL pipeline framework."""

    enum_values = MatchReason.enum_values()
    enum_literal = ", ".join(f"'{value}'" for value in enum_values)
    splink_label = MatchReason.SPLINK.value.replace("'", "''")
    unmatched_label = MatchReason.UNMATCHED.value.replace("'", "''")

    pipeline = create_sql_pipeline(
        con,
        [
            InputBinding("exact_matches", df_exact_matches),
            InputBinding("splink_matches", df_splink_matches),
            InputBinding("canonical_addresses", df_canonical),
        ],
        [
            _prepare_splink_candidates(
                match_weight_threshold=match_weight_threshold,
                distinguishability_threshold=distinguishability_threshold,
                enum_literal=enum_literal,
                splink_label=splink_label,
            ),
            _combine_exact_and_splink_matches(
                enum_literal=enum_literal,
                unmatched_label=unmatched_label,
            ),
        ],
        pipeline_name="Match candidate selection",
        pipeline_description="Filter Splink matches and merge with deterministic outputs.",
    )

    if debug_options is not None and debug_options.debug_mode:
        pipeline.show_plan()

    return pipeline.run(options=debug_options)


__all__ = ["select_top_match_candidates"]
