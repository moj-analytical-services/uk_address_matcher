from __future__ import annotations

import math
from typing import TYPE_CHECKING

from uk_address_matcher.analysis.accuracy_sql import (
    correct_share_of_total_sql,
    f1_from_counts_sql,
    ratio_sql,
    wrong_match_rate_sql,
)
from uk_address_matcher.analysis.sql_helpers import sql_literal
from uk_address_matcher.analysis.validation import requires_ukam_label
from uk_address_matcher.sql_pipeline.match_reasons import MatchReason

if TYPE_CHECKING:
    import duckdb


def resolve_splink_threshold_match_weight(
    *,
    splink_match_weight_threshold: float | None,
    splink_match_probability_threshold: float | None,
) -> float | None:
    if (
        splink_match_weight_threshold is not None
        and splink_match_probability_threshold is not None
    ):
        raise ValueError(
            "Provide only one of splink_match_weight_threshold or "
            "splink_match_probability_threshold."
        )
    if splink_match_probability_threshold is None:
        return splink_match_weight_threshold
    if not 0.0 <= splink_match_probability_threshold <= 1.0:
        raise ValueError(
            "splink_match_probability_threshold must be between 0.0 and 1.0 inclusive."
        )
    return math.log2(
        splink_match_probability_threshold / (1.0 - splink_match_probability_threshold)
    )


@requires_ukam_label("relation", function_name="_accuracy_table")
def build_accuracy_table(
    con: duckdb.DuckDBPyConnection,
    relation: duckdb.DuckDBPyRelation,
    *,
    splink_match_weight_threshold: float | None = None,
    splink_match_probability_threshold: float | None = None,
) -> duckdb.DuckDBPyRelation:

    threshold_match_weight = resolve_splink_threshold_match_weight(
        splink_match_weight_threshold=splink_match_weight_threshold,
        splink_match_probability_threshold=splink_match_probability_threshold,
    )
    splink_accepted_sql = (
        "TRUE"
        if threshold_match_weight is None
        else f"COALESCE(m.match_weight, -1000.0) >= {threshold_match_weight}"
    )
    splink_value = sql_literal(MatchReason.SPLINK.value)
    enum_values = str(MatchReason.enum_values())
    splink_reason_sql = f"'{splink_value}'::ENUM {enum_values}"
    precision_sql = ratio_sql(
        "correct_matches",
        "rows_matched_in_stage",
        when_zero_sql="NULL::DOUBLE",
    )
    recall_sql = ratio_sql(
        "correct_matches",
        "(SELECT total_input_rows FROM totals)",
        when_zero_sql="NULL::DOUBLE",
    )
    f1_sql = f1_from_counts_sql(
        tp_sql="correct_matches",
        fp_sql="(rows_matched_in_stage - correct_matches)",
        fn_sql="((SELECT total_input_rows FROM totals) - correct_matches)",
    )
    wrong_match_rate_expr = wrong_match_rate_sql(
        rows_matched_sql="rows_matched_in_stage",
        correct_matches_sql="correct_matches",
    )
    correct_share_expr = correct_share_of_total_sql(
        correct_matches_sql="correct_matches",
        total_rows_sql="(SELECT total_input_rows FROM totals)",
    )

    return con.sql(
        f"""
        WITH scored AS (
            SELECT
                CASE
                    WHEN m.match_reason IS NULL THEN 'unmatched'
                    WHEN split_part(m.match_reason::VARCHAR, ':', 1) = 'exact'
                        THEN 'exact_matches'
                    ELSE split_part(m.match_reason::VARCHAR, ':', 1)
                END AS stage,
                CASE
                    WHEN m.match_reason IS NULL THEN FALSE
                    WHEN m.match_reason = {splink_reason_sql} THEN {splink_accepted_sql}
                    ELSE TRUE
                END AS is_matched,
                CASE
                    WHEN m.match_reason IS NULL THEN FALSE
                    WHEN m.match_reason = {splink_reason_sql} THEN {splink_accepted_sql}
                    ELSE TRUE
                END
                AND CAST(m.resolved_canonical_id AS VARCHAR)
                    = CAST(m.ukam_label AS VARCHAR) AS is_correct
            FROM ({relation.sql_query()}) AS m
        ),
        totals AS (
            SELECT COUNT(*) AS total_input_rows FROM scored
        ),
        all_rows AS (
            SELECT
                CASE
                    WHEN GROUPING(stage) = 1 THEN 'overall'
                    ELSE stage
                END AS stage,
                SUM(CASE WHEN is_matched THEN 1 ELSE 0 END) AS rows_matched_in_stage,
                SUM(CASE WHEN is_correct THEN 1 ELSE 0 END) AS correct_matches
            FROM scored
            GROUP BY GROUPING SETS ((stage), ())
        )
        SELECT
            stage,
            rows_matched_in_stage,
            correct_matches,
            rows_matched_in_stage - correct_matches AS wrong_matches,
            ROUND(
                {precision_sql},
                6
            ) AS precision,
            ROUND(
                {wrong_match_rate_expr},
                2
            ) AS wrong_match_rate,
            ROUND(
                {correct_share_expr},
                2
            ) AS correct_share_of_total,
            ROUND(
                {recall_sql},
                6
            ) AS recall,
            ROUND(
                {f1_sql},
                6
            ) AS f1
        FROM all_rows
        ORDER BY
            rows_matched_in_stage DESC,
            stage
        """
    )
