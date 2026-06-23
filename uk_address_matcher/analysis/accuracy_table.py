from __future__ import annotations

import math
from typing import TYPE_CHECKING

from uk_address_matcher.analysis.accuracy_analysis import (
    build_match_weight_rounding_expression,
    compute_precision_recall_auc,
)
from uk_address_matcher.analysis.accuracy_sql import (
    build_threshold_metrics_sql,
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


_OPERATING_POINT_PRECISION_TARGETS: tuple[tuple[str, float], ...] = (
    ("99_5", 0.995),
    ("99_0", 0.99),
    ("98_0", 0.98),
)
_DEFAULT_PRECISION_TARGET = 0.99
_THRESHOLD_ROUNDING = 0.1


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


def _ensure_match_weight_column(
    con: duckdb.DuckDBPyConnection,
    relation: duckdb.DuckDBPyRelation,
) -> duckdb.DuckDBPyRelation:
    if "match_weight" in relation.columns:
        return relation

    relation_sql = relation.sql_query()
    return con.sql(
        f"""
        SELECT
            source_relation.*,
            NULL::DOUBLE AS match_weight
        FROM ({relation_sql}) AS source_relation
        """
    )


def _canonical_relation_for_accuracy(
    con: duckdb.DuckDBPyConnection,
    relation: duckdb.DuckDBPyRelation,
    canonical_relation: duckdb.DuckDBPyRelation | None,
) -> duckdb.DuckDBPyRelation:
    if canonical_relation is not None:
        return canonical_relation

    relation_sql = relation.sql_query()
    return con.sql(
        f"""
        SELECT DISTINCT CAST(ukam_label AS VARCHAR) AS unique_id
        FROM ({relation_sql}) AS m
        WHERE ukam_label IS NOT NULL
        """
    )


def _sql_double_literal(value: float | None) -> str:
    if value is None:
        return "NULL::DOUBLE"
    return f"{float(value)!r}::DOUBLE"


def _sql_bigint_literal(value: int | None) -> str:
    if value is None:
        return "NULL::BIGINT"
    return f"{int(value)}::BIGINT"


def _find_operating_point(
    con: duckdb.DuckDBPyConnection,
    threshold_metrics_sql: str,
    *,
    precision_target: float,
) -> tuple[float | None, float | None]:
    row = con.sql(
        f"""
        SELECT
            truth_threshold,
            recall
        FROM ({threshold_metrics_sql}) AS threshold_metrics
        WHERE precision >= {precision_target}
          AND recall IS NOT NULL
        ORDER BY recall DESC, truth_threshold ASC
        LIMIT 1
        """
    ).fetchone()
    if row is None:
        return None, None
    threshold_value, recall_value = row
    return float(threshold_value), float(recall_value)


def _build_threshold_summary(
    con: duckdb.DuckDBPyConnection,
    relation: duckdb.DuckDBPyRelation,
    canonical_relation: duckdb.DuckDBPyRelation | None,
    *,
    threshold_match_weight: float | None,
) -> dict[str, float | int | None]:
    threshold_relation = _ensure_match_weight_column(con, relation)
    resolved_canonical_relation = _canonical_relation_for_accuracy(
        con,
        relation,
        canonical_relation,
    )
    rounding_expr = build_match_weight_rounding_expression(_THRESHOLD_ROUNDING)
    threshold_metrics_sql = build_threshold_metrics_sql(rounding_expr)

    splink_value = sql_literal(MatchReason.SPLINK.value)
    enum_values = str(MatchReason.enum_values())
    splink_reason_sql = f"'{splink_value}'::ENUM {enum_values}"
    accepted_sql = (
        "TRUE"
        if threshold_match_weight is None
        else f"COALESCE(m.match_weight, -1000.0) >= {threshold_match_weight}"
    )

    con.register("__ukam_threshold_matches__", threshold_relation)
    con.register("__ukam_threshold_canonical__", resolved_canonical_relation)
    try:
        pr_auc = compute_precision_recall_auc(con, threshold_metrics_sql)
        summary: dict[str, float | int | None] = {"pr_auc": pr_auc}

        operating_points: dict[str, tuple[float | None, float | None]] = {}
        for suffix, precision_target in _OPERATING_POINT_PRECISION_TARGETS:
            operating_points[suffix] = _find_operating_point(
                con,
                threshold_metrics_sql,
                precision_target=precision_target,
            )
            threshold_value, recall_value = operating_points[suffix]
            summary[f"threshold_at_precision_{suffix}"] = threshold_value
            summary[f"recall_at_precision_{suffix}"] = recall_value

        default_threshold = threshold_match_weight
        if default_threshold is None:
            default_threshold = operating_points["99_0"][0]
        summary["default_threshold"] = default_threshold

        if default_threshold is None:
            summary.update(
                {
                    "default_precision": None,
                    "default_recall": None,
                    "default_false_match_rate": None,
                    "default_missed_match_rate": None,
                    "default_true_no_match_rejection_rate": None,
                    "default_predicted_no_match_npv": None,
                    "wrong_canonical_id_count": None,
                    "true_match_predicted_no_match_count": None,
                    "true_no_match_forced_to_canonical_id_count": None,
                }
            )
            return summary

        threshold_row = con.sql(
            f"""
            WITH canonical_ids AS (
                SELECT DISTINCT CAST(unique_id AS VARCHAR) AS unique_id
                FROM __ukam_threshold_canonical__
            ),
            scored AS (
                SELECT
                    CASE WHEN c.unique_id IS NOT NULL THEN 1 ELSE 0 END
                        AS clerical_positive,
                    CASE
                        WHEN c.unique_id IS NOT NULL
                         AND CAST(m.resolved_canonical_id AS VARCHAR)
                            = CAST(m.ukam_label AS VARCHAR)
                        THEN 1
                        ELSE 0
                    END AS is_true_positive,
                    CASE
                        WHEN m.match_reason IS NULL THEN FALSE
                        WHEN m.match_reason = {splink_reason_sql} THEN {accepted_sql}
                        ELSE TRUE
                    END AS is_accepted
                FROM __ukam_threshold_matches__ AS m
                LEFT JOIN canonical_ids AS c
                    ON CAST(m.ukam_label AS VARCHAR) = c.unique_id
            ),
            counts AS (
                SELECT
                    SUM(CASE WHEN is_accepted AND is_true_positive = 1 THEN 1 ELSE 0 END)
                        AS tp,
                    SUM(CASE WHEN is_accepted AND is_true_positive = 0 THEN 1 ELSE 0 END)
                        AS fp,
                    SUM(
                        CASE
                            WHEN NOT is_accepted AND clerical_positive = 1 THEN 1
                            ELSE 0
                        END
                    )
                        AS fn,
                    SUM(
                        CASE
                            WHEN NOT is_accepted AND clerical_positive = 0 THEN 1
                            ELSE 0
                        END
                    )
                        AS tn,
                    SUM(CASE
                        WHEN is_accepted
                         AND clerical_positive = 1
                         AND is_true_positive = 0
                        THEN 1
                        ELSE 0
                    END) AS wrong_canonical_id_count,
                    SUM(
                        CASE
                            WHEN NOT is_accepted AND clerical_positive = 1 THEN 1
                            ELSE 0
                        END
                    )
                        AS true_match_predicted_no_match_count,
                    SUM(CASE WHEN is_accepted AND clerical_positive = 0 THEN 1 ELSE 0 END)
                        AS true_no_match_forced_to_canonical_id_count,
                    SUM(clerical_positive) AS p,
                    SUM(1 - clerical_positive) AS n
                FROM scored
            )
            SELECT
                CASE
                    WHEN tp + fp = 0 THEN 1.0
                    ELSE tp::DOUBLE / (tp + fp)
                END AS precision,
                tp::DOUBLE / NULLIF(p::DOUBLE, 0.0) AS recall,
                fp::DOUBLE / NULLIF((tp + fp)::DOUBLE, 0.0) AS false_match_rate,
                fn::DOUBLE / NULLIF(p::DOUBLE, 0.0) AS missed_match_rate,
                tn::DOUBLE / NULLIF(n::DOUBLE, 0.0) AS true_no_match_rejection_rate,
                tn::DOUBLE / NULLIF((tn + fn)::DOUBLE, 0.0) AS predicted_no_match_npv,
                wrong_canonical_id_count,
                true_match_predicted_no_match_count,
                true_no_match_forced_to_canonical_id_count
            FROM counts
            """
        ).fetchone()

        summary.update(
            {
                "default_precision": float(threshold_row[0])
                if threshold_row[0] is not None
                else None,
                "default_recall": float(threshold_row[1])
                if threshold_row[1] is not None
                else None,
                "default_false_match_rate": float(threshold_row[2])
                if threshold_row[2] is not None
                else None,
                "default_missed_match_rate": float(threshold_row[3])
                if threshold_row[3] is not None
                else None,
                "default_true_no_match_rejection_rate": float(threshold_row[4])
                if threshold_row[4] is not None
                else None,
                "default_predicted_no_match_npv": float(threshold_row[5])
                if threshold_row[5] is not None
                else None,
                "wrong_canonical_id_count": int(threshold_row[6])
                if threshold_row[6] is not None
                else None,
                "true_match_predicted_no_match_count": int(threshold_row[7])
                if threshold_row[7] is not None
                else None,
                "true_no_match_forced_to_canonical_id_count": int(threshold_row[8])
                if threshold_row[8] is not None
                else None,
            }
        )
        return summary
    finally:
        con.unregister("__ukam_threshold_matches__")
        con.unregister("__ukam_threshold_canonical__")


@requires_ukam_label("relation", function_name="_accuracy_table")
def build_accuracy_table(
    con: duckdb.DuckDBPyConnection,
    relation: duckdb.DuckDBPyRelation,
    *,
    canonical_relation: duckdb.DuckDBPyRelation | None = None,
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
    threshold_summary = _build_threshold_summary(
        con,
        relation,
        canonical_relation,
        threshold_match_weight=threshold_match_weight,
    )
    threshold_summary_sql = f"""
        SELECT
            {_sql_double_literal(threshold_summary["pr_auc"])} AS pr_auc,
            {_sql_double_literal(threshold_summary["threshold_at_precision_99_5"])}
                AS threshold_at_precision_99_5,
            {_sql_double_literal(threshold_summary["recall_at_precision_99_5"])}
                AS recall_at_precision_99_5,
            {_sql_double_literal(threshold_summary["threshold_at_precision_99_0"])}
                AS threshold_at_precision_99_0,
            {_sql_double_literal(threshold_summary["recall_at_precision_99_0"])}
                AS recall_at_precision_99_0,
            {_sql_double_literal(threshold_summary["threshold_at_precision_98_0"])}
                AS threshold_at_precision_98_0,
            {_sql_double_literal(threshold_summary["recall_at_precision_98_0"])}
                AS recall_at_precision_98_0,
            {_sql_double_literal(threshold_summary["default_threshold"])}
                AS default_threshold,
            {_sql_double_literal(threshold_summary["default_precision"])}
                AS default_threshold_precision,
            {_sql_double_literal(threshold_summary["default_recall"])}
                AS default_threshold_recall,
            {_sql_double_literal(threshold_summary["default_false_match_rate"])}
                AS default_threshold_false_match_rate,
            {_sql_double_literal(threshold_summary["default_missed_match_rate"])}
                AS default_threshold_missed_match_rate,
            {_sql_double_literal(threshold_summary["default_true_no_match_rejection_rate"])}
                AS default_threshold_true_no_match_rejection_rate,
            {_sql_double_literal(threshold_summary["default_predicted_no_match_npv"])}
                AS default_threshold_predicted_no_match_npv,
            {_sql_bigint_literal(threshold_summary["wrong_canonical_id_count"])}
                AS wrong_canonical_id_count,
            {_sql_bigint_literal(threshold_summary["true_match_predicted_no_match_count"])}
                AS true_match_predicted_no_match_count,
            {_sql_bigint_literal(threshold_summary["true_no_match_forced_to_canonical_id_count"])}
                AS true_no_match_forced_to_canonical_id_count
    """

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
                COUNT(*) AS total_rows_in_stage,
                SUM(CASE WHEN is_matched THEN 1 ELSE 0 END) AS matched_rows_in_stage,
                SUM(CASE WHEN is_correct THEN 1 ELSE 0 END) AS correct_matches
            FROM scored
            GROUP BY GROUPING SETS ((stage), ())
        ),
        threshold_summary AS (
            {threshold_summary_sql}
        )
        SELECT
            a.stage,
            CASE
                WHEN a.stage = 'unmatched' THEN a.total_rows_in_stage
                ELSE a.matched_rows_in_stage
            END AS rows_matched_in_stage,
            a.correct_matches,
            CASE
                WHEN a.stage = 'unmatched' THEN 0
                ELSE a.matched_rows_in_stage - a.correct_matches
            END AS wrong_matches,
            CASE
                WHEN a.stage = 'unmatched' THEN NULL
                ELSE ROUND(
                    {precision_sql},
                    6
                )
            END AS precision,
            CASE
                WHEN a.stage = 'unmatched' THEN NULL
                ELSE ROUND(
                    {wrong_match_rate_expr},
                    2
                )
            END AS wrong_match_rate,
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
            ) AS f1,
            CASE
                WHEN a.stage = 'overall' THEN ROUND(ts.pr_auc, 6)
                ELSE NULL
            END AS pr_auc,
            CASE
                WHEN a.stage = 'overall' THEN ROUND(ts.recall_at_precision_99_5, 6)
                ELSE NULL
            END AS recall_at_precision_99_5,
            CASE
                WHEN a.stage = 'overall' THEN ROUND(ts.threshold_at_precision_99_5, 6)
                ELSE NULL
            END AS threshold_at_precision_99_5,
            CASE
                WHEN a.stage = 'overall' THEN ROUND(ts.recall_at_precision_99_0, 6)
                ELSE NULL
            END AS recall_at_precision_99_0,
            CASE
                WHEN a.stage = 'overall' THEN ROUND(ts.threshold_at_precision_99_0, 6)
                ELSE NULL
            END AS threshold_at_precision_99_0,
            CASE
                WHEN a.stage = 'overall' THEN ROUND(ts.recall_at_precision_98_0, 6)
                ELSE NULL
            END AS recall_at_precision_98_0,
            CASE
                WHEN a.stage = 'overall' THEN ROUND(ts.threshold_at_precision_98_0, 6)
                ELSE NULL
            END AS threshold_at_precision_98_0,
            CASE
                WHEN a.stage = 'overall' THEN ROUND(ts.default_threshold, 6)
                ELSE NULL
            END AS default_threshold,
            CASE
                WHEN a.stage = 'overall' THEN ROUND(ts.default_threshold_precision, 6)
                ELSE NULL
            END AS default_threshold_precision,
            CASE
                WHEN a.stage = 'overall' THEN ROUND(ts.default_threshold_recall, 6)
                ELSE NULL
            END AS default_threshold_recall,
            CASE
                WHEN a.stage = 'overall'
                    THEN ROUND(ts.default_threshold_false_match_rate, 6)
                ELSE NULL
            END AS default_threshold_false_match_rate,
            CASE
                WHEN a.stage = 'overall'
                    THEN ROUND(ts.default_threshold_missed_match_rate, 6)
                ELSE NULL
            END AS default_threshold_missed_match_rate,
            CASE
                WHEN a.stage = 'overall'
                    THEN ROUND(ts.default_threshold_true_no_match_rejection_rate, 6)
                ELSE NULL
            END AS default_threshold_true_no_match_rejection_rate,
            CASE
                WHEN a.stage = 'overall'
                    THEN ROUND(ts.default_threshold_predicted_no_match_npv, 6)
                ELSE NULL
            END AS default_threshold_predicted_no_match_npv,
            CASE
                WHEN a.stage = 'overall' THEN ts.wrong_canonical_id_count
                ELSE NULL
            END AS wrong_canonical_id_count,
            CASE
                WHEN a.stage = 'overall' THEN ts.true_match_predicted_no_match_count
                ELSE NULL
            END AS true_match_predicted_no_match_count,
            CASE
                WHEN a.stage = 'overall'
                    THEN ts.true_no_match_forced_to_canonical_id_count
                ELSE NULL
            END AS true_no_match_forced_to_canonical_id_count
        FROM all_rows AS a
        CROSS JOIN threshold_summary AS ts
        ORDER BY
            CASE
                WHEN a.stage = 'unmatched' THEN a.total_rows_in_stage
                ELSE a.matched_rows_in_stage
            END DESC,
            a.stage
        """
    )
