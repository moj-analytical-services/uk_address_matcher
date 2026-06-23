from __future__ import annotations

from uk_address_matcher.sql_pipeline.match_reasons import MatchReason


def ratio_sql(
    numerator_sql: str,
    denominator_sql: str,
    *,
    when_zero_sql: str,
) -> str:
    """Return a guarded ratio expression.

    Args:
        numerator_sql: SQL expression for the numerator.
        denominator_sql: SQL expression for the denominator.
        when_zero_sql: SQL literal/expression returned when denominator is zero.
    """
    return (
        f"CASE WHEN ({denominator_sql}) = 0 THEN {when_zero_sql} "
        f"ELSE ({numerator_sql})::DOUBLE / NULLIF(({denominator_sql})::DOUBLE, 0) END"
    )


def f1_from_counts_sql(*, tp_sql: str, fp_sql: str, fn_sql: str) -> str:
    """Return an F1 expression from TP/FP/FN SQL expressions."""
    return (
        "CASE "
        f"WHEN 2.0 * ({tp_sql}) + ({fp_sql}) + ({fn_sql}) = 0 THEN 0.0 "
        f"ELSE 2.0 * ({tp_sql}) / (2.0 * ({tp_sql}) + ({fp_sql}) + ({fn_sql})) "
        "END"
    )


def percentage_ratio_sql(
    numerator_sql: str,
    denominator_sql: str,
    *,
    when_zero_sql: str,
) -> str:
    """Return a guarded percentage ratio expression."""
    ratio_expr = ratio_sql(
        numerator_sql,
        denominator_sql,
        when_zero_sql=when_zero_sql,
    )
    return f"100.0 * ({ratio_expr})"


def wrong_match_rate_sql(*, rows_matched_sql: str, correct_matches_sql: str) -> str:
    """Return wrong-match percentage over matched rows."""
    return percentage_ratio_sql(
        f"({rows_matched_sql}) - ({correct_matches_sql})",
        rows_matched_sql,
        when_zero_sql="NULL::DOUBLE",
    )


def correct_share_of_total_sql(*, correct_matches_sql: str, total_rows_sql: str) -> str:
    """Return percentage of total rows that are correctly matched."""
    return percentage_ratio_sql(
        correct_matches_sql,
        total_rows_sql,
        when_zero_sql="NULL::DOUBLE",
    )


def hit_rate_at_k_sql(*, rank_sql: str, k: int) -> str:
    """Return hit-rate at k expression for a rank column."""
    return f"AVG(CASE WHEN {rank_sql} <= {k} THEN 1.0 ELSE 0.0 END)"


def count_true_in_top_k_sql(*, rank_sql: str, k: int) -> str:
    """Return count of rows where rank is at most k."""
    return f"SUM(CASE WHEN {rank_sql} <= {k} THEN 1 ELSE 0 END)"


def count_flagged_rows_sql(*, flag_sql: str) -> str:
    """Return count of rows where the integer flag equals 1."""
    return f"SUM(CASE WHEN {flag_sql} = 1 THEN 1 ELSE 0 END)"


def precision_at_k_sql(*, rank_sql: str, k: int, has_predictions_sql: str) -> str:
    """Return precision@k using an emitted-predictions denominator."""
    return ratio_sql(
        count_true_in_top_k_sql(rank_sql=rank_sql, k=k),
        count_flagged_rows_sql(flag_sql=has_predictions_sql),
        when_zero_sql="NULL::DOUBLE",
    )


def reciprocal_rank_mean_sql(*, rank_sql: str) -> str:
    """Return mean reciprocal rank expression for a rank column."""
    return f"AVG(CASE WHEN {rank_sql} IS NULL THEN 0.0 ELSE 1.0 / {rank_sql} END)"


def average_rank_within_k_sql(*, rank_sql: str, k: int) -> str:
    """Return average true rank expression capped to rows where rank <= k."""
    return (
        "AVG(CASE "
        f"WHEN {rank_sql} IS NOT NULL AND {rank_sql} <= {k} "
        f"THEN {rank_sql}::DOUBLE ELSE NULL END)"
    )


def build_threshold_metrics_sql(rounding_expr: str) -> str:
    """Return threshold-sweep SQL parameterised by the score-rounding expression."""
    splink_value = MatchReason.SPLINK.value.replace("'", "''")
    enum_values = str(MatchReason.enum_values())
    splink_reason_sql = f"'{splink_value}'::ENUM {enum_values}"

    return f"""
    WITH canonical_ids AS (
        SELECT DISTINCT unique_id FROM __ukam_threshold_canonical__
    ),
    labelled AS (
        SELECT
            m.unique_id,
            CASE WHEN c.unique_id IS NOT NULL THEN 1 ELSE 0 END AS clerical_positive,
            CASE
                WHEN c.unique_id IS NOT NULL
                 AND m.resolved_canonical_id = m.ukam_label
                THEN 1
                ELSE 0
            END AS true_positive_row,
            CASE
                WHEN m.match_reason IS NULL THEN CAST(-999 AS DOUBLE)
                WHEN m.match_reason = {splink_reason_sql} THEN {rounding_expr}
                ELSE CAST(999 AS DOUBLE)
            END AS match_weight_adj
        FROM __ukam_threshold_matches__ m
        LEFT JOIN canonical_ids c ON m.ukam_label = c.unique_id
    ),
    grouped AS (
        SELECT
            match_weight_adj                                           AS truth_threshold,
            SUM(true_positive_row)                                     AS tp_row,
            SUM(1 - true_positive_row)                                 AS not_tp_row,
            SUM(CASE WHEN true_positive_row = 0 AND clerical_positive = 0
                     THEN 1 ELSE 0 END)                                AS fp_neg_at,
            SUM(clerical_positive)                                     AS cp,
            SUM(1 - clerical_positive)                                 AS cn
        FROM labelled
        GROUP BY match_weight_adj
    ),
    stats AS (
        SELECT
            truth_threshold,
            SUM(tp_row)    OVER (ORDER BY truth_threshold DESC)         AS tp,
            SUM(not_tp_row) OVER (ORDER BY truth_threshold DESC)        AS fp,
            SUM(fp_neg_at) OVER (ORDER BY truth_threshold DESC)         AS fp_neg,
            SUM(cp) OVER ()                                             AS p,
            SUM(cn) OVER ()                                             AS n
        FROM grouped
    ),
    truth_space AS (
        SELECT
            truth_threshold,
            p                                                          AS p,
            n                                                          AS n,
            CAST(tp              AS DOUBLE)                            AS tp,
            CAST(fp              AS DOUBLE)                            AS fp,
            CAST(fp_neg          AS DOUBLE)                            AS fp_neg,
            CAST(p - tp          AS DOUBLE)                            AS fn,
            CAST(GREATEST(n - fp_neg, 0) AS DOUBLE)                    AS tn
        FROM stats
    )
    SELECT
        truth_threshold,
        CASE
            WHEN truth_threshold >=  999 THEN 1.0
            WHEN truth_threshold <= -999 THEN 0.0
            ELSE power(2, truth_threshold)
                / (1.0 + power(2, truth_threshold))
        END AS match_probability,
        tp                                                              AS tp,
        tn                                                              AS tn,
        fp                                                              AS fp,
        fp_neg                                                          AS fp_neg,
        fn                                                              AS fn,
        tp / NULLIF(p, 0)                                               AS tp_rate,
        tn / NULLIF(CAST(n AS DOUBLE), 0)                               AS tn_rate,
        fp_neg / NULLIF(CAST(n AS DOUBLE), 0)                           AS fp_rate,
        fn / NULLIF(p, 0)                                               AS fn_rate,
        CASE WHEN tp + fp = 0 THEN 1.0 ELSE tp / (tp + fp) END         AS precision,
        tp / NULLIF(p, 0)                                               AS recall,
        CASE
            WHEN 2.0 * tp + fp + fn = 0 THEN 0.0
            ELSE 2.0 * tp / (2.0 * tp + fp + fn)
        END                                                             AS f1
    FROM truth_space
    ORDER BY truth_threshold ASC
    """
