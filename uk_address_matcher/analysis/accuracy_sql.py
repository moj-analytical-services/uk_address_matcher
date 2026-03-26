from __future__ import annotations


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
