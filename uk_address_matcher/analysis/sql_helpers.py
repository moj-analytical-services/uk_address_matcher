from __future__ import annotations


def sql_literal(value: str) -> str:
    """Escape a string for safe embedding as a SQL string literal."""
    return value.replace("'", "''")


def percent_string_sql(*, value_sql: str, decimals: int = 1) -> str:
    """Format a [0, 1] metric as a percentage string."""
    return f"concat(CAST(ROUND(({value_sql}) * 100.0, {decimals}) AS VARCHAR), '%')"


def signed_pp_delta_sql(
    *,
    delta_sql: str,
    decimals: int = 1,
    zero_text: str = "0.0 pp",
) -> str:
    """Format a numeric delta as signed percentage points."""
    format_spec = f"%.{decimals}f"
    return (
        "CASE "
        f"WHEN ({delta_sql}) > 0 THEN concat('+', "
        f"printf('{format_spec}', ({delta_sql}) * 100.0), ' pp') "
        f"WHEN ({delta_sql}) < 0 THEN concat("
        f"printf('{format_spec}', ({delta_sql}) * 100.0), ' pp') "
        f"ELSE '{zero_text}' END"
    )


def signed_count_delta_sql(*, delta_sql: str) -> str:
    """Format an integer delta with explicit plus sign for positive values."""
    return (
        "CASE "
        f"WHEN ({delta_sql}) > 0 THEN concat('+', format('{{:,}}', ({delta_sql}))) "
        f"WHEN ({delta_sql}) < 0 THEN format('{{:,}}', ({delta_sql})) "
        "ELSE '0' END"
    )


def signed_decimal_delta_sql(
    *,
    delta_sql: str,
    decimals: int,
    zero_text: str,
) -> str:
    """Format a signed decimal delta with optional plus sign."""
    return (
        "CASE "
        f"WHEN ({delta_sql}) > 0 THEN concat('+', "
        f"CAST(ROUND(({delta_sql}), {decimals}) AS VARCHAR)) "
        f"WHEN ({delta_sql}) < 0 THEN CAST(ROUND(({delta_sql}), {decimals}) AS VARCHAR) "
        f"ELSE '{zero_text}' END"
    )


def baseline_or_delta_sql(
    *,
    is_baseline_sql: str,
    baseline_value_sql: str,
    delta_value_sql: str,
) -> str:
    """Return baseline formatting for baseline rows, otherwise delta formatting."""
    return (
        "CASE "
        f"WHEN {is_baseline_sql} THEN {baseline_value_sql} "
        f"ELSE {delta_value_sql} END"
    )
