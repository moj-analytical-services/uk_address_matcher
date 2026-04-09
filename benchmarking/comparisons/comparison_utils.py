from __future__ import annotations

from typing import Any


def index_by_stage(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {str(row["stage"]): row for row in rows if "stage" in row}


def to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def delta(current: Any, baseline: Any) -> float | None:
    current_f = to_float(current)
    baseline_f = to_float(baseline)
    if current_f is None or baseline_f is None:
        return None
    return round(current_f - baseline_f, 8)


def comparison_stage_sort_key(
    stage: str,
    baseline_row: dict[str, Any],
    current_row: dict[str, Any],
) -> tuple[int, float | int | str, str]:
    stage_orders = [
        value
        for value in (
            to_int(baseline_row.get("stage_order")),
            to_int(current_row.get("stage_order")),
        )
        if value is not None
    ]
    if stage_orders:
        return (0, min(stage_orders), stage)

    matched_counts = [
        value
        for value in (
            to_int(baseline_row.get("rows_matched_in_stage")),
            to_int(current_row.get("rows_matched_in_stage")),
        )
        if value is not None
    ]
    if matched_counts:
        return (1, -max(matched_counts), stage)

    elapsed_seconds = [
        value
        for value in (
            to_float(baseline_row.get("elapsed_seconds")),
            to_float(current_row.get("elapsed_seconds")),
        )
        if value is not None
    ]
    if elapsed_seconds:
        return (2, min(elapsed_seconds), stage)

    return (3, stage, stage)


def select_primary_accuracy_row(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {}

    return max(
        rows,
        key=lambda row: (
            to_int(row.get("rows_matched_in_stage")) or -1,
            to_int(row.get("correct_matches")) or -1,
            str(row.get("stage", "")),
        ),
    )
