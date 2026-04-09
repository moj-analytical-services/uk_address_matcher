from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable

from benchmarking.comparisons.comparison_utils import (
    comparison_stage_sort_key as _comparison_stage_sort_key,
    delta as _delta,
    index_by_stage as _index_by_stage,
    select_primary_accuracy_row as _select_primary_accuracy_row,
    to_float as _to_float,
    to_int as _to_int,
)
from benchmarking.comparisons.markdown_summary import (
    write_comparison_markdown_summary as _write_comparison_markdown_summary,
)
from benchmarking.insights.types import BenchmarkComparisonSummary


def _sql_string_literal(value: str) -> str:
    return value.replace("'", "''")


def _build_compact_table_sql(
    *,
    json_path: str,
    baseline_run_timestamp: str | None,
    comparison_run_timestamp: str | None,
    stage_metric_columns_sql: str,
    sort_value_sql: str,
    sort_direction: str,
    baseline_columns_sql: str,
    comparison_columns_sql: str,
    output_columns: list[str],
) -> str:
    escaped_path = _sql_string_literal(json_path)
    baseline_timestamp = _sql_string_literal(baseline_run_timestamp or "unknown")
    comparison_timestamp = _sql_string_literal(comparison_run_timestamp or "unknown")
    output_columns_sql = ",\n        ".join(output_columns)
    return f"""
    WITH raw AS (
        SELECT *
        FROM read_json_auto('{escaped_path}')
    ),
    stage_metrics_raw AS (
        SELECT
            stage,
{stage_metric_columns_sql}
        FROM raw
        GROUP BY stage
    ),
    stage_metrics AS (
        SELECT
            *,
            {sort_value_sql} AS stage_sort_value
        FROM stage_metrics_raw
    ),
    metadata AS (
        SELECT
            max(CASE WHEN run_type = 'baseline' THEN run_hash END) AS baseline_hash,
            max(CASE WHEN run_type = 'comparison' THEN run_hash END)
                AS comparison_hash
        FROM raw
    ),
    baseline_row AS (
        SELECT
            'baseline' AS version,
            (SELECT baseline_hash FROM metadata) AS version_hash,
            string_agg(
                stage,
                ' | '
                ORDER BY stage_sort_value {sort_direction}, stage
            ) AS stages_run,
{baseline_columns_sql},
            '{baseline_timestamp}' AS run_timestamp,
            1 AS _row_order
        FROM stage_metrics
    ),
    comparison_row AS (
        SELECT
            'comparison' AS version,
            (SELECT comparison_hash FROM metadata) AS version_hash,
            string_agg(
                stage,
                ' | '
                ORDER BY stage_sort_value {sort_direction}, stage
            ) AS stages_run,
{comparison_columns_sql},
            '{comparison_timestamp}' AS run_timestamp,
            2 AS _row_order
        FROM stage_metrics
    )
    SELECT
        version,
        version_hash,
        stages_run,
        {output_columns_sql},
        run_timestamp
    FROM (
        SELECT * FROM baseline_row
        UNION ALL
        SELECT * FROM comparison_row
    ) AS combined
    ORDER BY _row_order
    """


def _build_run_comparison_row(
    *,
    row: dict[str, Any],
    run_type: str,
    run_hash: str,
    git_commit_hash: str | None,
    baseline_hash: str,
    comparison_hash: str,
    stage: str,
    value_builder: Callable[[dict[str, Any]], dict[str, Any]],
) -> dict[str, Any]:
    return {
        "run_type": run_type,
        "run_hash": run_hash,
        "git_commit_hash": git_commit_hash,
        "baseline_hash": baseline_hash,
        "comparison_hash": comparison_hash,
        "stage": stage,
        **value_builder(row),
    }


def _build_comparison_rows(
    *,
    baseline_rows: list[dict[str, Any]],
    current_rows: list[dict[str, Any]],
    baseline_hash: str,
    current_hash: str,
    baseline_git_commit_hash: str | None,
    current_git_commit_hash: str | None,
    value_builder: Callable[[dict[str, Any]], dict[str, Any]],
) -> list[dict[str, Any]]:
    baseline_index = _index_by_stage(baseline_rows)
    current_index = _index_by_stage(current_rows)
    stages = sorted(
        set(baseline_index).union(current_index),
        key=lambda stage: _comparison_stage_sort_key(
            stage,
            baseline_index.get(stage, {}),
            current_index.get(stage, {}),
        ),
    )

    rows: list[dict[str, Any]] = []
    for stage in stages:
        baseline = baseline_index.get(stage, {})
        current = current_index.get(stage, {})
        rows.extend(
            [
                _build_run_comparison_row(
                    row=baseline,
                    run_type="baseline",
                    run_hash=baseline_hash,
                    git_commit_hash=baseline_git_commit_hash,
                    baseline_hash=baseline_hash,
                    comparison_hash=current_hash,
                    stage=stage,
                    value_builder=value_builder,
                ),
                _build_run_comparison_row(
                    row=current,
                    run_type="comparison",
                    run_hash=current_hash,
                    git_commit_hash=current_git_commit_hash,
                    baseline_hash=baseline_hash,
                    comparison_hash=current_hash,
                    stage=stage,
                    value_builder=value_builder,
                ),
            ]
        )

    return rows


def build_accuracy_compact_table_sql(
    *,
    json_path: str,
    baseline_run_timestamp: str | None = None,
    comparison_run_timestamp: str | None = None,
) -> str:
    stage_metric_columns_sql = """
            max(CASE WHEN run_type = 'baseline' THEN rows_matched_in_stage END)
                AS baseline_rows_matched_in_stage,
            max(CASE WHEN run_type = 'comparison' THEN rows_matched_in_stage END)
                AS comparison_rows_matched_in_stage,
            max(CASE WHEN run_type = 'baseline' THEN correct_matches END)
                AS baseline_correct_matches,
            max(CASE WHEN run_type = 'comparison' THEN correct_matches END)
                AS comparison_correct_matches,
            max(CASE WHEN run_type = 'baseline' THEN precision END)
                AS baseline_precision,
            max(CASE WHEN run_type = 'comparison' THEN precision END)
                AS comparison_precision,
            max(CASE WHEN run_type = 'baseline' THEN recall END)
                AS baseline_recall,
            max(CASE WHEN run_type = 'comparison' THEN recall END)
                AS comparison_recall,
            max(CASE WHEN run_type = 'baseline' THEN f1 END)
                AS baseline_f1,
            max(CASE WHEN run_type = 'comparison' THEN f1 END)
                AS comparison_f1
    """.strip()
    baseline_columns_sql = """
            string_agg(
                CAST(CAST(coalesce(baseline_correct_matches, 0) AS BIGINT) AS VARCHAR),
                ' | '
                ORDER BY stage_sort_value DESC, stage
            ) AS delta_matches,
            string_agg(
                CASE
                    WHEN baseline_precision IS NULL THEN 'n/a'
                    ELSE printf('%.4f%%', baseline_precision * 100)
                END,
                ' | '
                ORDER BY stage_sort_value DESC, stage
            ) AS precision,
            string_agg(
                CASE
                    WHEN baseline_recall IS NULL THEN 'n/a'
                    ELSE printf('%.4f%%', baseline_recall * 100)
                END,
                ' | '
                ORDER BY stage_sort_value DESC, stage
            ) AS recall,
            string_agg(
                CASE
                    WHEN baseline_f1 IS NULL THEN 'n/a'
                    ELSE printf('%.4f%%', baseline_f1 * 100)
                END,
                ' | '
                ORDER BY stage_sort_value DESC, stage
            ) AS f1
    """.strip()
    comparison_columns_sql = """
            string_agg(
                CAST(
                    CAST(
                        coalesce(comparison_correct_matches, 0)
                        - coalesce(baseline_correct_matches, 0)
                        AS BIGINT
                    )
                    AS VARCHAR
                ),
                ' | '
                ORDER BY stage_sort_value DESC, stage
            ) AS delta_matches,
            string_agg(
                CASE
                    WHEN comparison_precision IS NULL OR baseline_precision IS NULL
                    THEN 'n/a'
                    ELSE printf(
                        '%+.4f pp',
                        (comparison_precision - baseline_precision) * 100
                    )
                END,
                ' | '
                ORDER BY stage_sort_value DESC, stage
            ) AS precision,
            string_agg(
                CASE
                    WHEN comparison_recall IS NULL OR baseline_recall IS NULL
                    THEN 'n/a'
                    ELSE printf(
                        '%+.4f pp',
                        (comparison_recall - baseline_recall) * 100
                    )
                END,
                ' | '
                ORDER BY stage_sort_value DESC, stage
            ) AS recall,
            string_agg(
                CASE
                    WHEN comparison_f1 IS NULL OR baseline_f1 IS NULL
                    THEN 'n/a'
                    ELSE printf(
                        '%+.4f pp',
                        (comparison_f1 - baseline_f1) * 100
                    )
                END,
                ' | '
                ORDER BY stage_sort_value DESC, stage
            ) AS f1
    """.strip()
    return _build_compact_table_sql(
        json_path=json_path,
        baseline_run_timestamp=baseline_run_timestamp,
        comparison_run_timestamp=comparison_run_timestamp,
        stage_metric_columns_sql=stage_metric_columns_sql,
        sort_value_sql=(
            "greatest("
            "coalesce(baseline_rows_matched_in_stage, 0), "
            "coalesce(comparison_rows_matched_in_stage, 0)"
            ")"
        ),
        sort_direction="DESC",
        baseline_columns_sql=baseline_columns_sql,
        comparison_columns_sql=comparison_columns_sql,
        output_columns=["delta_matches", "precision", "recall", "f1"],
    )


def build_stage_diagnostics_compact_table_sql(
    *,
    json_path: str,
    baseline_run_timestamp: str | None = None,
    comparison_run_timestamp: str | None = None,
) -> str:
    stage_metric_columns_sql = """
            max(CASE WHEN run_type = 'baseline' THEN rows_entering_stage END)
                AS baseline_rows_entering_stage,
            max(CASE WHEN run_type = 'comparison' THEN rows_entering_stage END)
                AS comparison_rows_entering_stage,
            max(CASE WHEN run_type = 'baseline' THEN rows_matched_in_stage END)
                AS baseline_rows_matched_in_stage,
            max(CASE WHEN run_type = 'comparison' THEN rows_matched_in_stage END)
                AS comparison_rows_matched_in_stage,
            max(CASE WHEN run_type = 'baseline' THEN stage_match_rate END)
                AS baseline_stage_match_rate,
            max(CASE WHEN run_type = 'comparison' THEN stage_match_rate END)
                AS comparison_stage_match_rate,
            max(CASE WHEN run_type = 'baseline' THEN elapsed_seconds END)
                AS baseline_elapsed_seconds,
            max(CASE WHEN run_type = 'comparison' THEN elapsed_seconds END)
                AS comparison_elapsed_seconds
    """.strip()
    baseline_columns_sql = """
            string_agg(
                CAST(
                    CAST(coalesce(baseline_rows_entering_stage, 0) AS BIGINT) AS VARCHAR
                ),
                ' | '
                ORDER BY stage_sort_value ASC, stage
            ) AS delta_rows_entering_stage,
            string_agg(
                CAST(
                    CAST(coalesce(baseline_rows_matched_in_stage, 0) AS BIGINT) AS VARCHAR
                ),
                ' | '
                ORDER BY stage_sort_value ASC, stage
            ) AS delta_rows_matched_in_stage,
            string_agg(
                CASE
                    WHEN baseline_stage_match_rate IS NULL THEN 'n/a'
                    ELSE printf('%.4f%%', baseline_stage_match_rate)
                END,
                ' | '
                ORDER BY stage_sort_value ASC, stage
            ) AS stage_match_rate,
            string_agg(
                CASE
                    WHEN baseline_elapsed_seconds IS NULL THEN 'n/a'
                    ELSE printf('%.4fs', baseline_elapsed_seconds)
                END,
                ' | '
                ORDER BY stage_sort_value ASC, stage
            ) AS elapsed_seconds
    """.strip()
    comparison_columns_sql = """
            string_agg(
                CAST(
                    CAST(
                        coalesce(comparison_rows_entering_stage, 0)
                        - coalesce(baseline_rows_entering_stage, 0)
                        AS BIGINT
                    )
                    AS VARCHAR
                ),
                ' | '
                ORDER BY stage_sort_value ASC, stage
            ) AS delta_rows_entering_stage,
            string_agg(
                CAST(
                    CAST(
                        coalesce(comparison_rows_matched_in_stage, 0)
                        - coalesce(baseline_rows_matched_in_stage, 0)
                        AS BIGINT
                    )
                    AS VARCHAR
                ),
                ' | '
                ORDER BY stage_sort_value ASC, stage
            ) AS delta_rows_matched_in_stage,
            string_agg(
                CASE
                    WHEN comparison_stage_match_rate IS NULL
                        OR baseline_stage_match_rate IS NULL
                    THEN 'n/a'
                    ELSE printf(
                        '%+.4f pp',
                        comparison_stage_match_rate - baseline_stage_match_rate
                    )
                END,
                ' | '
                ORDER BY stage_sort_value ASC, stage
            ) AS stage_match_rate,
            string_agg(
                CASE
                    WHEN comparison_elapsed_seconds IS NULL
                        OR baseline_elapsed_seconds IS NULL
                    THEN 'n/a'
                    ELSE printf(
                        '%+.4fs',
                        comparison_elapsed_seconds - baseline_elapsed_seconds
                    )
                END,
                ' | '
                ORDER BY stage_sort_value ASC, stage
            ) AS elapsed_seconds
    """.strip()
    return _build_compact_table_sql(
        json_path=json_path,
        baseline_run_timestamp=baseline_run_timestamp,
        comparison_run_timestamp=comparison_run_timestamp,
        stage_metric_columns_sql=stage_metric_columns_sql,
        sort_value_sql=(
            "least("
            "coalesce(baseline_elapsed_seconds, 1e308), "
            "coalesce(comparison_elapsed_seconds, 1e308)"
            ")"
        ),
        sort_direction="ASC",
        baseline_columns_sql=baseline_columns_sql,
        comparison_columns_sql=comparison_columns_sql,
        output_columns=[
            "delta_rows_entering_stage",
            "delta_rows_matched_in_stage",
            "stage_match_rate",
            "elapsed_seconds",
        ],
    )


def build_accuracy_comparison_rows(
    *,
    baseline_rows: list[dict[str, Any]],
    current_rows: list[dict[str, Any]],
    baseline_hash: str,
    current_hash: str,
    baseline_git_commit_hash: str | None = None,
    current_git_commit_hash: str | None = None,
) -> list[dict[str, Any]]:
    def value_builder(row: dict[str, Any]) -> dict[str, Any]:
        return {
            "rows_matched_in_stage": _to_int(row.get("rows_matched_in_stage")),
            "correct_matches": _to_int(row.get("correct_matches")),
            "wrong_matches": _to_int(row.get("wrong_matches")),
            "precision": _to_float(row.get("precision")),
            "recall": _to_float(row.get("recall")),
            "f1": _to_float(row.get("f1")),
            "wrong_match_rate": _to_float(row.get("wrong_match_rate")),
            "correct_share_of_total": _to_float(row.get("correct_share_of_total")),
        }

    return _build_comparison_rows(
        baseline_rows=baseline_rows,
        current_rows=current_rows,
        baseline_hash=baseline_hash,
        current_hash=current_hash,
        baseline_git_commit_hash=baseline_git_commit_hash,
        current_git_commit_hash=current_git_commit_hash,
        value_builder=value_builder,
    )


def build_stage_diagnostics_comparison_rows(
    *,
    baseline_rows: list[dict[str, Any]],
    current_rows: list[dict[str, Any]],
    baseline_hash: str,
    current_hash: str,
    baseline_git_commit_hash: str | None = None,
    current_git_commit_hash: str | None = None,
) -> list[dict[str, Any]]:
    def value_builder(row: dict[str, Any]) -> dict[str, Any]:
        return {
            "stage_order": _to_int(row.get("stage_order")),
            "rows_entering_stage": _to_int(row.get("rows_entering_stage")),
            "rows_matched_in_stage": _to_int(row.get("rows_matched_in_stage")),
            "stage_match_rate": _to_float(row.get("stage_match_rate")),
            "share_of_total_input_matched": _to_float(
                row.get("share_of_total_input_matched")
            ),
            "elapsed_seconds": _to_float(row.get("elapsed_seconds")),
        }

    return _build_comparison_rows(
        baseline_rows=baseline_rows,
        current_rows=current_rows,
        baseline_hash=baseline_hash,
        current_hash=current_hash,
        baseline_git_commit_hash=baseline_git_commit_hash,
        current_git_commit_hash=current_git_commit_hash,
        value_builder=value_builder,
    )


def _build_notes(overall_delta: dict[str, float | None]) -> list[str]:
    notes: list[str] = []

    for metric, direction_text in (
        ("precision", "precision"),
        ("recall", "recall"),
        ("f1", "f1"),
    ):
        delta = overall_delta.get(metric)
        if delta is None:
            continue
        if delta > 0:
            notes.append(f"Overall {direction_text} improved by {delta * 100:.4f} pp.")
        elif delta < 0:
            notes.append(
                f"Overall {direction_text} regressed by {abs(delta) * 100:.4f} pp."
            )

    runtime_delta = overall_delta.get("total_runtime_seconds")
    if runtime_delta is not None:
        if runtime_delta < 0:
            notes.append(f"Total runtime improved by {abs(runtime_delta):.3f}s.")
        elif runtime_delta > 0:
            notes.append(f"Total runtime increased by {runtime_delta:.3f}s.")

    if not notes:
        notes.append("No material overall deltas detected.")
    return notes


def build_comparison_summary(
    *,
    baseline_hash: str,
    current_hash: str,
    baseline_accuracy_rows: list[dict[str, Any]],
    current_accuracy_rows: list[dict[str, Any]],
    baseline_stage_rows: list[dict[str, Any]],
    current_stage_rows: list[dict[str, Any]],
    baseline_total_runtime_seconds: float | None,
    current_total_runtime_seconds: float | None,
    summary_path: Path,
    chart_paths: list[Path],
    markdown_report_path: Path | None = None,
    dataset_label: str | None = None,
    baseline_git_commit_hash: str | None = None,
    current_git_commit_hash: str | None = None,
    baseline_created_at_utc: str | None = None,
    current_created_at_utc: str | None = None,
    accuracy_comparison_rows: list[dict[str, Any]] | None = None,
    stage_diagnostics_comparison_rows: list[dict[str, Any]] | None = None,
) -> BenchmarkComparisonSummary:
    baseline_stages = _index_by_stage(baseline_stage_rows)
    current_stages = _index_by_stage(current_stage_rows)

    overall_baseline = _select_primary_accuracy_row(baseline_accuracy_rows)
    overall_current = _select_primary_accuracy_row(current_accuracy_rows)
    overall_delta = {
        "precision": _delta(
            overall_current.get("precision"),
            overall_baseline.get("precision"),
        ),
        "recall": _delta(
            overall_current.get("recall"),
            overall_baseline.get("recall"),
        ),
        "f1": _delta(overall_current.get("f1"), overall_baseline.get("f1")),
        "wrong_match_rate": _delta(
            overall_current.get("wrong_match_rate"),
            overall_baseline.get("wrong_match_rate"),
        ),
        "total_runtime_seconds": _delta(
            current_total_runtime_seconds,
            baseline_total_runtime_seconds,
        ),
    }

    all_stages = sorted(set(baseline_stages).union(current_stages))
    stage_deltas: dict[str, dict[str, float | None]] = {}
    for stage in all_stages:
        base_row = baseline_stages.get(stage, {})
        current_row = current_stages.get(stage, {})
        stage_deltas[stage] = {
            "rows_matched_in_stage": _delta(
                current_row.get("rows_matched_in_stage"),
                base_row.get("rows_matched_in_stage"),
            ),
            "stage_match_rate": _delta(
                current_row.get("stage_match_rate"),
                base_row.get("stage_match_rate"),
            ),
            "elapsed_seconds": _delta(
                current_row.get("elapsed_seconds"),
                base_row.get("elapsed_seconds"),
            ),
        }

    summary = BenchmarkComparisonSummary(
        baseline_hash=baseline_hash,
        current_hash=current_hash,
        overall_delta=overall_delta,
        stage_deltas=stage_deltas,
        notes=_build_notes(overall_delta),
        summary_path=summary_path.as_posix(),
        chart_paths=[path.as_posix() for path in chart_paths],
        markdown_report_path=(
            markdown_report_path.as_posix() if markdown_report_path is not None else None
        ),
        accuracy_comparison_rows=accuracy_comparison_rows,
        stage_diagnostics_comparison_rows=stage_diagnostics_comparison_rows,
    )

    summary_path.write_text(
        json.dumps(
            {
                "baseline_hash": summary.baseline_hash,
                "current_hash": summary.current_hash,
                "overall_delta": summary.overall_delta,
                "stage_deltas": summary.stage_deltas,
                "notes": summary.notes,
                "chart_paths": summary.chart_paths,
                "markdown_report_path": summary.markdown_report_path,
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    if markdown_report_path is not None:
        _write_comparison_markdown_summary(
            path=markdown_report_path,
            dataset_label=dataset_label,
            baseline_hash=baseline_hash,
            current_hash=current_hash,
            baseline_git_commit_hash=baseline_git_commit_hash,
            current_git_commit_hash=current_git_commit_hash,
            baseline_created_at_utc=baseline_created_at_utc,
            current_created_at_utc=current_created_at_utc,
            notes=summary.notes,
            baseline_accuracy_rows=baseline_accuracy_rows,
            current_accuracy_rows=current_accuracy_rows,
            baseline_stage_rows=baseline_stage_rows,
            current_stage_rows=current_stage_rows,
            chart_paths=chart_paths,
            summary_path=summary_path,
        )

    return summary
