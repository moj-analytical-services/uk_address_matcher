from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from tempfile import NamedTemporaryFile

import duckdb

from benchmarking.comparisons.comparison_artifacts import (
    build_accuracy_compact_table_sql,
    build_stage_diagnostics_compact_table_sql,
)
from benchmarking.constants import BENCHMARK_PROJECT_ROOT
from benchmarking.insights.types import BenchmarkComparisonSummary

_REPO_ROOT = Path(BENCHMARK_PROJECT_ROOT)


def _absolute_path(path_value: str | None) -> str | None:
    if path_value is None:
        return None

    path = Path(path_value)
    if path.is_absolute():
        return path.as_posix()

    return (_REPO_ROOT / path).as_posix()


def _show_rows_as_table(
    *,
    con: duckdb.DuckDBPyConnection,
    title: str,
    rows: list[dict[str, object]] | None,
    baseline_run_timestamp: str | None = None,
    comparison_run_timestamp: str | None = None,
) -> None:
    if not rows:
        print(f"\n{title}: unavailable")
        return

    with NamedTemporaryFile(mode="w", suffix=".json", encoding="utf-8") as handle:
        json.dump(rows, handle)
        handle.flush()

        path = handle.name.replace("'", "''")
        print(f"\n{title}:")
        relation = con.sql(f"SELECT * FROM read_json_auto('{path}')")
        columns = set(relation.columns)

        if {
            "stage",
            "run_type",
            "correct_matches",
            "f1",
            "precision",
            "recall",
        }.issubset(columns):
            con.sql(
                build_accuracy_compact_table_sql(
                    json_path=handle.name,
                    baseline_run_timestamp=baseline_run_timestamp,
                    comparison_run_timestamp=comparison_run_timestamp,
                )
            ).show(max_width=50000)
            return

        if {
            "stage",
            "run_type",
            "rows_entering_stage",
            "rows_matched_in_stage",
            "stage_match_rate",
            "elapsed_seconds",
        }.issubset(columns):
            con.sql(
                build_stage_diagnostics_compact_table_sql(
                    json_path=handle.name,
                    baseline_run_timestamp=baseline_run_timestamp,
                    comparison_run_timestamp=comparison_run_timestamp,
                )
            ).show(max_width=50000)
            return

        relation.show(max_width=50000)


def _format_run_timestamp(value: str | None) -> str:
    if not value:
        return "unknown"

    normalised = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalised)
    except ValueError:
        return value

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    utc_time = parsed.astimezone(UTC)
    return utc_time.strftime("%Y-%m-%d %H:%M:%S UTC")


def _lookup_run_timestamps(
    *,
    history_path: str | None,
    baseline_hash: str,
    comparison_hash: str,
) -> tuple[str | None, str | None]:
    if not history_path:
        return (None, None)

    resolved_history_path = _absolute_path(history_path)
    if resolved_history_path is None:
        return (None, None)

    path = Path(resolved_history_path)
    if not path.exists():
        return (None, None)

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return (None, None)

    runs_by_hash = payload.get("runs_by_hash", {})
    baseline = runs_by_hash.get(baseline_hash, {})
    comparison = runs_by_hash.get(comparison_hash, {})
    return (
        baseline.get("created_at_utc"),
        comparison.get("created_at_utc"),
    )


def print_comparison_report(
    *,
    comparison: BenchmarkComparisonSummary,
    history_path: str | None = None,
) -> None:
    display_con = duckdb.connect(database=":memory:")
    baseline_ts_raw, comparison_ts_raw = _lookup_run_timestamps(
        history_path=history_path,
        baseline_hash=comparison.baseline_hash,
        comparison_hash=comparison.current_hash,
    )
    baseline_ts = _format_run_timestamp(baseline_ts_raw)
    comparison_ts = _format_run_timestamp(comparison_ts_raw)

    print("Comparison completed")
    print(f"- current_hash: {comparison.current_hash}")
    print(f"- baseline_hash: {comparison.baseline_hash}")
    print(f"- summary: {_absolute_path(comparison.summary_path)}")
    if comparison.markdown_report_path is not None:
        print(f"- pr_markdown: {_absolute_path(comparison.markdown_report_path)}")
    if comparison.chart_paths:
        print("- charts:")
        for chart_path in comparison.chart_paths:
            print(f"  - {_absolute_path(chart_path)}")

    _show_rows_as_table(
        con=display_con,
        title="Accuracy comparison table",
        rows=comparison.accuracy_comparison_rows,
        baseline_run_timestamp=baseline_ts,
        comparison_run_timestamp=comparison_ts,
    )
    _show_rows_as_table(
        con=display_con,
        title="Stage diagnostics comparison table",
        rows=comparison.stage_diagnostics_comparison_rows,
        baseline_run_timestamp=baseline_ts,
        comparison_run_timestamp=comparison_ts,
    )
