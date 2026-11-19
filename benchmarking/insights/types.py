from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import duckdb


@dataclass(frozen=True)
class DatasetDiagnostics:
    successful_matches: duckdb.DuckDBPyRelation | None
    incorrect_matches: duckdb.DuckDBPyRelation | None
    lowest_similarity_incorrect: duckdb.DuckDBPyRelation | None
    highest_similarity_incorrect: duckdb.DuckDBPyRelation | None
    suspicious_incorrect_summary: duckdb.DuckDBPyRelation | None
    suspicious_incorrect_records: duckdb.DuckDBPyRelation | None
    unmatched_records: duckdb.DuckDBPyRelation | None
    unmatched_top_splink: duckdb.DuckDBPyRelation | None
    splink_available: bool


@dataclass(frozen=True)
class BenchmarkOutputOptions:
    show_splink_comparisons: bool = True
    show_successful_matches: bool = False
    show_incorrect_matches: bool = True
    show_similarity_score_checks: bool = True
    show_unmatched_records: bool = False

    def enable_diagnostics(self) -> bool:
        return any(
            (
                self.show_successful_matches,
                self.show_incorrect_matches,
                self.show_similarity_score_checks,
                self.show_unmatched_records,
            )
        )

    def include_successful_matches(self) -> bool:
        return self.show_successful_matches

    def include_incorrect_matches(self) -> bool:
        return self.show_incorrect_matches or self.show_similarity_score_checks

    def include_similarity_score_checks(self) -> bool:
        return self.show_similarity_score_checks

    def include_unmatched_records(self) -> bool:
        return self.show_unmatched_records


@dataclass(frozen=True)
class BenchmarkComparisonSummary:
    baseline_run_id: str
    current_run_id: str
    baseline_hash: str
    current_hash: str
    overall_delta: dict[str, float | None]
    stage_deltas: dict[str, dict[str, float | None]]
    notes: list[str]
    summary_path: str
    chart_paths: list[str]
    markdown_report_path: str | None = None
    accuracy_comparison_rows: list[dict[str, Any]] | None = None
    stage_diagnostics_comparison_rows: list[dict[str, Any]] | None = None


@dataclass(frozen=True)
class PersistedBenchmarkRun:
    run_id: str
    run_hash: str
    group_key: str
    created_at_utc: str
    run_dir: str
    manifest_path: str
    history_path: str
    deduplicated: bool
    git_commit_hash: str | None
    comparison: BenchmarkComparisonSummary | None = None
    comparison_warning: str | None = None
    precision_recall_chart_path: str | None = None
