from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from benchmarking.constants import BENCHMARK_PROJECT_ROOT
from benchmarking.insights.types import BenchmarkOutputOptions
from uk_address_matcher.analysis.splink_comparison_metrics import (
    build_splink_model_comparison,
)

if TYPE_CHECKING:
    from benchmarking.runner import BenchmarkRunResult


_REPO_ROOT = Path(BENCHMARK_PROJECT_ROOT)


def _display_path(path_value: str | None) -> str | None:
    if path_value is None:
        return None

    path = Path(path_value)
    if path.is_absolute():
        return path.as_posix()

    return (_REPO_ROOT / path).as_posix()


def _show_via_sql(result: BenchmarkRunResult, relation) -> None:
    relation.show(max_width=50000)


def _sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _print_by_match_reason(
    result: BenchmarkRunResult,
    relation,
    *,
    title: str,
    section_header: str,
) -> None:
    reason_rel = result.con.sql(
        f"""
        SELECT DISTINCT match_reason
        FROM ({relation.sql_query()}) AS data
        WHERE match_reason IS NOT NULL
        ORDER BY match_reason
        """
    )
    reasons = reason_rel.fetchall()

    if not reasons:
        print(f"\n{section_header}")
        print(title)
        print("No rows to display.")
        return

    print(f"\n{section_header}")
    print(title)
    for row in reasons:
        reason_name = str(row[0])
        escaped_reason = _sql_literal(reason_name)
        reason_table = result.con.sql(
            f"""
            SELECT * EXCLUDE (match_reason)
            FROM ({relation.sql_query()}) AS data
            WHERE match_reason = '{escaped_reason}'
            """
        )
        row_count = int(reason_table.aggregate("COUNT(*) AS row_count").fetchone()[0])
        print(f"\nmatch_reason: {reason_name}. Showing {row_count} records")
        _show_via_sql(result, reason_table)


def print_benchmark_summary(
    results: list[BenchmarkRunResult],
    *,
    splink_baseline_weight: float | None = None,
    splink_comparison_weights: list[float] | None = None,
    top_k_precision_at_metrics: list[int] | None = None,
    output_options: BenchmarkOutputOptions | None = None,
) -> None:
    output_options = output_options or BenchmarkOutputOptions()

    print("\nBenchmark summary")
    for result in results:
        print(f"\nDataset: {result.dataset_key}")

        print(
            "Timings: "
            f"data_load={result.timings['data_load']:.2f}s, "
            f"match_pipeline={result.timings['match_pipeline']:.2f}s"
        )

        if result.accuracy_table is not None:
            print("\nAccuracy table:")
            _show_via_sql(result, result.accuracy_table)

        if result.stage_diagnostics_table is not None:
            print("\nStage diagnostics:")
            _show_via_sql(result, result.stage_diagnostics_table)

        if output_options.show_splink_comparisons and splink_baseline_weight is not None:
            table_name = f"simple_bench_matches_{result.dataset_key}"
            relation = result.con.table(table_name)

            print("\nSplink threshold comparison:")

            if not result.splink_available:
                print("Splink not active for this run. Skipping Splink comparisons.")
                continue

            precision_metrics = top_k_precision_at_metrics
            if precision_metrics is not None and result.splink_predictions is None:
                print(
                    "Splink predictions unavailable; showing top-1 threshold "
                    "comparison only."
                )
                precision_metrics = None

            splink_matches = build_splink_model_comparison(
                result.con,
                relation,
                baseline_match_weight=splink_baseline_weight,
                splink_comparison_weights=splink_comparison_weights,
                predictions_relation=result.splink_predictions,
                precision_at_metrics=precision_metrics,
            )

            print("--- SPLINK Comparison: Headline ---")
            _show_via_sql(result, splink_matches.headline_table)
            print(splink_matches.total_input_rows)
            print("--- SPLINK Comparison: Delta ---")
            _show_via_sql(result, splink_matches.delta_table)


def print_run_persistence_summary(results: list[BenchmarkRunResult]) -> None:
    persisted_results = [result for result in results if result.persisted_run is not None]
    if not persisted_results:
        return

    print("\nRun persistence")
    for result in persisted_results:
        persisted = result.persisted_run
        if persisted is None:
            continue

        print(f"\nDataset: {result.dataset_key}")
        print(
            f"hash={persisted.run_hash} "
            f"deduplicated={persisted.deduplicated} "
            f"manifest={_display_path(persisted.manifest_path)}"
        )
        if persisted.comparison_warning is not None:
            print(f"Warning: {persisted.comparison_warning}")
        if persisted.comparison is None:
            continue

        comparison = persisted.comparison
        summary_path = _display_path(comparison.summary_path)
        print(
            "Compared against baseline "
            f"{comparison.baseline_hash}. Summary: {summary_path}"
        )
        if comparison.markdown_report_path is not None:
            print(f"PR markdown: {_display_path(comparison.markdown_report_path)}")
        if comparison.notes:
            print("Comparison notes:")
            for note in comparison.notes:
                print(f"- {note}")
        if comparison.chart_paths:
            print("Comparison charts:")
            for path in comparison.chart_paths:
                print(f"- {_display_path(path)}")


def print_diagnostics(
    results: list[BenchmarkRunResult],
    output_options: BenchmarkOutputOptions | None = None,
) -> None:
    output_options = output_options or BenchmarkOutputOptions()

    for result in results:
        diagnostics = result.diagnostics
        if diagnostics is None:
            continue

        print(f"\nDataset diagnostics: {result.dataset_key}")

        if output_options.show_successful_matches:
            _print_by_match_reason(
                result,
                diagnostics.successful_matches,
                title="Diagnostics: 5 random successful matches",
                section_header="---- SUCCESSFUL MATCHES ----",
            )

        if output_options.show_incorrect_matches:
            _print_by_match_reason(
                result,
                diagnostics.incorrect_matches,
                title="Diagnostics: 10 incorrect matches",
                section_header="---- INCORRECT MATCHES ----",
            )

        if output_options.show_similarity_score_checks:
            print("\nDiagnostics: similarity score checks")
            print("\nDiagnostics: 10 incorrect matches with lowest similarity")
            _show_via_sql(result, diagnostics.lowest_similarity_incorrect)
            print("\nDiagnostics: 10 incorrect matches with highest similarity")
            _show_via_sql(result, diagnostics.highest_similarity_incorrect)
            print("\nDiagnostics: suspicious incorrect-match summary")
            _show_via_sql(result, diagnostics.suspicious_incorrect_summary)

        if output_options.show_unmatched_records:
            print("\nDiagnostics: unmatched records with highest Splink comparison")
            if diagnostics.unmatched_top_splink is None:
                if diagnostics.splink_available:
                    print("No Splink candidate rows found for sampled unmatched records.")
                else:
                    print("Splink not active for this run.")
            else:
                _show_via_sql(result, diagnostics.unmatched_top_splink)
