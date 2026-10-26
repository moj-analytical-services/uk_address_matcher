from __future__ import annotations

from dataclasses import dataclass
from time import perf_counter
from typing import TYPE_CHECKING, Any

from benchmarking.config.datasets import (
    get_dataset_definition,
    list_dataset_keys,
    load_dataset,
)
from benchmarking.constants import BENCHMARK_RESULTS_ROOT
from benchmarking.insights.diagnostics import build_dataset_diagnostics
from benchmarking.insights.reporting import print_run_persistence_summary
from benchmarking.insights.run_persistence import (
    generate_benchmark_run_id,
    persist_benchmark_run,
)
from benchmarking.insights.summary import fetch_overall_summary
from benchmarking.insights.types import (
    BenchmarkOutputOptions,
    DatasetDiagnostics,
    PersistedBenchmarkRun,
)
from benchmarking.utils.io import setup_connection
from uk_address_matcher import AddressMatcher

if TYPE_CHECKING:
    import duckdb


@dataclass(frozen=True)
class BenchmarkRunResult:
    dataset_key: str
    dataset_label: str
    total_rows: int
    matched_rows: int
    correct_matches: int
    precision: float | None
    recall: float | None
    match_reason_breakdown: duckdb.DuckDBPyRelation
    timings: dict[str, float]
    con: duckdb.DuckDBPyConnection
    diagnostics: DatasetDiagnostics | None = None
    initialisation_diagnostics: list[dict[str, Any]] | None = None
    accuracy_table: duckdb.DuckDBPyRelation | None = None
    stage_diagnostics_table: duckdb.DuckDBPyRelation | None = None
    precision_recall_curve_records: list[dict[str, Any]] | None = None
    splink_predictions: duckdb.DuckDBPyRelation | None = None
    splink_available: bool = False
    persisted_run: PersistedBenchmarkRun | None = None


def resolve_dataset_selection(selection: str | list[str]) -> list[str]:
    available = list_dataset_keys()
    if selection == "all":
        return available

    selected = [selection] if isinstance(selection, str) else list(selection)
    unknown = [name for name in selected if name not in available]
    if unknown:
        raise ValueError(
            "Unknown dataset selection "
            f"{unknown}. Valid options: {', '.join(available)} or 'all'."
        )
    return selected


def print_available_datasets() -> None:
    print("Available datasets:")
    for key in list_dataset_keys():
        definition = get_dataset_definition(key)
        print(f"- {key}: {definition['label']} ({definition['file_name']})")

    print()


def run_single_dataset(
    con: duckdb.DuckDBPyConnection,
    dataset_key: str,
    canonical_path: str,
    stages: list,
    run_id: str | None = None,
    sample_mode: bool = False,
    canonical_address_filter: str | None = None,
    diagnostic_output_options: BenchmarkOutputOptions | None = None,
    cleaning_num_chunks: int = 10,
    persist_results: bool = False,
    results_root: str = BENCHMARK_RESULTS_ROOT,
    enable_comparison_charts: bool = True,
    comparison_baseline_run_id: str | None = None,
    matching_num_chunks: int = 1,
) -> BenchmarkRunResult:
    dataset = get_dataset_definition(dataset_key)
    timings: dict[str, float] = {}
    total_start = perf_counter()

    data_load_start = perf_counter()
    df_messy = load_dataset(con, dataset_key=dataset_key, sample_mode=sample_mode)
    timings["data_load"] = perf_counter() - data_load_start
    total_input_rows = int(df_messy.aggregate("COUNT(*) AS row_count").fetchone()[0])

    pipeline_start = perf_counter()
    matcher = AddressMatcher(
        canonical_addresses=canonical_path,
        addresses_to_match=df_messy,
        con=con,
        stages=stages,
        canonical_address_filter=canonical_address_filter,
        cleaning_num_chunks=cleaning_num_chunks,
        matching_num_chunks=matching_num_chunks,
    )
    match_result = matcher.match()
    matches = match_result.matches(all_columns=True)
    table_name = f"simple_bench_matches_{dataset_key}"
    con.sql(f"DROP TABLE IF EXISTS {table_name}")
    matches.to_table(table_name)
    match_result._relation = con.table(table_name)
    timings["match_pipeline"] = perf_counter() - pipeline_start

    initialisation_diagnostics = getattr(
        match_result,
        "_initialisation_diagnostics",
        None,
    )
    if initialisation_diagnostics:
        for row in initialisation_diagnostics:
            phase = str(row["phase"])
            timings[f"{phase}_seconds"] = float(row["elapsed_seconds"])
            for key in (
                "process_rss_before_mb",
                "process_rss_after_mb",
                "process_rss_delta_mb",
            ):
                value = row.get(key)
                if value is not None:
                    timings[f"{phase}_{key}"] = float(value)

    timings["total_runtime"] = perf_counter() - total_start
    accuracy_rel = match_result._accuracy_table()
    stage_diagnostics_rel = match_result._stage_diagnostics_table()
    precision_recall_curve_records = match_result.accuracy_data()
    by_reason_rel = accuracy_rel
    total_rows, matched_rows, correct_matches, precision, recall = fetch_overall_summary(
        con,
        accuracy_rel,
        total_input_rows=total_input_rows,
    )

    splink_predictions = None
    try:
        splink_predictions = match_result._splink_predictions()
    except ValueError:
        splink_predictions = None

    diagnostics: DatasetDiagnostics | None = None
    if (
        diagnostic_output_options is not None
        and diagnostic_output_options.enable_diagnostics()
    ):
        canonical_relation = getattr(match_result, "_canonical_relation", None)
        diagnostics_start = perf_counter()

        diagnostics = build_dataset_diagnostics(
            con,
            matches_table_name=table_name,
            messy_relation=df_messy,
            canonical_relation=canonical_relation,
            splink_predictions=splink_predictions,
            output_options=diagnostic_output_options,
        )
        timings["diagnostics"] = perf_counter() - diagnostics_start

    persisted_run: PersistedBenchmarkRun | None = None
    if persist_results:
        persisted_run = persist_benchmark_run(
            run_id=run_id,
            dataset_key=dataset_key,
            dataset_label=dataset["label"],
            stages=stages,
            accuracy_table=accuracy_rel,
            stage_diagnostics_table=stage_diagnostics_rel,
            timings=timings,
            total_rows=total_rows,
            matched_rows=matched_rows,
            correct_matches=correct_matches,
            precision=precision,
            recall=recall,
            precision_recall_curve_rows=precision_recall_curve_records,
            comparison_baseline_run_id=comparison_baseline_run_id,
            results_root=results_root,
            enable_chart_exports=enable_comparison_charts,
        )

    return BenchmarkRunResult(
        dataset_key=dataset_key,
        dataset_label=dataset["label"],
        total_rows=total_rows,
        matched_rows=matched_rows,
        correct_matches=correct_matches,
        precision=precision,
        recall=recall,
        match_reason_breakdown=by_reason_rel,
        accuracy_table=accuracy_rel,
        stage_diagnostics_table=stage_diagnostics_rel,
        timings=timings,
        con=con,
        diagnostics=diagnostics,
        initialisation_diagnostics=initialisation_diagnostics,
        precision_recall_curve_records=precision_recall_curve_records,
        splink_predictions=splink_predictions,
        splink_available=splink_predictions is not None,
        persisted_run=persisted_run,
    )


def run_selected_datasets(
    selected_datasets: str | list[str],
    canonical_path: str,
    stages: list,
    run_id: str | None = None,
    sample_mode: bool = False,
    canonical_address_filter: str | None = None,
    diagnostic_output_options: BenchmarkOutputOptions | None = None,
    cleaning_num_chunks: int = 10,
    persist_results: bool = False,
    results_root: str = BENCHMARK_RESULTS_ROOT,
    enable_comparison_charts: bool = True,
    comparison_baseline_run_id: str | None = None,
    matching_num_chunks: int = 1,
) -> list[BenchmarkRunResult]:
    selected = resolve_dataset_selection(selected_datasets)
    effective_run_id = run_id.strip() if isinstance(run_id, str) else run_id
    if not effective_run_id:
        effective_run_id = generate_benchmark_run_id()

    results: list[BenchmarkRunResult] = []
    for dataset_key in selected:
        con = setup_connection()
        print(f"\nRunning benchmark for dataset: {dataset_key}")
        result = run_single_dataset(
            con=con,
            dataset_key=dataset_key,
            canonical_path=canonical_path,
            stages=stages,
            run_id=effective_run_id,
            sample_mode=sample_mode,
            canonical_address_filter=canonical_address_filter,
            diagnostic_output_options=diagnostic_output_options,
            cleaning_num_chunks=cleaning_num_chunks,
            matching_num_chunks=matching_num_chunks,
            persist_results=persist_results,
            results_root=results_root,
            enable_comparison_charts=enable_comparison_charts,
            comparison_baseline_run_id=comparison_baseline_run_id,
        )
        print(
            f"Completed dataset '{dataset_key}' in {result.timings['total_runtime']:.2f}s"
        )
        results.append(result)

    if persist_results:
        print_run_persistence_summary(results)

    return results
