from __future__ import annotations

from dataclasses import dataclass
from time import perf_counter
from typing import TYPE_CHECKING

from benchmarking.config.datasets import (
    get_dataset_definition,
    list_dataset_keys,
    load_dataset,
)
from benchmarking.insights.diagnostics import build_dataset_diagnostics
from benchmarking.insights.metrics import (
    summarise_by_match_reason,
    summarise_precision_recall,
    summarise_run_totals,
)
from benchmarking.insights.types import DatasetDiagnostics
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
    run_totals: duckdb.DuckDBPyRelation
    timings: dict[str, float]
    con: duckdb.DuckDBPyConnection
    diagnostics: DatasetDiagnostics | None = None


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


def run_single_dataset(
    con: duckdb.DuckDBPyConnection,
    dataset_key: str,
    canonical_path: str,
    stages: list,
    sample_mode: bool = False,
    canonical_address_filter: str | None = None,
    enable_diagnostics: bool = False,
) -> BenchmarkRunResult:
    dataset = get_dataset_definition(dataset_key)
    timings: dict[str, float] = {}
    total_start = perf_counter()

    data_load_start = perf_counter()
    df_messy = load_dataset(con, dataset_key=dataset_key, sample_mode=sample_mode)
    timings["data_load"] = perf_counter() - data_load_start

    pipeline_start = perf_counter()
    matcher = AddressMatcher(
        canonical_addresses=canonical_path,
        addresses_to_match=df_messy,
        con=con,
        stages=stages,
        canonical_address_filter=canonical_address_filter,
    )
    match_result = matcher.match()
    matcher_metrics = match_result.match_metrics(order="descending")
    matches = match_result.matches(all_columns=True)
    table_name = f"simple_bench_matches_{dataset_key}"
    con.sql(f"DROP TABLE IF EXISTS {table_name}")
    matches.to_table(table_name)
    timings["match_pipeline"] = perf_counter() - pipeline_start

    summary = summarise_precision_recall(con, table_name).fetchone()
    timings["total_runtime"] = perf_counter() - total_start
    by_reason_rel = summarise_by_match_reason(
        con,
        table_name,
        matcher_metrics,
    )
    run_totals_rel = summarise_run_totals(
        con,
        table_name,
        timings["total_runtime"],
    )

    diagnostics: DatasetDiagnostics | None = None
    if enable_diagnostics:
        canonical_relation = getattr(match_result, "_canonical_relation", None)
        splink_predictions = None
        try:
            splink_predictions = match_result._splink_predictions()
        except ValueError:
            splink_predictions = None

        diagnostics = build_dataset_diagnostics(
            con,
            matches_table_name=table_name,
            messy_relation=df_messy,
            canonical_relation=canonical_relation,
            splink_predictions=splink_predictions,
        )

    return BenchmarkRunResult(
        dataset_key=dataset_key,
        dataset_label=dataset["label"],
        total_rows=int(summary[0]),
        matched_rows=int(summary[1]),
        correct_matches=int(summary[2]),
        precision=float(summary[3]) if summary[3] is not None else None,
        recall=float(summary[4]) if summary[4] is not None else None,
        match_reason_breakdown=by_reason_rel,
        run_totals=run_totals_rel,
        timings=timings,
        con=con,
        diagnostics=diagnostics,
    )


def run_selected_datasets(
    selected_datasets: str | list[str],
    canonical_path: str,
    stages: list,
    sample_mode: bool = False,
    canonical_address_filter: str | None = None,
    enable_diagnostics: bool = False,
) -> list[BenchmarkRunResult]:
    selected = resolve_dataset_selection(selected_datasets)
    con = setup_connection()

    results: list[BenchmarkRunResult] = []
    for dataset_key in selected:
        print(f"\nRunning benchmark for dataset: {dataset_key}")
        result = run_single_dataset(
            con=con,
            dataset_key=dataset_key,
            canonical_path=canonical_path,
            stages=stages,
            sample_mode=sample_mode,
            canonical_address_filter=canonical_address_filter,
            enable_diagnostics=enable_diagnostics,
        )
        results.append(result)

    return results
