from __future__ import annotations

import logging

from benchmarking.constants import (
    APPLY_CANONICAL_FILTER,
    BENCHMARK_RESULTS_ROOT,
    CANONICAL_FILTER_SQL,
    CLEANING_NUM_CHUNKS,
    SPLINK_BASELINE_WEIGHT,
    SPLINK_COMPARISON_WEIGHTS,
    TOP_K_PRECISION_AT_METRICS,
)
from benchmarking.insights.reporting import (
    print_benchmark_summary,
)
from benchmarking.insights.types import BenchmarkOutputOptions
from benchmarking.runner import print_available_datasets, run_selected_datasets
from benchmarking.settings import (
    CANONICAL_PATH,
    SAMPLE_MODE,
)
from uk_address_matcher import ExactMatchStage, PeeledAddressStage, SplinkStage

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# SELECTED_DATASETS: str | list[str] = "all"
SELECTED_DATASETS: str | list[str] = "pooled_councils"
# SELECTED_DATASETS: str | list[str] = "hackney"
# SELECTED_DATASETS: str | list[str] = "aberdeenshire"
# SELECTED_DATASETS: str | list[str] = "lambeth_llpg"
STAGES = [
    ExactMatchStage(True),
    PeeledAddressStage(),
    SplinkStage(final_match_weight_threshold=SPLINK_BASELINE_WEIGHT),
]
# Set to a persisted run hash to force a specific baseline, or use "latest"
# to compare against the latest other persisted run for the same dataset.
# Run IDs are preferred; internal dedupe hashes are still accepted for older runs.
COMPARISON_BASELINE_RUN_ID: str | None = "192e02e409b7f100"


# Defaults: print benchmark summaries and persistence output.
# Diagnostics stay opt-in via the separate reporting helper.
# OUTPUT_OPTIONS = BenchmarkOutputOptions()
OUTPUT_OPTIONS = BenchmarkOutputOptions(
    show_incorrect_matches=False,
    show_similarity_score_checks=False,
    show_successful_matches=False,
    show_unmatched_records=False,
    show_splink_comparisons=False,
)

print(f"Applying canonical filter: {APPLY_CANONICAL_FILTER}")


print_available_datasets()

results = run_selected_datasets(
    selected_datasets=SELECTED_DATASETS,
    canonical_path=CANONICAL_PATH,
    stages=STAGES,
    sample_mode=SAMPLE_MODE,
    canonical_address_filter=(CANONICAL_FILTER_SQL if APPLY_CANONICAL_FILTER else None),
    diagnostic_output_options=OUTPUT_OPTIONS,
    cleaning_num_chunks=CLEANING_NUM_CHUNKS,
    persist_results=True,
    results_root=BENCHMARK_RESULTS_ROOT,
    enable_comparison_charts=True,
    comparison_baseline_run_id=COMPARISON_BASELINE_RUN_ID,
)
print_benchmark_summary(
    results,
    splink_baseline_weight=SPLINK_BASELINE_WEIGHT,
    splink_comparison_weights=SPLINK_COMPARISON_WEIGHTS,
    top_k_precision_at_metrics=TOP_K_PRECISION_AT_METRICS,
    output_options=OUTPUT_OPTIONS,
)
