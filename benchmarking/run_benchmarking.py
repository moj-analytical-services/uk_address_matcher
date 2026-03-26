from __future__ import annotations

import logging

from benchmarking.config.datasets import (
    get_dataset_definition,
    list_dataset_keys,
)
from benchmarking.insights.reporting import (
    print_benchmark_summary,
)
from benchmarking.insights.types import BenchmarkOutputOptions
from benchmarking.runner import run_selected_datasets
from benchmarking.settings import (
    CANONICAL_FILTER_SQL,
    CANONICAL_PATH,
    SAMPLE_MODE,
)
from uk_address_matcher import (
    ExactMatchStage,
    PeeledAddressStage,
    SplinkStage,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# Optional Splink-only comparison table shown after benchmark summary.
# Set to `None` to disable threshold-comparison output.
SPLINK_BASELINE_WEIGHT: float | None = 10.0
SPLINK_COMPARISON_WEIGHTS: list[float] | None = [6.0, 8.0, 12.0]
TOP_K_PRECISION_AT_METRICS: list[int] = [1, 3, 5]

# SELECTED_DATASETS: str | list[str] = "all"
SELECTED_DATASETS: str | list[str] = "hackney"
STAGES = [
    ExactMatchStage(),
    PeeledAddressStage(),
    SplinkStage(final_match_weight_threshold=SPLINK_BASELINE_WEIGHT),
]
APPLY_CANONICAL_FILTER = True
CLEANING_NUM_CHUNKS = 1


# Defaults: always print summary sections (timings, accuracy, diagnostics),
# with selected diagnostics enabled and successful/unmatched diagnostics opt-in.
# OUTPUT_OPTIONS = BenchmarkOutputOptions()
OUTPUT_OPTIONS = BenchmarkOutputOptions(
    show_incorrect_matches=False,
    show_similarity_score_checks=False,
    show_successful_matches=False,
    show_unmatched_records=False,
)

print(f"Applying canonical filter: {APPLY_CANONICAL_FILTER}")


def print_available_datasets() -> None:
    print("Available datasets:")
    for key in list_dataset_keys():
        definition = get_dataset_definition(key)
        print(f"- {key}: {definition['label']} ({definition['s3_key']})")

    print()


print_available_datasets()

results = run_selected_datasets(
    selected_datasets=SELECTED_DATASETS,
    canonical_path=CANONICAL_PATH,
    stages=STAGES,
    sample_mode=SAMPLE_MODE,
    canonical_address_filter=(CANONICAL_FILTER_SQL if APPLY_CANONICAL_FILTER else None),
    enable_diagnostics=OUTPUT_OPTIONS.enable_diagnostics(),
    cleaning_num_chunks=CLEANING_NUM_CHUNKS,
)
print_benchmark_summary(
    results,
    splink_baseline_weight=SPLINK_BASELINE_WEIGHT,
    splink_comparison_weights=SPLINK_COMPARISON_WEIGHTS,
    top_k_precision_at_metrics=TOP_K_PRECISION_AT_METRICS,
    output_options=OUTPUT_OPTIONS,
)
