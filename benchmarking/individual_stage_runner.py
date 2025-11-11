from pathlib import Path
from typing import Optional

import duckdb

from benchmarking.analysis import (
    analyse_mismatches,
    calculate_accuracy_metrics,
)
from benchmarking.analysis.mismatches import print_mismatch_analysis
from benchmarking.analysis.reporting import print_benchmark
from benchmarking.datasets import get_dataset_info, load_benchmark_data
from benchmarking.utils.io import setup_connection
from benchmarking.utils.timing import time_phase
from uk_address_matcher.linking_model.exact_matching.matching_stages import (
    StageName,
    _run_stage,
)
from uk_address_matcher.post_linkage.analyse_results import calculate_match_metrics
from uk_address_matcher.sql_pipeline.runner import DebugOptions

# ============================================================================
# Configuration
# ============================================================================

DATASET_NAME = "lambeth_council"
OS_DATA_PATH: Path | None = None
DEBUG_OPTIONS: Optional[DebugOptions] = None
# DEBUG_OPTIONS = DebugOptions(
#     pretty_print_sql=True, debug_mode=True, debug_show_sql=True, debug_incremental=True
# )
EXPLAIN = False
pipeline_variants = [StageName.UNIQUE_TRIGRAM]

# Analysis configuration
MISMATCH_SAMPLES_PER_REASON = 10  # Random samples per match_reason
TOP_WORST_MISMATCHES = 10  # Worst mismatches by similarity

# ============================================================================
# Setup
# ============================================================================

print("Initialising benchmark environment...")
con = setup_connection()
# TODO(ThomasHepworth): LRU cache isn't working...
df_messy_clean, df_os_clean = load_benchmark_data(con, DATASET_NAME, OS_DATA_PATH)


# Get dataset info for reporting
dataset_info = get_dataset_info(DATASET_NAME)

matches_by_variant: dict[str, duckdb.DuckDBPyRelation] = {}
variant_timings: dict[str, dict[str, float]] = {}

for pipeline_variant in pipeline_variants:
    # Print clear benchmark header
    print_benchmark(
        dataset_name=dataset_info.name,
        variant_name=pipeline_variant.value,
    )

    with time_phase(variant_timings, pipeline_variant.value, "pipeline"):
        matches = _run_stage(
            con,
            pipeline_variant,
            df_messy_clean,
            df_os_clean,
            debug_options=DEBUG_OPTIONS,
            explain=EXPLAIN,
        ).join(
            df_messy_clean.select(
                "ukam_address_id", "unique_id", "original_address_concat", "postcode"
            ),
            condition="ukam_address_id",
            how="left",
        )
        matches.show(max_width=200)

    pipeline_duration = variant_timings[pipeline_variant.value]["pipeline"]
    print(f"⏱  Pipeline completed in {pipeline_duration:.2f} seconds.\n")

    matches_by_variant[pipeline_variant.value] = matches

    # Match reason breakdown
    print("--- Match Reason Breakdown ---\n")
    calculate_match_metrics(matches)

    # Accuracy metrics
    print("\n--- Accuracy Metrics ---\n")
    accuracy = calculate_accuracy_metrics(matches)
    accuracy.show()

    # Mismatch analysis (only if there are incorrect matches)
    incorrect_count = (
        matches.filter(
            "match_reason IS NOT NULL AND unique_id != resolved_canonical_id"
        )
        .count("*")
        .fetchone()[0]
    )

    if incorrect_count > 0:
        print(
            f"\n📊 Found {incorrect_count:,} incorrect matches. Analysing mismatches...\n"
        )
        mismatch_results = analyse_mismatches(
            matches=matches,
            canonical=df_os_clean,
            samples_per_reason=MISMATCH_SAMPLES_PER_REASON,
            top_worst=TOP_WORST_MISMATCHES,
        )
        print_mismatch_analysis(mismatch_results)
    else:
        print("\n✓ No incorrect matches found!\n")
