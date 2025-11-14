from pathlib import Path
from typing import Optional

import duckdb

from benchmarking.analysis import (
    analyse_mismatches,
    calculate_accuracy_metrics,
    print_stages_benchmark_header,
)
from benchmarking.analysis.mismatches import print_mismatch_analysis
from benchmarking.datasets import get_dataset_info, load_benchmark_data
from benchmarking.utils.io import setup_connection
from benchmarking.utils.pipelines import run_deterministic_pipeline
from benchmarking.utils.timing import time_phase
from uk_address_matcher.linking_model.exact_matching import StageName
from uk_address_matcher.post_linkage.analyse_results import calculate_match_metrics
from uk_address_matcher.sql_pipeline.runner import DebugOptions

# ============================================================================
# Configuration
# ============================================================================

DATASET_NAME = "lambeth_council"
OS_DATA_PATH: Path | None = None
# DEBUG_OPTIONS: Optional[DebugOptions] = DebugOptions(
#     pretty_print_sql=True, debug_incremental=True, debug_mode=True, debug_show_sql=True
# )
DEBUG_OPTIONS: Optional[DebugOptions] = None
EXPLAIN = False

# Analysis configuration
MISMATCH_SAMPLES_PER_REASON = 10  # Random samples per match_reason
TOP_WORST_MISMATCHES = 10  # Worst mismatches by similarity

# ============================================================================
# Setup
# ============================================================================

print("Initialising benchmark environment...")
con = setup_connection()
df_messy_clean, df_os_clean = load_benchmark_data(con, DATASET_NAME, OS_DATA_PATH)

# Get dataset info for reporting
dataset_info = get_dataset_info(DATASET_NAME)

# Define pipeline variants
# Each variant specifies which optional stages to enable (exact matching always runs)
pipeline_variants = {
    # "exact_match_only": {
    #     "enabled_stages": None,  # Only exact matching (always-on)
    # },
    "exact_match_then_trie": {
        "enabled_stages": [StageName.TRIE],  # Exact matching + trie
    },
}

matches_by_variant: dict[str, duckdb.DuckDBPyRelation] = {}
variant_timings: dict[str, dict[str, float]] = {}

for label, variant_spec in pipeline_variants.items():
    # Print clear benchmark header
    print_stages_benchmark_header(
        dataset_name=dataset_info.name,
        variant_name=label,
        enabled_stages=variant_spec["enabled_stages"],
    )

    with time_phase(variant_timings, label, "pipeline"):
        matches = run_deterministic_pipeline(
            con=con,
            df_to_match=df_messy_clean,
            df_canonical=df_os_clean,
            enabled_stage_names=variant_spec["enabled_stages"],
            pipeline_name=f"Exact benchmark - {label}",
            debug_options=DEBUG_OPTIONS,
            explain=EXPLAIN,
        )

    pipeline_duration = variant_timings[label]["pipeline"]
    print(f"⏱  Pipeline completed in {pipeline_duration:.2f} seconds.\n")

    # Check row counts
    input_count = df_messy_clean.count("*").fetchone()[0]
    output_count = matches.count("*").fetchone()[0]
    if input_count == output_count:
        print(f"✓ Row counts match: {input_count:,} rows\n")
    else:
        print(
            f"⚠  Row count mismatch: input={input_count:,}, output={output_count:,} "
            f"(difference: {output_count - input_count:+,d})\n"
        )

    matches_by_variant[label] = matches

    # Match reason breakdown
    print("--- Match Reason Breakdown ---\n")
    print(calculate_match_metrics(matches))

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
