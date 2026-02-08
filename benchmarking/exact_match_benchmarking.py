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
from uk_address_matcher.linking_model.matching import StageName
from uk_address_matcher.post_linkage.analyse_results import calculate_match_metrics
from uk_address_matcher.sql_pipeline.runner import DebugOptions

# ============================================================================
# Configuration
# ============================================================================

# DATASET_NAME = "lambeth_council"
DATASET_NAME = "hackney_council"
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
    # "exact_match_then_trigram": {
    #     "enabled_stages": [StageName.UNIQUE_TRIGRAM],  # Exact matching + trigram
    # },
    "exact_match_with_all_stages": {
        "enabled_stages": [
            # StageName.PEELED_ADDRESS,
            # StageName.UNIQUE_TRIGRAM,
            # StageName.JARO_WINKLER,
            # StageName.DAMERAU_LEVENSHTEIN,
        ],
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
    accuracy = calculate_accuracy_metrics(matches, df_os_clean)
    accuracy.show()

    # Mismatch analysis (only if there are incorrect matches)
    incorrect_count = (
        matches.filter(
            "match_reason IS NOT NULL AND ukam_label != resolved_canonical_id"
        )
        .count("*")
        .fetchone()[0]
    )

    if incorrect_count > 0:
        print(
            f"\n📊 Found {incorrect_count:,} incorrect matches. Analysing mismatches...\n"
        )
        mismatch_results = analyse_mismatches(
            ukam_matches=matches,
            ukam_canonical=df_os_clean,
            ukam_messy=df_messy_clean,
            samples_per_reason=MISMATCH_SAMPLES_PER_REASON,
            top_worst=TOP_WORST_MISMATCHES,
        )
        print_mismatch_analysis(mismatch_results)
    else:
        print("\n✓ No incorrect matches found!\n")

# Some more things to check for when we are scanning:
# - BLOCK > indicates a flat block
# BLOCK B STANNARD HALL CORMONT ROAD LONDON                       │ STANNARD HALL CORMONT ROAD LONDON
# - SHOP > indicates a shop within a block > we probably want the root version of this...
# Interestingly, it definitely exists in the data, not sure where it's gone...
# 268 KNIGHTS HILL LONDON                                         │ SHOP 268 KNIGHTS HILL LONDON > this one, looks like there are two entries in OS
# - FLAT > indicates a flat within a building > we probably want the root version of this...
# FLAT 44 ALPHA HOUSE 4 BETA PLACE LONDON                         │ 44 ALPHA HOUSE 4 BETA PLACE LONDON

# Other flat issues...
# GROUND FIRST SECOND AND THIRD FLOORS 352 KENNINGTON ROAD LONDON │ 352 KENNINGTON ROAD LONDON - 10001112167
# This is failing as we have FLOORS (plural) - if we get this, we need to try and parse our keywords...

# Need to remove ` tokens... FLAT 2 84 KING`S AVENUE LONDON is wrong and could be exact matched
# KIOSK (or variants) should be labelled as a kisok... Depot?
# PUBLIC PAVEMENT
# Delivery <> -> that should be labelled as delivery point only
# MOBILE UNIT
# BOTTOM FLAT 25 ST MARTINS ROAD LONDON │ TOP FLAT 25 ST MARTINS ROAD LONDON

# IDs to spot check:
# 100021880251 -> 17 BUTTERWORTH COURT PENDENNIS ROAD LONDON │ TOP 17 PENDENNIS ROAD LONDON
# 100021861861 -> MIDDLE 135 LANDOR ROAD LONDON │ BUSINESS 135LANDOR ROAD LONDON
# 10090196829 -> NEWHAVEN 23 THURLOW PARK ROAD LONDON │ LYNDHURST 23 THURLOW PARK ROAD LONDON
