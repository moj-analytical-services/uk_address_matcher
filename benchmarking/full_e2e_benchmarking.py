from pathlib import Path
from typing import Optional

from benchmarking.analysis import (
    print_stages_benchmark_header,
)
from benchmarking.analysis.accuracy import calculate_accuracy_metrics
from benchmarking.analysis.mismatches import analyse_mismatches, print_mismatch_analysis
from benchmarking.datasets import get_dataset_info, load_benchmark_data
from benchmarking.utils.io import setup_connection
from benchmarking.utils.pipelines import run_deterministic_pipeline
from benchmarking.utils.timing import format_timing_summary, time_phase
from uk_address_matcher import (
    best_matches_with_distinguishability,
    get_linker,
    improve_predictions_using_distinguishing_tokens,
)
from uk_address_matcher.linking_model.exact_matching.matching_stages import StageName
from uk_address_matcher.post_linkage.analyse_results import calculate_match_metrics
from uk_address_matcher.post_linkage.match_candidate_selection import (
    select_top_match_candidates,
)
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
SAMPLE_MODE = False

# Analysis configuration
MISMATCH_SAMPLES_PER_REASON = 10  # Random samples per match_reason
TOP_WORST_MISMATCHES = 10  # Worst mismatches by similarity

# ============================================================================
# Setup
# ============================================================================

print("Initialising benchmark environment...")
con = setup_connection()
df_messy_clean, df_os_clean = load_benchmark_data(
    con,
    DATASET_NAME,
    OS_DATA_PATH,
    include_term_frequencies=True,
    sample_mode=SAMPLE_MODE,
)
con.sql("DROP TABLE IF EXISTS df_messy")
df_messy_clean.to_table("df_messy_clean")
df_os_clean.show(max_width=100000)
# Get dataset info for reporting
dataset_info = get_dataset_info(DATASET_NAME)

variant_label = "exact_match"
variant_timings: dict[str, dict[str, float]] = {}

print_stages_benchmark_header(
    dataset_name=dataset_info.name,
    variant_name=variant_label,
)

with time_phase(variant_timings, variant_label, "pipeline"):
    deterministic_matches = run_deterministic_pipeline(
        con=con,
        # For benchmarking, we want to load the messy data in with the column `dataset_name`.
        # This needs to be removed for matching, but is joined back on later.
        df_to_match=df_messy_clean.select("* EXCLUDE (dataset_name)").execute(),
        df_canonical=df_os_clean,
        pipeline_name=f"Exact benchmark - {variant_label}",
        debug_options=DEBUG_OPTIONS,
        explain=EXPLAIN,
        enabled_stage_names=[StageName.UNIQUE_TRIGRAM],
    )

pipeline_duration = variant_timings[variant_label]["pipeline"]
print(f"⏱  Pipeline completed in {pipeline_duration:.2f} seconds.\n")

with time_phase(variant_timings, variant_label, "splink_linking"):
    linker = get_linker(
        df_addresses_to_match=deterministic_matches,
        df_addresses_to_search_within=df_os_clean,
        con=con,
        include_full_postcode_block=True,
        retain_intermediate_calculation_columns=True,
    )
    display(linker.visualisations.match_weights_chart())
    for i, br2 in enumerate(
        linker._settings_obj._blocking_rules_to_generate_predictions
    ):
        print(f"{i}: {br2}")
    df_predict = linker.inference.predict(threshold_match_weight=10)
    df_predict_ddb = df_predict.as_duckdbpyrelation()

splink_duration = variant_timings[variant_label]["splink_linking"]
print(f"⏱  Splink linking completed in {splink_duration:.2f} seconds.\n")

with time_phase(variant_timings, variant_label, "distinguishing_tokens"):
    df_predict_improved = improve_predictions_using_distinguishing_tokens(
        df_predict=df_predict_ddb,
        con=con,
        match_weight_threshold=-20,
    )

distinguishing_duration = variant_timings[variant_label]["distinguishing_tokens"]
print(
    f"⏱  Distinguishing token adjustment completed in {distinguishing_duration:.2f} seconds.\n"
)

with time_phase(variant_timings, variant_label, "candidate_selection"):
    best_matches = best_matches_with_distinguishability(
        df_predict=df_predict_improved,
        df_addresses_to_match=deterministic_matches,
        con=con,
    )
    match_candidates = select_top_match_candidates(
        con=con,
        df_exact_matches=deterministic_matches,
        df_splink_matches=best_matches,
        df_canonical=df_os_clean,
        match_weight_threshold=10,
        distinguishability_threshold=5,
        # include_unmatched=True,
        include_unmatched=False,
    )

candidate_duration = variant_timings[variant_label]["candidate_selection"]
print(f"⏱  Candidate selection completed in {candidate_duration:.2f} seconds.\n")

# Bring `dataset_name` back for analysis
match_candidates = match_candidates.join(
    df_messy_clean.select("ukam_address_id, dataset_name"),
    condition="ukam_address_id",
    how="left",
)
print("--- Match Reason Breakdown ---\n")
print(calculate_match_metrics(match_candidates))

print("\n--- Accuracy Metrics ---\n")
accuracy = calculate_accuracy_metrics(match_candidates)
accuracy.show(max_width=10000)

incorrect_count = (
    match_candidates.filter(
        "match_reason IS NOT NULL AND unique_id != resolved_canonical_id"
    )
    .count("*")
    .fetchone()[0]
)

# if incorrect_count > 0:
#     print(
#         f"\n📊 Found {incorrect_count:,} incorrect matches. Analysing mismatches...\n"
#     )
#     mismatch_results = analyse_mismatches(
#         ukam_matches=match_candidates,
#         ukam_canonical=df_os_clean,
#         ukam_messy=df_messy_clean,
#         samples_per_reason=MISMATCH_SAMPLES_PER_REASON,
#         top_worst=TOP_WORST_MISMATCHES,
#     )
#     print_mismatch_analysis(mismatch_results)
# else:
#     print("\n✓ No incorrect matches found!\n")

print("\nTiming summary:")
for line in format_timing_summary(variant_timings):
    print(line)

recs = df_predict_ddb.filter("match_key = 18").limit(5).df().to_dict(orient="records")
linker.visualisations.waterfall_chart(recs)

df_predict_ddb.filter("match_key = 18").limit(5).show(max_width=100000)

to_select = df_predict_ddb.limit(5).select("ukam_address_id_r", "ukam_address_id_l")

sql = """
select * from
df_predict_improved

inner join
to_select
on df_predict_improved.ukam_address_id_r = to_select.ukam_address_id_r
"""
con.sql(sql).show(max_width=100000)


sql = """
select *
from match_candidates
where ukam_address_id in (select ukam_address_id_r from to_select)
"""
con.sql(sql).show(max_width=100000)


sql = """
select count(*)
from match_candidates
where match_reason = 'splink: probabilistic match'
"""
con.sql(sql).show(max_width=100000)
