import os
import time

import duckdb
import pandas as pd
from IPython.display import display

from uk_address_matcher import (
    best_matches_summary,
    best_matches_with_distinguishability,
    calculate_match_metrics,
    clean_data_using_precomputed_rel_tok_freq,
    get_linker,
    improve_predictions_using_distinguishing_tokens,
    run_deterministic_match_pass,
)
from uk_address_matcher.sql_pipeline.runner import RunOptions

pd.options.display.max_colwidth = 1000

# -----------------------------------------------------------------------------
# Step 1: Load in some example data
# -----------------------------------------------------------------------------

# If you're using your own data you need the following columns:
#
# +-------------------+--------------------------------------------------------+
# |      Column       |                     Description                        |
# +-------------------+--------------------------------------------------------+
# | unique_id         | Unique identifier for each record                      |
# | source_dataset    | Populated with a constant string identifying the       |
# |                   | dataset, e.g. 'epc'                                    |
# | address_concat    | Full address concatenated as without  postcode         |
# | postcode          | Postcode                                               |
# +-------------------+--------------------------------------------------------+


# Any additional columns should be retained as-is by the cleaning code

p_ch = "./example_data/companies_house_addresess_postcode_overlap.parquet"
p_fhrs = "./example_data/fhrs_addresses_sample.parquet"

con = duckdb.connect(database=":memory:")

con.execute("INSTALL splink_udfs FROM community; LOAD splink_udfs;")

df_ch = con.read_parquet(p_ch).order("postcode")
df_fhrs = con.read_parquet(p_fhrs).order("postcode")

# Apply limit if TEST_LIMIT environment variable is set
if os.getenv("TEST_LIMIT"):
    df_ch = df_ch.limit(50)
    df_fhrs = df_fhrs.limit(50)

# -----------------------------------------------------------------------------
# Step 2: Clean the data/feature engineering to prepare for matching model
# -----------------------------------------------------------------------------
run_options = RunOptions(
    pretty_print_sql=True, debug_mode=True, debug_show_sql=True, debug_incremental=True
)

df_ch_clean = clean_data_using_precomputed_rel_tok_freq(df_ch, con=con)
df_fhrs_clean = clean_data_using_precomputed_rel_tok_freq(df_fhrs, con=con)

# -----------------------------------------------------------------------------
# Step 2.5: Run exact matching to reduce the number of records to consider
# -----------------------------------------------------------------------------

exact_match_results = run_deterministic_match_pass(
    con=con,
    df_addresses_to_match=df_fhrs_clean,
    df_addresses_to_search_within=df_ch_clean,
)

exact_match_summary = calculate_match_metrics(exact_match_results)
print("\nExact match results summary:")
print(exact_match_summary)

# -----------------------------------------------------------------------------
# Step 3: First pass - Link the data using Splink
# -----------------------------------------------------------------------------

linker = get_linker(
    df_addresses_to_match=df_fhrs_clean,
    df_addresses_to_search_within=df_ch_clean,
    con=con,
    include_full_postcode_block=True,
    retain_intermediate_calculation_columns=True,
)

df_predict = linker.inference.predict(threshold_match_weight=-50)
df_predict_ddb = df_predict.as_duckdbpyrelation()

# -----------------------------------------------------------------------------
# Step 4: Second pass - Improve predictions using distinguishing tokens
# -----------------------------------------------------------------------------

start_time = time.time()
df_predict_improved = improve_predictions_using_distinguishing_tokens(
    df_predict=df_predict_ddb,
    con=con,
    match_weight_threshold=-20,
)

df_predict_improved.show(max_width=500, max_rows=20)

end_time = time.time()
print(f"Time taken: {end_time - start_time} seconds")

# -----------------------------------------------------------------------------
# Step 5: Compare results before and after the second pass
# -----------------------------------------------------------------------------

print("\nResults before second pass:")
dsum_1 = best_matches_summary(
    df_predict=df_predict_ddb, df_addresses_to_match=df_fhrs, con=con
)
dsum_1.show(max_width=500, max_rows=20)

print("\nResults after second pass:")
dsum_2 = best_matches_summary(
    df_predict=df_predict_improved, df_addresses_to_match=df_fhrs, con=con
)
dsum_2.show(max_width=500, max_rows=20)


# -----------------------------------------------------------------------------
# Step 6: Inspect the result
# -----------------------------------------------------------------------------

# Show matches with a weight of >5 and distinguishability of >5
best_matches = best_matches_with_distinguishability(
    df_predict=df_predict_improved,
    df_addresses_to_match=df_fhrs,
    con=con,
)

best_matches_distinguishability_sql = """

select *
from best_matches
where match_weight > 5 and distinguishability > 5
ORDER BY random()
LIMIT 20
"""

print("\nRandom 20 matches for FHRS addresses:")
con.sql(best_matches_distinguishability_sql).show(max_width=500)


# Then take a random example and show the record comparison and waterfall
random_match_sql = """
SELECT
    unique_id_r as fhrs_id,
    unique_id_l as ch_id,
    match_weight
FROM df_predict_improved
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id_r ORDER BY match_weight DESC) = 1
ORDER BY RANDOM()
LIMIT 1
"""

# Get a random matched pair
random_match = con.sql(random_match_sql).fetchall()[0]
fhrs_id, ch_id, match_weight = random_match

# Show detailed comparison of the random match
print(
    f"\nDetailed comparison for randomly selected match (weight: {match_weight:.2f}):"
)

comparison_sql = f"""
SELECT
    'FHRS' as source,
    unique_id,
    original_address_concat,
    postcode,
    flat_positional,
    flat_letter,
    numeric_token_1,
    numeric_token_2,
    numeric_token_3
FROM df_fhrs_clean
WHERE unique_id = '{fhrs_id}'

UNION ALL

SELECT
    'Companies House' as source,
    unique_id,
    original_address_concat,
    postcode,
    flat_positional,
    flat_letter,
    numeric_token_1,
    numeric_token_2,
    numeric_token_3
FROM df_ch_clean
WHERE unique_id = '{ch_id}'
"""

con.sql(comparison_sql).show(max_width=500)

# Show top matches for this FHRS ID
top_matches_sql = f"""
SELECT
    concat_ws(' ', original_address_concat_r, postcode_r) as fhrs_address,
    concat_ws(' ', original_address_concat_l, postcode_l) as ch_address,
    match_weight,
    overlapping_tokens_this_l_and_r,
    tokens_elsewhere_in_block_but_not_this,
    overlapping_bigrams_this_l_and_r,
    bigrams_elsewhere_in_block_but_not_this,



FROM df_predict_improved
WHERE unique_id_r = '{fhrs_id}'
ORDER BY match_weight DESC
LIMIT 5
"""

print(f"\nTop 5 potential matches for FHRS ID {fhrs_id}:")
con.sql(top_matches_sql).show(max_width=5000)

# Get waterfall chart data for the matched pair
waterfall_sql = f"""
SELECT *
FROM df_predict_ddb
WHERE unique_id_r = '{fhrs_id}' AND unique_id_l = '{ch_id}'
"""

waterfall_data = con.sql(waterfall_sql)

# Display waterfall chart
print("\nWaterfall chart showing match weight components:")
display(
    linker.visualisations.waterfall_chart(
        waterfall_data.df().to_dict(orient="records"), filter_nulls=False
    )
)
