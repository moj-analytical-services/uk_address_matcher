import os
import time

import duckdb
import pandas as pd

from uk_address_matcher import (
    ExactMatchStage,
    SplinkStage,
    TrigramStage,
    calculate_match_metrics,
    prepare_data_for_matching,
    run_matching,
)

pd.options.display.max_colwidth = 1000

# -----------------------------------------------------------------------------
# Step 1: Load in some example data
# -----------------------------------------------------------------------------

# If you're using your own data you need the following columns:
# +-------------------+----------------------+----------------------------------------+
# |      Column       | DuckDB dtype         |               Description               |
# +-------------------+----------------------+----------------------------------------+
# | unique_id         | BIGINT or VARCHAR    | Unique identifier for each record       |
# | source_dataset    | VARCHAR              | Source dataset label, e.g. 'epc'        |
# | address_concat    | VARCHAR              | Full address (without postcode)         |
# | postcode          | VARCHAR              | Postcode                                |
# +-------------------+----------------------+----------------------------------------+


# Any additional columns should be retained as-is by the cleaning code

p_ch = "./example_data/companies_house_addresess_postcode_overlap.parquet"
p_fhrs = "./example_data/fhrs_addresses_sample.parquet"

con = duckdb.connect(database=":memory:")

con.execute("INSTALL splink_udfs FROM community; LOAD splink_udfs;")

# Read our example data in and ensure unique_id is the correct data type
df_ch = con.read_parquet(p_ch)
df_fhrs = con.read_parquet(p_fhrs)

# Apply limit if TEST_LIMIT environment variable is set
if os.getenv("TEST_LIMIT"):
    df_ch = df_ch.limit(250)
    df_fhrs = df_fhrs.limit(250)

# -----------------------------------------------------------------------------
# Step 2: Clean the data/feature engineering to prepare for matching model
# -----------------------------------------------------------------------------
df_ch_clean = prepare_data_for_matching(df_ch, con=con)
df_fhrs_clean = prepare_data_for_matching(df_fhrs, con=con)

# -----------------------------------------------------------------------------
# Step 3: Run unified matching pipeline
# -----------------------------------------------------------------------------

start_time = time.time()
match_candidates = run_matching(
    con=con,
    df_messy_clean=df_fhrs_clean,
    df_canonical_clean=df_ch_clean,
    stages=[
        ExactMatchStage(),
        TrigramStage(),
        SplinkStage(
            predict_threshold_match_weight=-50,
            improve_threshold_match_weight=-20,
            final_match_weight_threshold=15,
            final_distinguishability_threshold=None,
            include_full_postcode_block=True,
        ),
    ],
)
end_time = time.time()
print(f"Time taken: {end_time - start_time} seconds")

print("\nCombined match candidates summary:")
calculate_match_metrics(match_candidates).show(max_width=500, max_rows=20)

# -----------------------------------------------------------------------------
# Step 4: Inspect top records for each match reason
# -----------------------------------------------------------------------------

match_reasons = [
    row[0] for row in match_candidates.project("match_reason").distinct().fetchall()
]

for match_reason_value in match_reasons:
    if match_reason_value is None:
        continue

    match_reason_sql_value = str(match_reason_value).replace("'", "''")
    print(f"\n=== Show 10 records in match_reason '{match_reason_value}' ===")
    match_candidates.filter(f"match_reason = '{match_reason_sql_value}'").limit(
        10
    ).show(max_width=500, max_rows=10)
