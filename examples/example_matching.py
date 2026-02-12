import os

import duckdb

from uk_address_matcher import (
    AddressMatcher,
    ExactMatchStage,
    SplinkStage,
)

# -----------------------------------------------------------------------------
# Input data requirements
# -----------------------------------------------------------------------------
# Your input tables can be DuckDB relations, Pandas DataFrames, or anything
# DuckDB can query. The matcher expects the following columns to be present.
#
# Required columns
# | Column         | DuckDB dtype         | What it's used for                              |
# |----------------|----------------------|--------------------------------------------------|
# | unique_id      | BIGINT or VARCHAR     | Stable identifier for each row (must be unique) |
# | address_concat | VARCHAR               | Address text *excluding postcode* (recommended) |
#
# Optional columns
# | Column         | DuckDB dtype         | What it's used for                              |
# |----------------|----------------------|--------------------------------------------------|
# | postcode       | VARCHAR               | Improves speed + accuracy (recommended)         |
#
# Postcode handling rules:
#
# 1. If you provide a separate `postcode` column:
#    - `address_concat` should ideally NOT contain the postcode.
#    - The matcher will use the structured postcode for blocking and matching.
#
# 2. If you do NOT provide a `postcode` column:
#    - `address_concat` may include the postcode.
#    - The matcher will attempt to extract it during cleaning.
# -----------------------------------------------------------------------------

# Example input files (canonical addresses vs. addresses to match)
p_ch = "./example_data/companies_house_addresess_postcode_overlap.parquet"
p_fhrs = "./example_data/fhrs_addresses_sample.parquet"

# DuckDB connection used for all processing (in-memory for convenience)
con = duckdb.connect(database=":memory:")

# Load inputs
df_ch = con.read_parquet(p_ch)
df_fhrs = con.read_parquet(p_fhrs)

# Optional: limit rows for quick local testing (set TEST_LIMIT to any value)
if os.getenv("TEST_LIMIT"):
    df_ch = df_ch.limit(250)
    df_fhrs = df_fhrs.limit(250)

# -----------------------------------------------------------------------------
# Configure and run the matcher
# -----------------------------------------------------------------------------
# Stages run in order; earlier stages typically find "easy" matches cheaply.
# - ExactMatchStage(): deterministic rules / exact matches
# - SplinkStage(): probabilistic matching for fuzzier cases (typos, formatting)
matcher = AddressMatcher(
    canonical_addresses=df_ch,
    addresses_to_match=df_fhrs,
    con=con,
    stages=[
        ExactMatchStage(),
        SplinkStage(
            # Lower = more permissive; higher = more conservative.
            # Tune these based on your desired precision/recall trade-off.
            predict_threshold_match_weight=-20,
            final_match_weight_threshold=12,
            # When True, uses full postcode as a blocking key to reduce the
            # candidate search space (faster, usually higher precision).
            include_full_postcode_block=True,
        ),
    ],
)

result = matcher.match()

# -----------------------------------------------------------------------------
# Preview results
# -----------------------------------------------------------------------------
print("=== First 10 matched records ===")
result.limit(10).show(max_width=500)

# -----------------------------------------------------------------------------
# Explore results by match reason
# -----------------------------------------------------------------------------
# `match_reason` indicates which stage/rule produced the match, which is useful
# for QA and for tuning thresholds.
match_reasons = [row[0] for row in result.project("match_reason").distinct().fetchall()]

for reason in match_reasons:
    if reason is None:
        continue

    # Escape single quotes so we can safely filter in SQL
    escaped = str(reason).replace("'", "''")

    print(f"\n=== 10 records matched by '{reason}' ===")
    result.filter(f"match_reason = '{escaped}'").limit(10).show(
        max_width=500, max_rows=10
    )
