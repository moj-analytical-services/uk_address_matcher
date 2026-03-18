import duckdb

from uk_address_matcher import (
    AddressMatcher,
    ExactMatchStage,
    PeeledAddressStage,
    SplinkStage,
    ukam_datasets,
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
messy, canonical = ukam_datasets.fictional_london
print("=== Sample input data ===")
print("Messy addresses:")
messy.show(max_rows=1, max_width=500)
print("\nCanonical addresses:")
canonical.show(max_rows=5, max_width=500)

# DuckDB connection used for all processing (in-memory for convenience)
con = duckdb.connect(database=":memory:")

# -----------------------------------------------------------------------------
# Configure and run the matcher
# -----------------------------------------------------------------------------
# Find available stages:
print(AddressMatcher.available_stages())

# Stages run in order; earlier stages typically find "easy" matches cheaply.
# - ExactMatchStage(): deterministic rules / exact matches
# - SplinkStage(): probabilistic matching for fuzzier cases (typos, formatting)
matcher = AddressMatcher(
    canonical_addresses=canonical,
    addresses_to_match=messy,
    con=con,
    stages=[
        ExactMatchStage(),
        PeeledAddressStage(),
        SplinkStage(
            predict_threshold_match_weight=-20,
            final_match_weight_threshold=12,
            include_full_postcode_block=True,
            retain_intermediate_calculation_columns=True,
        ),
    ],
)

match_result = matcher.match()

# The underlying DuckDB relation is always available via .matches(),
# which includes all matched records and metadata about the match process
result = match_result.matches()

# -----------------------------------------------------------------------------
# Preview results
# -----------------------------------------------------------------------------
print("=== First 10 matched records ===")
result.limit(10).show(max_width=500)

# -----------------------------------------------------------------------------
# Match-reason breakdown
# -----------------------------------------------------------------------------
# match_metrics() groups rows by match_reason and shows counts + percentages.
print("\n=== Match metrics ===")
match_result.match_metrics().show()

# -----------------------------------------------------------------------------
# Splink inspection helpers (available when a Splink stage runs)
# -----------------------------------------------------------------------------
print("\n=== Splink predictions sample ===")
splink_results = match_result._splink_predictions(limit=5)
splink_results.show(max_width=500)
