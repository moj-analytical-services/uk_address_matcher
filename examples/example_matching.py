import duckdb

from uk_address_matcher import (
    AddressMatcher,
    ExactMatchStage,
    PeeledAddressStage,
    SplinkStage,
)
from uk_address_matcher.datasets import ukam_datasets

# -----------------------------------------------------------------------------
# Input data requirements
# -----------------------------------------------------------------------------
# Canonical input is a DuckDB relation or a prepared-folder path. Messy input
# can also be a list of dictionaries or AddressRecord objects.
#
# Required columns
# | Column         | DuckDB dtype         | What it's used for                              |
# |----------------|----------------------|--------------------------------------------------|
# | address_id     | BIGINT or VARCHAR     | Entity ID; unique_id is a supported alias       |
# | address_concat | VARCHAR               | Address text, optionally including postcode     |
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

with matcher.match() as match_result:
    result = match_result.matches()
    print("=== First 10 matched records ===")
    result.limit(10).show(max_width=500)

    print("\n=== Successful matches ===")
    match_result.matches(include_unmatched=False).limit(10).show(max_width=500)

    print("\n=== Matches enriched with input data ===")
    enriched_result = match_result.join_input_columns(
        canonical=[
            "clean_full_address",
            {"input_column": "postcode", "alias": "canonical_input_postcode"},
        ],
        messy=[{"input_column": "postcode", "alias": "messy_input_postcode"}],
    )
    enriched_result.limit(10).show(max_width=500)

    print("\n=== Match metrics ===")
    match_result.match_metrics().show()

con.close()
