import os

import duckdb

from uk_address_matcher import (
    AddressMatcher,
    ExactMatchStage,
    SplinkStage,
)

# Required input columns:
#
# | Column         | DuckDB dtype       | Description                         |
# |----------------|--------------------|-------------------------------------|
# | unique_id      | BIGINT or VARCHAR   | Unique identifier for each record   |
# | address_concat | VARCHAR             | Full address (without postcode)     |
# | postcode       | VARCHAR             | Postcode                            |
#
# Any additional columns are retained through the pipeline.

# You can alternatively provide a full address string including the postcode in `address_concat` and leave `postcode` blank. The matcher will attempt to extract the postcode from the full address string during cleaning. However, providing a separate `postcode` column is recommended for better performance and accuracy, as it allows the matcher to use the postcode for candidate retrieval and matching without needing to parse it out of the full address string.

p_ch = "./example_data/companies_house_addresess_postcode_overlap.parquet"
p_fhrs = "./example_data/fhrs_addresses_sample.parquet"

con = duckdb.connect(database=":memory:")

df_ch = con.read_parquet(p_ch)
df_fhrs = con.read_parquet(p_fhrs)

if os.getenv("TEST_LIMIT"):
    df_ch = df_ch.limit(250)
    df_fhrs = df_fhrs.limit(250)


matcher = AddressMatcher(
    canonical_addresses=df_ch,
    addresses_to_match=df_fhrs,
    con=con,
    stages=[
        ExactMatchStage(),
        SplinkStage(
            predict_threshold_match_weight=-20,
            final_match_weight_threshold=12,
            include_full_postcode_block=True,
        ),
    ],
)

result = matcher.match()

print("=== First 10 matched records ===")
result.limit(10).show(max_width=500)


# Inspect results by match reason
match_reasons = [row[0] for row in result.project("match_reason").distinct().fetchall()]

for reason in match_reasons:
    if reason is None:
        continue
    escaped = str(reason).replace("'", "''")
    print(f"\n=== 10 records matched by '{reason}' ===")
    result.filter(f"match_reason = '{escaped}'").limit(10).show(
        max_width=500, max_rows=10
    )
