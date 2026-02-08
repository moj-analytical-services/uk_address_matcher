import os

import duckdb

from uk_address_matcher import (
    ExactMatchStage,
    SplinkStage,
    TrigramStage,
    prepare_data_for_matching,
    run_matching,
)

con = duckdb.connect(":default:")
con.execute("INSTALL splink_udfs FROM community; LOAD splink_udfs;")

# Step 1: Seed a single messy address to search for
con.execute(
    """
    create or replace table df_messy as
    select
        '1' as unique_id,
        '10 downing street westminster london' as address_concat,
        'SW1A 3BC' as postcode
    """
)

df_messy = con.table("df_messy")
print(" - messy records prepared:", df_messy.count("*").fetchall()[0][0])

# Step 2: Clean the messy record using the standard pipeline
df_messy_clean = prepare_data_for_matching(df_messy, con=con)
df_messy_clean.show(max_width=5000, max_rows=20)


full_os_path = os.getenv(
    "OS_CLEAN_PATH",
    "read_parquet('secret_data/ord_surv/os_clean.parquet')",
)

sql = f"""
select *
from {full_os_path}
"""
df_os_clean = con.sql(sql)
df_os_clean


# Step 3: Run unified matching (deterministic + Splink stages)
match_candidates = run_matching(
    con=con,
    df_messy_clean=df_messy_clean,
    df_canonical_clean=df_os_clean,
    stages=[
        ExactMatchStage(),
        TrigramStage(),
        SplinkStage(
            predict_threshold_match_weight=-100,
            improve_threshold_match_weight=-25,
            final_match_weight_threshold=-25,
            final_distinguishability_threshold=None,
            include_full_postcode_block=True,
            include_outside_postcode_block=True,
            retain_intermediate_calculation_columns=True,
        ),
    ],
)
match_candidates.show(max_width=5000, max_rows=20)
