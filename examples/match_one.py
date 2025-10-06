import os

import duckdb

from uk_address_matcher import (
    clean_data_using_precomputed_rel_tok_freq,
    get_linker,
    run_deterministic_match_pass,
)
from uk_address_matcher.post_linkage.analyse_results import (
    best_matches_with_distinguishability,
)
from uk_address_matcher.post_linkage.identify_distinguishing_tokens import (
    improve_predictions_using_distinguishing_tokens,
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
df_messy_clean = clean_data_using_precomputed_rel_tok_freq(df_messy, con=con)
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


# Step 3: Run deterministic exact matching to prune easy wins
df_exact_matches = run_deterministic_match_pass(
    con=con,
    df_addresses_to_match=df_messy_clean,
    df_addresses_to_search_within=df_os_clean,
)

exact_matches = df_exact_matches.filter("resolved_canonical_id IS NOT NULL")
exact_match_count = exact_matches.count("*").fetchall()[0][0]
if exact_match_count:
    print(f"Exact matches found: {exact_match_count}")
    exact_matches.show(max_width=5000, max_rows=20)
else:
    print("No exact matches found; proceeding with probabilistic matching only.")

# Step 3b: Collect unresolved records to check if any remain for processing
# NB: the `get_linker` function will automatically error if you supply no unresolved records,
# so you can choose to catch that error instead of this check if preferred.
df_unresolved = df_exact_matches.filter("resolved_canonical_id IS NULL")
unresolved_count = df_unresolved.count("*").fetchall()[0][0]
if unresolved_count == 0:
    print(
        "All records resolved deterministically; skipping probabilistic matching stage."
    )
else:
    # Step 4: Build a Splink linker with the filtered search space
    linker = get_linker(
        df_addresses_to_match=df_unresolved,
        df_addresses_to_search_within=df_os_clean,
        con=con,
        include_full_postcode_block=True,
        include_outside_postcode_block=True,
        retain_intermediate_calculation_columns=True,
    )

    # Step 5: Run probabilistic matching (Splink inference)
    df_predict = linker.inference.predict(threshold_match_weight=-100)

    df_predict_ddb = df_predict.as_duckdbpyrelation()
    df_predict_ddb.show(max_width=5000, max_rows=20)

    res = df_predict_ddb.df().to_dict(orient="records")
    linker.visualisations.waterfall_chart(res)

    df_predict_improved = improve_predictions_using_distinguishing_tokens(
        df_predict=df_predict_ddb,
        con=con,
        match_weight_threshold=-25,
        top_n_matches=5,
        use_bigrams=True,
    )

    best_matches = best_matches_with_distinguishability(
        df_predict=df_predict_improved, df_addresses_to_match=df_messy, con=con
    )
    best_matches.show(max_width=5000, max_rows=20)
