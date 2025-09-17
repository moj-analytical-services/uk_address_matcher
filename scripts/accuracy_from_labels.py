import duckdb
import pandas as pd


from uk_address_matcher import (
    clean_data_using_precomputed_rel_tok_freq,
    get_linker,
    evaluate_predictions_against_labels,
    inspect_match_results_vs_labels,
)
from uk_address_matcher.post_linkage.identify_distinguishing_tokens import (
    improve_predictions_using_distinguishing_tokens,
)
from uk_address_matcher.linking_model.training import get_settings_for_training
from uk_address_matcher.post_linkage.analyse_results import (
    best_matches_with_distinguishability,
)

try:
    from IPython.display import display
except ImportError:

    def display(x):
        print("Display (mock):", type(x))


duckdb_con = duckdb.connect(database=":memory:")

# Deliberate
messy_data = [
    {
        "unique_id": "M1",
        "address_concat": "THE OLD FARM COTTAGE PAD FARM BADGERCROFT ROAD PIKING",
        "postcode": "ZZ1 0ZZ",
        "correct_unique_id": "C1",
    },
]

canonical_data = [
    {
        "unique_id": "C1",
        "address_concat": "OLD FARM COTTAGE BADGERCROFT ROAD PIKING",
        "postcode": "ZZ1 0ZZ",
    },
    {
        "unique_id": "C2",
        "address_concat": "PAD FARM HOUSE BADGERCROFT ROAD PIKING",
        "postcode": "ZZ1 0ZZ",
    },
]

messy_addresses_raw_df = pd.DataFrame(messy_data)
canonical_addresses_raw_df = pd.DataFrame(canonical_data)


messy_addresses_raw = duckdb_con.table("messy_addresses_raw_df")
canonical_addresses_raw = duckdb_con.table("canonical_addresses_raw_df")


labels_rel = duckdb_con.sql("""
    SELECT
        unique_id,
        correct_unique_id::VARCHAR AS correct_unique_id
    FROM messy_addresses_raw
    WHERE correct_unique_id IS NOT NULL
""")

df_os_rel = canonical_addresses_raw
messy_data_rel = messy_addresses_raw

df_messy_data_clean_rel = clean_data_using_precomputed_rel_tok_freq(
    messy_data_rel.select("unique_id", "address_concat", "postcode"), con=duckdb_con
)

df_os_clean_rel = clean_data_using_precomputed_rel_tok_freq(df_os_rel, con=duckdb_con)

settings = get_settings_for_training()

linker = get_linker(
    df_addresses_to_match=df_messy_data_clean_rel,
    df_addresses_to_search_within=df_os_clean_rel,
    con=duckdb_con,
    include_full_postcode_block=False,
    include_outside_postcode_block=True,
    retain_intermediate_calculation_columns=True,
    settings=settings,
)

df_predict = linker.inference.predict(threshold_match_weight=-20)
df_predict_rel = df_predict.as_duckdbpyrelation()

df_predict_improved_rel = improve_predictions_using_distinguishing_tokens(
    df_predict=df_predict_rel,
    con=duckdb_con,
    match_weight_threshold=-10,
    top_n_matches=5,
    use_bigrams=True,
)


df_predict_with_distinguishability_rel = best_matches_with_distinguishability(
    df_predict=df_predict_improved_rel,
    df_addresses_to_match=messy_data_rel.select(
        "unique_id", "address_concat", "postcode"
    ),
    con=duckdb_con,
)

evaluation_results_rel = evaluate_predictions_against_labels(
    labels=labels_rel,
    df_predict_with_distinguishability=df_predict_with_distinguishability_rel,
    con=duckdb_con,
)
print("Evaluation Results:")
evaluation_results_rel.show()


inspect_match_results_vs_labels(
    labels=labels_rel,
    df_predict_improved=df_predict_improved_rel,
    df_predict_with_distinguishability=df_predict_with_distinguishability_rel,
    df_os_addresses=df_os_rel,
    df_messy_data_clean=df_messy_data_clean_rel,
    df_os_addresses_clean=df_os_clean_rel,
    df_predict_original=df_predict_rel,
    linker=linker,
    con=duckdb_con,
    example_number=1,
)
