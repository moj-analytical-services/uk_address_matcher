import duckdb
import pandas as pd

from uk_address_matcher import (
    ExactMatchStage,
    SplinkStage,
    TrigramStage,
    evaluate_predictions_against_labels,
    prepare_data_for_matching,
    run_matching,
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
        "ukam_label": "C1",
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

df_messy_data_clean_rel = prepare_data_for_matching(messy_data_rel, con=duckdb_con)

df_os_clean_rel = prepare_data_for_matching(
    duckdb_con.from_df(df_os_rel), con=duckdb_con
)

match_candidates_rel = run_matching(
    con=duckdb_con,
    df_messy_clean=df_messy_data_clean_rel,
    df_canonical_clean=df_os_clean_rel,
    stages=[
        ExactMatchStage(),
        TrigramStage(),
        SplinkStage(
            predict_threshold_match_weight=-20,
            improve_threshold_match_weight=-10,
            final_match_weight_threshold=-10,
            final_distinguishability_threshold=None,
            include_full_postcode_block=False,
            include_outside_postcode_block=True,
            retain_intermediate_calculation_columns=True,
        ),
    ],
)

evaluation_results_rel = evaluate_predictions_against_labels(
    match_candidates=match_candidates_rel,
    con=duckdb_con,
)
print("Evaluation Results:")
evaluation_results_rel.show()
