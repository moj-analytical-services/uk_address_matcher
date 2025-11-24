import logging
from pathlib import Path

import duckdb

from tests.utils import prepare_combined_test_data

# Import necessary functions from the library
from uk_address_matcher import (
    clean_data_with_term_frequencies,
    evaluate_predictions_against_labels,
    get_linker,
    inspect_match_results_vs_labels,
)
from uk_address_matcher.linking_model.training import get_settings_for_training
from uk_address_matcher.post_linkage.analyse_results import (
    best_matches_with_distinguishability,
)
from uk_address_matcher.post_linkage.identify_distinguishing_tokens import (
    improve_predictions_using_distinguishing_tokens,
)

logger = logging.getLogger("uk_address_matcher")


def test_address_matching_workflow_runs():
    """
    Test that the full address matching workflow runs without errors using test data.
    """
    duckdb_con = duckdb.connect(database=":memory:")
    yaml_path = Path(__file__).parent / "edge_case_addresses.yaml"

    messy_addresses_raw, canonical_addresses_raw = prepare_combined_test_data(
        yaml_path, duckdb_con
    )

    labels_rel = duckdb_con.sql(
        """
        SELECT
            unique_id,
            true_match_id::VARCHAR AS correct_unique_id
        FROM (
            SELECT * FROM messy_addresses_raw
        )
        WHERE true_match_id IS NOT NULL
    """
    )

    df_os_rel = canonical_addresses_raw
    messy_data_rel = messy_addresses_raw

    df_messy_data_clean_rel = clean_data_with_term_frequencies(
        messy_data_rel.project("unique_id, source_dataset, address_concat, postcode"),
        con=duckdb_con,
    )

    df_os_clean_rel = clean_data_with_term_frequencies(
        df_os_rel.project("unique_id, source_dataset, address_concat, postcode"),
        con=duckdb_con,
    )

    settings = get_settings_for_training()

    linker = get_linker(
        df_addresses_to_match=df_messy_data_clean_rel,
        df_addresses_to_search_within=df_os_clean_rel,
        con=duckdb_con,
        include_full_postcode_block=False,
        include_outside_postcode_block=True,
        retain_intermediate_calculation_columns=True,  # Still needed for inspect
        settings=settings,
    )

    df_predict = linker.inference.predict(threshold_match_weight=-20)
    df_predict_rel = df_predict.as_duckdbpyrelation()

    df_predict_improved_rel = improve_predictions_using_distinguishing_tokens(
        df_predict=df_predict_rel,
        con=duckdb_con,
        match_weight_threshold=-10,
        top_n_matches=3,
        use_bigrams=True,
    )

    df_predict_with_distinguishability_rel = best_matches_with_distinguishability(
        df_predict=df_predict_improved_rel,
        df_addresses_to_match=df_messy_data_clean_rel,
        con=duckdb_con,
    )

    evaluation_results_rel = evaluate_predictions_against_labels(
        labels=labels_rel,
        df_predict_with_distinguishability=df_predict_with_distinguishability_rel,
        con=duckdb_con,
    )
    logger.info("Evaluation Results:")
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
        unique_id_r="1",  # Inspect the first messy record
        example_number=1,  # Fallback (shouldn't be needed with unique_id_r set)
    )
