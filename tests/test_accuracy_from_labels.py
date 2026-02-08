import logging
from pathlib import Path

import duckdb

from tests.utils import prepare_combined_test_data

# Import necessary functions from the library
from uk_address_matcher import (
    ExactMatchStage,
    SplinkStage,
    TrigramStage,
    evaluate_predictions_against_labels,
    prepare_data_for_matching,
    run_matching,
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

    df_os_rel = canonical_addresses_raw.select("unique_id, address_concat, postcode")
    messy_data_rel = messy_addresses_raw.select(
        "unique_id, address_concat, postcode, true_match_id::VARCHAR AS ukam_label"
    )

    df_messy_data_clean_rel = prepare_data_for_matching(
        messy_data_rel,
        con=duckdb_con,
    )

    df_os_clean_rel = prepare_data_for_matching(
        df_os_rel,
        con=duckdb_con,
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
    logger.info("Evaluation Results:")
    evaluation_results_rel.show()
