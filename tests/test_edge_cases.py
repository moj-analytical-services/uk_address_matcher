import logging
from pathlib import Path

import duckdb
import pytest
from splink import block_on

from tests.utils import prepare_combined_test_data
from uk_address_matcher import prepare_data_for_matching
from uk_address_matcher.linking_model.splink_model import _get_linker
from uk_address_matcher.post_linkage.identify_distinguishing_tokens import (
    improve_predictions_using_distinguishing_tokens,
)

# Splink match weight constants
MATCH_WEIGHT_THRESHOLD_PREDICT = -50
MATCH_WEIGHT_THRESHOLD_IMPROVE = -20

logger = logging.getLogger(__name__)


def run_matcher_workflow(messy_addresses, canonical_addresses, duckdb_con=None):
    """
    Run the complete address matching workflow.

    Args:
        messy_addresses: DuckDB relation with messy addresses
        canonical_addresses: DuckDB relation with canonical addresses
        duckdb_con: Optional DuckDB connection (defaults to in-memory database)

    Returns:
        DuckDB relation: Matching results with predicted and true match IDs
    """
    if duckdb_con is None:
        duckdb_con = duckdb.connect(database=":memory:")

    # Clean the input data
    messy_clean = prepare_data_for_matching(messy_addresses, con=duckdb_con)
    canonical_clean = prepare_data_for_matching(canonical_addresses, con=duckdb_con)

    # Configure the linker
    columns_to_retain = ["original_address_concat", "true_match_id"]
    linker = _get_linker(
        df_addresses_to_match=messy_clean,
        df_addresses_to_search_within=canonical_clean,
        con=duckdb_con,
        include_full_postcode_block=True,
        additional_columns_to_retain=columns_to_retain,
    )
    linker._settings_obj._blocking_rules_to_generate_predictions = [
        block_on("test_block").get_blocking_rule("duckdb")
    ]

    # Predict matches (first pass)
    predicted_matches = linker.inference.predict(
        threshold_match_weight=MATCH_WEIGHT_THRESHOLD_PREDICT,
    ).as_duckdbpyrelation()

    # Improve predictions (second pass)
    improved_matches = improve_predictions_using_distinguishing_tokens(
        df_predict=predicted_matches,
        con=duckdb_con,
        match_weight_threshold=MATCH_WEIGHT_THRESHOLD_IMPROVE,
    )

    # Join true match IDs from the messy data
    sql = """
    SELECT p.*, c.true_match_id
    FROM improved_matches p
    LEFT JOIN messy_clean c
    ON p.unique_id_r = c.unique_id
    """
    return improved_matches.query("improved_matches", sql)


def evaluate_matching_results(matching_results, duckdb_con):
    """
    Evaluate address matching results, including the
    distinguishability metric and mismatch details.

    Args:
        matching_results: DuckDB relation with matching results
        duckdb_con: DuckDB connection

    Returns:
        dict: Results including total cases, correct matches,
            match rate, distinguishability, and mismatches
    """
    # Register the results table in DuckDB
    duckdb_con.register("results", matching_results)

    # This is the matching-results table.

    # matching_results.show(max_width=50000)
    sql = """
    SELECT
        unique_id_r AS test_block_id,
        unique_id_l AS match_id,
        match_weight,
        true_match_id,
        CASE WHEN unique_id_l = true_match_id THEN 1 ELSE 0 END AS is_correct_match
    FROM results
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id_r ORDER BY match_weight DESC) = 1;
    """

    duckdb_con.sql(sql).create("top_matches_in_window")
    # top_matches_in_window.show(max_width=50000)

    # Example top-match rows can be printed via `top_matches_in_window.show(...)`.

    sql = """
    SELECT
        r.unique_id_r AS test_block_id,
        r.unique_id_l AS match_id,
        r.match_weight,
        r.true_match_id,
        t.match_weight AS top_match_weight,
        t.match_id AS top_match_id,
        t.is_correct_match AS is_top_match_correct,
        t.match_weight - r.match_weight AS score_diff_from_top,
        CASE WHEN r.unique_id_l = r.true_match_id THEN 1 ELSE 0 END AS is_correct_match
    FROM results r
    JOIN top_matches_in_window t ON r.unique_id_r = t.test_block_id
    order by test_block_id, r.match_weight desc;
    """
    # duckdb_con.sql(sql).show(max_width=50000)

    # Detailed scored rows can be printed via `duckdb_con.sql(sql).show(...)`.

    results_with_top_score = duckdb_con.sql(sql)
    all_matches = results_with_top_score.fetchall()

    reward_penalty_query = """
    SELECT
        a.test_block_id,
        a.top_match_id,
        a.true_match_id,
        a.is_top_match_correct,
        -- Reward: difference between top match and second best match
        (SELECT MIN(score_diff_from_top)
         FROM results_with_top_score a2
         WHERE a2.test_block_id = a.test_block_id
           AND a2.match_id != a.top_match_id) AS reward,
        -- Penalty: difference between top match and true match
        (SELECT score_diff_from_top
         FROM results_with_top_score a2
         WHERE a2.test_block_id = a.test_block_id
           AND a2.match_id = a.true_match_id) AS penalty
    FROM results_with_top_score a
    WHERE a.match_id = a.top_match_id
    """
    # duckdb_con.sql(reward_penalty_query).show(max_width=50000)

    # Reward/penalty rows can be printed via
    # `duckdb_con.sql(reward_penalty_query).show(...)`.

    # Get all matches for reporting
    top_matches = duckdb_con.sql(reward_penalty_query).fetchall()

    # Initialize counters and results
    total_cases = len(top_matches)
    correct_matches = sum(match[3] for match in top_matches)
    total_reward = 0.0
    mismatches = []

    # Process each test block to collect mismatch details
    for row in top_matches:
        (
            test_block_id,
            top_match_id,
            true_match_id,
            is_top_match_correct,
            reward,
            penalty,
        ) = row

        # Get all matches for this test block
        block_matches = [m for m in all_matches if m[0] == test_block_id]

        # Add to total reward based on whether top match is correct
        if is_top_match_correct:
            # Add reward (may be None if there's only one match)
            total_reward += reward if reward is not None else float("inf")
        else:
            # Subtract penalty (may be None if true match isn't in results)
            total_reward -= penalty if penalty is not None else float("inf")

            # Collect mismatch details
            mismatch_query = f"""
            WITH
            messy_record AS (
                SELECT 'Messy Record' AS record_type, address_concat AS address,
                    postcode, NULL AS match_weight
                FROM messy_table_combined
                WHERE unique_id = {test_block_id}
            ),
            true_match AS (
                SELECT 'True Match' AS record_type, address_concat AS address,
                    postcode,
                                        (
                                                SELECT match_weight
                                                FROM results
                                                WHERE unique_id_r = {test_block_id}
                                                    AND unique_id_l = {true_match_id}
                                        )
                    AS match_weight
                FROM canonical_table_combined
                WHERE unique_id = {true_match_id}
            ),
            false_match AS (
                SELECT 'False Match' AS record_type, address_concat AS address,
                    postcode, {block_matches[0][2]} AS match_weight
                FROM canonical_table_combined
                WHERE unique_id = {top_match_id}
            )
            SELECT * FROM messy_record
            UNION ALL SELECT * FROM true_match
            UNION ALL SELECT * FROM false_match

            """

            details = duckdb_con.execute(mismatch_query).fetchall()

            mismatch = {
                "test_block_id": test_block_id,
                "distinguishability_penalty": penalty
                if penalty is not None
                else float("inf"),
                "records": [
                    {
                        "record_type": r[0],
                        "address": r[1],
                        "postcode": r[2],
                        "match_weight": r[3],
                    }
                    for r in details
                ],
            }
            mismatches.append(mismatch)

    # Calculate match rate
    match_rate = (correct_matches / total_cases * 100) if total_cases > 0 else 0

    # Compile results
    results = {
        "total_cases": total_cases,
        "correct_matches": correct_matches,
        "match_rate": match_rate,
        "total_reward": total_reward,
        "mismatches": mismatches,
    }

    return results


def print_matching_results(test_results):
    """
    Print the address matching results, including reward metrics and mismatch details.

    Args:
        test_results: Dictionary with evaluation results
    """
    logger.info("\nAddress Matching Results:")
    logger.info(f"Total test cases: {test_results['total_cases']}")
    logger.info(f"Correct matches: {test_results['correct_matches']}")
    logger.info(f"Match rate: {test_results['match_rate']:.2f}%")
    logger.info(
        f"Total reward: {test_results['total_reward']:.2f}\n"
    )  # Renamed from total_distinguishability

    if test_results["mismatches"]:
        logger.info("Details of mismatches:")
        logger.info("-" * 80)
        for mismatch in test_results["mismatches"]:
            logger.info(f"Test Block ID: {mismatch['test_block_id']}")
            penalty = (
                f"{mismatch['distinguishability_penalty']:.2f}"
                if mismatch["distinguishability_penalty"] != float("inf")
                else "inf"
            )
            logger.info(f"Distinguishability Penalty: {penalty}")
            logger.info(
                f"{'Record Type':<15} {'Address':<60} {'Postcode':<10} {'Match Weight'}"
            )
            logger.info("-" * 100)
            for record in mismatch["records"]:
                weight = (
                    f"{record['match_weight']:.2f}"
                    if record["match_weight"] is not None
                    else "N/A"
                )
                logger.info(
                    f"{record['record_type']:<15} "
                    f"{record['address']:<60} "
                    f"{record['postcode']:<10} {weight}"
                )
            logger.info("-" * 100)
            logger.info("")


def test_address_matching_combined():
    """
    Test that address matching correctly identifies expected matches
    using a combined dataset.

    Stores results in pytest._test_results for test runner access.
    """
    duckdb_con = duckdb.connect(database=":memory:")
    yaml_path = Path(__file__).parent / "edge_case_addresses.yaml"

    # Prepare data
    messy_addresses, canonical_addresses = prepare_combined_test_data(
        yaml_path, duckdb_con
    )

    # Run matching workflow
    matching_results = run_matcher_workflow(
        messy_addresses, canonical_addresses, duckdb_con
    )

    # Evaluate results
    test_results = evaluate_matching_results(matching_results, duckdb_con)

    # Print results (for local testing)
    print_matching_results(test_results)

    # Store for pytest
    pytest._test_results = test_results
