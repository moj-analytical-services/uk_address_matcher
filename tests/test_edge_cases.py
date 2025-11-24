from pathlib import Path

import duckdb
import pytest
from splink import block_on

from tests.utils import prepare_combined_test_data
from uk_address_matcher import clean_data_with_term_frequencies, get_linker
from uk_address_matcher.post_linkage.identify_distinguishing_tokens import (
    improve_predictions_using_distinguishing_tokens,
)

# Constants
MATCH_WEIGHT_THRESHOLD_PREDICT = -50
MATCH_WEIGHT_THRESHOLD_IMPROVE = -20


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
    messy_clean = clean_data_with_term_frequencies(messy_addresses, con=duckdb_con)
    canonical_clean = clean_data_with_term_frequencies(
        canonical_addresses, con=duckdb_con
    )

    # Configure the linker
    columns_to_retain = ["original_address_concat", "true_match_id"]
    linker = get_linker(
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
    return duckdb_con.sql(sql)


def evaluate_matching_results(matching_results, duckdb_con):
    """
    Evaluate address matching results, including the distinguishability metric and mismatch details.

    Args:
        matching_results: DuckDB relation with matching results
        duckdb_con: DuckDB connection

    Returns:
        dict: Results including total cases, correct matches, match rate, distinguishability, and mismatches
    """
    # Register the results table in DuckDB
    duckdb_con.register("results", matching_results)

    # This is the results (matching results) table
    # ┌─────────────────────┬─────────────────────┬─────────────┬─────────────┬──┬────────────────────────────────────────────────────────────┬────────────┬──────────────────────────────────────────────┬────────────┬───────────────┐
    # │    match_weight     │  match_probability  │ unique_id_r │ unique_id_l │  │                 original_address_concat_l                  │ postcode_l │          original_address_concat_r           │ postcode_r │ true_match_id │
    # │       double        │       double        │    int64    │    int64    │  │                          varchar                           │  varchar   │                   varchar                    │  varchar   │     int64     │
    # ├─────────────────────┼─────────────────────┼─────────────┼─────────────┼──┼────────────────────────────────────────────────────────────┼────────────┼──────────────────────────────────────────────┼────────────┼───────────────┤
    # │  3.1609619625582304 │  0.8994394653665296 │           1 │        1003 │  │ FLAT FIRST FLOOR 29 PEPPERPOT ROAD LONDON                  │ W11 1AA    │ FIRST FLOOR FLAT 21 PEPPERPOT ROAD LONDON    │ W11 1AA    │          1001 │
    # │  3.1609619625582304 │  0.8994394653665296 │           1 │        1004 │  │ FLAT FIRST FLOOR 19 PEPPERPOT ROAD LONDON                  │ W11 1AA    │ FIRST FLOOR FLAT 21 PEPPERPOT ROAD LONDON    │ W11 1AA    │          1001 │
    # │   11.14441284034456 │  0.9995584238236682 │           1 │        1001 │  │ FLAT A FIRST AND SECOND FLOORS 21 PEPPERPOT ROAD LONDON    │ W11 1AA    │ FIRST FLOOR FLAT 21 PEPPERPOT ROAD LONDON    │ W11 1AA    │          1001 │

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

    top_matches_in_window = duckdb_con.sql(sql)
    # top_matches_in_window.show(max_width=50000)

    # ┌───────────────┬──────────┬────────────────────┬───────────────┬──────────────────┐
    # │ test_block_id │ match_id │    match_weight    │ true_match_id │ is_correct_match │
    # │     int64     │  int64   │       double       │     int64     │      int32       │
    # ├───────────────┼──────────┼────────────────────┼───────────────┼──────────────────┤
    # │             5 │     5001 │  22.77768031918279 │          5001 │                1 │
    # │             2 │     2001 │ 21.947176097435314 │          2001 │                1 │
    # │             1 │     1001 │  11.14441284034456 │          1001 │                1 │
    # │             4 │     4001 │ 22.727785600825005 │          4001 │                1 │
    # │             3 │     3001 │ 14.853329687543958 │          3001 │                1 │
    # │             6 │     6003 │  9.885603210085975 │          6001 │                0 │

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

    #     ┌───────────────┬──────────┬─────────────────────┬───────────────┬────────────────────┬──────────────┬──────────────────────┬─────────────────────┬──────────────────┐
    # │ test_block_id │ match_id │    match_weight     │ true_match_id │  top_match_weight  │ top_match_id │ is_top_match_correct │ score_diff_from_top │ is_correct_match │
    # │     int64     │  int64   │       double        │     int64     │       double       │    int64     │        int32         │       double        │      int32       │
    # ├───────────────┼──────────┼─────────────────────┼───────────────┼────────────────────┼──────────────┼──────────────────────┼─────────────────────┼──────────────────┤
    # │             1 │     1003 │  3.1609619625582304 │          1001 │  11.14441284034456 │         1001 │                    1 │  7.9834508777863284 │                0 │
    # │             1 │     1004 │  3.1609619625582304 │          1001 │  11.14441284034456 │         1001 │                    1 │  7.9834508777863284 │                0 │
    # │             1 │     1001 │   11.14441284034456 │          1001 │  11.14441284034456 │         1001 │                    1 │                 0.0 │                1 │
    # │             1 │     1002 │    6.19441284034456 │          1001 │  11.14441284034456 │         1001 │                    1 │   4.949999999999999 │                0 │
    # │             1 │     1005 │ -0.7055871596554408 │          1001 │  11.14441284034456 │         1001 │                    1 │               11.85 │                0 │

    # └────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

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

    #     ┌───────────────┬──────────────┬───────────────┬──────────────────────┬────────────────────┬─────────┐
    # │ test_block_id │ top_match_id │ true_match_id │ is_top_match_correct │       reward       │ penalty │
    # │     int64     │    int64     │     int64     │        int32         │       double       │ double  │
    # ├───────────────┼──────────────┼───────────────┼──────────────────────┼────────────────────┼─────────┤
    # │             2 │         2001 │          2001 │                    1 │  20.76985560833095 │     0.0 │
    # │             3 │         3001 │          3001 │                    1 │  18.17726083259552 │     0.0 │
    # │             4 │         4001 │          4001 │                    1 │ 17.819855608330947 │     0.0 │
    # │             1 │         1001 │          1001 │                    1 │  4.949999999999999 │     0.0 │
    # │             5 │         5001 │          5001 │                    1 │  22.35161146423435 │     0.0 │
    # │             6 │         6003 │          6001 │                    0 │                0.0 │    9.35 │

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
                    (SELECT match_weight FROM results WHERE unique_id_r = {test_block_id} AND unique_id_l = {true_match_id})
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
    print("\nAddress Matching Results:")
    print(f"Total test cases: {test_results['total_cases']}")
    print(f"Correct matches: {test_results['correct_matches']}")
    print(f"Match rate: {test_results['match_rate']:.2f}%")
    print(
        f"Total reward: {test_results['total_reward']:.2f}\n"
    )  # Renamed from total_distinguishability

    if test_results["mismatches"]:
        print("Details of mismatches:")
        print("-" * 80)
        for mismatch in test_results["mismatches"]:
            print(f"Test Block ID: {mismatch['test_block_id']}")
            penalty = (
                f"{mismatch['distinguishability_penalty']:.2f}"
                if mismatch["distinguishability_penalty"] != float("inf")
                else "inf"
            )
            print(f"Distinguishability Penalty: {penalty}")
            print(
                f"{'Record Type':<15} {'Address':<60} {'Postcode':<10} {'Match Weight'}"
            )
            print("-" * 100)
            for record in mismatch["records"]:
                weight = (
                    f"{record['match_weight']:.2f}"
                    if record["match_weight"] is not None
                    else "N/A"
                )
                print(
                    f"{record['record_type']:<15} {record['address']:<60} {record['postcode']:<10} {weight}"
                )
            print("-" * 100)
            print()


def test_address_matching_combined():
    """
    Test that address matching correctly identifies expected matches using a combined dataset.

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
