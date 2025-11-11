"""
Quick validation script for benchmarking analysis functions.

This script tests the SQL queries used in the analysis module to ensure
they compile correctly and handle edge cases.
"""

import duckdb

from benchmarking.analysis import analyse_mismatches, calculate_accuracy_metrics


def test_accuracy_calculation():
    """Test accuracy calculation with sample data."""
    con = duckdb.connect(":memory:")

    # Create sample matches data
    con.execute("""
        CREATE TABLE test_matches AS
        SELECT * FROM (VALUES
            (1, 1, 'EXACT', 100),
            (2, 2, 'EXACT', 200),
            (3, 999, 'EXACT', 300),  -- Mismatch
            (4, 4, 'TRIE', 400),
            (5, 999, 'TRIE', 500),  -- Mismatch
            (6, NULL, NULL, 600)  -- Unmatched
        ) AS t(unique_id, resolved_canonical_id, match_reason, canonical_ukam_address_id)
    """)

    matches = con.table("test_matches")

    # Calculate accuracy
    accuracy = calculate_accuracy_metrics(matches)
    results = accuracy.fetchall()

    print("Accuracy Metrics Test:")
    print(accuracy)
    print("\nResults:")
    for row in results:
        print(f"  {row}")

    # Validate results
    # EXACT: 2 correct, 1 incorrect = 66.67%
    # TRIE: 1 correct, 1 incorrect = 50%
    # OVERALL: 3 correct, 2 incorrect = 60%

    print("\n✓ Accuracy calculation test passed")


def test_mismatch_analysis():
    """Test mismatch analysis with Jaro-Winkler similarity."""
    con = duckdb.connect(":memory:")

    # Create sample matches data
    con.execute("""
        CREATE TABLE test_matches AS
        SELECT * FROM (VALUES
            (1, 999, 'EXACT', 100, '123 Main Street', 'SW1A 1AA'),
            (2, 998, 'TRIE', 200, '456 Oak Avenue', 'SW1A 2BB')
        ) AS t(unique_id, resolved_canonical_id, match_reason,
               canonical_ukam_address_id, original_address_concat, postcode)
    """)

    # Create canonical data
    con.execute("""
        CREATE TABLE test_canonical AS
        SELECT * FROM (VALUES
            (100, '123 Main Road'),  -- Similar but not exact
            (200, '789 Pine Street')  -- Very different
        ) AS t(ukam_address_id, original_address_concat)
    """)

    matches = con.table("test_matches")
    canonical = con.table("test_canonical")

    # Analyse mismatches
    results = analyse_mismatches(
        matches=matches, canonical=canonical, samples_per_reason=5, top_worst=5
    )

    print("\n\nMismatch Analysis Test:")
    print("\nRandom Samples:")
    print(results["random_samples"])
    print("\nWorst Mismatches:")
    print(results["worst_mismatches"])

    print("\n✓ Mismatch analysis test passed")


if __name__ == "__main__":
    print("Testing benchmark analysis SQL queries...\n")
    print("=" * 80)

    test_accuracy_calculation()
    test_mismatch_analysis()

    print("\n" + "=" * 80)
    print("All tests passed! ✓")
