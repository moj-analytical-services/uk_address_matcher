from unittest.mock import patch

import pytest

from uk_address_matcher import (
    clean_data_pre_term_frequencies,
    derive_term_frequencies_table,
)
from uk_address_matcher.cleaning.chunking_strategies import (
    prepare_data_for_matching,
)


@pytest.fixture
def fhrs_data(duck_con):
    """Load FHRS example data for testing."""
    path = "./example_data/fhrs_addresses_sample.parquet"
    return duck_con.read_parquet(path)


@pytest.fixture
def mock_chunk_size_1k():
    """Mock _calculate_chunk_size to use 1k minimum instead of 10k for testing."""

    def _calculate_chunk_size_1k(total_records: int, num_of_chunks: int) -> int:
        # Ensure chunk size is reasonable: minimum 1k records per chunk (for testing)
        num_of_chunks = min(num_of_chunks, max(1, total_records // 1_000))
        chunk_size = (total_records + num_of_chunks - 1) // num_of_chunks
        return chunk_size

    with patch(
        "uk_address_matcher.cleaning.chunking_strategies._calculate_chunk_size",
        side_effect=_calculate_chunk_size_1k,
    ):
        yield


def test_chunking_yields_same_result_as_no_chunking(
    duck_con, fhrs_data, mock_chunk_size_1k
):
    num_chunks = 5

    # Process without chunking (baseline)
    baseline = clean_data_pre_term_frequencies(fhrs_data, con=duck_con)
    baseline.to_table("baseline_rel")

    # Process with chunking
    chunked = clean_data_pre_term_frequencies(
        fhrs_data, con=duck_con, num_of_chunks=num_chunks
    )
    chunked.to_table("chunked_rel")

    query = """
        SELECT
            COUNT(*) as count_records,
            COUNT(DISTINCT ukam_address_id) as count_distinct_ids,
            COUNT(*) = COUNT(DISTINCT ukam_address_id) as no_duplicates
        FROM {relation}
    """

    # Get baseline metrics
    baseline_stats = duck_con.sql(query.format(relation="baseline_rel")).fetchone()

    baseline_count, baseline_distinct_ids, baseline_no_dupes = baseline_stats

    # Get chunked metrics
    chunked_stats = duck_con.sql(query.format(relation="chunked_rel")).fetchone()

    chunked_count, chunked_distinct_ids, chunked_no_dupes = chunked_stats

    # Basic sanity check on expected record count
    assert chunked_count == baseline_count == 5000

    # Record count is identical
    assert baseline_count == chunked_count, (
        f"Record count mismatch: baseline={baseline_count}, chunked={chunked_count}"
    )

    # No duplicate hashes in either result set
    assert baseline_no_dupes, (
        f"Baseline has duplicate ukam_address_id values: "
        f"{baseline_count} records but only {baseline_distinct_ids} distinct IDs"
    )

    assert chunked_no_dupes, (
        f"Chunked has duplicate ukam_address_id values: "
        f"{chunked_count} records but only {chunked_distinct_ids} distinct IDs"
    )

    # Same number of distinct IDs in both
    assert baseline_distinct_ids == chunked_distinct_ids, (
        "Distinct ID count mismatch: "
        f"baseline={baseline_distinct_ids}, "
        f"chunked={chunked_distinct_ids}"
    )

    # All clean_full_address values are identical
    # (via set intersection to avoid order issues).
    baseline_addresses = baseline.select("clean_full_address").fetchall()
    chunked_addresses = chunked.select("clean_full_address").fetchall()

    # Use sets so we don't need to worry about order
    baseline_set = set(row[0] for row in baseline_addresses)
    chunked_set = set(row[0] for row in chunked_addresses)

    assert baseline_set == chunked_set, (
        f"Address sets differ: "
        f"only_in_baseline={len(baseline_set - chunked_set)}, "
        f"only_in_chunked={len(chunked_set - baseline_set)}"
    )

    # Check that both have the same number of distinct addresses
    assert len(baseline_set) == len(chunked_set), (
        "Distinct address count mismatch: "
        f"baseline={len(baseline_set)}, "
        f"chunked={len(chunked_set)}"
    )


def test_clean_data_using_precomputed_rel_tok_freq(
    duck_con, fhrs_data, mock_chunk_size_1k
):
    no_chunk_rel = prepare_data_for_matching(fhrs_data, con=duck_con)
    no_chunk_count = no_chunk_rel.count("*").fetchone()[0]

    chunked_rel = prepare_data_for_matching(fhrs_data, con=duck_con, num_of_chunks=5)
    chunked_count = chunked_rel.count("*").fetchone()[0]

    # Confirm we get the expected number of records out
    assert no_chunk_count == chunked_count == 5000

    # Confirm all columns match
    chunked_columns_excl_tf = set(chunked_rel.columns)

    assert set(no_chunk_rel.columns) == chunked_columns_excl_tf, (
        f"Column sets differ: "
        f"only_in_no_chunk={set(no_chunk_rel.columns) - chunked_columns_excl_tf}, "
        f"only_in_chunked={chunked_columns_excl_tf - set(no_chunk_rel.columns)}"
    )


@pytest.mark.parametrize("use_data_specific_tfs", [True, False])
def test_token_rel_freq_arr_hist_consistent_across_chunks(
    duck_con, fhrs_data, mock_chunk_size_1k, use_data_specific_tfs
):
    """Verify token_rel_freq_arr_hist is identical irrespective of chunking strategy."""
    # Derive term frequencies if using data-specific TFs
    tf_lookup = (
        derive_term_frequencies_table(fhrs_data, con=duck_con)
        if use_data_specific_tfs
        else None
    )

    # Process without chunking (baseline)
    no_chunk = prepare_data_for_matching(
        fhrs_data,
        con=duck_con,
        num_of_chunks=1,
        term_frequency_lookup=tf_lookup,
    )
    no_chunk_hist = (
        no_chunk.order("unique_id").select("token_rel_freq_arr_hist").fetchall()[:50]
    )

    # Process with 5 chunks
    chunked = prepare_data_for_matching(
        fhrs_data,
        con=duck_con,
        num_of_chunks=5,
        term_frequency_lookup=tf_lookup,
    )
    chunked_hist = (
        chunked.order("unique_id").select("token_rel_freq_arr_hist").fetchall()[:50]
    )

    # First 50 records should have identical histograms
    assert no_chunk_hist == chunked_hist, (
        "Token frequency histograms differ for "
        f"use_data_specific_tfs={use_data_specific_tfs}"
    )
