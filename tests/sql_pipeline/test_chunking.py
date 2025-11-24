from unittest.mock import patch

import pytest

from uk_address_matcher import clean_data_with_minimal_steps
from uk_address_matcher.cleaning.chunking_strategies import (
    clean_data_with_term_frequencies,
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
    baseline = clean_data_with_minimal_steps(fhrs_data, con=duck_con)
    baseline.to_table("baseline_rel")

    # Process with chunking
    chunked = clean_data_with_minimal_steps(
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
        f"Distinct ID count mismatch: baseline={baseline_distinct_ids}, chunked={chunked_distinct_ids}"
    )

    # All clean_full_address values are identical (via set intersection to avoid order issues)
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
        f"Distinct address count mismatch: baseline={len(baseline_set)}, chunked={len(chunked_set)}"
    )


def test_clean_data_using_precomputed_rel_tok_freq(
    duck_con, fhrs_data, mock_chunk_size_1k
):
    no_chunk_rel = clean_data_with_term_frequencies(fhrs_data, con=duck_con)
    no_chunk_count = no_chunk_rel.count("*").fetchone()[0]

    chunked_rel = clean_data_with_term_frequencies(
        fhrs_data, con=duck_con, num_of_chunks=5
    )
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
    # Process without chunking (baseline)
    no_chunk = clean_data_with_term_frequencies(
        fhrs_data,
        con=duck_con,
        num_of_chunks=1,
        use_data_specific_term_frequencies=use_data_specific_tfs,
    )
    no_chunk_hist = (
        no_chunk.order("unique_id").select("token_rel_freq_arr_hist").fetchall()[:50]
    )

    # Process with 5 chunks
    chunked = clean_data_with_term_frequencies(
        fhrs_data,
        con=duck_con,
        num_of_chunks=5,
        use_data_specific_term_frequencies=use_data_specific_tfs,
    )
    chunked_hist = (
        chunked.order("unique_id").select("token_rel_freq_arr_hist").fetchall()[:50]
    )

    # First 50 records should have identical histograms
    assert no_chunk_hist == chunked_hist, (
        f"Token frequency histograms differ for use_data_specific_tfs={use_data_specific_tfs}"
    )


@pytest.mark.parametrize("use_data_specific_tfs", [True, False])
def test_numeric_term_frequency_columns_present_when_using_precomputed_tfs(
    duck_con, fhrs_data, use_data_specific_tfs
):
    """Verify numeric TF columns are present only when using precomputed TFs.

    When using precomputed TFs (use_data_specific_tfs=False), the output should include
    three numeric term frequency columns: tf_numeric_token_1, tf_numeric_token_2, tf_numeric_token_3.

    When using data-specific TFs (use_data_specific_tfs=True), these columns should not be present
    since numeric TFs are computed from the input data itself.
    """
    result = clean_data_with_term_frequencies(
        fhrs_data,
        con=duck_con,
        num_of_chunks=1,
        use_data_specific_term_frequencies=use_data_specific_tfs,
    )

    columns = set(result.columns)
    numeric_tf_columns = {
        "tf_numeric_token_1",
        "tf_numeric_token_2",
        "tf_numeric_token_3",
    }

    if use_data_specific_tfs:
        # Data-specific TFs should NOT have numeric TF columns
        assert not numeric_tf_columns.issubset(columns), (
            f"Expected numeric TF columns to be absent when using data-specific TFs, "
            f"but found: {numeric_tf_columns & columns}"
        )
    else:
        # Precomputed TFs SHOULD have numeric TF columns
        assert numeric_tf_columns.issubset(columns), (
            f"Expected numeric TF columns {numeric_tf_columns} when using precomputed TFs, "
            f"but only found columns: {columns}"
        )
