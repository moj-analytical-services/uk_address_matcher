import duckdb
import pytest

from uk_address_matcher import clean_data_with_term_frequencies, get_linker


def test_source_dataset_is_ignored():
    """
    Test that the source_dataset column in input data is ignored and
    the correct values are set in the output regardless of user input.

    The source_dataset_l should be set to 'c_' and the source_dataset_r should be set to 'm_'
    irrespective of what the user put in the input source dataset.
    """
    # Create a DuckDB connection
    con = duckdb.connect(":memory:")

    # Create test data with custom source_dataset values
    sql = """
    CREATE OR REPLACE TABLE test_messy AS
    SELECT
        '1' as unique_id,
        'a' as source_dataset,
        '10 DOWNING STREET LONDON' as address_concat,
        'SW1A 2AA' as postcode
    """
    con.execute(sql)

    sql = """
    CREATE OR REPLACE TABLE test_canonical AS
    SELECT
        '2' as unique_id,
        'z' as source_dataset,
        '10 DOWNING STREET LONDON' as address_concat,
        'SW1A 2AA' as postcode
    """
    con.execute(sql)
    test_messy = con.table("test_messy")
    test_canonical = con.table("test_canonical")

    # Verify source_dataset exists in input data
    assert "source_dataset" in test_messy.columns, (
        "source_dataset should exist in input messy data"
    )
    assert "source_dataset" in test_canonical.columns, (
        "source_dataset should exist in input canonical data"
    )

    # Clean the data
    messy_clean = clean_data_with_term_frequencies(test_messy, con=con)
    canonical_clean = clean_data_with_term_frequencies(test_canonical, con=con)

    # Verify source_dataset column is excluded from cleaned data
    assert "source_dataset" not in messy_clean.columns, (
        "source_dataset should be excluded from cleaned messy data"
    )
    assert "source_dataset" not in canonical_clean.columns, (
        "source_dataset should be excluded from cleaned canonical data"
    )

    # Create a linker with the cleaned data
    linker = get_linker(
        df_addresses_to_match=messy_clean,
        df_addresses_to_search_within=canonical_clean,
        con=con,
        include_full_postcode_block=True,
    )

    # Run prediction
    df_predict = linker.inference.predict(threshold_match_weight=-100)
    df_predict_ddb = df_predict.as_duckdbpyrelation()

    # Check the source_dataset values in the output
    sql = """
    SELECT DISTINCT source_dataset_l, source_dataset_r
    FROM df_predict_ddb
    """
    result = con.execute(sql).fetchall()

    # Assert that the source_dataset values are set to 'c_' and 'm_' regardless of input
    assert len(result) == 1, (
        "Expected exactly one distinct pair of source_dataset values"
    )
    source_dataset_l, source_dataset_r = result[0]
    assert source_dataset_l == "c_", "source_dataset_l should be 'c_'"
    assert source_dataset_r == "m_", "source_dataset_r should be 'm_'"


def test_get_linker_raises_error_with_source_dataset():
    """
    Test that get_linker raises an error when a source_dataset column is present in the input data.
    """
    # Create a DuckDB connection
    con = duckdb.connect(":memory:")

    # Create test data with source_dataset column
    sql = """
    CREATE OR REPLACE TABLE test_data AS
    SELECT
        '1' as unique_id,
        'test_source' as source_dataset,
        '10 DOWNING STREET LONDON' as address_concat,
        'SW1A 2AA' as postcode
    """
    con.execute(sql)
    test_data = con.table("test_data")

    # Create data without source_dataset column
    sql = """
    CREATE OR REPLACE TABLE test_data_no_source AS
    SELECT
        '2' as unique_id,
        '11 DOWNING STREET LONDON' as address_concat,
        'SW1A 2AA' as postcode
    """
    con.execute(sql)
    test_data_no_source = con.table("test_data_no_source")

    # Test error when source_dataset is in first dataset
    with pytest.raises(
        ValueError, match="Input datasets contain a 'source_dataset' column"
    ):
        get_linker(
            df_addresses_to_match=test_data,
            df_addresses_to_search_within=test_data_no_source,
            con=con,
        )

    # Test error when source_dataset is in second dataset
    with pytest.raises(
        ValueError, match="Input datasets contain a 'source_dataset' column"
    ):
        get_linker(
            df_addresses_to_match=test_data_no_source,
            df_addresses_to_search_within=test_data,
            con=con,
        )

    # Clean the data to remove source_dataset
    test_data_clean = clean_data_with_term_frequencies(test_data, con=con)

    # Verify this works without error
    get_linker(
        df_addresses_to_match=test_data_clean,
        df_addresses_to_search_within=test_data_no_source,
        con=con,
    )
