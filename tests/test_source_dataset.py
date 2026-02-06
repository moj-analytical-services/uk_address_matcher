import duckdb
import pytest

from uk_address_matcher import clean_data_with_term_frequencies, get_linker

# (messy_id, messy_address, postcode, canonical_id, should_match)
TEST_CASES = [
    # Letter mismatches
    ("m_flat_a", "FLAT A 10 KINGS ROAD LONDON", "SW3 4ND", "c_flat_c", False),
    ("m_flat_b", "FLAT B 10 KINGS ROAD LONDON", "SW3 4ND", "c_flat_c", False),
    ("m_flat_c", "FLAT C 10 KINGS ROAD LONDON", "SW3 4ND", "c_flat_c", True),
    # Number mismatches
    ("m_flat_1", "FLAT 1 20 HIGH STREET LONDON", "E1 6AA", "c_flat_3", False),
    ("m_flat_2", "FLAT 2 20 HIGH STREET LONDON", "E1 6AA", "c_flat_3", False),
    ("m_flat_3", "FLAT 3 20 HIGH STREET LONDON", "E1 6AA", "c_flat_3", True),
    # Combined number and letter
    ("m_flat_1a", "FLAT 1A 30 PARK LANE LONDON", "W1K 1BE", "c_flat_2b", False),
    ("m_flat_2a", "FLAT 2A 30 PARK LANE LONDON", "W1K 1BE", "c_flat_2b", False),
    ("m_flat_2b", "FLAT 2B 30 PARK LANE LONDON", "W1K 1BE", "c_flat_2b", True),
    # Positional (first vs second)
    (
        "m_first_pos",
        "FIRST FLOOR FLAT 40 QUEEN ST",
        "EC4R 1AA",
        "c_second_pos",
        False,
    ),
    (
        "m_second_pos",
        "SECOND FLOOR FLAT 40 QUEEN ST",
        "EC4R 1AA",
        "c_second_pos",
        True,
    ),
]

# Canonical addresses that messy records match against
CANONICAL_ADDRESSES = [
    ("c_flat_c", "FLAT C 10 KINGS ROAD LONDON", "SW3 4ND"),
    ("c_flat_3", "FLAT 3 20 HIGH STREET LONDON", "E1 6AA"),
    ("c_flat_2b", "FLAT 2B 30 PARK LANE LONDON", "W1K 1BE"),
    ("c_second_pos", "SECOND FLOOR FLAT 40 QUEEN STREET LONDON", "EC4R 1AA"),
]


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


def test_flat_penalties():
    match_weight_threshold = 10.0
    con = duckdb.connect()

    # Build messy addresses relation - use test_id as unique_id for lookup
    messy_values = ", ".join(
        f"('{uid}'::VARCHAR, '{addr}'::VARCHAR, '{pc}'::VARCHAR)"
        for uid, addr, pc, _, _ in TEST_CASES
    )
    messy_rel = con.sql(f"""
        SELECT * FROM (VALUES {messy_values})
        AS t(unique_id, address_concat, postcode)
    """)

    # Build canonical addresses relation
    canon_values = ", ".join(
        f"('{uid}'::VARCHAR, '{addr}'::VARCHAR, '{pc}'::VARCHAR)"
        for uid, addr, pc in CANONICAL_ADDRESSES
    )
    canon_rel = con.sql(f"""
        SELECT * FROM (VALUES {canon_values})
        AS t(unique_id, address_concat, postcode)
    """)

    # Clean the data
    messy_cleaned = clean_data_with_term_frequencies(messy_rel, con=con)
    canon_cleaned = clean_data_with_term_frequencies(canon_rel, con=con)

    # Get linker and run predictions
    linker = get_linker(messy_cleaned, canon_cleaned, con=con)
    predictions = linker.inference.predict(threshold_match_probability=0.00001)
    results_df = predictions.as_pandas_dataframe()

    # Build lookup: (messy_unique_id, canon_unique_id) -> match_weight
    match_weights = {}
    for _, row in results_df.iterrows():
        key = (row["unique_id_l"], row["unique_id_r"])
        match_weights[key] = row["match_weight"]
        # Also store reverse key since Splink can return either order
        key_rev = (row["unique_id_r"], row["unique_id_l"])
        match_weights[key_rev] = row["match_weight"]

    # Check all test cases
    failures = []
    for messy_id, _, _, canon_id, should_match in TEST_CASES:
        key = (messy_id, canon_id)
        mw = match_weights.get(key)

        if mw is None:
            if should_match:
                failures.append(
                    f"{messy_id} -> {canon_id}: no prediction found (expected match)"
                )
        elif should_match and mw < match_weight_threshold:
            failures.append(
                f"{messy_id} -> {canon_id}: MW={mw:.2f} < {match_weight_threshold} (expected match)"
            )
        elif not should_match and mw >= match_weight_threshold:
            failures.append(
                f"{messy_id} -> {canon_id}: MW={mw:.2f} >= {match_weight_threshold} (expected penalty)"
            )

    assert not failures, "Flat penalty failures:\n" + "\n".join(failures)
