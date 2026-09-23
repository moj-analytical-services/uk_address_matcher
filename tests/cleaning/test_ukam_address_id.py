from unittest.mock import patch

import pytest

import uk_address_matcher.cleaning.chunking_strategies as chunking_strategies
from uk_address_matcher.cleaning.chunking_strategies import (
    clean_data_pre_term_frequencies,
)


def test_duplicate_records_get_unique_ukam_address_id(duck_con):
    # Create test data with:
    # - Two identical addresses but different unique_ids
    #   (should get different ukam_address_id)
    # - Two completely identical rows including unique_id
    #   (should still get a distinct row-level ukam_address_id)
    sql = """
    CREATE OR REPLACE TABLE test_data AS
    SELECT * FROM (VALUES
        ('1', '10 DOWNING STREET LONDON', 'SW1A 2AA'),
        ('2', '10 DOWNING STREET LONDON', 'SW1A 2AA'),
        ('1', '10 DOWNING STREET LONDON', 'SW1A 2AA')
    ) AS t(unique_id, address_concat, postcode)
    """
    duck_con.execute(sql)
    test_data = duck_con.table("test_data")

    # Clean the data (single chunk to isolate the ID generation logic)
    cleaned = clean_data_pre_term_frequencies(test_data, con=duck_con, num_of_chunks=1)

    # Check that ukam_address_id column exists
    assert "ukam_address_id" in cleaned.columns, (
        "ukam_address_id column should exist in cleaned data"
    )
    assert "__ukam_row_id" not in cleaned.columns

    # Get the ukam_address_id values
    result = duck_con.sql(
        "SELECT unique_id, ukam_address_id "
        "FROM cleaned ORDER BY unique_id, ukam_address_id"
    ).fetchall()

    assert len(result) == 3, "Expected 3 records in output"
    assert len(set(result)) == 3

    ukam_ids = [ukam_address_id for _, ukam_address_id in result]
    assert sorted(ukam_ids) == [1, 2, 3]
    assert all(isinstance(ukam_address_id, int) for ukam_address_id in ukam_ids)


def test_chunking_with_duplicates_across_chunk_boundaries(duck_con):
    """
    Test that identical records landing in different chunks still
    receive distinct ukam_address_id values.
    """
    # Create data where identical records will likely span chunk boundaries
    # Using 20 records with 4 chunks = 5 records per chunk
    sql = """
    CREATE OR REPLACE TABLE test_data AS
    SELECT * FROM (VALUES
        ('1', '10 DOWNING STREET', 'SW1A 2AA'),
        ('2', '10 DOWNING STREET', 'SW1A 2AA'),
        ('3', '10 DOWNING STREET', 'SW1A 2AA'),
        ('4', '10 DOWNING STREET', 'SW1A 2AA'),
        ('5', '10 DOWNING STREET', 'SW1A 2AA'),
        ('6', '10 DOWNING STREET', 'SW1A 2AA'),
        ('7', '10 DOWNING STREET', 'SW1A 2AA'),
        ('8', '10 DOWNING STREET', 'SW1A 2AA'),
        ('9', '10 DOWNING STREET', 'SW1A 2AA'),
        ('1', '10 DOWNING STREET', 'SW1A 2AA'),
        ('1', '10 DOWNING STREET', 'SW1A 2AA'),
        ('2', '10 DOWNING STREET', 'SW1A 2AA'),
        ('2', '10 DOWNING STREET', 'SW1A 2AA'),
        ('2', '10 DOWNING STREET', 'SW1A 2AA'),
        ('2', '10 DOWNING STREET', 'SW1A 2AA'),
        ('2', '10 DOWNING STREET', 'SW1A 2AA'),
        ('2', '10 DOWNING STREET', 'SW1A 2AA'),
        ('2', '10 DOWNING STREET', 'SW1A 2AA'),
        ('19', '10 DOWNING STREET', 'SW1A 2AA'),
        ('20', '10 DOWNING STREET', 'SW1A 2AA')
    ) AS t(unique_id, address_concat, postcode)
    """
    duck_con.execute(sql)
    test_data = duck_con.table("test_data")

    # Process with 4 chunks (5 records each)
    cleaned = clean_data_pre_term_frequencies(test_data, con=duck_con, num_of_chunks=4)

    nonempty_chunks = duck_con.execute(
        """
        SELECT COUNT(DISTINCT CAST(abs(hash(address_concat)) % 4 AS INTEGER))
        FROM test_data
        """
    ).fetchone()[0]
    assert nonempty_chunks == 1

    # Get all ukam_address_id values
    result = cleaned.select("ukam_address_id").fetchall()
    ukam_ids = [row[0] for row in result]

    assert len(ukam_ids) == 20, f"Expected 20 records in output, got {len(ukam_ids)}"

    # All 20 identical records should have unique ukam_address_ids
    assert len(set(ukam_ids)) == 20, (
        f"All ukam_address_id values should be unique across chunks. "
        f"Got {len(set(ukam_ids))} unique values for 20 identical records."
    )

    assert sorted(ukam_ids) == list(range(1, 21))
    assert all(ukam_address_id is not None for ukam_address_id in ukam_ids)
    assert all(isinstance(ukam_address_id, int) for ukam_address_id in ukam_ids)
    assert (
        duck_con.execute("DESCRIBE SELECT ukam_address_id FROM cleaned").fetchone()[1]
        == "INTEGER"
    )


def test_existing_ukam_address_id_is_replaced_with_fresh_integer(duck_con):
    duck_con.execute(
        """
        CREATE OR REPLACE TABLE test_data AS
        SELECT * FROM (VALUES
            ('1', '10 DOWNING STREET', 'SW1A 2AA', 101::BIGINT),
            ('2', '11 DOWNING STREET', 'SW1A 2AA', 102::BIGINT)
        ) AS t(unique_id, address_concat, postcode, ukam_address_id)
        """
    )

    cleaned = clean_data_pre_term_frequencies(
        duck_con.table("test_data"),
        con=duck_con,
        num_of_chunks=2,
    )

    result = duck_con.sql("SELECT ukam_address_id FROM cleaned").fetchall()

    assert sorted(row[0] for row in result) == [1, 2]
    assert cleaned.columns.count("ukam_address_id") == 1


def test_ids_are_unique_and_contiguous(duck_con):
    duck_con.execute(
        """
        CREATE OR REPLACE TABLE test_data AS
        SELECT * FROM (VALUES
            ('2', '1 DOWNING STREET LONDON', 'SW1A 2AA'),
            ('1', '99 DOWNING STREET LONDON', 'SW1A 2AA'),
            ('3', '1 HIGH STREET OXFORD', 'OX1 1AA')
        ) AS t(unique_id, address_concat, postcode)
        """
    )

    cleaned = clean_data_pre_term_frequencies(
        duck_con.table("test_data"),
        con=duck_con,
        num_of_chunks=1,
    )

    ids = [row[0] for row in cleaned.select("ukam_address_id").fetchall()]

    assert sorted(ids) == [1, 2, 3]
    assert len(set(ids)) == len(ids)
    assert all(identifier is not None for identifier in ids)
    assert (
        duck_con.execute("DESCRIBE SELECT ukam_address_id FROM cleaned").fetchone()[1]
        == "INTEGER"
    )


def test_multi_chunk_id_association_matches_legacy_chunk_order(duck_con):
    duck_con.execute(
        """
        CREATE OR REPLACE TABLE test_data AS
        SELECT
            CAST(address_number AS VARCHAR) AS unique_id,
            CONCAT(CAST(address_number AS VARCHAR), ' HIGH STREET LONDON')
                AS address_concat,
            'SW1A 2AA' AS postcode
        FROM range(32) AS addresses(address_number)
        """
    )
    num_chunks = 4
    duck_con.execute(
        f"""
        CREATE OR REPLACE TABLE legacy_chunked AS
        SELECT
            *,
            CAST(abs(hash(address_concat)) % {num_chunks} AS INTEGER) AS chunk_index
        FROM test_data
        """
    )

    legacy_unique_ids = []
    for chunk_index in range(num_chunks):
        legacy_unique_ids.extend(
            row[0]
            for row in duck_con.execute(
                f"""
                SELECT unique_id
                FROM legacy_chunked
                WHERE chunk_index = {chunk_index}
                """
            ).fetchall()
        )

    cleaned = clean_data_pre_term_frequencies(
        duck_con.table("test_data"),
        con=duck_con,
        num_of_chunks=num_chunks,
    )

    expected = [
        (identifier, unique_id)
        for identifier, unique_id in enumerate(legacy_unique_ids, start=1)
    ]
    assert cleaned.select("ukam_address_id, unique_id").fetchall() == expected


def test_multi_chunk_physical_scan_has_ascending_ids(duck_con):
    duck_con.execute(
        """
        CREATE OR REPLACE TABLE test_data AS
        SELECT
            CAST(address_number AS VARCHAR) AS unique_id,
            CONCAT(CAST(address_number AS VARCHAR), ' HIGH STREET LONDON')
                AS address_concat,
            'SW1A 2AA' AS postcode
        FROM range(32) AS addresses(address_number)
        """
    )

    cleaned = clean_data_pre_term_frequencies(
        duck_con.table("test_data"),
        con=duck_con,
        num_of_chunks=4,
    )

    out_of_order_count = duck_con.execute(
        f"""
        WITH scanned AS (
            SELECT
                ukam_address_id,
                LAG(ukam_address_id) OVER () AS previous_id
            FROM ({cleaned.sql_query()}) AS cleaned
        )
        SELECT COUNT(*)
        FROM scanned
        WHERE previous_id IS NOT NULL
          AND ukam_address_id <= previous_id
        """
    ).fetchone()[0]

    assert out_of_order_count == 0


def test_chunk_cleaning_failure_drops_materialised_chunks(duck_con):
    duck_con.execute(
        """
        CREATE OR REPLACE TABLE test_data AS
        SELECT
            CAST(address_number AS VARCHAR) AS unique_id,
            CONCAT(CAST(address_number AS VARCHAR), ' HIGH STREET LONDON')
                AS address_concat,
            'SW1A 2AA' AS postcode
        FROM range(64) AS addresses(address_number)
        """
    )
    assert (
        duck_con.execute(
            """
            SELECT COUNT(DISTINCT CAST(abs(hash(address_concat)) % 4 AS INTEGER))
            FROM test_data
            """
        ).fetchone()[0]
        > 1
    )

    original_clean = chunking_strategies._clean_data_pre_term_frequencies
    call_count = 0

    def fail_on_second_chunk(address_table, con, *, debug_options=None):
        nonlocal call_count
        call_count += 1
        if call_count == 2:
            raise RuntimeError("forced chunk cleaning failure")
        return original_clean(address_table, con, debug_options=debug_options)

    with patch(
        "uk_address_matcher.cleaning.chunking_strategies._clean_data_pre_term_frequencies",
        side_effect=fail_on_second_chunk,
    ):
        with pytest.raises(RuntimeError, match="forced chunk cleaning failure"):
            clean_data_pre_term_frequencies(
                duck_con.table("test_data"),
                con=duck_con,
                num_of_chunks=4,
            )

    table_names = [name for (name,) in duck_con.execute("SHOW TABLES").fetchall()]
    leaked_tables = [
        name
        for name in table_names
        if name.startswith(
            (
                "__ukam_chunk_input_",
                "__ukam_chunked_input_",
                "__ukam_cleaned_chunk_",
            )
        )
    ]
    assert not leaked_tables
