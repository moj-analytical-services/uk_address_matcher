from uk_address_matcher import clean_data_pre_term_frequencies


def test_duplicate_records_get_unique_ukam_address_id(duck_con):
    # Create test data with:
    # - Two identical addresses but different unique_ids
    #   (should get different ukam_address_id)
    # - Two completely identical rows including unique_id
    #   (should get SAME ukam_address_id)
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

    # Get the ukam_address_id values
    result = duck_con.sql(
        "SELECT unique_id, ukam_address_id "
        "FROM cleaned ORDER BY unique_id, ukam_address_id"
    ).fetchall()

    assert len(result) == 3, "Expected 3 records in output"
    assert len(set(result)) == 3


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

    # Get all ukam_address_id values
    result = cleaned.select("ukam_address_id").fetchall()
    ukam_ids = [row[0] for row in result]

    assert len(ukam_ids) == 20, f"Expected 20 records in output, got {len(ukam_ids)}"

    # All 20 identical records should have unique ukam_address_ids
    assert len(set(ukam_ids)) == 20, (
        f"All ukam_address_id values should be unique across chunks. "
        f"Got {len(set(ukam_ids))} unique values for 20 identical records."
    )
