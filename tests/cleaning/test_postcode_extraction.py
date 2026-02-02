import duckdb

from uk_address_matcher.cleaning.pipelines import _clean_data_with_minimal_steps


def test_postcode_extraction_no_column_provided():
    """Test postcode extraction when no postcode column exists in input."""
    connection = duckdb.connect()

    test_cases = [
        # (input_address, expected_postcode, postcode_should_be_removed_from_address)
        ("10 HIGH STREET LONDON SW1A 1AA", "SW1A 1AA", True),
        ("FLAT 5 ACACIA AVENUE MANCHESTER M1 2AB", "M1 2AB", True),
        ("123 MAIN ROAD BIRMINGHAM B12 3CD", "B12 3CD", True),
        ("GIROBANK BOOTLE GIR 0AA", "GIR 0AA", True),  # Special postcode
        ("15 OAK LANE", None, False),  # No postcode in address
    ]

    for input_address, expected_postcode, should_remove in test_cases:
        input_rel = connection.sql(
            f"SELECT '1' as unique_id, '{input_address}' as address_concat"
        )

        result_rel = _clean_data_with_minimal_steps(input_rel, connection)
        result = result_rel.select(
            "unique_id, original_address_concat, clean_full_address, postcode"
        ).fetchall()[0]

        _, raw_address, cleaned_address, extracted_postcode = result

        # original_address_concat should be preserved verbatim
        assert raw_address == input_address

        # Check postcode extraction
        if expected_postcode:
            assert extracted_postcode == expected_postcode, (
                f"Expected postcode '{expected_postcode}', got '{extracted_postcode}'"
            )
        else:
            assert extracted_postcode is None, (
                f"Expected NULL postcode, got '{extracted_postcode}'"
            )

        # Check postcode removal from address
        if should_remove and expected_postcode:
            assert expected_postcode.replace(" ", "") not in cleaned_address.replace(
                " ", ""
            ), (
                f"Postcode '{expected_postcode}' should be removed from address, "
                f"but found in '{cleaned_address}'"
            )


def test_postcode_extraction_with_column_provided():
    """Test that postcode column takes precedence over postcode in address."""
    connection = duckdb.connect()

    test_cases = [
        # (address_with_postcode, provided_postcode, expected_postcode)
        ("10 HIGH STREET LONDON SW1A 2AA", "SW1A 1AA", "SW1A 1AA"),  # Column wins
        ("FLAT 5 ACACIA AVENUE M1 2AB", "M1 3CD", "M1 3CD"),  # Column wins
        ("123 MAIN ROAD B12 3CD", "B12 9XY", "B12 9XY"),  # Column wins
    ]

    for input_address, provided_postcode, expected_postcode in test_cases:
        input_rel = connection.sql(
            f"SELECT '1' as unique_id, '{input_address}' as address_concat, "
            f"'{provided_postcode}' as postcode"
        )

        result_rel = _clean_data_with_minimal_steps(input_rel, connection)
        result = result_rel.select(
            "unique_id, original_address_concat, clean_full_address, postcode"
        ).fetchall()[0]

        _, raw_address, cleaned_address, extracted_postcode = result

        # original_address_concat should be preserved verbatim
        assert raw_address == input_address

        # Postcode column should take precedence
        assert extracted_postcode == expected_postcode, (
            f"Expected postcode from column '{expected_postcode}', "
            f"got '{extracted_postcode}'"
        )

        # Postcode from address should still be removed
        postcode_in_address = (
            input_address.split()[-2] + " " + input_address.split()[-1]
        )
        if postcode_in_address != expected_postcode:
            # Only check if they're different (if same, it's fine if it appears)
            assert postcode_in_address.replace(" ", "") not in cleaned_address.replace(
                " ", ""
            ), (
                f"Postcode from address '{postcode_in_address}' should be removed, "
                f"but found in '{cleaned_address}'"
            )


def test_postcode_extraction_empty_column():
    """Test that empty/NULL postcode column falls back to extraction."""
    connection = duckdb.connect()

    test_cases = [
        # (input_address, provided_postcode, expected_extracted_postcode)
        ("10 HIGH STREET LONDON SW1A 1AA", "", "SW1A 1AA"),  # Empty string
        ("FLAT 5 ACACIA AVENUE M1 2AB", None, "M1 2AB"),  # NULL
        ("123 MAIN ROAD B12 3CD", "   ", "B12 3CD"),  # Whitespace only
    ]

    for input_address, provided_postcode, expected_postcode in test_cases:
        if provided_postcode is None:
            input_rel = connection.sql(
                f"SELECT '1' as unique_id, '{input_address}' as address_concat, "
                f"NULL as postcode"
            )
        else:
            input_rel = connection.sql(
                f"SELECT '1' as unique_id, '{input_address}' as address_concat, "
                f"'{provided_postcode}' as postcode"
            )

        result_rel = _clean_data_with_minimal_steps(input_rel, connection)
        result = result_rel.select(
            "unique_id, original_address_concat, clean_full_address, postcode"
        ).fetchall()[0]

        _, raw_address, cleaned_address, extracted_postcode = result

        # original_address_concat should be preserved verbatim
        assert raw_address == input_address

        # Should extract from address when column is empty/NULL
        assert extracted_postcode == expected_postcode, (
            f"Expected extracted postcode '{expected_postcode}', "
            f"got '{extracted_postcode}'"
        )

        # Postcode should be removed from address
        assert expected_postcode.replace(" ", "") not in cleaned_address.replace(
            " ", ""
        ), (
            f"Postcode '{expected_postcode}' should be removed from address, "
            f"but found in '{cleaned_address}'"
        )


def test_postcode_case_normalisation():
    """Test that postcodes are normalised to uppercase."""
    connection = duckdb.connect()

    test_cases = [
        # (input_address_or_postcode, expected_normalised)
        ("10 HIGH STREET sw1a 1aa", "SW1A 1AA"),  # Lowercase in address
        ("FLAT 5 ACACIA m1 2ab", "M1 2AB"),  # Lowercase in address
        ("15 OAK LANE b12 3cd", "B12 3CD"),  # Lowercase in address
    ]

    # Test lowercase in address (no postcode column)
    for input_address, expected_postcode in test_cases:
        input_rel = connection.sql(
            f"SELECT '1' as unique_id, '{input_address}' as address_concat"
        )

        result_rel = _clean_data_with_minimal_steps(input_rel, connection)
        result = result_rel.select("postcode").fetchall()[0]

        extracted_postcode = result[0]

        assert extracted_postcode == expected_postcode, (
            f"Expected uppercase postcode '{expected_postcode}', "
            f"got '{extracted_postcode}'"
        )

    # Test lowercase in postcode column
    input_rel = connection.sql(
        "SELECT '1' as unique_id, '10 HIGH STREET' as address_concat, "
        "'sw1a 1aa' as postcode"
    )

    result_rel = _clean_data_with_minimal_steps(input_rel, connection)
    result = result_rel.select("postcode").fetchall()[0]

    extracted_postcode = result[0]

    assert extracted_postcode == "SW1A 1AA", (
        f"Expected uppercase postcode 'SW1A 1AA', got '{extracted_postcode}'"
    )


def test_postcode_spacing_normalisation():
    """Test that postcode spacing is normalised to single space."""
    connection = duckdb.connect()

    test_cases = [
        # (input, expected)
        ("SW1A1AA", "SW1A 1AA"),  # No space
        ("SW1A  1AA", "SW1A 1AA"),  # Double space
        ("M12AB", "M1 2AB"),  # No space, short format
        ("GIR0AA", "GIR 0AA"),  # Special postcode, no space
    ]

    # Test with postcode column
    for input_postcode, expected_postcode in test_cases:
        input_rel = connection.sql(
            f"SELECT '1' as unique_id, '10 HIGH STREET' as address_concat, "
            f"'{input_postcode}' as postcode"
        )

        result_rel = _clean_data_with_minimal_steps(input_rel, connection)
        result = result_rel.select("postcode").fetchall()[0]

        extracted_postcode = result[0]

        assert extracted_postcode == expected_postcode, (
            f"Expected normalised postcode '{expected_postcode}', "
            f"got '{extracted_postcode}'"
        )


def test_postcode_extraction_preserves_other_columns():
    """Test that postcode extraction doesn't affect other columns."""
    connection = duckdb.connect()

    input_rel = connection.sql(
        """
        SELECT
            '1' as unique_id,
            '10 HIGH STREET LONDON SW1A 1AA' as address_concat,
            'some_value' as other_column,
            123 as numeric_column
        """
    )

    result_rel = _clean_data_with_minimal_steps(input_rel, connection)

    # Check that other columns are preserved
    assert "other_column" in result_rel.columns
    assert "numeric_column" in result_rel.columns

    result = result_rel.select("other_column, numeric_column").fetchall()[0]
    assert result[0] == "some_value"
    assert result[1] == 123


def test_postcode_column_case_insensitive():
    """Test that postcode column is matched case-insensitively."""
    connection = duckdb.connect()

    test_cases = [
        ("PostCode", "SW1A 1AA"),  # Mixed case
        ("POSTCODE", "M1 2AB"),  # Upper case
        ("postcode", "B12 3CD"),  # Lower case (already correct)
        ("PoStCoDe", "GIR 0AA"),  # Random case
    ]

    for column_name, postcode_value in test_cases:
        input_rel = connection.sql(
            f"""
            SELECT
                '1' as unique_id,
                '10 HIGH STREET' as address_concat,
                '{postcode_value}' as {column_name}
            """
        )

        result_rel = _clean_data_with_minimal_steps(input_rel, connection)

        # Check that the column was renamed to lowercase 'postcode'
        assert "postcode" in result_rel.columns, (
            f"Expected 'postcode' column for input column '{column_name}'"
        )

        # Check that the original column name is gone (if it wasn't 'postcode')
        if column_name != "postcode":
            assert column_name not in result_rel.columns, (
                f"Original column '{column_name}' should be renamed to 'postcode'"
            )

        # Check that the postcode value is preserved
        result = result_rel.select("postcode").fetchall()[0]
        extracted_postcode = result[0]
        assert extracted_postcode == postcode_value, (
            f"Expected postcode '{postcode_value}', got '{extracted_postcode}'"
        )


def test_no_postcode_results_in_null_not_empty_string():
    """Test that when no postcode exists in input, the cleaned postcode is NULL not empty string."""
    connection = duckdb.connect()

    test_cases = [
        # Addresses with no postcode in address_concat and no postcode column
        "10 HIGH STREET LONDON",
        "FLAT 5 ACACIA AVENUE MANCHESTER",
        "123 MAIN ROAD BIRMINGHAM",
        "15 OAK LANE",
        "THE COTTAGE",
    ]

    for input_address in test_cases:
        # Test 1: No postcode column at all
        input_rel = connection.sql(
            f"SELECT '1' as unique_id, '{input_address}' as address_concat"
        )

        result_rel = _clean_data_with_minimal_steps(input_rel, connection)
        result = result_rel.select("postcode").fetchall()[0]
        extracted_postcode = result[0]

        assert extracted_postcode is None, (
            f"Expected NULL postcode for '{input_address}' with no postcode column, "
            f"got '{extracted_postcode}' (type: {type(extracted_postcode)})"
        )

        # Test 2: Empty string in postcode column
        input_rel = connection.sql(
            f"SELECT '1' as unique_id, '{input_address}' as address_concat, '' as postcode"
        )

        result_rel = _clean_data_with_minimal_steps(input_rel, connection)
        result = result_rel.select("postcode").fetchall()[0]
        extracted_postcode = result[0]

        assert extracted_postcode is None, (
            f"Expected NULL postcode for '{input_address}' with empty postcode column, "
            f"got '{extracted_postcode}' (type: {type(extracted_postcode)})"
        )

        # Test 3: NULL in postcode column
        input_rel = connection.sql(
            f"SELECT '1' as unique_id, '{input_address}' as address_concat, NULL as postcode"
        )

        result_rel = _clean_data_with_minimal_steps(input_rel, connection)
        result = result_rel.select("postcode").fetchall()[0]
        extracted_postcode = result[0]

        assert extracted_postcode is None, (
            f"Expected NULL postcode for '{input_address}' with NULL postcode column, "
            f"got '{extracted_postcode}' (type: {type(extracted_postcode)})"
        )

        # Test 4: Whitespace-only in postcode column
        input_rel = connection.sql(
            f"SELECT '1' as unique_id, '{input_address}' as address_concat, '   ' as postcode"
        )

        result_rel = _clean_data_with_minimal_steps(input_rel, connection)
        result = result_rel.select("postcode").fetchall()[0]
        extracted_postcode = result[0]

        assert extracted_postcode is None, (
            f"Expected NULL postcode for '{input_address}' with whitespace-only postcode column, "
            f"got '{extracted_postcode}' (type: {type(extracted_postcode)})"
        )
