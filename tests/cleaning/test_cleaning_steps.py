import duckdb
import pytest

from uk_address_matcher.cleaning.steps import (
    _parse_out_business_unit,
    _parse_out_flat_position_and_letter,
    _remove_duplicate_end_tokens,
)
from uk_address_matcher.cleaning.steps.normalisation import (
    _clean_address_string_first_pass,
    _peel_common_uk_end_tokens,
)
from uk_address_matcher.sql_pipeline.runner import DebugOptions, DuckDBPipeline


def _run_single_stage(stage_factory, input_relation, connection):
    pipeline = DuckDBPipeline(connection, input_relation)
    pipeline.add_step(stage_factory())
    return pipeline.run(DebugOptions(pretty_print_sql=False))


def test_parse_out_flat_positional():
    connection = duckdb.connect()

    # Format of test cases:
    # (input_address, flat_positional, flat_letter, flat_number)
    # Note: When a number+letter pattern exists (e.g., 11A, 15B), only the LETTER is a flat determinant
    # The number is the building/house number, not a flat identifier
    test_cases = [
        ("11A SPITFIRE COURT BIRMINGHAM", None, "A", None),
        ("FLAT A 11 SPITFIRE COURT BIRMINGHAM", None, "A", None),
        ("BASEMENT FLAT A 11 SPITFIRE COURT BIRMINGHAM", "BASEMENT", "A", None),
        ("BASEMENT FLAT 11 243 SPITFIRE COURT BIRMINGHAM", "BASEMENT", None, "11"),
        ("GARDEN FLAT 11 243 SPITFIRE COURT BIRMINGHAM", "GARDEN", None, "11"),
        ("TOP FLOOR FLAT 12A HIGH STREET", "TOP FLOOR", "A", None),
        ("SECOND FLOOR FLAT 12 A HIGH STREET", "SECOND FLOOR", "A", None),
        ("GROUND FLOOR FLAT B 25 MAIN ROAD", "GROUND FLOOR", "B", None),
        ("FIRST FLOOR 15B LONDON ROAD", "FIRST FLOOR", "B", None),
        ("FLAT C MY HOUSE 120 MY ROAD", None, "C", None),
        ("FLAT 2 733 GIPSY HILL", None, None, "2"),
        ("2 7 GIPSY HILL", None, None, None),  # Ambiguous - no explicit FLAT indicator
        ("773 GIPSY HILL", None, None, None),
        ("FLAT C SECOND FLOOR 27 OK ROAD", "SECOND FLOOR", "C", None),
        ("FLAT A GROUND FLOOR 18 RAVENSWOOD STREET", "GROUND FLOOR", "A", None),
        ("FLAT 3/2 41 DUMMY ROAD", None, None, "2"),
        ("FLAT THE CROWN TESTING ROAD", None, None, None),
        (
            "FLAT 12A HIGH STREET",
            None,
            "A",
            None,
        ),  # adjacent letter after FLAT number, letter is the flat determinant
        (
            "15B LONDON ROAD",
            None,
            "B",
            None,
        ),  # digit+letter: letter is flat determinant, not the number
        ("BASEMENT 15B LONDON ROAD", "BASEMENT", "B", None),  # floor + digit+letter
        (
            "FLAT A MY HOUSE 120-122 SOME ROAD",
            None,
            "A",
            None,
        ),
        # Only one number - indicates that it's a house number, not flat number
        ("UPPER FLOOR FLAT 120 TEST", "UPPER FLOOR", None, None),
        # LOWER GROUND variants
        ("FLAT LOWER GROUND 35 ATOP THE HILL LONDON", "LOWER GROUND", None, None),
        ("LOWER GROUND FLAT 35 ATOP THE HILL LONDON", "LOWER GROUND", None, None),
        ("LOWER GROUND 35 ATOP THE HILL LONDON", "LOWER GROUND", None, None),
        ("LOWER FLOOR FLAT 10 TEST ROAD", "LOWER FLOOR", None, None),
        # UPPER GROUND variant
        ("UPPER GROUND FLAT 20 EXAMPLE STREET", "UPPER GROUND", None, None),
        # BLOCK indicates a flat block (e.g., "BLOCK B STANNARD HALL")
        ("BLOCK B STANNARD HALL 4 PILSWORTH ROAD BURY", "BLOCK", "B", None),
        ("FLAT 44 ALPHA HOUSE BLOCK A HIGH STREET", "BLOCK", "A", "44"),
        # Multi-floor patterns using "FLOORS" (plural)
        (
            "GROUND FIRST SECOND AND THIRD FLOORS 352 KENNINGTON ROAD",
            "GROUND FIRST SECOND AND THIRD FLOORS",
            None,
            None,
        ),
        (
            "FIRST AND SECOND FLOORS 45 MARKET STREET",
            "FIRST AND SECOND FLOORS",
            None,
            None,
        ),
        (
            "THIRD AND FOURTH FLOORS 99 EXAMPLE ROAD",
            "THIRD AND FOURTH FLOORS",
            None,
            None,
        ),
        # Comma-separated multi-floor patterns
        (
            "FIRST, SECOND AND THIRD FLOORS 20 HIGH STREET",
            "FIRST, SECOND AND THIRD FLOORS",
            None,
            None,
        ),
        (
            "GROUND, FIRST AND SECOND FLOORS 15 MAIN ROAD",
            "GROUND, FIRST AND SECOND FLOORS",
            None,
            None,
        ),
    ]

    input_relation = connection.sql(
        "SELECT * FROM (VALUES "
        + ",".join(f"('{address}', '{address}')" for address, _, _, _ in test_cases)
        + ") AS t(clean_full_address, original_address_concat)"
    )

    result = _run_single_stage(
        _parse_out_flat_position_and_letter, input_relation, connection
    )
    rows = result.fetchall()
    columns = result.columns
    positional_idx = columns.index("flat_positional")
    letter_idx = columns.index("flat_letter")
    number_idx = columns.index("flat_number")
    indicator_idx = columns.index("has_flat_indicator")

    for (address, expected_pos, expected_letter, expected_number), row in zip(
        test_cases, rows
    ):
        assert row[positional_idx] == expected_pos, (
            f"Address '{address}' expected positional '{expected_pos}' but got '{row[positional_idx]}'"
        )
        assert row[letter_idx] == expected_letter, (
            f"Address '{address}' expected letter '{expected_letter}' but got '{row[letter_idx]}'"
        )
        assert row[number_idx] == expected_number, (
            f"Address '{address}' expected number '{expected_number}' but got '{row[number_idx]}'"
        )
        # has_flat_indicator is True if any of the three fields are set,
        # OR if the word FLAT appears in the address
        expected_indicator = (
            any(
                value is not None
                for value in (expected_pos, expected_letter, expected_number)
            )
            or "FLAT" in address
        )
        assert row[indicator_idx] == expected_indicator, (
            f"Address '{address}' expected has_flat_indicator '{expected_indicator}' but got '{row[indicator_idx]}'"
        )


def test_remove_duplicate_end_tokens():
    connection = duckdb.connect()
    test_cases = [
        (
            "9A SOUTHVIEW ROAD SOUTHWICK LONDON LONDON",
            "9A SOUTHVIEW ROAD SOUTHWICK LONDON",
        ),
        (
            "1 HIGH STREET ST ALBANS ST ALBANS",
            "1 HIGH STREET ST ALBANS",
        ),
        (
            "2 CORINATION ROAD KINGS LANGLEY HERTFORDSHIRE HERTFORDSHIRE",
            "2 CORINATION ROAD KINGS LANGLEY HERTFORDSHIRE",
        ),
        (
            "FLAT 2 8 ORCHARD WAY MILTON KEYNES MILTON KEYNES",
            "FLAT 2 8 ORCHARD WAY MILTON KEYNES",
        ),
        (
            "9 SOUTHVIEW ROAD SOUTHWICK LONDON",
            "9 SOUTHVIEW ROAD SOUTHWICK LONDON",
        ),
        (
            "1 LONDON ROAD LONDON",
            "1 LONDON ROAD LONDON",
        ),
    ]

    input_relation = connection.sql(
        "SELECT * FROM (VALUES "
        + ",".join(f"('{address}')" for address, _ in test_cases)
        + ") AS t(clean_full_address)"
    )

    result = _run_single_stage(_remove_duplicate_end_tokens, input_relation, connection)
    rows = result.fetchall()

    for (address, expected), row in zip(test_cases, rows):
        assert row[0] == expected, (
            f"Address '{address}' expected '{expected}' but got '{row[0]}'"
        )


def test_replace_excluding_basement_terms_first_pass_cleaning():
    connection = duckdb.connect()
    test_cases = [
        ("EXCLUDING BASEMENT FLAT 1 2 ROAD", "EXCL_BSMT FLAT 1 2 ROAD"),
        ("EXCLUDING PART BASEMENT FLAT 1 2 ROAD", "EXCL_PT_BSMT FLAT 1 2 ROAD"),
        (
            "HSE EXC BST FLAT 1 2 ROAD",
            "HOUSE EXCL_BSMT FLAT 1 2 ROAD",
        ),
    ]

    input_relation = connection.sql(
        "SELECT * FROM (VALUES "
        + ",".join(f"('{address}')" for address, _ in test_cases)
        + ") AS t(clean_full_address)"
    )

    result = _run_single_stage(
        _clean_address_string_first_pass, input_relation, connection
    )
    rows = result.fetchall()

    for (address, expected), row in zip(test_cases, rows):
        assert row[0] == expected, (
            f"Address '{address}' expected '{expected}' but got '{row[0]}'"
        )


@pytest.mark.skip(reason="Peeling logic removed from cleaning steps")
def test_peel_common_uk_end_tokens():
    connection = duckdb.connect()

    # Format: (input_address, expected_clean_address, expected_peeled_tokens)
    # The peeled tokens are returned in reverse order (most recently peeled first)
    test_cases = [
        # Single city at end - should be peeled
        (
            "FLAT 5 SASKIA HOUSE 87-91 HACKNEY ROAD LONDON",
            "FLAT 5 SASKIA HOUSE 87-91 HACKNEY ROAD",
            ["LONDON"],
        ),
        # Multiple tokens to peel - city then region
        (
            "10 HIGH STREET MANCHESTER GREATER MANCHESTER",
            "10 HIGH STREET",
            ["MANCHESTER", "GREATER MANCHESTER"],
        ),
        # Multi-token pattern (GREATER LONDON) then UK
        (
            "5 PARK LANE LONDON GREATER LONDON UK",
            "5 PARK LANE",
            ["LONDON", "GREATER LONDON", "UK"],
        ),
        # London borough then London then Greater London
        (
            "25 MAIN ROAD HACKNEY LONDON GREATER LONDON",
            "25 MAIN ROAD",
            ["HACKNEY", "LONDON", "GREATER LONDON"],
        ),
        # County at end
        (
            "THE OLD RECTORY CHURCH LANE HERTFORDSHIRE",
            "THE OLD RECTORY CHURCH LANE",
            ["HERTFORDSHIRE"],
        ),
        # Multiple counties/regions
        (
            "15 STATION ROAD LEEDS WEST YORKSHIRE",
            "15 STATION ROAD",
            ["LEEDS", "WEST YORKSHIRE"],
        ),
        # Nothing to peel - unique end token
        (
            "42 ACACIA AVENUE SPRINGFIELD",
            "42 ACACIA AVENUE SPRINGFIELD",
            [],
        ),
        # Don't peel if the token appears mid-address (only peel from end)
        # HACKNEY appears in the middle, ROAD is the end token (not in list)
        (
            "87-91 HACKNEY ROAD",
            "87-91 HACKNEY ROAD",
            [],
        ),
        # Welsh address
        (
            "10 HIGH STREET CARDIFF SOUTH WALES",
            "10 HIGH STREET",
            ["CARDIFF", "SOUTH WALES"],
        ),
        # Scottish address
        (
            "5 PRINCES STREET EDINBURGH SCOTLAND",
            "5 PRINCES STREET",
            ["EDINBURGH", "SCOTLAND"],
        ),
        # Long chain of localities
        (
            "1 TEST ROAD LEWISHAM LONDON GREATER LONDON ENGLAND UK",
            "1 TEST ROAD",
            ["LEWISHAM", "LONDON", "GREATER LONDON", "ENGLAND", "UK"],
        ),
        # County with -SHIRE suffix
        (
            "ROSE COTTAGE MILL LANE CAMBRIDGESHIRE",
            "ROSE COTTAGE MILL LANE",
            ["CAMBRIDGESHIRE"],
        ),
        # Address ending with just a county town (not the county)
        (
            "50 MARKET SQUARE CAMBRIDGE",
            "50 MARKET SQUARE",
            ["CAMBRIDGE"],
        ),
        # West/East Sussex distinction
        (
            "12 SEAFRONT PARADE BRIGHTON EAST SUSSEX",
            "12 SEAFRONT PARADE",
            ["BRIGHTON", "EAST SUSSEX"],
        ),
        # Northern cities
        (
            "99 INDUSTRIAL ESTATE NEWCASTLE TYNE AND WEAR",
            "99 INDUSTRIAL ESTATE",
            ["NEWCASTLE", "TYNE AND WEAR"],
        ),
        # Midlands
        (
            "UNIT 5 BUSINESS PARK BIRMINGHAM WEST MIDLANDS",
            "UNIT 5 BUSINESS PARK",
            ["BIRMINGHAM", "WEST MIDLANDS"],
        ),
        # Address with postcode-like ending (shouldn't affect peeling)
        (
            "3 LONDON ROAD OXFORD",
            "3 LONDON ROAD",
            ["OXFORD"],
        ),
    ]

    # Build input - need ukam_address_id for the QUALIFY clause
    values = ",".join(
        f"('{address}', '{address}', '{i}')"
        for i, (address, _, _) in enumerate(test_cases)
    )
    input_relation = connection.sql(
        f"SELECT * FROM (VALUES {values}) AS t(clean_full_address, original_address_concat, ukam_address_id)"
    )

    result = _run_single_stage(_peel_common_uk_end_tokens, input_relation, connection)
    rows = result.fetchall()
    columns = result.columns
    peeled_idx = columns.index("peeled_tokens_list")
    id_idx = columns.index("ukam_address_id")

    # Build lookup by ukam_address_id since row order is not guaranteed
    results_by_id = {row[id_idx]: row for row in rows}

    for i, (input_address, expected_clean, expected_peeled) in enumerate(test_cases):
        row = results_by_id[str(i)]
        actual_peeled = row[peeled_idx] if row[peeled_idx] else []

        # The clean_full_address doesn't change in this stage - we just add common_end_tokens
        # So we check the peeled tokens
        assert list(actual_peeled) == expected_peeled, (
            f"Address '{input_address}' expected peeled tokens {expected_peeled} "
            f"but got {list(actual_peeled)}"
        )


def test_parse_out_business_unit():
    """Test business unit parsing for commercial addresses.

    Business addresses often have unit identifiers (UNIT, SUITE, OFFICE, etc.)
    that distinguish different tenants within the same building.
    """
    connection = duckdb.connect()

    # Format: (input_address, business_unit_type, business_unit_id, has_business_unit)
    test_cases = [
        # UNIT patterns
        ("UNIT C 32 PARKHALL BUSINESS CENTRE 40 MARTELL ROAD", "UNIT", "C", True),
        ("UNIT F 32 PARKHALL BUSINESS CENTRE 40 MARTELL ROAD", "UNIT", "F", True),
        ("UNIT 5 ENTERPRISE PARK BIRMINGHAM", "UNIT", "5", True),
        ("UNIT 5A INDUSTRIAL ESTATE", "UNIT", "5A", True),
        ("UNIT A5 BUSINESS CENTRE", "UNIT", "A5", True),
        ("UNITS 1-3 WAREHOUSE COMPLEX", "UNIT", "1", True),  # Plural form
        # SUITE patterns
        ("SUITE 100 TOWER HOUSE", "SUITE", "100", True),
        ("SUITE A GROUND FLOOR OFFICE BUILDING", "SUITE", "A", True),
        ("SUITE 2B COMMERCIAL CENTRE", "SUITE", "2B", True),
        # OFFICE patterns
        ("OFFICE 5 RIVERSIDE BUSINESS PARK", "OFFICE", "5", True),
        ("OFFICE A FIRST FLOOR 20 HIGH STREET", "OFFICE", "A", True),
        # WORKSHOP patterns
        ("WORKSHOP 3 INDUSTRIAL UNITS", "WORKSHOP", "3", True),
        ("WORKSHOP B THE YARD", "WORKSHOP", "B", True),
        # WAREHOUSE patterns
        ("WAREHOUSE A LOGISTICS PARK", "WAREHOUSE", "A", True),
        ("WAREHOUSE 7 DISTRIBUTION CENTRE", "WAREHOUSE", "7", True),
        # STUDIO patterns (common for creative/media businesses)
        ("STUDIO 4 CREATIVE QUARTER", "STUDIO", "4", True),
        ("STUDIO B ARTS COMPLEX", "STUDIO", "B", True),
        # Non-business addresses (should not match)
        ("FLAT A 15 HIGH STREET", None, None, False),
        ("123 MAIN ROAD LONDON", None, None, False),
        ("THE COTTAGE VILLAGE LANE", None, None, False),
        # Edge cases
        ("UNIT UNKNOWN ADDRESS", None, None, False),  # UNIT without ID
        ("MY OFFICE IS HERE 10 STREET", None, None, False),  # OFFICE in wrong context
    ]

    input_relation = connection.sql(
        "SELECT * FROM (VALUES "
        + ",".join(f"('{address}', '{address}')" for address, _, _, _ in test_cases)
        + ") AS t(clean_full_address, original_address_concat)"
    )

    result = _run_single_stage(_parse_out_business_unit, input_relation, connection)
    rows = result.fetchall()
    columns = result.columns
    type_idx = columns.index("business_unit_type")
    id_idx = columns.index("business_unit_id")
    indicator_idx = columns.index("has_business_unit")

    for (address, expected_type, expected_id, expected_indicator), row in zip(
        test_cases, rows
    ):
        assert row[type_idx] == expected_type, (
            f"Address '{address}' expected type '{expected_type}' "
            f"but got '{row[type_idx]}'"
        )
        assert row[id_idx] == expected_id, (
            f"Address '{address}' expected id '{expected_id}' but got '{row[id_idx]}'"
        )
        assert row[indicator_idx] == expected_indicator, (
            f"Address '{address}' expected has_business_unit={expected_indicator} "
            f"but got {row[indicator_idx]}"
        )
