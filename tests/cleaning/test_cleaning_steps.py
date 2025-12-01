import duckdb

from uk_address_matcher.cleaning.steps import (
    _parse_out_flat_position_and_letter,
    _remove_duplicate_end_tokens,
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
        ("BASEMENT FLAT 11 SPITFIRE COURT 243 BIRMINGHAM", "BASEMENT", None, "11"),
        ("GARDEN FLAT 11 SPITFIRE COURT 243 BIRMINGHAM", "GARDEN", None, "11"),
        ("TOP FLOOR FLAT 12A HIGH STREET", "TOP FLOOR", "A", None),
        ("SECOND FLOOR FLAT 12 A HIGH STREET", "SECOND FLOOR", "A", None),
        ("GROUND FLOOR FLAT B 25 MAIN ROAD", "GROUND FLOOR", "B", None),
        ("FIRST FLOOR 15B LONDON ROAD", "FIRST FLOOR", "B", None),
        ("FLAT C MY HOUSE 120 MY ROAD", None, "C", None),
        ("FLAT 2 69 GIPSY HILL", None, None, "2"),
        ("2 69 GIPSY HILL", None, None, "2"),
        ("69 GIPSY HILL", None, None, None),
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
