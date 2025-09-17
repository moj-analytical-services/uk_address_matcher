import duckdb

from uk_address_matcher.cleaning.cleaning_steps import (
    parse_out_flat_position_and_letter,
    remove_duplicate_end_tokens,
)
from uk_address_matcher.cleaning.pipeline import DuckDBPipeline


def _run_single_stage(stage_factory, input_relation, connection):
    pipeline = DuckDBPipeline(connection, input_relation)
    pipeline.add_step(stage_factory())
    return pipeline.run(pretty_print_sql=False)


def test_parse_out_flat_positional():
    connection = duckdb.connect()
    test_cases = [
        (
            "11A SPITFIRE COURT 243 BIRMINGHAM",
            None,
            "A",
        ),
        (
            "FLAT A 11 SPITFIRE COURT 243 BIRMINGHAM",
            None,
            "A",
        ),
        (
            "BASEMENT FLAT A 11 SPITFIRE COURT 243 BIRMINGHAM",
            "BASEMENT",
            "A",
        ),
        (
            "BASEMENT FLAT 11 SPITFIRE COURT 243 BIRMINGHAM",
            "BASEMENT",
            None,
        ),
        (
            "GARDEN FLAT 11 SPITFIRE COURT 243 BIRMINGHAM",
            "GARDEN",
            None,
        ),
        (
            "TOP FLOOR FLAT 12A HIGH STREET",
            "TOP FLOOR",
            "A",
        ),
        (
            "GROUND FLOOR FLAT B 25 MAIN ROAD",
            "GROUND FLOOR",
            "B",
        ),
        (
            "FIRST FLOOR 15B LONDON ROAD",
            "FIRST FLOOR",
            "B",
        ),
        (
            "UNIT C MY HOUSE 120 MY ROAD",
            None,
            "C",
        ),
    ]

    input_relation = connection.sql(
        "SELECT * FROM (VALUES "
        + ",".join(f"('{address}')" for address, _, _ in test_cases)
        + ") AS t(address_concat)"
    )

    result = _run_single_stage(
        parse_out_flat_position_and_letter, input_relation, connection
    )
    rows = result.fetchall()

    for (address, expected_pos, expected_letter), row in zip(test_cases, rows):
        assert row[-2] == expected_pos, (
            f"Address '{address}' expected positional '{expected_pos}' but got '{row[-2]}'"
        )
        assert row[-1] == expected_letter, (
            f"Address '{address}' expected letter '{expected_letter}' but got '{row[-1]}'"
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
        + ") AS t(address_concat)"
    )

    result = _run_single_stage(remove_duplicate_end_tokens, input_relation, connection)
    rows = result.fetchall()

    for (address, expected), row in zip(test_cases, rows):
        assert row[0] == expected, (
            f"Address '{address}' expected '{expected}' but got '{row[0]}'"
        )
