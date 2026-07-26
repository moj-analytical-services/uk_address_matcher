import logging

import duckdb

from uk_address_matcher.cleaning import chunking_strategies
from uk_address_matcher.cleaning.chunking_strategies import prepare_data_for_matching
from uk_address_matcher.cleaning.steps import (
    _parse_out_business_unit,
    _parse_out_flat_position_and_letter,
    _parse_out_sub_premise_location,
    _remove_duplicate_end_tokens,
    _separate_distinguishing_start_tokens_from_with_respect_to_adjacent_records,
)
from uk_address_matcher.sql_pipeline.runner import DebugOptions, DuckDBPipeline


def _run_single_stage(stage_factory, input_relation, connection):
    pipeline = DuckDBPipeline(connection, input_relation)
    pipeline.add_step(stage_factory())
    return pipeline.run(DebugOptions(pretty_print_sql=False))


def test_separate_distinguishing_tokens_uses_valid_local_neighbours():
    connection = duckdb.connect()
    input_relation = connection.sql(
        """
        SELECT * FROM (VALUES
            (1, 'A1', 'FLAT A 1 HIGH STREET CAMDEN LONDON', 'preserved-a'),
            (2, 'A2', '1 HIGH STREET CAMDEN LONDON', 'preserved-b'),
            (3, 'B1', 'OLD STATION HOUSE RAINBOW LANE TAUNTON', 'preserved-c'),
            (4, 'B2', 'NEW STATION RAINBOW LANE TAUNTON', 'preserved-d'),
            (5, 'C1', '9 SOLO ROAD YORK', 'preserved-e')
        ) AS t(ukam_address_id, unique_id, clean_full_address, source_marker)
        """
    )

    result = _run_single_stage(
        _separate_distinguishing_start_tokens_from_with_respect_to_adjacent_records,
        input_relation,
        connection,
    )
    assert result.columns.count("ukam_address_id") == 1
    actual = {
        unique_id: (distinguishing, common, source_marker)
        for unique_id, distinguishing, common, source_marker in result.project(
            """
            unique_id,
            distinguishing_adj_start_tokens,
            common_adj_start_tokens,
            source_marker
            """
        ).fetchall()
    }

    assert actual == {
        "A1": (
            ["FLAT", "A"],
            ["1", "HIGH", "STREET", "CAMDEN", "LONDON"],
            "preserved-a",
        ),
        "A2": ([], ["1", "HIGH", "STREET", "CAMDEN", "LONDON"], "preserved-b"),
        "B1": (
            ["OLD", "STATION", "HOUSE"],
            ["RAINBOW", "LANE", "TAUNTON"],
            "preserved-c",
        ),
        "B2": (
            ["NEW", "STATION"],
            ["RAINBOW", "LANE", "TAUNTON"],
            "preserved-d",
        ),
        "C1": ([], ["9", "SOLO", "ROAD", "YORK"], "preserved-e"),
    }


def test_prepare_data_derives_distinguishing_tokens_across_cleaning_chunks(
    monkeypatch,
):
    connection = duckdb.connect()
    candidate_addresses = [
        f"FLAT {letter} 1 HIGH STREET CAMDEN LONDON" for letter in ("A", "B", "C", "D")
    ]
    partitioned_addresses = connection.sql(
        "SELECT address, abs(hash(address)) % 2 AS partition FROM (VALUES "
        + ", ".join(f"('{address}')" for address in candidate_addresses)
        + ") AS candidates(address)"
    ).fetchall()
    first_address = partitioned_addresses[0][0]
    first_partition = partitioned_addresses[0][1]
    second_address = next(
        address
        for address, partition in partitioned_addresses[1:]
        if partition != first_partition
    )
    input_relation = connection.sql(
        f"""
        SELECT * FROM (VALUES
            ('A1', '{first_address}', 'N1 1AA'),
            ('A2', '{second_address}', 'N1 1AA')
        ) AS t(unique_id, address_concat, postcode)
        """
    )

    monkeypatch.setattr(
        chunking_strategies,
        "_calculate_chunk_size",
        lambda total_records, num_of_chunks: 1,
    )
    result = prepare_data_for_matching(
        input_relation,
        con=connection,
        num_of_chunks=2,
        derive_distinguishing_wrt_adjacent_records=True,
        dataset_role="canonical",
        show_progress=False,
    )

    rows = result.project(
        "unique_id, distinguishing_adj_start_tokens, common_adj_start_tokens"
    ).fetchall()
    assert len(rows) == 2
    assert all(distinguishing for _, distinguishing, _ in rows)
    assert all(
        common == ["1", "HIGH", "STREET", "CAMDEN", "LONDON"] for _, _, common in rows
    )


def test_separate_distinguishing_tokens_skips_same_id_to_offset_three():
    connection = duckdb.connect()
    input_relation = connection.sql(
        """
        SELECT * FROM (VALUES
            (1, 'U4', 'DELTA LANE TAUNTON'),
            (2, 'U3', 'ALPHA RAINBOW LANE TAUNTON'),
            (3, 'U3', 'ALPHB RAINBOW LANE TAUNTON'),
            (4, 'U3', 'ALPHC RAINBOW LANE TAUNTON')
        ) AS t(ukam_address_id, unique_id, clean_full_address)
        """
    )

    result = _run_single_stage(
        _separate_distinguishing_start_tokens_from_with_respect_to_adjacent_records,
        input_relation,
        connection,
    )
    target = (
        result.filter("clean_full_address = 'ALPHC RAINBOW LANE TAUNTON'")
        .project("distinguishing_adj_start_tokens, common_adj_start_tokens")
        .fetchone()
    )

    assert target == (["ALPHC", "RAINBOW"], ["LANE", "TAUNTON"])


def test_parse_out_flat_positional():
    connection = duckdb.connect()

    def _sql_literal(value):
        if value is None:
            return "NULL"
        return "'" + str(value).replace("'", "''") + "'"

    # Format of test cases:
    # (input_address, flat_positional, flat_letter, flat_number)
    # Note: When a number+letter pattern exists (e.g., 11A, 15B),
    # only the LETTER is a flat determinant.
    # The number is the building/house number, not a flat identifier
    test_cases = [
        ("11A SPITFIRE COURT BIRMINGHAM", None, "A", None),
        ("FLAT A 11 SPITFIRE COURT BIRMINGHAM", None, "A", None),
        ("BASEMENT FLAT A 11 SPITFIRE COURT BIRMINGHAM", "BASEMENT", "A", None),
        (
            "BASEMENT FLAT 11 243 SPITFIRE COURT BIRMINGHAM",
            "BASEMENT",
            None,
            "11",
        ),
        ("GARDEN FLAT 11 243 SPITFIRE COURT BIRMINGHAM", "GARDEN", None, "11"),
        ("TOP FLOOR FLAT 12A HIGH STREET", "TOP FLOOR", "A", "12"),
        ("SECOND FLOOR FLAT 12 A HIGH STREET", "SECOND FLOOR", "A", "12"),
        ("GROUND FLOOR FLAT B 25 MAIN ROAD", "GROUND FLOOR", "B", None),
        ("FIRST FLOOR 15B LONDON ROAD", "FIRST FLOOR", "B", None),
        ("FLAT C MY HOUSE 120 MY ROAD", None, "C", None),
        ("FLAT 2 733 GIPSY HILL", None, None, "2"),
        (
            "2 7 GIPSY HILL",
            None,
            None,
            None,
        ),  # Ambiguous - no explicit FLAT indicator
        ("773 GIPSY HILL", None, None, None),
        ("FLAT C SECOND FLOOR 27 OK ROAD", "SECOND FLOOR", "C", None),
        ("FLAT A GROUND FLOOR 18 RAVENSWOOD STREET", "GROUND FLOOR", "A", None),
        ("FLAT 3/2 41 DUMMY ROAD", None, None, "2"),
        ("FLAT THE CROWN TESTING ROAD", None, None, None),
        (
            "FLAT 12A HIGH STREET",
            None,
            "A",
            "12",
        ),
        (
            "FLAT 2B 10 KINGS ROAD LONDON",
            None,
            "B",
            "2",
        ),
        (
            "15B LONDON ROAD",
            None,
            "B",
            None,
        ),  # digit+letter: letter is flat determinant, not the number
        (
            "BASEMENT 15B LONDON ROAD",
            "BASEMENT",
            "B",
            None,
        ),  # floor + digit+letter
        (
            "FLAT A MY HOUSE 120-122 SOME ROAD",
            None,
            "A",
            None,
        ),
        # Only one number - indicates that it's a house number, not flat number
        ("UPPER FLOOR FLAT 120 TEST", "UPPER FLOOR", None, None),
        # LOWER GROUND variants (canonicalised to GROUND FLOOR)
        ("FLAT LOWER GROUND 35 ATOP THE HILL LONDON", "GROUND FLOOR", None, None),
        ("LOWER GROUND FLAT 35 ATOP THE HILL LONDON", "GROUND FLOOR", None, None),
        ("LOWER GROUND 35 ATOP THE HILL LONDON", "GROUND FLOOR", None, None),
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
        + ",".join(
            "(" + ", ".join([_sql_literal(address), _sql_literal(address)]) + ")"
            for address, _, _, _ in test_cases
        )
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

    for (
        address,
        expected_pos,
        expected_letter,
        expected_number,
    ), row in zip(test_cases, rows):
        assert row[positional_idx] == expected_pos, (
            f"Address '{address}' expected positional '{expected_pos}' "
            f"but got '{row[positional_idx]}'"
        )
        assert row[letter_idx] == expected_letter, (
            f"Address '{address}' expected letter '{expected_letter}' "
            f"but got '{row[letter_idx]}'"
        )
        assert row[number_idx] == expected_number, (
            f"Address '{address}' expected number '{expected_number}' "
            f"but got '{row[number_idx]}'"
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
            f"Address '{address}' expected has_flat_indicator '{expected_indicator}' "
            f"but got '{row[indicator_idx]}'"
        )


def test_parse_out_sub_premise_location():
    connection = duckdb.connect()

    test_cases = [
        ("GROUND FLOOR RIGHT FLAT 4 UFTON ROAD LONDON", "RIGHT"),
        ("GROUND FLOOR LEFT FLAT 4 UFTON ROAD LONDON", "LEFT"),
        ("FLAT SECOND FLOOR CENTRE 5 MORESBY ROAD LONDON", "CENTRE"),
        ("FLAT FIRST FLOOR FRONT 176 LOWER CLAPTON ROAD LONDON", "FRONT"),
        ("FLAT FIRST FLOOR REAR 176 LOWER CLAPTON ROAD LONDON", "REAR"),
        (
            "MAISONETTE BASEMENT AND GROUND FLOOR RIGHT 46 LANSDOWNE DRIVE LONDON",
            "RIGHT",
        ),
        ("COMMERCIAL CENTRE 40 MARTELL ROAD", None),
        ("GROUND FLOOR FLAT 4 UFTON ROAD LONDON", None),
    ]

    input_relation = connection.sql(
        "SELECT * FROM (VALUES "
        + ",".join(f"('{address}', '{address}')" for address, _expected in test_cases)
        + ") AS t(clean_full_address, original_address_concat)"
    )

    pipeline = DuckDBPipeline(connection, input_relation)
    pipeline.add_step(_parse_out_flat_position_and_letter())
    pipeline.add_step(_parse_out_sub_premise_location())
    result = pipeline.run(DebugOptions(pretty_print_sql=False))

    location_idx = result.columns.index("sub_premise_location")
    rows = result.fetchall()

    for (address, expected_location), row in zip(test_cases, rows):
        assert row[location_idx] == expected_location, (
            f"Address '{address}' expected location '{expected_location}' "
            f"but got '{row[location_idx]}'"
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


def test_supplied_postcode_does_not_strip_postcode_like_floor_tokens():
    connection = duckdb.connect()
    input_relation = connection.sql(
        """
        SELECT * FROM (VALUES
            ('a', 'FLAT D1 2ND FLR EXAMPLE HOUSE LONDON', 'SE1 2AB', '1'),
            ('b', 'UNIT Q3 2ND FLR EXAMPLE WORKS LONDON', 'SE1 2AB', '2'),
            ('c', 'FLAT A 3RD FLR EXAMPLE HOUSE LONDON', 'SE1 2AB', '3'),
            ('d', '10 DOWNING STREET SW1A 2AA', 'SW1A 2AA', '4'),
            ('e', '10 DOWNING STREET SW1A 2AA', NULL, '5')
        ) AS t(unique_id, address_concat, postcode, ukam_label)
        """
    )

    cleaned = prepare_data_for_matching(
        input_relation,
        con=connection,
        num_of_chunks=1,
        dataset_role="messy",
    )

    actual = (
        cleaned.project(
            """
            unique_id,
            postcode,
            clean_full_address
            """
        )
        .order("unique_id")
        .fetchall()
    )

    assert actual == [
        (
            "a",
            "SE1 2AB",
            "FLAT D 1 SECOND FLOOR EXAMPLE HOUSE LONDON",
        ),
        (
            "b",
            "SE1 2AB",
            "UNIT Q 3 SECOND FLOOR EXAMPLE WORKS LONDON",
        ),
        (
            "c",
            "SE1 2AB",
            "FLAT A THIRD FLOOR EXAMPLE HOUSE LONDON",
        ),
        ("d", "SW1A 2AA", "10 DOWNING STREET"),
        ("e", "SW1A 2AA", "10 DOWNING STREET"),
    ]


def test_prepare_data_progress_off_suppresses_stage_status_logs(caplog):
    connection = duckdb.connect()
    input_relation = connection.sql(
        """
        SELECT * FROM (VALUES
            ('1', '10 DOWNING STREET', 'SW1A 2AA')
        ) AS t(unique_id, address_concat, postcode)
        """
    )

    with caplog.at_level(logging.DEBUG, logger="uk_address_matcher"):
        cleaned = prepare_data_for_matching(
            input_relation,
            con=connection,
            show_progress="off",
        )

    assert cleaned.count("*").fetchone()[0] == 1
    stage_prefixes = ("Cleaning and preprocessing", "Applying term frequencies")
    assert not any(
        record.getMessage().startswith(stage_prefix)
        and (
            " records across " in record.getMessage()
            or " completed:" in record.getMessage()
        )
        for record in caplog.records
        for stage_prefix in stage_prefixes
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
