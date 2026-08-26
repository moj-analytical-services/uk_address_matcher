import duckdb
import pytest

from uk_address_matcher.linking_model.matching.stages.peeled import (
    _build_suffix_peel_regex_sql_literal,
)


@pytest.fixture
def connection():
    """Create a fresh DuckDB connection for each test."""
    return duckdb.connect()


def _run_peel_single(address: str, con: duckdb.DuckDBPyConnection) -> tuple[str, bool]:
    """Run regex-based peeling on a single address."""
    pattern_sql = _build_suffix_peel_regex_sql_literal()
    escaped_address = address.replace("'", "''")

    row = con.sql(
        f"""
        WITH input AS (
            SELECT
                regexp_replace(
                    upper(trim('{escaped_address}')),
                    '\\s+',
                    ' ',
                    'g'
                ) AS address
        )
        SELECT
            trim(regexp_replace(address, '{pattern_sql}', '')) AS peeled_address,
            trim(regexp_replace(address, '{pattern_sql}', '')) <> address AS did_peel
        FROM input
        """
    ).fetchone()
    return row[0], row[1]


@pytest.mark.parametrize(
    "address,expected,expected_did_peel",
    [
        ("10 HIGH STREET LONDON", "10 HIGH STREET", True),
        (
            "10 HIGH STREET MANCHESTER GREATER MANCHESTER",
            "10 HIGH STREET",
            True,
        ),
        (
            "5 PARK LANE LONDON GREATER LONDON UK",
            "5 PARK LANE LONDON GREATER LONDON UK",
            False,
        ),
        (
            "25 MAIN ROAD HACKNEY LONDON GREATER LONDON",
            "25 MAIN ROAD",
            True,
        ),
        (
            "1 HIGH STREET LONDON BOROUGH OF BEXLEY",
            "1 HIGH STREET",
            True,
        ),
        (
            "2 HIGH STREET COUNTY BOROUGH OF BLAENAU GWENT",
            "2 HIGH STREET",
            True,
        ),
        (
            "3 HIGH STREET CITY OF EDINBURGH",
            "3 HIGH STREET",
            True,
        ),
        (
            "4 HIGH STREET CITY AND COUNTY OF SWANSEA",
            "4 HIGH STREET",
            True,
        ),
        (
            "5 HIGH STREET ROYAL BOROUGH OF KENSINGTON AND CHELSEA",
            "5 HIGH STREET",
            True,
        ),
        (
            "5 HIGH STREET LONDON BOROUGH OF HAMMERSMITH AND FULHAM",
            "5 HIGH STREET",
            True,
        ),
        (
            "5 HIGH STREET LONDON BOROUGH OF BARKING AND DAGENHAM",
            "5 HIGH STREET",
            True,
        ),
        (
            "5 HIGH STREET LONDON BOROUGH OF RICHMOND UPON THAMES",
            "5 HIGH STREET",
            True,
        ),
        (
            "6 HIGH STREET CITY OF LONDON",
            "6 HIGH STREET",
            True,
        ),
        (
            "7 HIGH STREET ROYAL BOROUGH OF KINGSTON UPON THAMES",
            "7 HIGH STREET",
            True,
        ),
        ("3 HIGH STREET WALTHAMSTOW", "3 HIGH STREET", True),
        (
            "THE OLD RECTORY CHURCH LANE HERTFORDSHIRE",
            "THE OLD RECTORY CHURCH LANE",
            True,
        ),
        (
            "1 TEST ROAD LEWISHAM LONDON GREATER LONDON ENGLAND UK",
            "1 TEST ROAD LEWISHAM LONDON GREATER LONDON ENGLAND UK",
            False,
        ),
        ("42 ACACIA AVENUE SPRINGFIELD", "42 ACACIA AVENUE SPRINGFIELD", False),
        ("87-91 HACKNEY ROAD", "87-91 HACKNEY ROAD", False),
        ("LONDON", "", True),
        ("", "", False),
    ],
)
def test_peel_end_tokens_exact(connection, address, expected, expected_did_peel):
    """Exact end-token peeling works as expected."""
    assert _run_peel_single(address, connection) == (expected, expected_did_peel)


def test_regex_peeling_does_not_handle_fuzzy_typoes(connection):
    """Regex peeling intentionally requires exact token matches."""
    peeled_address, did_peel = _run_peel_single("10 HIGH STREET LONDN", connection)
    assert peeled_address == "10 HIGH STREET LONDN"
    assert did_peel is False


def test_regex_peeling_handles_long_suffix_chains(connection):
    """Regex peeling removes chained suffix tokens in one pass."""
    peeled_address, did_peel = _run_peel_single(
        "1 TEST ROAD LONDON GREATER LONDON CITY OF LONDON",
        connection,
    )
    assert peeled_address == "1 TEST ROAD"
    assert did_peel is True
