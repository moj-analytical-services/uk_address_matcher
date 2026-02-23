import re

import duckdb

from uk_address_matcher.linking_model.matching.stages.peeled import (
    _build_suffix_peel_regex_sql_literal,
)


def _normalise(address: str) -> str:
    return " ".join(address.strip().upper().split())


def _python_peel(address: str) -> str:
    sql_literal = _build_suffix_peel_regex_sql_literal()
    regex_pattern = sql_literal.replace("''", "'").replace("\\\\", "\\")
    normalised = _normalise(address)
    return re.sub(regex_pattern, "", normalised).strip()


def _duckdb_peel(con: duckdb.DuckDBPyConnection, address: str) -> str:
    pattern_sql = _build_suffix_peel_regex_sql_literal()
    escaped_address = address.replace("'", "''")
    row = con.sql(
        f"""
        SELECT trim(
            regexp_replace(
                regexp_replace(upper(trim('{escaped_address}')), '\\s+', ' ', 'g'),
                '{pattern_sql}',
                ''
            )
        ) AS peeled
        """
    ).fetchone()
    return row[0]


def test_regex_sql_and_python_reference_are_equivalent(duck_con):
    addresses = [
        "10 HIGH STREET LONDON",
        "200 PARK AVENUE LONDON GREATER LONDON",
        "50 MAIN ROAD TUNBRIDGE WELLS",
        "75 OAK DRIVE",
        "10 TEST LANE HACKNEY LONDON",
        "1 TEST ROAD LONDON GREATER LONDON WEST MIDLANDS",
        "LONG PREFIX TOKEN TOKEN TOKEN TOKEN TOKEN TOKEN WEST MIDLANDS",
        "LONDON",
        "",
    ]

    for address in addresses:
        assert _duckdb_peel(duck_con, address) == _python_peel(address)


def test_regex_sql_and_python_reference_are_equivalent_on_synthetic_batch(duck_con):
    rows = duck_con.sql(
        """
        WITH base AS (
            SELECT
                i,
                CASE
                    WHEN i % 5 = 0 THEN 'UNIT ' || i::VARCHAR || ' LONDON'
                    WHEN i % 5 = 1 THEN 'UNIT ' || i::VARCHAR || ' LONDON GREATER LONDON'
                    WHEN i % 5 = 2 THEN 'UNIT ' || i::VARCHAR || ' TUNBRIDGE WELLS'
                    WHEN i % 5 = 3 THEN 'UNIT ' || i::VARCHAR || ' WEST MIDLANDS'
                    ELSE 'UNIT ' || i::VARCHAR
                END AS address
            FROM range(5000) AS t(i)
        )
        SELECT address
        FROM base
        """
    ).fetchall()

    pattern_sql = _build_suffix_peel_regex_sql_literal()
    duck_rows = duck_con.sql(
        f"""
        WITH base AS (
            SELECT
                i,
                CASE
                    WHEN i % 5 = 0 THEN 'UNIT ' || i::VARCHAR || ' LONDON'
                    WHEN i % 5 = 1 THEN 'UNIT ' || i::VARCHAR || ' LONDON GREATER LONDON'
                    WHEN i % 5 = 2 THEN 'UNIT ' || i::VARCHAR || ' TUNBRIDGE WELLS'
                    WHEN i % 5 = 3 THEN 'UNIT ' || i::VARCHAR || ' WEST MIDLANDS'
                    ELSE 'UNIT ' || i::VARCHAR
                END AS address
            FROM range(5000) AS t(i)
        )
        SELECT
            address,
            trim(
                regexp_replace(
                    regexp_replace(upper(trim(address)), '\\s+', ' ', 'g'),
                    '{pattern_sql}',
                    ''
                )
            ) AS peeled
        FROM base
        """
    ).fetchall()

    python_rows = [(address, _python_peel(address)) for (address,) in rows]

    assert duck_rows == python_rows
