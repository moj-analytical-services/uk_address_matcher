import duckdb
import pytest

from uk_address_matcher.linking_model.matching.stages.peeled import (
    PEEL_ITERATIONS,
    _build_peel_ctes,
    _load_peeling_lookup_sql,
)
from uk_address_matcher.sql_pipeline.runner import DebugOptions, DuckDBPipeline
from uk_address_matcher.sql_pipeline.steps import CTEStep, pipeline_stage


@pytest.fixture
def connection():
    """Create a fresh DuckDB connection for each test."""
    return duckdb.connect()


def _run_peel_single(address: str, con: duckdb.DuckDBPyConnection) -> list[str]:
    """Run peeled-stage token peeling on a single address."""

    @pipeline_stage(stage_output="peeled_for_test")
    def _peel_for_test():
        peel_steps, final_name = _build_peel_ctes(
            prefix="test",
            source_placeholder="input",
        )
        return [
            CTEStep("uk_end_tokens_lookup", _load_peeling_lookup_sql()),
            *peel_steps,
            CTEStep(
                "peeled_for_test",
                f"""
                SELECT
                    ukam_address_id,
                    clean_full_address,
                    peeled_tokens_list
                FROM {{{final_name}}}
                """,
            ),
        ]

    input_relation = con.sql(
        f"SELECT '{address}' AS clean_full_address, "
        f"'{address}' AS original_address_concat, "
        "'E1 1AA' AS postcode, "
        "'0' AS ukam_address_id"
    )

    pipeline = DuckDBPipeline(con, input_relation)
    pipeline.add_step(_peel_for_test())
    result = pipeline.run(DebugOptions(pretty_print_sql=False))

    row = result.fetchone()
    return list(row[result.columns.index("peeled_tokens_list")] or [])


@pytest.mark.parametrize(
    "address,expected",
    [
        ("10 HIGH STREET LONDON", ["LONDON"]),
        (
            "10 HIGH STREET MANCHESTER GREATER MANCHESTER",
            ["MANCHESTER", "GREATER MANCHESTER"],
        ),
        ("5 PARK LANE LONDON GREATER LONDON UK", ["LONDON", "GREATER LONDON", "UK"]),
        (
            "25 MAIN ROAD HACKNEY LONDON GREATER LONDON",
            ["HACKNEY", "LONDON", "GREATER LONDON"],
        ),
        ("THE OLD RECTORY CHURCH LANE HERTFORDSHIRE", ["HERTFORDSHIRE"]),
        (
            "1 TEST ROAD LEWISHAM LONDON GREATER LONDON ENGLAND UK",
            ["LEWISHAM", "LONDON", "GREATER LONDON", "ENGLAND", "UK"],
        ),
        ("42 ACACIA AVENUE SPRINGFIELD", []),  # nothing to peel
        ("87-91 HACKNEY ROAD", []),  # mid-address token not peeled
        ("LONDON", ["LONDON"]),  # single token that is a city
        ("", []),  # empty address
    ],
)
def test_peel_end_tokens_exact(connection, address, expected):
    """Exact end-token peeling works as expected."""
    assert _run_peel_single(address, connection) == expected


@pytest.mark.skip(
    reason="Fuzzy typo handling is under review and may be deprecated soon."
)
@pytest.mark.parametrize(
    "address,expected",
    [
        ("10 HIGH STREET LONDN", ["LONDON"]),  # deletion
        ("10 HIGH STREET CARDIF", ["CARDIFF"]),  # deletion
        ("15 STATION ROAD MANCHSTER", ["MANCHESTER"]),  # deletion
        ("5 PARK LANE LONDNO", ["LONDON"]),  # transposition
        ("UNIT 5 BUSINESS PARK BIRMINGAHM", ["BIRMINGHAM"]),  # transposition
        ("THE OLD RECTORY HERTFORDSHRIE", ["HERTFORDSHIRE"]),  # transposition
        ("5 PRINCES STREET EDINBRUGH", ["EDINBURGH"]),  # transposition
        # Short tokens (< 4 chars) block peeling;
        # UC doesn't match, so LONDON isn't reached.
        ("10 HIGH STREET LONDON UC", []),  # UC blocks further peeling
        (
            "25 MAIN ROAD HACKENY LONDON GREATER LONDON",
            ["HACKNEY", "LONDON", "GREATER LONDON"],
        ),
        ("10 HIGH STREET LONXYZ", []),  # too distant - no match
    ],
)
def test_peel_end_tokens_lookup_supports_fuzzy_single_token_typoes(
    connection,
    address,
    expected,
):
    """Lookup SQL supports edit-distance-1 single-token typo matching."""
    assert _run_peel_single(address, connection) == expected


def test_lookup_keys_are_distinct_by_lookup_key(connection):
    """Final lookup table has one row per lookup_key."""
    row = connection.sql(
        """
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT lookup_key) AS distinct_lookup_keys
        FROM (
            {lookup_sql}
        ) AS lookup
        """.format(lookup_sql=_load_peeling_lookup_sql())
    ).fetchone()

    assert row[0] == row[1]


def test_exact_lookup_key_is_preferred_over_fuzzy_variant(connection):
    """Exact key wins when multiple candidates could map to same lookup key."""
    row = connection.sql(
        """
        SELECT pattern, token_count
        FROM (
            {lookup_sql}
        ) AS lookup
        WHERE lookup_key = 'LONDON'
        """.format(lookup_sql=_load_peeling_lookup_sql())
    ).fetchone()

    assert row == ("LONDON", 1)


@pytest.mark.skip(
    reason="Multi-token fuzzy peeling may be removed, so keep this on hold."
)
def test_multi_token_fuzzy_not_supported(connection):
    """Multi-token fuzzy (e.g. GREATOR LONDON) only peels exact matches."""
    peeled = _run_peel_single("5 PARK LANE LONDON GREATOR LONDON", connection)
    assert peeled == ["LONDON"]


def test_peeling_is_capped_by_iteration_limit(connection):
    """Peeling depth is capped at PEEL_ITERATIONS."""
    peeled = _run_peel_single(
        "1 TEST ROAD LONDON UK ENGLAND LONDON UK ENGLAND",
        connection,
    )
    assert len(peeled) == PEEL_ITERATIONS
