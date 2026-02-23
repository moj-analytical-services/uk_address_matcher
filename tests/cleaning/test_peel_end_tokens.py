import duckdb
import pytest

from uk_address_matcher.linking_model.matching.stages.peeled import (
    MAX_PEELED_WORDS,
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


def _run_peel_single(address: str, con: duckdb.DuckDBPyConnection) -> dict[str, object]:
    """Run peeled-stage peeling on a single address and return key outputs."""

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
                    peeled_word_count,
                    did_peel,
                    peeled_address
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
    return {
        "peeled_word_count": row[result.columns.index("peeled_word_count")],
        "did_peel": row[result.columns.index("did_peel")],
        "peeled_address": row[result.columns.index("peeled_address")],
    }


@pytest.mark.parametrize(
    "address,expected",
    [
        ("10 HIGH STREET LONDON", ["LONDON"]),
        (
            "10 HIGH STREET MANCHESTER GREATER MANCHESTER",
            ["MANCHESTER", "GREATER MANCHESTER"],
        ),
        (
            "5 PARK LANE LONDON GREATER LONDON UK",
            [],
        ),
        (
            "25 MAIN ROAD HACKNEY LONDON GREATER LONDON",
            ["HACKNEY", "LONDON", "GREATER LONDON"],
        ),
        ("THE OLD RECTORY CHURCH LANE HERTFORDSHIRE", ["HERTFORDSHIRE"]),
        (
            "1 TEST ROAD LEWISHAM LONDON GREATER LONDON ENGLAND UK",
            [],
        ),
        ("42 ACACIA AVENUE SPRINGFIELD", []),  # nothing to peel
        ("87-91 HACKNEY ROAD", []),  # mid-address token not peeled
        ("LONDON", ["LONDON"]),  # single token that is a city
        ("", []),  # empty address
    ],
)
def test_peel_end_tokens_exact(connection, address, expected):
    """Exact end-token peeling works as expected."""
    result = _run_peel_single(address, connection)
    assert result["peeled_word_count"] == sum(len(token.split(" ")) for token in expected)
    assert result["did_peel"] == (len(expected) > 0)


@pytest.mark.parametrize(
    "address",
    [
        "10 HIGH STREET LONDN",
        "10 HIGH STREET CARDIF",
        "15 STATION ROAD MANCHSTER",
        "5 PARK LANE LONDNO",
        "THE OLD RECTORY HERTFORDSHRIE",
    ],
)
def test_peel_end_tokens_exact_only_no_fuzzy_typo_support(connection, address):
    """Lookup is exact-only and no longer peels typo variants."""
    result = _run_peel_single(address, connection)
    assert result["peeled_word_count"] == 0


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
    """Exact key is present and mapped to itself."""
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


def test_multi_token_fuzzy_not_supported(connection):
    """Multi-token fuzzy is unsupported, but exact trailing tokens can still peel."""
    result = _run_peel_single("5 PARK LANE LONDON GREATOR LONDON", connection)
    assert result["peeled_word_count"] == 1


def test_country_tokens_not_peeled_in_this_stage(connection):
    """Country/high-level denomination tokens are excluded from peeling."""
    result = _run_peel_single("10 HIGH STREET UNITED KINGDOM", connection)
    assert result["peeled_word_count"] == 0


def test_peeling_is_capped_by_iteration_limit(connection):
    """Peeling depth is capped by total peeled words."""
    result = _run_peel_single(
        "1 TEST ROAD LONDON UK ENGLAND LONDON UK ENGLAND",
        connection,
    )
    assert result["peeled_word_count"] <= MAX_PEELED_WORDS
    assert result["peeled_word_count"] <= PEEL_ITERATIONS
