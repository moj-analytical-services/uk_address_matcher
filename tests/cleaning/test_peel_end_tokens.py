import duckdb
import pytest

from uk_address_matcher.cleaning.steps.normalisation import _peel_common_uk_end_tokens
from uk_address_matcher.sql_pipeline.runner import DebugOptions, DuckDBPipeline


@pytest.fixture
def connection():
    """Create a fresh DuckDB connection for each test."""
    return duckdb.connect()


def _run_peel_single(
    address: str,
    con: duckdb.DuckDBPyConnection,
    fuzzy_threshold: int = 0,
) -> list:
    """Run peeling on a single address and return the peeled tokens."""
    input_relation = con.sql(
        f"SELECT '{address}' AS clean_full_address, "
        f"'{address}' AS original_address_concat, "
        "'0' AS ukam_address_id"
    )
    pipeline = DuckDBPipeline(con, input_relation)
    pipeline.add_step(_peel_common_uk_end_tokens(fuzzy_threshold=fuzzy_threshold))
    result = pipeline.run(DebugOptions(pretty_print_sql=False))
    row = result.fetchone()
    return list(row[result.columns.index("peeled_tokens_list")] or [])


# --- Exact matching tests (run by default) ---
@pytest.mark.skip(reason="Peeling logic removed from cleaning steps")
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
    """Test exact matching of end tokens (default behaviour)."""
    assert _run_peel_single(address, connection, fuzzy_threshold=0) == expected


# --- Fuzzy matching tests (requires fuzzy_threshold=1) ---
# Note: Fuzzy matching only works on tokens with 4+ characters to avoid false positives
# Uses pre-computed typo variants (deletions, transpositions) for O(1) lookup
@pytest.mark.skip(reason="Peeling logic removed from cleaning steps")
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
        # Short tokens (< 4 chars) block peeling - UC doesn't match, so LONDON isn't reached
        ("10 HIGH STREET LONDON UC", []),  # UC blocks further peeling
        (
            "25 MAIN ROAD HACKENY LONDON GREATER LONDON",
            ["HACKNEY", "LONDON", "GREATER LONDON"],
        ),
        ("10 HIGH STREET LONXYZ", []),  # too distant - no match
    ],
)
def test_peel_end_tokens_fuzzy(connection, address, expected):
    """Test fuzzy matching of end tokens (typos with edit distance <= 1)."""
    assert _run_peel_single(address, connection, fuzzy_threshold=1) == expected


@pytest.mark.skip(reason="Peeling logic removed from cleaning steps")
def test_multi_token_fuzzy_not_supported(connection):
    """Multi-token fuzzy (e.g. GREATOR LONDON) only peels exact matches.

    This documents current behaviour - fuzzy only works on single-token patterns.
    """
    peeled = _run_peel_single(
        "5 PARK LANE LONDON GREATOR LONDON", connection, fuzzy_threshold=1
    )
    # Only exact "LONDON" matches are peeled, not "GREATOR LONDON" as "GREATER LONDON"
    assert len(peeled) >= 1
    assert "LONDON" in peeled


@pytest.mark.skip(reason="Peeling logic removed from cleaning steps")
def test_fuzzy_threshold_above_1_raises_error():
    """fuzzy_threshold > 1 is not supported and should raise ValueError."""
    with pytest.raises(ValueError, match="fuzzy_threshold=2 is not supported"):
        _peel_common_uk_end_tokens(fuzzy_threshold=2)
