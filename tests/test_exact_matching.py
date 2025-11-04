import pytest

from uk_address_matcher.linking_model.exact_matching import run_deterministic_match_pass
from uk_address_matcher.sql_pipeline.match_reasons import MatchReason


@pytest.fixture
def test_data(duck_con):
    """Set up test data as DuckDB PyRelations for exact matching tests."""
    df_fuzzy = duck_con.sql(
        """
        SELECT *
        FROM (
            VALUES
                (
                    1,
                    '4 Sample Street',
                    'CC3 3CC',
                    ARRAY['4', 'sample', 'street'],
                    1::BIGINT
                ),
                (
                    10,
                    '4 Sample Street',
                    'CC3 3CC',
                    ARRAY['4', 'sample', 'street'],
                    2::BIGINT
                ),
                (
                    2,
                    '5 Demo Rd',
                    'DD4 4DD',
                    ARRAY['5', 'demo', 'rd'],
                    3::BIGINT
                ),
                (
                    2,
                    '5 Demo Rd',
                    'DD4 4DD',
                    ARRAY['5', 'demo', 'rd'],
                    4::BIGINT
                ),
                (
                    2,
                    '5 Demo Road',
                    'DD4 4DD',
                    ARRAY['5', 'demo', 'road'],
                    5::BIGINT
                ),
                (
                    2,
                    '5 Demo Road',
                    'DD4 4DD',
                    ARRAY['5', 'demo', 'road'],
                    6::BIGINT
                ),
                (
                    2,
                    '4 Sample St',
                    'CC3 3CC',
                    ARRAY['4', 'sample', 'st'],
                    7::BIGINT
                ),
                (
                    3,
                    '999 Mystery Lane',
                    'EE5 5EE',
                    ARRAY['999', 'mystery', 'lane'],
                    8::BIGINT
                )
        ) AS t(
            unique_id,
            original_address_concat,
            postcode,
            address_tokens,
            ukam_address_id
        )
        """
    )

    df_canonical = duck_con.sql(
        """
        SELECT *
        FROM (
            VALUES
                (
                    1000,
                    '4 Sample Street',
                    'CC3 3CC',
                    ARRAY['4', 'sample', 'street'],
                    1
                ),
                (
                    2000,
                    '5 Demo Rd',
                    'DD4 4DD',
                    ARRAY['5', 'demo', 'road'],
                    2
                )
        ) AS t(
            unique_id,
            original_address_concat,
            postcode,
            address_tokens,
            ukam_address_id
        )
        """
    )

    return df_fuzzy, df_canonical


def test_unmatched_records_retain_original_unique_id(duck_con, test_data):
    df_fuzzy, df_canonical = test_data

    results = run_deterministic_match_pass(
        duck_con,
        df_fuzzy,
        df_canonical,
        enabled_stage_names=["resolve_with_trie"],
    )

    columns = set(results.columns)
    assert "unique_id" in columns
    assert "resolved_canonical_id" in columns
    assert "fuzzy_unique_id" not in columns
    assert "exact_match_canonical_id_bigint" not in columns
    assert "trie_match_unique_id_bigint" not in columns
    assert "resolved_canonical_unique_id_bigint" not in columns

    rows = (
        results.project(
            "unique_id, resolved_canonical_id, match_reason, ukam_address_id"
        )
        .order("ukam_address_id")
        .fetchall()
    )
    assert rows == [
        (1, 1000, MatchReason.EXACT.value, 1),
        (10, 1000, MatchReason.EXACT.value, 2),
        (2, 2000, MatchReason.EXACT.value, 3),
        (2, 2000, MatchReason.EXACT.value, 4),
        (2, 2000, MatchReason.TRIE.value, 5),
        (2, 2000, MatchReason.TRIE.value, 6),
        (2, 1000, MatchReason.TRIE.value, 7),
        (3, None, None, 8),
    ]


# When a non-unique unique_id field exists in our fuzzy addresses,
# the trie stage will inflate our row count (due to the output and required
# joins). This test checks confirms that this issue does not occur.
# We've resolved this issue by implementing a ukam_address_id surrogate key
# to guarantee uniqueness of the input records.
@pytest.mark.parametrize(
    "enabled_stages",
    [
        ["resolve_with_trie"],  # Exact + trie
        None,  # Exact only
    ],
)
def test_trie_stage_does_not_inflate_row_count(duck_con, enabled_stages, test_data):
    df_fuzzy, df_canonical = test_data

    results = run_deterministic_match_pass(
        duck_con,
        df_fuzzy,
        df_canonical,
        enabled_stage_names=enabled_stages,
    )

    input_row_count = df_fuzzy.count("*").fetchone()[0]
    total_rows = results.count("*").fetchone()[0]
    output_ids = results.order("ukam_address_id").project("ukam_address_id").fetchall()
    input_ids = df_fuzzy.order("ukam_address_id").project("ukam_address_id").fetchall()

    assert total_rows == input_row_count, (
        "Deterministic pipeline should not change row count; "
        f"expected {input_row_count}, got {total_rows}"
    )
    assert output_ids == input_ids, "Pipeline must preserve ukam_address_id coverage"
