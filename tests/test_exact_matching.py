import pytest

from uk_address_matcher.linking_model.exact_matching import run_deterministic_match_pass

pytestmark = pytest.mark.skip(reason="Temporarily skipped during refactoring")


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
                    '4 SAMPLE STREET',
                    '4 SAMPLE STREET',
                    'CC3 3CC',
                    ARRAY['4', 'sample', 'street'],
                    1::BIGINT
                ),
                (
                    10,
                    '4 SAMPLE STREET',
                    '4 SAMPLE STREET',
                    'CC3 3CC',
                    ARRAY['4', 'sample', 'street'],
                    2::BIGINT
                ),
                (
                    2,
                    '5 DEMO RD',
                    '5 DEMO RD',
                    'DD4 4DD',
                    ARRAY['5', 'demo', 'rd'],
                    3::BIGINT
                ),
                (
                    2,
                    '5 DEMO RD',
                    '5 DEMO RD',
                    'DD4 4DD',
                    ARRAY['5', 'demo', 'rd'],
                    4::BIGINT
                ),
                (
                    2,
                    '5 DEMO ROAD',
                    '5 DEMO ROAD',
                    'DD4 4DD',
                    ARRAY['5', 'demo', 'road'],
                    5::BIGINT
                ),
                (
                    2,
                    '5 DEMO ROAD',
                    '5 DEMO ROAD',
                    'DD4 4DD',
                    ARRAY['5', 'demo', 'road'],
                    6::BIGINT
                ),
                (
                    2,
                    '4 SAMPLE ST',
                    '4 SAMPLE ST',
                    'CC3 3CC',
                    ARRAY['4', 'sample', 'st'],
                    7::BIGINT
                ),
                (
                    3,
                    '999 MYSTERY LANE',
                    '999 MYSTERY LANE',
                    'EE5 5EE',
                    ARRAY['999', 'mystery', 'lane'],
                    8::BIGINT
                )
        ) AS t(
            unique_id,
            original_address_concat,
            clean_full_address,
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
                    '4 SAMPLE STREET',
                    '4 SAMPLE STREET',
                    'CC3 3CC',
                    ARRAY['4', 'sample', 'street'],
                    1
                ),
                (
                    2000,
                    '5 DEMO RD',
                    '5 DEMO RD',
                    'DD4 4DD',
                    ARRAY['5', 'demo', 'road'],
                    2
                )
        ) AS t(
            unique_id,
            original_address_concat,
            clean_full_address,
            postcode,
            address_tokens,
            ukam_address_id
        )
        """
    )

    return df_fuzzy, df_canonical


# When a non-unique unique_id field exists in our fuzzy addresses,
# the trie stage will inflate our row count (due to the output and required
# joins). This test checks confirms that this issue does not occur.
# We've resolved this issue by implementing a ukam_address_id surrogate key
# to guarantee uniqueness of the input records.
@pytest.mark.parametrize(
    "enabled_stages",
    [
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
