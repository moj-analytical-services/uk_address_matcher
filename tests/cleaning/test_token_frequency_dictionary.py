import pytest

from uk_address_matcher.cleaning.pipelines import (
    QUEUE_POST_TF,
    _clean_data_using_precomputed_rel_tok_freq,
    _create_term_frequency_tables,
)
from uk_address_matcher.cleaning.steps.term_frequencies import (
    _add_numeric_term_frequencies_using_registered_df,
    _add_term_frequencies_to_address_tokens_using_registered_df,
)
from uk_address_matcher.sql_pipeline.runner import create_sql_pipeline


def _input(con):
    return con.sql("""
        SELECT *, NULL::VARCHAR AS numeric_token_1,
            NULL::VARCHAR AS numeric_token_2, NULL::VARCHAR AS numeric_token_3
        FROM (VALUES
            (1, ['BETA', 'ALPHA', 'BETA', 'UNKNOWN', NULL, 'NULL_FREQ']),
            (2, ['ZERO', 'ALPHA']), (3, []), (4, NULL),
            (5, [NULL, NULL]), (6, ['UNKNOWN', 'LONDON', 'LONDON']),
            (NULL, ['ALPHA'])
        ) AS addresses(ukam_address_id, address_without_numbers_tokenised)
    """)


def _stage_result(con, source, use_enum_lookup, post_tf=False):
    stages = [
        _add_term_frequencies_to_address_tokens_using_registered_df(
            use_enum_lookup=use_enum_lookup
        )
    ]
    if post_tf:
        stages += [_add_numeric_term_frequencies_using_registered_df, *QUEUE_POST_TF]
    return create_sql_pipeline(con, source, stages).run()


def _assert_equal(actual, expected):
    assert actual.columns == expected.columns
    assert actual.types == expected.types
    assert actual.order("ukam_address_id").fetchall() == (
        expected.order("ukam_address_id").fetchall()
    )


@pytest.mark.parametrize("post_tf", [False, True])
@pytest.mark.parametrize("lookup", ["default", "custom", "empty"])
def test_dictionary_preserves_token_features(duck_con, lookup, post_tf):
    custom = duck_con.sql("""
        SELECT * FROM (VALUES
            ('ALPHA', 0.25::DOUBLE), ('BETA', 0.25), ('ZERO', 0),
            ('NULL_FREQ', NULL), ('LONDON', 0.01), (NULL, 0.9)
        ) AS frequencies(token, rel_freq)
    """)
    if lookup == "empty":
        custom = custom.limit(0)
    _create_term_frequency_tables(duck_con, None if lookup == "default" else custom)
    source = _input(duck_con)
    expected = _stage_result(duck_con, source, False, post_tf)
    actual = _stage_result(duck_con, source, True, post_tf)
    _assert_equal(actual, expected)
    assert [row[0] for row in actual.order("ukam_address_id").fetchall()] == [1, 2, 5, 6]


def test_duplicate_custom_lookup_keeps_join_based_semantics(duck_con):
    lookup = duck_con.sql("""
        SELECT * FROM (VALUES ('ALPHA', 0.1::DOUBLE), ('ALPHA', 0.2))
        AS frequencies(token, rel_freq)
    """)
    _create_term_frequency_tables(duck_con, lookup)
    source = _input(duck_con)
    actual = _clean_data_using_precomputed_rel_tok_freq(
        source, duck_con, pre_cleaned_addresses=True
    )
    expected = _stage_result(duck_con, source, False, post_tf=True)
    _assert_equal(actual, expected)
    assert duck_con.table("__ukam__tmp_dense_rel_tok_freq").fetchone()[1] is False


def test_dictionary_is_rebuilt_and_handles_wide_enum_codes(duck_con):
    source = duck_con.sql("""
        SELECT 1 AS ukam_address_id,
            ['TOKEN_00255', 'TOKEN_65535', 'TOKEN_65536'] AS
                address_without_numbers_tokenised
    """)
    for count in [65537, 1]:
        lookup = duck_con.sql(f"""
            SELECT 'TOKEN_' || lpad(i::VARCHAR, 5, '0') AS token,
                i::DOUBLE / 100000 AS rel_freq
            FROM range({count}) AS generated(i)
        """)
        _create_term_frequency_tables(duck_con, lookup)
        _assert_equal(
            _stage_result(duck_con, source, True),
            _stage_result(duck_con, source, False),
        )


@pytest.mark.parametrize("collation", ["column", "default"])
def test_collated_custom_lookup_keeps_join_based_semantics(duck_con, collation):
    if collation == "default":
        duck_con.execute("SET default_collation = 'nocase'")
    token_expr = "'alpha'::VARCHAR" + (" COLLATE nocase" if collation == "column" else "")
    lookup = duck_con.sql(f"SELECT {token_expr} AS token, 0.25::DOUBLE AS rel_freq")
    _create_term_frequency_tables(duck_con, lookup)
    source = _input(duck_con)
    actual = _clean_data_using_precomputed_rel_tok_freq(
        source, duck_con, pre_cleaned_addresses=True
    )
    _assert_equal(actual, _stage_result(duck_con, source, False, post_tf=True))
    assert duck_con.table("__ukam__tmp_dense_rel_tok_freq").fetchone()[1] is False
    frequencies = (
        _stage_result(duck_con, source, False)
        .filter("ukam_address_id = 2")
        .fetchone()[-1]
    )
    assert frequencies[1] == {"tok": "ALPHA", "rel_freq": 0.25}
