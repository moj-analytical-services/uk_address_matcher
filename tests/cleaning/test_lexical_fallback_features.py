from uk_address_matcher.cleaning.steps.term_frequencies import (
    _first_unusual_token,
    _use_first_unusual_token_if_no_numeric_token,
)
from uk_address_matcher.cleaning.steps.tokenisation import (
    _add_lexical_fallback_features,
)
from uk_address_matcher.sql_pipeline.runner import create_sql_pipeline


def test_lexical_fallback_feature_is_only_populated_for_empty_numeric_tokens(duck_con):
    addresses = duck_con.sql("""
        SELECT * FROM (VALUES
            ('word', 'POLLINICK', []::VARCHAR[]),
            ('numbered', '31', ['31']::VARCHAR[]),
            ('alphanumeric', 'B2', []::VARCHAR[]),
            ('unknown_numbers', 'CHURCH', NULL::VARCHAR[])
        ) AS rows(id, numeric_token_1, numeric_tokens)
    """)

    result = _add_lexical_fallback_features(addresses)
    result = _add_lexical_fallback_features(result)

    fallback_by_id = {
        row[0]: row[1] for row in result.project("id, lexical_fallback_token").fetchall()
    }

    assert fallback_by_id == {
        "word": "POLLINICK",
        "numbered": None,
        "alphanumeric": None,
        "unknown_numbers": None,
    }
    assert result.columns.count("lexical_fallback_token") == 1
    assert "lexical_postcode_key" not in result.columns


def test_unusual_token_moves_to_lexical_column_without_changing_numeric_tokens(
    duck_con,
):
    addresses = duck_con.sql("""
        SELECT * FROM (VALUES
            (
                'numberless',
                NULL::VARCHAR,
                []::VARCHAR[],
                NULL::DOUBLE,
                [
                    struct_pack(tok := 'POLLINICK', rel_freq := 0.0001),
                    struct_pack(tok := 'HOUSE', rel_freq := 0.25)
                ]
            ),
            (
                'legacy_numberless',
                'POLLINICK',
                []::VARCHAR[],
                0.0001,
                [
                    struct_pack(tok := 'POLLINICK', rel_freq := 0.0001),
                    struct_pack(tok := 'HOUSE', rel_freq := 0.25)
                ]
            ),
            (
                'numbered',
                '31',
                ['31']::VARCHAR[],
                0.75,
                [
                    struct_pack(tok := 'ROAD', rel_freq := 0.0001),
                    struct_pack(tok := 'HOUSE', rel_freq := 0.25)
                ]
            ),
            (
                'unknown_numbers',
                NULL::VARCHAR,
                NULL::VARCHAR[],
                NULL::DOUBLE,
                [
                    struct_pack(tok := 'POLLINICK', rel_freq := 0.0001),
                    struct_pack(tok := 'HOUSE', rel_freq := 0.25)
                ]
            )
        ) AS rows(
            id,
            numeric_token_1,
            numeric_tokens,
            tf_numeric_token_1,
            token_rel_freq_arr
        )
    """)
    pipeline = create_sql_pipeline(
        con=duck_con,
        input_rel=addresses,
        stage_specs=[
            _first_unusual_token,
            _use_first_unusual_token_if_no_numeric_token,
        ],
    )
    result = pipeline.run()

    values_by_id = {
        row[0]: row[1:]
        for row in result.project("""
            id,
            numeric_tokens,
            numeric_token_1,
            tf_numeric_token_1,
            lexical_fallback_token,
            list_transform(token_rel_freq_arr, lambda x: x.tok)
        """).fetchall()
    }

    assert values_by_id == {
        "numberless": ([], None, None, "POLLINICK", ["HOUSE"]),
        "legacy_numberless": ([], None, None, "POLLINICK", ["HOUSE"]),
        "numbered": (["31"], "31", 0.75, None, ["ROAD", "HOUSE"]),
        "unknown_numbers": (None, None, None, None, ["POLLINICK", "HOUSE"]),
    }
