from uk_address_matcher import PostcodeFreeMatchStage
from uk_address_matcher.linking_model.matching.runner import _run_matching
from uk_address_matcher.sql_pipeline.match_reasons import MatchReason


def _canonical(duck_con):
    return duck_con.sql("""
        SELECT *
        FROM (
            VALUES
                (
                    10, '1001', '10 EXAMPLE ROAD TOWNTON',
                    ['10', 'EXAMPLE', 'ROAD', 'TOWNTON'], ['10']
                ),
                (
                    11, '1001', '10 EXAMPLE ROAD TOWNTON',
                    ['10', 'EXAMPLE', 'ROAD', 'TOWNTON'], ['10']
                ),
                (
                    20, '2001', '20 CONFLICT ROAD TOWNTON',
                    ['20', 'CONFLICT', 'ROAD', 'TOWNTON'], ['20']
                ),
                (
                    21, '2002', '20 CONFLICT ROAD TOWNTON',
                    ['20', 'CONFLICT', 'ROAD', 'TOWNTON'], ['20']
                ),
                (
                    30, '3001', '30 ROAD TOWNTON',
                    ['30', 'ROAD', 'TOWNTON'], ['30']
                ),
                (
                    40, '4001', 'NO NUMBER ROAD TOWNTON',
                    ['NO', 'NUMBER', 'ROAD', 'TOWNTON'], []
                )
        ) AS rows(
            ukam_address_id, unique_id, clean_full_address,
            clean_full_address_tokens, numeric_tokens
        )
    """)


def test_postcode_free_matches_only_unique_eligible_blank_postcodes(duck_con):
    messy = duck_con.sql("""
        SELECT *
        FROM (
            VALUES
                (1, '10 EXAMPLE ROAD TOWNTON', NULL),
                (2, '20 CONFLICT ROAD TOWNTON', ''),
                (3, '30 ROAD TOWNTON', NULL),
                (4, 'NO NUMBER ROAD TOWNTON', ' '),
                (5, '10 EXAMPLE ROAD TOWNTON', 'AA1 1AA'),
                (6, '50 UNKNOWN ROAD TOWNTON', NULL)
        ) AS rows(ukam_address_id, clean_full_address, postcode)
    """)

    matches = PostcodeFreeMatchStage().find_matches(
        duck_con,
        "postcode_free",
        messy,
        _canonical(duck_con),
    )

    assert matches is not None
    assert matches.fetchall() == [
        (
            1,
            "1001",
            10,
            MatchReason.POSTCODE_FREE_FULL.value,
            None,
            None,
        )
    ]


def test_postcode_free_returns_none_without_blank_postcodes(duck_con):
    messy = duck_con.sql("""
        SELECT 1 AS ukam_address_id, '10 EXAMPLE ROAD TOWNTON'
            AS clean_full_address, 'AA1 1AA' AS postcode
    """)

    assert (
        PostcodeFreeMatchStage().find_matches(
            duck_con,
            "postcode_free",
            messy,
            _canonical(duck_con),
        )
        is None
    )


def test_postcode_free_reason_updates_matching_results(duck_con):
    messy = duck_con.sql("""
        SELECT 1 AS ukam_address_id, 'source-1' AS unique_id,
               '10 EXAMPLE ROAD TOWNTON' AS clean_full_address,
               '10 Example Road Townton' AS original_address_concat,
               NULL::VARCHAR AS postcode
    """)
    canonical = _canonical(duck_con).select("*, 'AA1 1AA' AS postcode")

    matches, diagnostics = _run_matching(
        duck_con,
        messy,
        canonical,
        stages=[PostcodeFreeMatchStage()],
    )

    assert matches is not None
    assert matches.select(
        "resolved_canonical_id, match_reason::VARCHAR AS match_reason"
    ).fetchall() == [
        ("1001", MatchReason.POSTCODE_FREE_FULL.value),
    ]
    assert len(diagnostics) == 1
    assert diagnostics[0]["stage"] == "postcodefreematchstage"
    assert diagnostics[0]["unmatched_before"] == 1
    assert diagnostics[0]["matched_this_stage"] == 1
    assert diagnostics[0]["elapsed_seconds"] >= 0
