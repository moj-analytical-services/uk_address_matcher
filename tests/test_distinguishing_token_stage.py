import pytest

from uk_address_matcher import DistinguishingTokenStage
from uk_address_matcher.sql_pipeline.match_reasons import MatchReason


def test_distinguishing_token_stage_applies_safe_gap_rules(duck_con):
    messy = duck_con.sql("""
        SELECT * FROM (VALUES
            (1, 'A B C', 'AA1 1AA'),
            (2, 'A X Y B C', 'AA1 1AA'),
            (3, 'A X B Y C', 'AA1 1AA'),
            (4, 'A B X Y C', 'AA1 1AA'),
            (5, 'A X Y Z B C', 'AA1 1AA'),
            (6, 'A 7 B C', 'AA1 1AA'),
            (7, 'A HOUSE B C', 'AA1 1AA'),
            (8, 'A C B', 'AA1 1AA'),
            (9, 'A B C', 'AA1 1AB'),
            (10, 'OTHER A B C', 'AA1 1AA')
        ) AS rows(ukam_address_id, clean_full_address, postcode)
    """)
    canonical = duck_con.sql("""
        SELECT * FROM (VALUES
            (101, 'canonical-a', 'A B C', 'AA1 1AA', ['A']::VARCHAR[])
        ) AS rows(
            ukam_address_id,
            unique_id,
            clean_full_address,
            postcode,
            distinguishing_adj_start_tokens
        )
    """)

    matches = DistinguishingTokenStage().find_matches(
        con=duck_con,
        stage_name="distinguishingtokenstage",
        df_unmatched=messy,
        df_canonical=canonical,
    )

    assert matches is not None
    assert matches.order("ukam_address_id").fetchall() == [
        (1, 101, "canonical-a", MatchReason.DISTINGUISHING_TOKEN.value),
        (2, 101, "canonical-a", MatchReason.DISTINGUISHING_TOKEN.value),
        (3, 101, "canonical-a", MatchReason.DISTINGUISHING_TOKEN.value),
        (4, 101, "canonical-a", MatchReason.DISTINGUISHING_TOKEN.value),
    ]


def test_distinguishing_token_stage_rejects_ambiguous_canonical_ids(duck_con):
    messy = duck_con.sql("""
        SELECT * FROM (VALUES
            (1, 'A B C', 'AA1 1AA'),
            (2, 'D E F', 'DD1 1DD')
        ) AS rows(ukam_address_id, clean_full_address, postcode)
    """)
    canonical = duck_con.sql("""
        SELECT * FROM (VALUES
            (101, 'canonical-a', 'A B C', 'AA1 1AA', ['A']::VARCHAR[]),
            (102, 'canonical-b', 'A B C', 'AA1 1AA', ['A']::VARCHAR[]),
            (103, 'canonical-d', 'D E F', 'DD1 1DD', ['D']::VARCHAR[]),
            (104, 'canonical-d', 'D E F', 'DD1 1DD', ['D']::VARCHAR[])
        ) AS rows(
            ukam_address_id,
            unique_id,
            clean_full_address,
            postcode,
            distinguishing_adj_start_tokens
        )
    """)

    matches = DistinguishingTokenStage().find_matches(
        con=duck_con,
        stage_name="distinguishingtokenstage",
        df_unmatched=messy,
        df_canonical=canonical,
    )

    assert matches is not None
    assert matches.fetchall() == [
        (2, 103, "canonical-d", MatchReason.DISTINGUISHING_TOKEN.value)
    ]


def test_distinguishing_token_stage_requires_prepared_prefix(duck_con):
    messy = duck_con.sql("""
        SELECT 1 AS ukam_address_id, 'A B C' AS clean_full_address,
            'AA1 1AA' AS postcode
    """)
    canonical = duck_con.sql("""
        SELECT 101 AS ukam_address_id, 'canonical-a' AS unique_id,
            'A B C' AS clean_full_address, 'AA1 1AA' AS postcode
    """)

    with pytest.raises(
        ValueError,
        match="derive_distinguishing_wrt_adjacent_records=True",
    ):
        DistinguishingTokenStage().find_matches(
            con=duck_con,
            stage_name="distinguishingtokenstage",
            df_unmatched=messy,
            df_canonical=canonical,
        )
