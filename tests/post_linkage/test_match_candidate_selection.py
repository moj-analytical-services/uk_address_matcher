import pytest

from uk_address_matcher.post_linkage.match_candidate_selection import (
    select_top_match_candidates,
)
from uk_address_matcher.sql_pipeline.match_reasons import MatchReason


@pytest.fixture
def canonical_addresses_small(duck_con):
    return duck_con.sql(
        """
        SELECT *
        FROM (
            VALUES
                (100, '10 DOWNING STREET', 'SW1A 2AA'),
                (101, '10 DOWNING STREET ANNEX', 'SW1A 2AA'),
                (102, '11 DOWNING STREET', 'SW1A 2AA'),
                (103, '12 DOWNING STREET', 'SW1A 2AA')
        ) AS t(unique_id, original_address_concat, postcode)
        """
    )


@pytest.fixture
def exact_matches_with_duplicates(duck_con):
    exact_reason = MatchReason.EXACT.value
    return duck_con.sql(
        f"""
        SELECT *
        FROM (
            VALUES
                (1, '10 Downing St', 'SW1A 2AA', '{exact_reason}', 100),
                (2, '11 Downing St', 'SW1A 2AA', '{exact_reason}', 102),
                (3, '12 Downing St', 'SW1A 2AA', '{exact_reason}', 103)
        ) AS t(
            unique_id,
            original_address_concat,
            postcode,
            match_reason,
            resolved_canonical_id
        )
        """
    )


@pytest.fixture
def splink_candidates_with_duplicates(duck_con):
    return duck_con.sql(
        """
        SELECT *
        FROM (
            VALUES
                (1, 100, 0.85, 5.0),
                (1, 101, 0.85, 9.5),
                (1, 102, 0.85, 9.5),
                (3, 103, 0.92, 3.0),
                (3, 101, 0.91, 8.0)
        ) AS t(unique_id_r, unique_id_l, match_weight, distinguishability)
        """
    )


@pytest.fixture
def exact_match_only_relation(duck_con):
    exact_reason = MatchReason.EXACT.value
    return duck_con.sql(
        f"""
        SELECT *
        FROM (
            VALUES
                (999, '99 Downing St', 'SW1A 2AA', '{exact_reason}', 103)
        ) AS t(
            unique_id,
            original_address_concat,
            postcode,
            match_reason,
            resolved_canonical_id
        )
        """
    )


@pytest.fixture
def empty_splink_matches(duck_con):
    return duck_con.sql(
        """
        SELECT *
        FROM (
            SELECT
                CAST(NULL AS BIGINT) AS unique_id_r,
                CAST(NULL AS BIGINT) AS unique_id_l,
                CAST(NULL AS DOUBLE) AS match_weight,
                CAST(NULL AS DOUBLE) AS distinguishability
        )
        WHERE 1 = 0
        """
    )


def test_select_top_match_candidates_prefers_highest_ranked_splink_match(
    duck_con,
    canonical_addresses_small,
    exact_matches_with_duplicates,
    splink_candidates_with_duplicates,
):
    result = select_top_match_candidates(
        con=duck_con,
        df_exact_matches=exact_matches_with_duplicates,
        df_splink_matches=splink_candidates_with_duplicates,
        df_canonical=canonical_addresses_small,
        match_weight_threshold=-100.0,
        distinguishability_threshold=None,
    )

    rows = result.filter("unique_id = 1").to_df()
    assert len(rows) == 1
    assert rows.iloc[0]["resolved_canonical_id"] == 101
    assert rows.iloc[0]["match_reason"] == MatchReason.SPLINK.value

    deterministic_only_row = result.filter("unique_id = 2").to_df()
    assert len(deterministic_only_row) == 1
    assert deterministic_only_row.iloc[0]["resolved_canonical_id"] == 102
    assert deterministic_only_row.iloc[0]["match_reason"] == MatchReason.EXACT.value

    third_row = result.filter("unique_id = 3").to_df()
    assert len(third_row) == 1
    assert third_row.iloc[0]["resolved_canonical_id"] == 103
    assert third_row.iloc[0]["match_reason"] == MatchReason.SPLINK.value


def test_select_top_match_candidates_handles_empty_splink_relation(
    duck_con,
    canonical_addresses_small,
    exact_match_only_relation,
    empty_splink_matches,
):
    result = select_top_match_candidates(
        con=duck_con,
        df_exact_matches=exact_match_only_relation,
        df_splink_matches=empty_splink_matches,
        df_canonical=canonical_addresses_small,
        match_weight_threshold=5.0,
        distinguishability_threshold=None,
    )

    # Here, we are basically testing that no errors are raised and the process
    # completes, despite the Splink matches being empty.
    rows = result.order("unique_id")
    assert rows.count("*").fetchall()[0][0] == 1
    assert rows.select("match_weight").fetchall()[0][0] is None
    assert rows.select("distinguishability").fetchall()[0][0] is None
