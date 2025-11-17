import pandas as pd
import pytest

from uk_address_matcher.post_linkage.match_candidate_selection import (
    _prepare_splink_candidates,
    select_top_match_candidates,
)
from uk_address_matcher.sql_pipeline.match_reasons import MatchReason
from uk_address_matcher.sql_pipeline.runner import InputBinding, create_sql_pipeline


@pytest.fixture
def canonical_addresses_small(duck_con):
    return duck_con.sql(
        """
        SELECT *
        FROM (
            VALUES
                (100, 1100, '10 DOWNING STREET', 'SW1A 2AA'),
                (101, 1101, '10 DOWNING STREET ANNEX', 'SW1A 2AA'),
                (102, 1102, '11 DOWNING STREET', 'SW1A 2AA'),
                (103, 1103, '12 DOWNING STREET', 'SW1A 2AA'),
                (104, 1104, '13 DOWNING STREET', 'SW1A 2AA')
        ) AS t(unique_id, ukam_address_id, original_address_concat, postcode)
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
                (1, 1, '10 Downing St', 'SW1A 2AA', '{exact_reason}', 100, 1100),
                (2, 2, '11 Downing St', 'SW1A 2AA', '{exact_reason}', 102, 1102),
                (3, 3, '12 Downing St', 'SW1A 2AA', '{exact_reason}', 103, 1103),
                -- This record should be overwritten by a Splink match
                (4, 4, '14 Downing St', 'SW1A 2AA', NULL, NULL, NULL),
                -- This record should only appear is we request unmatched inclusion
                (5, 5, '15 Downing St', 'SW1A 2AA', NULL, NULL, NULL)
        ) AS t(
            unique_id,
            ukam_address_id,
            original_address_concat,
            postcode,
            match_reason,
            resolved_canonical_id,
            canonical_ukam_address_id
        )
        """
    )


@pytest.fixture
def splink_candidates_with_duplicates(duck_con):
    # _r = record from fuzzy addresses
    # _l = record from canonical addresses
    return duck_con.sql(
        """
        SELECT *
        FROM (
            VALUES
                (101, 1, 1001, 1, '10 Downing St Variant', 'SW1A 2AA', 0.82, 9.5, '02: Distinguishability > 5'),
                (100, 1, 1000, 1, '10 Downing St Variant', 'SW1A 2AA', 0.85, 5.0, '03: Distinguishability > 1'),
                (102, 2, 1002, 2, '11 Downing St Variant', 'SW1A 2AA', 0.91, 4.0, '03: Distinguishability > 1'),
                (104, 2, 1004, 2, '11 Downing St Variant', 'SW1A 2AA', 0.87, 7.5, '02: Distinguishability > 5'),
                (103, 3, 1003, 3, '12 Downing St Variant', 'SW1A 2AA', 0.92, 3.0, '03: Distinguishability > 1'),
                (101, 3, 1001, 3, '12 Downing St Variant', 'SW1A 2AA', 0.91, 8.0, '02: Distinguishability > 5'),
                (104, 4, 1004, 4, '14 Downing St Variant', 'SW1A 2AA', 0.94, 6.5, '02: Distinguishability > 5'),
                (101, 4, 1001, 4, '14 Downing St Variant', 'SW1A 2AA', 0.80, 2.0, '03: Distinguishability > 1')
        ) AS t(
            unique_id_l,
            unique_id_r,
            ukam_address_id_l,
            ukam_address_id_r,
            address_concat_r,
            postcode_r,
            match_weight,
            distinguishability,
            distinguishability_category
        )
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
                (999, 2999, '99 Downing St', 'SW1A 2AA', '{exact_reason}', 103, 1103)
        ) AS t(
            unique_id,
            ukam_address_id,
            original_address_concat,
            postcode,
            match_reason,
            resolved_canonical_id,
            canonical_ukam_address_id
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
                CAST(NULL AS BIGINT) AS unique_id_l,
                CAST(NULL AS BIGINT) AS unique_id_r,
                CAST(NULL AS BIGINT) AS ukam_address_id_l,
                CAST(NULL AS BIGINT) AS ukam_address_id_r,
                CAST(NULL AS VARCHAR) AS address_concat_r,
                CAST(NULL AS VARCHAR) AS postcode_r,
                CAST(NULL AS DOUBLE) AS match_weight,
                CAST(NULL AS DOUBLE) AS distinguishability,
                CAST(NULL AS VARCHAR) AS distinguishability_category
        )
        WHERE 1 = 0
        """
    )


def test_prepare_splink_candidates_returns_top_results(
    duck_con,
    splink_candidates_with_duplicates,
):
    result = create_sql_pipeline(
        con=duck_con,
        input_rel=[
            InputBinding("splink_matches__ukam", splink_candidates_with_duplicates)
        ],
        stage_specs=[
            _prepare_splink_candidates(
                match_weight_threshold=-100.0,
                distinguishability_threshold=None,
            ),
        ],
    ).run()

    # Confirm unique IDs from fuzzy input only appear once in output
    assert (
        splink_candidates_with_duplicates.select("unique_id_r")
        .distinct()
        .count("*")
        .fetchone()[0]
        == result.count("*").fetchone()[0]
    )
    # Check that we have selected the top match for unique IDs
    expected_matches = [(1, 0.85), (2, 0.91), (3, 0.92), (4, 0.94)]
    for unique_id, match_weight in expected_matches:
        assert (
            float(
                result.filter(f"unique_id = {unique_id}")
                .select("match_weight")
                .fetchone()[0]
            )
            == match_weight
        )


# Confirms that we prioritise exact matches over Splink matches
@pytest.mark.parametrize(
    "include_unmatched,expected_ids,unmatched_id_present",
    [
        (False, [1, 2, 3, 4], False),  # Exclude unmatched: ID 5 not present
        (True, [1, 2, 3, 4, 5], True),  # Include unmatched: ID 5 present
    ],
)
def test_select_top_match_candidates_prioritises_exact_matches(
    duck_con,
    canonical_addresses_small,
    exact_matches_with_duplicates,
    splink_candidates_with_duplicates,
    include_unmatched,
    expected_ids,
    unmatched_id_present,
):
    result = select_top_match_candidates(
        con=duck_con,
        df_exact_matches=exact_matches_with_duplicates,
        df_splink_matches=splink_candidates_with_duplicates,
        df_canonical=canonical_addresses_small,
        match_weight_threshold=-100.0,
        distinguishability_threshold=None,
        include_unmatched=include_unmatched,
    )

    df = result.order("unique_id").to_df().set_index("unique_id")

    # Check that the expected IDs are present (no duplicates)
    assert list(df.index) == expected_ids

    # Check exact matches (IDs 1, 2, 3)
    exact_expectations = {
        1: 100,
        2: 102,
        3: 103,
    }
    for unique_id, canonical_id in exact_expectations.items():
        row = df.loc[unique_id]
        assert row["resolved_canonical_id"] == canonical_id
        assert row["match_reason"] == MatchReason.EXACT.value
        assert pd.isna(row["match_weight"])
        assert pd.isna(row["distinguishability"])

    # Check Splink match (ID 4 had no exact match but has Splink match)
    splink_row = df.loc[4]
    assert splink_row["resolved_canonical_id"] == 104
    assert splink_row["match_reason"] == MatchReason.SPLINK.value
    assert splink_row["match_weight"] == 0.94
    assert splink_row["distinguishability"] == 6.5

    # Check unmatched record if present (ID 5 has no match anywhere)
    if unmatched_id_present:
        unmatched_row = df.loc[5]
        assert pd.isna(unmatched_row["resolved_canonical_id"])
        assert pd.isna(unmatched_row["match_reason"])
        assert pd.isna(unmatched_row["match_weight"])
        assert pd.isna(unmatched_row["distinguishability"])


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
    # This isn't a scenario we will encounter in practice, but it's good to ensure
    # robustness for testing/debugging scenarios.
    rows = result.order("unique_id")
    assert rows.count("*").fetchall()[0][0] == 1
    assert rows.select("match_weight").fetchall()[0][0] is None
    assert rows.select("distinguishability").fetchall()[0][0] is None
