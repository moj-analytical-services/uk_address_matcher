import pytest

from uk_address_matcher import (
    ExactMatchStage,
    PeeledAddressStage,
)
from uk_address_matcher.linking_model.matching import runner as matching_runner
from uk_address_matcher.linking_model.matching.runner import _run_matching


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
                    CAST([] AS VARCHAR[]),
                    1::BIGINT
                ),
                (
                    10,
                    '4 SAMPLE STREET',
                    '4 SAMPLE STREET',
                    'CC3 3CC',
                    CAST([] AS VARCHAR[]),
                    2::BIGINT
                ),
                (
                    2,
                    '5 DEMO RD',
                    '5 DEMO RD',
                    'DD4 4DD',
                    CAST([] AS VARCHAR[]),
                    3::BIGINT
                ),
                (
                    2,
                    '5 DEMO RD',
                    '5 DEMO RD',
                    'DD4 4DD',
                    CAST([] AS VARCHAR[]),
                    4::BIGINT
                ),
                (
                    2,
                    '5 DEMO ROAD',
                    '5 DEMO ROAD',
                    'DD4 4DD',
                    CAST([] AS VARCHAR[]),
                    5::BIGINT
                ),
                (
                    2,
                    '5 DEMO ROAD',
                    '5 DEMO ROAD',
                    'DD4 4DD',
                    CAST([] AS VARCHAR[]),
                    6::BIGINT
                ),
                (
                    2,
                    '4 SAMPLE ST',
                    '4 SAMPLE ST',
                    'CC3 3CC',
                    CAST([] AS VARCHAR[]),
                    7::BIGINT
                ),
                (
                    3,
                    '999 MYSTERY LANE',
                    '999 MYSTERY LANE',
                    'EE5 5EE',
                    CAST([] AS VARCHAR[]),
                    8::BIGINT
                )
        ) AS t(
            unique_id,
            original_address_concat,
            clean_full_address,
            postcode,
            peeled_tokens_list,
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
                    CAST([] AS VARCHAR[]),
                    1
                ),
                (
                    2000,
                    '5 DEMO RD',
                    '5 DEMO RD',
                    'DD4 4DD',
                    CAST([] AS VARCHAR[]),
                    2
                )
        ) AS t(
            unique_id,
            original_address_concat,
            clean_full_address,
            postcode,
            peeled_tokens_list,
            ukam_address_id
        )
        """
    )

    return df_fuzzy, df_canonical


# -----------------------------------------------------------------------------
# Peeled address matching tests
# -----------------------------------------------------------------------------


@pytest.fixture
def peeled_test_data(duck_con):
    """Test data for peeled address matching with locality tokens to remove."""
    # Fuzzy addresses with various peeled token scenarios
    df_fuzzy = duck_con.sql(
        """
        SELECT *
        FROM (
            VALUES
                -- Case 1: Single peeled token (LONDON)
                -- '100 HIGH STREET LONDON' should match '100 HIGH STREET' after peeling
                (
                    1,
                    '100 HIGH STREET LONDON',
                    '100 HIGH STREET LONDON',
                    'SW1A 1AA',
                    ARRAY['LONDON'],
                    ARRAY['100']::VARCHAR[],
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    1::BIGINT
                ),
                -- Case 2: Multi-token peeled (GREATER LONDON counts as 2 words)
                -- '200 PARK AVENUE LONDON GREATER LONDON' peels to '200 PARK AVENUE'
                (
                    2,
                    '200 PARK AVENUE LONDON GREATER LONDON',
                    '200 PARK AVENUE LONDON GREATER LONDON',
                    'SW1A 2BB',
                    ARRAY['LONDON', 'GREATER LONDON'],
                    ARRAY['200']::VARCHAR[],
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    2::BIGINT
                ),
                -- Case 3: Two-word multi-token (TUNBRIDGE WELLS)
                -- '50 MAIN ROAD TUNBRIDGE WELLS' peels to '50 MAIN ROAD'
                (
                    3,
                    '50 MAIN ROAD TUNBRIDGE WELLS',
                    '50 MAIN ROAD TUNBRIDGE WELLS',
                    'TN1 1AA',
                    ARRAY['TUNBRIDGE WELLS'],
                    ARRAY['50']::VARCHAR[],
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    3::BIGINT
                ),
                -- Case 4: No peeling (address not ending in locality)
                -- Should NOT match via peeled matching (exact match only)
                (
                    4,
                    '75 OAK DRIVE',
                    '75 OAK DRIVE',
                    'SW1A 1AA',
                    CAST([] AS VARCHAR[]),
                    ARRAY['75']::VARCHAR[],
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    4::BIGINT
                ),
                -- Case 5: Multiple single tokens peeled
                -- '10 TEST LANE HACKNEY LONDON' peels to '10 TEST LANE'
                (
                    5,
                    '10 TEST LANE HACKNEY LONDON',
                    '10 TEST LANE HACKNEY LONDON',
                    'E8 1AA',
                    ARRAY['HACKNEY', 'LONDON'],
                    ARRAY['10']::VARCHAR[],
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    5::BIGINT
                ),
                -- Case 6: Address that matches only after peeling on canonical side
                -- Fuzzy has no peeling, but canonical does
                (
                    6,
                    '300 CHURCH ROAD',
                    '300 CHURCH ROAD',
                    'M1 1AA',
                    CAST([] AS VARCHAR[]),
                    ARRAY['300']::VARCHAR[],
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    6::BIGINT
                )
        ) AS t(
            unique_id,
            original_address_concat,
            clean_full_address,
            postcode,
            peeled_tokens_list,
            numeric_tokens,
            has_flat_indicator,
            flat_positional,
            flat_letter,
            flat_number,
            has_business_unit,
            business_unit_type,
            business_unit_id,
            ukam_address_id
        )
        """
    )

    # Canonical addresses - some with peeling, some without
    df_canonical = duck_con.sql(
        """
        SELECT *
        FROM (
            VALUES
                -- Matches Case 1: same postcode, peeled address = '100 HIGH STREET'
                (
                    1001,
                    '100 HIGH STREET',
                    '100 HIGH STREET',
                    'SW1A 1AA',
                    CAST([] AS VARCHAR[]),
                    ARRAY['100']::VARCHAR[],
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    101
                ),
                -- Matches Case 2: same postcode, peeled address = '200 PARK AVENUE'
                (
                    1002,
                    '200 PARK AVENUE',
                    '200 PARK AVENUE',
                    'SW1A 2BB',
                    CAST([] AS VARCHAR[]),
                    ARRAY['200']::VARCHAR[],
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    102
                ),
                -- Matches Case 3: same postcode, peeled address = '50 MAIN ROAD'
                (
                    1003,
                    '50 MAIN ROAD',
                    '50 MAIN ROAD',
                    'TN1 1AA',
                    CAST([] AS VARCHAR[]),
                    ARRAY['50']::VARCHAR[],
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    103
                ),
                -- Case 4 canonical: same as fuzzy (exact match, not peeled match)
                (
                    1004,
                    '75 OAK DRIVE',
                    '75 OAK DRIVE',
                    'SW1A 1AA',
                    CAST([] AS VARCHAR[]),
                    ARRAY['75']::VARCHAR[],
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    104
                ),
                -- Matches Case 5: peeled address = '10 TEST LANE'
                (
                    1005,
                    '10 TEST LANE',
                    '10 TEST LANE',
                    'E8 1AA',
                    CAST([] AS VARCHAR[]),
                    ARRAY['10']::VARCHAR[],
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    105
                ),
                -- Matches Case 6: canonical has peeling, fuzzy doesn't
                -- '300 CHURCH ROAD MANCHESTER' peels to '300 CHURCH ROAD'
                (
                    1006,
                    '300 CHURCH ROAD MANCHESTER',
                    '300 CHURCH ROAD MANCHESTER',
                    'M1 1AA',
                    ARRAY['MANCHESTER'],
                    ARRAY['300']::VARCHAR[],
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    106
                ),
                -- Non-matching canonical (different postcode)
                (
                    9999,
                    '100 HIGH STREET',
                    '100 HIGH STREET',
                    'XX9 9XX',
                    CAST([] AS VARCHAR[]),
                    ARRAY['100']::VARCHAR[],
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    999
                )
        ) AS t(
            unique_id,
            original_address_concat,
            clean_full_address,
            postcode,
            peeled_tokens_list,
            numeric_tokens,
            has_flat_indicator,
            flat_positional,
            flat_letter,
            flat_number,
            has_business_unit,
            business_unit_type,
            business_unit_id,
            ukam_address_id
        )
        """
    )

    return df_fuzzy, df_canonical


def test_peeled_address_matching_finds_matches(duck_con, peeled_test_data):
    """Test that peeled address matching correctly finds matches after removing
    locality tokens."""
    df_fuzzy, df_canonical = peeled_test_data

    results, _ = _run_matching(
        con=duck_con,
        df_messy_clean=df_fuzzy,
        df_canonical_clean=df_canonical,
        stages=[ExactMatchStage(), PeeledAddressStage()],
    )

    # Convert to list of dicts for easier assertions
    results_df = results.fetchdf()

    # Check that we got all input rows back
    assert len(results_df) == 6, f"Expected 6 rows, got {len(results_df)}"

    # Check specific matches
    matched = results_df[results_df["resolved_canonical_id"].notna()]
    matched_dict = dict(zip(matched["ukam_address_id"], matched["resolved_canonical_id"]))

    # Case 1: '100 HIGH STREET LONDON' -> '100 HIGH STREET' (canonical 1001)
    assert matched_dict.get(1) == 1001, "Case 1 should match canonical 1001"

    # Case 2: '200 PARK AVENUE LONDON GREATER LONDON'
    # -> '200 PARK AVENUE' (canonical 1002)
    assert matched_dict.get(2) == 1002, "Case 2 should match canonical 1002"

    # Case 3: '50 MAIN ROAD TUNBRIDGE WELLS' -> '50 MAIN ROAD' (canonical 1003)
    assert matched_dict.get(3) == 1003, "Case 3 should match canonical 1003"

    # Case 4: No peeling - matches via exact match (EXACT_MATCHES is always on)
    # Note: This matches via "exact: full match", not peeled_address
    assert matched_dict.get(4) == 1004, "Case 4 should match via exact match"

    # Case 5: '10 TEST LANE HACKNEY LONDON' -> '10 TEST LANE' (canonical 1005)
    assert matched_dict.get(5) == 1005, "Case 5 should match canonical 1005"

    # Case 6: Canonical has peeling, fuzzy doesn't - should still match
    assert matched_dict.get(6) == 1006, "Case 6 should match canonical 1006"


def test_run_matching_handles_non_identifier_uid(duck_con, peeled_test_data, monkeypatch):
    """Ensure temporary table names remain SQL-safe even for unusual run IDs."""
    df_fuzzy, df_canonical = peeled_test_data

    monkeypatch.setattr(matching_runner, "_uid", lambda n=6: "abc-def")

    results, _ = matching_runner._run_matching(
        con=duck_con,
        df_messy_clean=df_fuzzy,
        df_canonical_clean=df_canonical,
        stages=[ExactMatchStage(), PeeledAddressStage()],
    )

    assert results.count("*").fetchone()[0] == df_fuzzy.count("*").fetchone()[0]


def test_exact_matching_can_ignore_flat_keyword(duck_con):
    """Exact phase 2 should match with FLAT removal when flat fields align."""

    df_fuzzy = duck_con.sql(
        """
        SELECT *
        FROM (
            VALUES
                (
                    1,
                    'FLAT 2 10 KINGS ROAD',
                    'FLAT 2 10 KINGS ROAD',
                    'SW1A 1AA',
                    CAST([] AS VARCHAR[]),
                    ARRAY['2', '10']::VARCHAR[],
                    TRUE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    '2',
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    1::BIGINT
                )
        ) AS t(
            unique_id,
            original_address_concat,
            clean_full_address,
            postcode,
            peeled_tokens_list,
            numeric_tokens,
            has_flat_indicator,
            flat_positional,
            flat_letter,
            flat_number,
            has_business_unit,
            business_unit_type,
            business_unit_id,
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
                    '2 10 KINGS ROAD',
                    '2 10 KINGS ROAD',
                    'SW1A 1AA',
                    CAST([] AS VARCHAR[]),
                    ARRAY['2', '10']::VARCHAR[],
                    TRUE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    '2',
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    100::BIGINT
                )
        ) AS t(
            unique_id,
            original_address_concat,
            clean_full_address,
            postcode,
            peeled_tokens_list,
            numeric_tokens,
            has_flat_indicator,
            flat_positional,
            flat_letter,
            flat_number,
            has_business_unit,
            business_unit_type,
            business_unit_id,
            ukam_address_id
        )
        """
    )

    results, _ = _run_matching(
        con=duck_con,
        df_messy_clean=df_fuzzy,
        df_canonical_clean=df_canonical,
        stages=[ExactMatchStage()],
    )

    results_df = results.fetchdf()
    assert results_df.iloc[0]["resolved_canonical_id"] == 1000
    assert (
        results_df.iloc[0]["match_reason"]
        == "exact_flat_retraction: match after removing FLAT keyword"
    )


def test_exact_matching_can_disable_flat_retraction_phase(duck_con):
    """With phase 2 disabled, FLAT-only fallback matches are not emitted."""

    df_fuzzy = duck_con.sql(
        """
        SELECT *
        FROM (
            VALUES
                (
                    1,
                    'FLAT 2 10 KINGS ROAD',
                    'FLAT 2 10 KINGS ROAD',
                    'SW1A 1AA',
                    CAST([] AS VARCHAR[]),
                    ARRAY['2', '10']::VARCHAR[],
                    TRUE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    '2',
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    1::BIGINT
                )
        ) AS t(
            unique_id,
            original_address_concat,
            clean_full_address,
            postcode,
            peeled_tokens_list,
            numeric_tokens,
            has_flat_indicator,
            flat_positional,
            flat_letter,
            flat_number,
            has_business_unit,
            business_unit_type,
            business_unit_id,
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
                    '2 10 KINGS ROAD',
                    '2 10 KINGS ROAD',
                    'SW1A 1AA',
                    CAST([] AS VARCHAR[]),
                    ARRAY['2', '10']::VARCHAR[],
                    TRUE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    '2',
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    100::BIGINT
                )
        ) AS t(
            unique_id,
            original_address_concat,
            clean_full_address,
            postcode,
            peeled_tokens_list,
            numeric_tokens,
            has_flat_indicator,
            flat_positional,
            flat_letter,
            flat_number,
            has_business_unit,
            business_unit_type,
            business_unit_id,
            ukam_address_id
        )
        """
    )

    results, _ = _run_matching(
        con=duck_con,
        df_messy_clean=df_fuzzy,
        df_canonical_clean=df_canonical,
        stages=[ExactMatchStage(enable_flat_retraction=False)],
    )

    results_df = results.fetchdf()
    assert results_df["resolved_canonical_id"].isna().all()
    assert results_df["match_reason"].isna().all()


def test_exact_matching_flat_retraction_requires_unique_candidate(duck_con):
    """Exact phase 2 should not match when multiple canonical IDs are possible."""

    df_fuzzy = duck_con.sql(
        """
        SELECT *
        FROM (
            VALUES
                (
                    1,
                    'FLAT 2 10 KINGS ROAD',
                    'FLAT 2 10 KINGS ROAD',
                    'SW1A 1AA',
                    CAST([] AS VARCHAR[]),
                    ARRAY['2', '10']::VARCHAR[],
                    TRUE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    '2',
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    1::BIGINT
                )
        ) AS t(
            unique_id,
            original_address_concat,
            clean_full_address,
            postcode,
            peeled_tokens_list,
            numeric_tokens,
            has_flat_indicator,
            flat_positional,
            flat_letter,
            flat_number,
            has_business_unit,
            business_unit_type,
            business_unit_id,
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
                    '2 10 KINGS ROAD',
                    '2 10 KINGS ROAD',
                    'SW1A 1AA',
                    CAST([] AS VARCHAR[]),
                    ARRAY['2', '10']::VARCHAR[],
                    TRUE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    '2',
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    100::BIGINT
                ),
                (
                    1001,
                    '2 10 KINGS ROAD',
                    '2 10 KINGS ROAD',
                    'SW1A 1AA',
                    CAST([] AS VARCHAR[]),
                    ARRAY['2', '10']::VARCHAR[],
                    TRUE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    '2',
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    101::BIGINT
                )
        ) AS t(
            unique_id,
            original_address_concat,
            clean_full_address,
            postcode,
            peeled_tokens_list,
            numeric_tokens,
            has_flat_indicator,
            flat_positional,
            flat_letter,
            flat_number,
            has_business_unit,
            business_unit_type,
            business_unit_id,
            ukam_address_id
        )
        """
    )

    results, _ = _run_matching(
        con=duck_con,
        df_messy_clean=df_fuzzy,
        df_canonical_clean=df_canonical,
        stages=[ExactMatchStage()],
    )

    results_df = results.fetchdf()
    assert results_df["resolved_canonical_id"].isna().all()
    assert results_df["match_reason"].isna().all()


def test_exact_matching_flat_retraction_can_require_unit_evidence(duck_con):
    """Conservative phase 2 rejects FLAT + single-number shell addresses."""

    df_fuzzy = duck_con.sql(
        """
        SELECT *
        FROM (
            VALUES
                (
                    1,
                    'FLAT 126 SOUTH LAMBETH ROAD',
                    'FLAT 126 SOUTH LAMBETH ROAD',
                    'SW8 1AA',
                    CAST([] AS VARCHAR[]),
                    ARRAY['126']::VARCHAR[],
                    TRUE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    1::BIGINT
                )
        ) AS t(
            unique_id,
            original_address_concat,
            clean_full_address,
            postcode,
            peeled_tokens_list,
            numeric_tokens,
            has_flat_indicator,
            flat_positional,
            flat_letter,
            flat_number,
            has_business_unit,
            business_unit_type,
            business_unit_id,
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
                    '126 SOUTH LAMBETH ROAD',
                    '126 SOUTH LAMBETH ROAD',
                    'SW8 1AA',
                    CAST([] AS VARCHAR[]),
                    ARRAY['126']::VARCHAR[],
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    100::BIGINT
                )
        ) AS t(
            unique_id,
            original_address_concat,
            clean_full_address,
            postcode,
            peeled_tokens_list,
            numeric_tokens,
            has_flat_indicator,
            flat_positional,
            flat_letter,
            flat_number,
            has_business_unit,
            business_unit_type,
            business_unit_id,
            ukam_address_id
        )
        """
    )

    conservative_results, _ = _run_matching(
        con=duck_con,
        df_messy_clean=df_fuzzy,
        df_canonical_clean=df_canonical,
        stages=[ExactMatchStage(enable_flat_retraction=True)],
    )
    conservative_df = conservative_results.fetchdf()
    assert conservative_df["resolved_canonical_id"].isna().all()
    assert conservative_df["match_reason"].isna().all()


def test_exact_matching_flat_retraction_unit_evidence_can_use_numeric_tokens(duck_con):
    """Conservative phase 2 can use parsed numeric token structure as unit evidence."""

    df_fuzzy = duck_con.sql(
        """
        SELECT *
        FROM (
            VALUES
                (
                    1,
                    'FLAT 2 126 SOUTH LAMBETH ROAD',
                    'FLAT 2 126 SOUTH LAMBETH ROAD',
                    'SW8 1AA',
                    CAST([] AS VARCHAR[]),
                    ARRAY['2', '126']::VARCHAR[],
                    TRUE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    1::BIGINT
                )
        ) AS t(
            unique_id,
            original_address_concat,
            clean_full_address,
            postcode,
            peeled_tokens_list,
            numeric_tokens,
            has_flat_indicator,
            flat_positional,
            flat_letter,
            flat_number,
            has_business_unit,
            business_unit_type,
            business_unit_id,
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
                    '2 126 SOUTH LAMBETH ROAD',
                    '2 126 SOUTH LAMBETH ROAD',
                    'SW8 1AA',
                    CAST([] AS VARCHAR[]),
                    ARRAY['2', '126']::VARCHAR[],
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    100::BIGINT
                )
        ) AS t(
            unique_id,
            original_address_concat,
            clean_full_address,
            postcode,
            peeled_tokens_list,
            numeric_tokens,
            has_flat_indicator,
            flat_positional,
            flat_letter,
            flat_number,
            has_business_unit,
            business_unit_type,
            business_unit_id,
            ukam_address_id
        )
        """
    )

    results, _ = _run_matching(
        con=duck_con,
        df_messy_clean=df_fuzzy,
        df_canonical_clean=df_canonical,
        stages=[ExactMatchStage(enable_flat_retraction=True)],
    )

    results_df = results.fetchdf()
    assert results_df.iloc[0]["resolved_canonical_id"] == 1000
    assert (
        results_df.iloc[0]["match_reason"]
        == "exact_flat_retraction: match after removing FLAT keyword"
    )


def test_exact_matching_flat_retraction_allows_one_sided_flat_detail(duck_con):
    """Exact phase 2 should allow one-sided flat detail when not contradictory."""

    df_fuzzy = duck_con.sql(
        """
        SELECT *
        FROM (
            VALUES
                (
                    1,
                    'FLAT 2 10 KINGS ROAD',
                    'FLAT 2 10 KINGS ROAD',
                    'SW1A 1AA',
                    CAST([] AS VARCHAR[]),
                    ARRAY['2', '10']::VARCHAR[],
                    TRUE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    '2',
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    1::BIGINT
                )
        ) AS t(
            unique_id,
            original_address_concat,
            clean_full_address,
            postcode,
            peeled_tokens_list,
            numeric_tokens,
            has_flat_indicator,
            flat_positional,
            flat_letter,
            flat_number,
            has_business_unit,
            business_unit_type,
            business_unit_id,
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
                    '2 10 KINGS ROAD',
                    '2 10 KINGS ROAD',
                    'SW1A 1AA',
                    CAST([] AS VARCHAR[]),
                    ARRAY['2', '10']::VARCHAR[],
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    100::BIGINT
                )
        ) AS t(
            unique_id,
            original_address_concat,
            clean_full_address,
            postcode,
            peeled_tokens_list,
            numeric_tokens,
            has_flat_indicator,
            flat_positional,
            flat_letter,
            flat_number,
            has_business_unit,
            business_unit_type,
            business_unit_id,
            ukam_address_id
        )
        """
    )

    results, _ = _run_matching(
        con=duck_con,
        df_messy_clean=df_fuzzy,
        df_canonical_clean=df_canonical,
        stages=[ExactMatchStage()],
    )

    results_df = results.fetchdf()
    assert results_df.iloc[0]["resolved_canonical_id"] == 1000
    assert (
        results_df.iloc[0]["match_reason"]
        == "exact_flat_retraction: match after removing FLAT keyword"
    )


def test_exact_matching_flat_retraction_rejects_contradictory_flat_fields(duck_con):
    """Exact phase 2 should reject explicit conflicts in parsed flat fields."""

    df_fuzzy = duck_con.sql(
        """
        SELECT *
        FROM (
            VALUES
                (
                    1,
                    'FLAT 2 10 KINGS ROAD',
                    'FLAT 2 10 KINGS ROAD',
                    'SW1A 1AA',
                    CAST([] AS VARCHAR[]),
                    ARRAY['2', '10']::VARCHAR[],
                    TRUE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    '2',
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    1::BIGINT
                )
        ) AS t(
            unique_id,
            original_address_concat,
            clean_full_address,
            postcode,
            peeled_tokens_list,
            numeric_tokens,
            has_flat_indicator,
            flat_positional,
            flat_letter,
            flat_number,
            has_business_unit,
            business_unit_type,
            business_unit_id,
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
                    '3 10 KINGS ROAD',
                    '3 10 KINGS ROAD',
                    'SW1A 1AA',
                    CAST([] AS VARCHAR[]),
                    ARRAY['3', '10']::VARCHAR[],
                    TRUE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    '3',
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    100::BIGINT
                )
        ) AS t(
            unique_id,
            original_address_concat,
            clean_full_address,
            postcode,
            peeled_tokens_list,
            numeric_tokens,
            has_flat_indicator,
            flat_positional,
            flat_letter,
            flat_number,
            has_business_unit,
            business_unit_type,
            business_unit_id,
            ukam_address_id
        )
        """
    )

    results, _ = _run_matching(
        con=duck_con,
        df_messy_clean=df_fuzzy,
        df_canonical_clean=df_canonical,
        stages=[ExactMatchStage()],
    )

    results_df = results.fetchdf()
    assert results_df["resolved_canonical_id"].isna().all()
    assert results_df["match_reason"].isna().all()


def test_exact_matching_flat_retraction_does_not_strip_additional_tokens(duck_con):
    """Phase 2 strips FLAT only and does not remove unrelated standalone tokens."""

    df_fuzzy = duck_con.sql(
        """
        SELECT *
        FROM (
            VALUES
                (
                    1,
                    'THE FLAT 2 AT 10 KINGS ROAD',
                    'THE FLAT 2 AT 10 KINGS ROAD',
                    'SW1A 1AA',
                    CAST([] AS VARCHAR[]),
                    ARRAY['2', '10']::VARCHAR[],
                    TRUE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    '2',
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    1::BIGINT
                )
        ) AS t(
            unique_id,
            original_address_concat,
            clean_full_address,
            postcode,
            peeled_tokens_list,
            numeric_tokens,
            has_flat_indicator,
            flat_positional,
            flat_letter,
            flat_number,
            has_business_unit,
            business_unit_type,
            business_unit_id,
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
                    '2 10 KINGS ROAD',
                    '2 10 KINGS ROAD',
                    'SW1A 1AA',
                    CAST([] AS VARCHAR[]),
                    ARRAY['2', '10']::VARCHAR[],
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    100::BIGINT
                )
        ) AS t(
            unique_id,
            original_address_concat,
            clean_full_address,
            postcode,
            peeled_tokens_list,
            numeric_tokens,
            has_flat_indicator,
            flat_positional,
            flat_letter,
            flat_number,
            has_business_unit,
            business_unit_type,
            business_unit_id,
            ukam_address_id
        )
        """
    )

    results, _ = _run_matching(
        con=duck_con,
        df_messy_clean=df_fuzzy,
        df_canonical_clean=df_canonical,
        stages=[ExactMatchStage(enable_flat_retraction=True)],
    )
    results_df = results.fetchdf()
    assert results_df["resolved_canonical_id"].isna().all()
    assert results_df["match_reason"].isna().all()


def test_peeled_address_matching_preserves_row_count(duck_con, peeled_test_data):
    """Test that peeled address matching doesn't inflate or reduce row count."""
    df_fuzzy, df_canonical = peeled_test_data

    results, _ = _run_matching(
        con=duck_con,
        df_messy_clean=df_fuzzy,
        df_canonical_clean=df_canonical,
        stages=[ExactMatchStage(), PeeledAddressStage()],
    )

    input_row_count = df_fuzzy.count("*").fetchone()[0]
    output_row_count = results.count("*").fetchone()[0]

    assert output_row_count == input_row_count, (
        f"Row count changed: input={input_row_count}, output={output_row_count}"
    )


def test_peeled_address_matching_match_reason(duck_con, peeled_test_data):
    """Test that peeled matches have the correct match_reason."""
    df_fuzzy, df_canonical = peeled_test_data

    results, _ = _run_matching(
        con=duck_con,
        df_messy_clean=df_fuzzy,
        df_canonical_clean=df_canonical,
        stages=[ExactMatchStage(), PeeledAddressStage()],
    )

    results_df = results.fetchdf()
    matched = results_df[results_df["resolved_canonical_id"].notna()]

    # Check match reasons - should have a mix of exact and peeled matches
    match_reasons = matched["match_reason"].value_counts().to_dict()

    # Case 4 (75 OAK DRIVE) should match via exact: full match (EXACT_MATCHES always on)
    assert "exact: full match" in match_reasons, (
        f"Should have at least one exact match. Got: {match_reasons}"
    )

    # Cases 1, 2, 3, 5, 6 should match via peeled_address
    peeled_reason = "peeled_address: match after removing common UK end tokens"
    assert peeled_reason in match_reasons, (
        f"Should have at least one peeled_address match. Got: {match_reasons}"
    )


def test_peeled_address_multi_word_token_handling(duck_con):
    """Test that multi-word peeled tokens like 'TUNBRIDGE WELLS' are handled correctly.

    The key challenge: peeled_tokens_list=['TUNBRIDGE WELLS'] has length 1,
    but we need to remove 2 words from the tokenised clean_full_address.
    """
    # Setup: fuzzy has 'TUNBRIDGE WELLS' as a single entry in peeled_tokens_list
    df_fuzzy = duck_con.sql(
        """
        SELECT *
        FROM (
            VALUES
                (
                    1,
                    '10 TEST STREET TUNBRIDGE WELLS',
                    '10 TEST STREET TUNBRIDGE WELLS',
                    'TN1 1AA',
                    ARRAY['TUNBRIDGE WELLS'],
                    ARRAY['10']::VARCHAR[],
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    1::BIGINT
                )
        ) AS t(
            unique_id,
            original_address_concat,
            clean_full_address,
            postcode,
            peeled_tokens_list,
            numeric_tokens,
            has_flat_indicator,
            flat_positional,
            flat_letter,
            flat_number,
            has_business_unit,
            business_unit_type,
            business_unit_id,
            ukam_address_id
        )
        """
    )

    # Canonical: '10 TEST STREET' (no locality suffix)
    df_canonical = duck_con.sql(
        """
        SELECT *
        FROM (
            VALUES
                (
                    1000,
                    '10 TEST STREET',
                    '10 TEST STREET',
                    'TN1 1AA',
                    CAST([] AS VARCHAR[]),
                    ARRAY['10']::VARCHAR[],
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    100
                )
        ) AS t(
            unique_id,
            original_address_concat,
            clean_full_address,
            postcode,
            peeled_tokens_list,
            numeric_tokens,
            has_flat_indicator,
            flat_positional,
            flat_letter,
            flat_number,
            has_business_unit,
            business_unit_type,
            business_unit_id,
            ukam_address_id
        )
        """
    )

    results, _stage_diagnostics = _run_matching(
        con=duck_con,
        df_messy_clean=df_fuzzy,
        df_canonical_clean=df_canonical,
        stages=[ExactMatchStage(), PeeledAddressStage()],
    )

    results_df = results.fetchdf()
    assert results_df.iloc[0]["resolved_canonical_id"] == 1000, (
        "Multi-word token 'TUNBRIDGE WELLS' should be correctly counted as 2 words"
    )
