import pytest

from uk_address_matcher import (
    ExactMatchStage,
    PeeledAddressStage,
)
from uk_address_matcher.linking_model.matching import runner as matching_runner
from uk_address_matcher.linking_model.matching.runner import _run_matching
from uk_address_matcher.sql_pipeline.match_reasons import MatchReason


@pytest.fixture
def test_data(duck_con):
    """Set up test data as DuckDB PyRelations for exact matching tests."""
    df_fuzzy = duck_con.sql("""
        SELECT *,
            NULL::VARCHAR AS sub_premise_location,
            NULL::VARCHAR AS numeric_token_1,
            NULL::VARCHAR AS numeric_token_2,
            NULL::VARCHAR AS numeric_token_3
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
        """)

    df_canonical = duck_con.sql("""
        SELECT *,
            NULL::VARCHAR AS sub_premise_location,
            NULL::VARCHAR AS numeric_token_1,
            NULL::VARCHAR AS numeric_token_2,
            NULL::VARCHAR AS numeric_token_3
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
        """)

    return df_fuzzy, df_canonical


ADDRESS_ROW_COLUMNS_SQL = """
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
"""

NO_WS_MESSY_ROW_SQL = """
(
    1,
    '12 HIGH ROAD',
    '12 HIGH ROAD',
    'AA1 1AA',
    CAST([] AS VARCHAR[]),
    ARRAY['12', '10']::VARCHAR[],
    FALSE,
    NULL::VARCHAR,
    NULL::VARCHAR,
    NULL::VARCHAR,
    FALSE,
    NULL::VARCHAR,
    NULL::VARCHAR,
    1::BIGINT
)
"""

NO_WS_CANONICAL_ROW_SQL = """
(
    1001,
    '12HIGHROAD',
    '12HIGHROAD',
    'AA1 1AA',
    CAST([] AS VARCHAR[]),
    ARRAY['12']::VARCHAR[],
    FALSE,
    NULL::VARCHAR,
    NULL::VARCHAR,
    NULL::VARCHAR,
    FALSE,
    NULL::VARCHAR,
    NULL::VARCHAR,
    10::BIGINT
)
"""

FLAT_MESSY_ROW_SQL = """
(
    1,
    'FLAT 2 10 HIGH STREET',
    'FLAT 2 10 HIGH STREET',
    'AA1 1AA',
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
"""

FLAT_CANONICAL_ROW_SQL = """
(
    1002,
    '2 10 HIGH STREET',
    '2 10 HIGH STREET',
    'AA1 1AA',
    CAST([] AS VARCHAR[]),
    ARRAY['2', '10']::VARCHAR[],
    TRUE,
    NULL::VARCHAR,
    NULL::VARCHAR,
    '2',
    FALSE,
    NULL::VARCHAR,
    NULL::VARCHAR,
    20::BIGINT
)
"""

FLAT_CANONICAL_DUPLICATE_ROW_SQL = """
(
    1002,
    '2 10 HIGH STREET',
    '2 10 HIGH STREET',
    'AA1 1AA',
    CAST([] AS VARCHAR[]),
    ARRAY['2', '10']::VARCHAR[],
    TRUE,
    NULL::VARCHAR,
    NULL::VARCHAR,
    '2',
    FALSE,
    NULL::VARCHAR,
    NULL::VARCHAR,
    21::BIGINT
)
"""


def _address_relation_from_values(duck_con, values_sql: str):
    return duck_con.sql(f"""
        SELECT *,
            NULL::VARCHAR AS sub_premise_location,
            numeric_tokens[1] AS numeric_token_1,
            numeric_tokens[2] AS numeric_token_2,
            numeric_tokens[3] AS numeric_token_3
        FROM (
            VALUES
                {values_sql}
        ) AS t(
            {ADDRESS_ROW_COLUMNS_SQL}
        )
        """)


def test_exact_matching_applies_no_whitespace_fallback_by_default(duck_con):
    df_messy = _address_relation_from_values(duck_con, NO_WS_MESSY_ROW_SQL)
    df_canonical = _address_relation_from_values(duck_con, NO_WS_CANONICAL_ROW_SQL)

    results, _ = _run_matching(
        con=duck_con,
        df_messy_clean=df_messy,
        df_canonical_clean=df_canonical,
        stages=[ExactMatchStage()],
    )
    row = results.fetchdf().iloc[0]
    assert row["resolved_canonical_id"] == 1001
    assert row["match_reason"] == MatchReason.EXACT_NO_WHITESPACE.value


def test_exact_matching_applies_flat_retraction_with_heuristics(duck_con):
    df_messy = _address_relation_from_values(duck_con, FLAT_MESSY_ROW_SQL)
    df_canonical = _address_relation_from_values(duck_con, FLAT_CANONICAL_ROW_SQL)

    results, _ = _run_matching(
        con=duck_con,
        df_messy_clean=df_messy,
        df_canonical_clean=df_canonical,
        stages=[ExactMatchStage()],
    )
    row = results.fetchdf().iloc[0]
    assert row["resolved_canonical_id"] == 1002
    assert row["match_reason"] == MatchReason.EXACT_FLAT_RETRACTION.value


def test_exact_matching_skips_flat_retraction_when_disabled(duck_con):
    df_messy = _address_relation_from_values(duck_con, FLAT_MESSY_ROW_SQL)
    df_canonical = _address_relation_from_values(duck_con, FLAT_CANONICAL_ROW_SQL)

    results, _ = _run_matching(
        con=duck_con,
        df_messy_clean=df_messy,
        df_canonical_clean=df_canonical,
        stages=[ExactMatchStage(enable_flat_retraction=False)],
    )
    matched = results.fetchdf().dropna(subset=["resolved_canonical_id"])
    assert matched.empty


def test_exact_matching_flat_retraction_requires_unique_canonical_row(duck_con):
    df_messy = _address_relation_from_values(duck_con, FLAT_MESSY_ROW_SQL)
    df_canonical = _address_relation_from_values(
        duck_con,
        f"{FLAT_CANONICAL_ROW_SQL},\n{FLAT_CANONICAL_DUPLICATE_ROW_SQL}",
    )

    results, _ = _run_matching(
        con=duck_con,
        df_messy_clean=df_messy,
        df_canonical_clean=df_canonical,
        stages=[ExactMatchStage()],
    )
    matched = results.fetchdf().dropna(subset=["resolved_canonical_id"])
    assert matched.empty


def test_exact_matching_flat_retraction_rejects_conflicting_sub_premise_location(
    duck_con,
):
    df_messy = _address_relation_from_values(duck_con, FLAT_MESSY_ROW_SQL).select(
        "* EXCLUDE (sub_premise_location), 'RIGHT'::VARCHAR AS sub_premise_location"
    )
    df_canonical = _address_relation_from_values(duck_con, FLAT_CANONICAL_ROW_SQL).select(
        "* EXCLUDE (sub_premise_location), 'LEFT'::VARCHAR AS sub_premise_location"
    )

    results, _ = _run_matching(
        con=duck_con,
        df_messy_clean=df_messy,
        df_canonical_clean=df_canonical,
        stages=[ExactMatchStage()],
    )
    matched = results.fetchdf().dropna(subset=["resolved_canonical_id"])
    assert matched.empty


# -----------------------------------------------------------------------------
# Peeled address matching tests
# -----------------------------------------------------------------------------


@pytest.fixture
def peeled_test_data(duck_con):
    """Test data for peeled address matching with locality tokens to remove."""
    # Fuzzy addresses with various peeled token scenarios
    df_fuzzy = duck_con.sql("""
        SELECT *,
            NULL::VARCHAR AS sub_premise_location,
            numeric_tokens[1] AS numeric_token_1,
            numeric_tokens[2] AS numeric_token_2,
            numeric_tokens[3] AS numeric_token_3
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
        """)

    # Canonical addresses - some with peeling, some without
    df_canonical = duck_con.sql("""
        SELECT *,
            NULL::VARCHAR AS sub_premise_location,
            numeric_tokens[1] AS numeric_token_1,
            numeric_tokens[2] AS numeric_token_2,
            numeric_tokens[3] AS numeric_token_3
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
        """)

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
    df_fuzzy = duck_con.sql("""
        SELECT *,
            NULL::VARCHAR AS sub_premise_location,
            numeric_tokens[1] AS numeric_token_1,
            numeric_tokens[2] AS numeric_token_2,
            numeric_tokens[3] AS numeric_token_3
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
        """)

    # Canonical: '10 TEST STREET' (no locality suffix)
    df_canonical = duck_con.sql("""
        SELECT *,
            NULL::VARCHAR AS sub_premise_location,
            numeric_tokens[1] AS numeric_token_1,
            numeric_tokens[2] AS numeric_token_2,
            numeric_tokens[3] AS numeric_token_3
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
        """)

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


def test_peeled_address_stripped_matching_is_enabled_by_default(duck_con):
    df_fuzzy = duck_con.sql("""
        SELECT *,
            NULL::VARCHAR AS sub_premise_location,
            numeric_tokens[1] AS numeric_token_1,
            numeric_tokens[2] AS numeric_token_2,
            numeric_tokens[3] AS numeric_token_3
        FROM (
            VALUES
                (
                    1,
                    '10 TEST-LANE HACKNEY LONDON',
                    '10 TEST-LANE HACKNEY LONDON',
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
        """)

    df_canonical = duck_con.sql("""
        SELECT *,
            NULL::VARCHAR AS sub_premise_location,
            numeric_tokens[1] AS numeric_token_1,
            numeric_tokens[2] AS numeric_token_2,
            numeric_tokens[3] AS numeric_token_3
        FROM (
            VALUES
                (
                    1000,
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
        """)

    default_results, _ = _run_matching(
        con=duck_con,
        df_messy_clean=df_fuzzy,
        df_canonical_clean=df_canonical,
        stages=[ExactMatchStage(), PeeledAddressStage()],
    )
    disabled_results, _ = _run_matching(
        con=duck_con,
        df_messy_clean=df_fuzzy,
        df_canonical_clean=df_canonical,
        stages=[
            ExactMatchStage(),
            PeeledAddressStage(enable_whitespace_punctuation_stripping=False),
        ],
    )

    default_row = default_results.fetchdf().iloc[0]
    disabled_row = disabled_results.select("resolved_canonical_id").fetchone()[0]

    assert default_row["resolved_canonical_id"] == 1000
    assert default_row["match_reason"] == (
        "peeled_address_stripped: match after peeling and removing whitespace "
        "and punctuation"
    )
    assert disabled_row is None
