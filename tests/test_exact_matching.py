import pytest

from uk_address_matcher.linking_model.exact_matching import run_deterministic_match_pass


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
                    ARRAY['4', 'SAMPLE', 'STREET'],
                    CAST([] AS VARCHAR[]),
                    1::BIGINT
                ),
                (
                    10,
                    '4 SAMPLE STREET',
                    '4 SAMPLE STREET',
                    'CC3 3CC',
                    ARRAY['4', 'SAMPLE', 'STREET'],
                    CAST([] AS VARCHAR[]),
                    2::BIGINT
                ),
                (
                    2,
                    '5 DEMO RD',
                    '5 DEMO RD',
                    'DD4 4DD',
                    ARRAY['5', 'DEMO', 'RD'],
                    CAST([] AS VARCHAR[]),
                    3::BIGINT
                ),
                (
                    2,
                    '5 DEMO RD',
                    '5 DEMO RD',
                    'DD4 4DD',
                    ARRAY['5', 'DEMO', 'RD'],
                    CAST([] AS VARCHAR[]),
                    4::BIGINT
                ),
                (
                    2,
                    '5 DEMO ROAD',
                    '5 DEMO ROAD',
                    'DD4 4DD',
                    ARRAY['5', 'DEMO', 'ROAD'],
                    CAST([] AS VARCHAR[]),
                    5::BIGINT
                ),
                (
                    2,
                    '5 DEMO ROAD',
                    '5 DEMO ROAD',
                    'DD4 4DD',
                    ARRAY['5', 'DEMO', 'ROAD'],
                    CAST([] AS VARCHAR[]),
                    6::BIGINT
                ),
                (
                    2,
                    '4 SAMPLE ST',
                    '4 SAMPLE ST',
                    'CC3 3CC',
                    ARRAY['4', 'SAMPLE', 'ST'],
                    CAST([] AS VARCHAR[]),
                    7::BIGINT
                ),
                (
                    3,
                    '999 MYSTERY LANE',
                    '999 MYSTERY LANE',
                    'EE5 5EE',
                    ARRAY['999', 'MYSTERY', 'LANE'],
                    CAST([] AS VARCHAR[]),
                    8::BIGINT
                )
        ) AS t(
            unique_id,
            original_address_concat,
            clean_full_address,
            postcode,
            address_tokens,
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
                    ARRAY['4', 'SAMPLE', 'STREET'],
                    CAST([] AS VARCHAR[]),
                    1
                ),
                (
                    2000,
                    '5 DEMO RD',
                    '5 DEMO RD',
                    'DD4 4DD',
                    ARRAY['5', 'DEMO', 'ROAD'],
                    CAST([] AS VARCHAR[]),
                    2
                )
        ) AS t(
            unique_id,
            original_address_concat,
            clean_full_address,
            postcode,
            address_tokens,
            peeled_tokens_list,
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
@pytest.mark.skip(reason="Temporarily skipped during refactoring")
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
                    ARRAY['100', 'HIGH', 'STREET', 'LONDON'],
                    ARRAY['LONDON'],
                    ARRAY['100']::VARCHAR[],
                    FALSE,
                    NULL::VARCHAR,
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
                    ARRAY['200', 'PARK', 'AVENUE', 'LONDON', 'GREATER', 'LONDON'],
                    ARRAY['LONDON', 'GREATER LONDON'],
                    ARRAY['200']::VARCHAR[],
                    FALSE,
                    NULL::VARCHAR,
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
                    ARRAY['50', 'MAIN', 'ROAD', 'TUNBRIDGE', 'WELLS'],
                    ARRAY['TUNBRIDGE WELLS'],
                    ARRAY['50']::VARCHAR[],
                    FALSE,
                    NULL::VARCHAR,
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
                    ARRAY['75', 'OAK', 'DRIVE'],
                    CAST([] AS VARCHAR[]),
                    ARRAY['75']::VARCHAR[],
                    FALSE,
                    NULL::VARCHAR,
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
                    ARRAY['10', 'TEST', 'LANE', 'HACKNEY', 'LONDON'],
                    ARRAY['HACKNEY', 'LONDON'],
                    ARRAY['10']::VARCHAR[],
                    FALSE,
                    NULL::VARCHAR,
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
                    ARRAY['300', 'CHURCH', 'ROAD'],
                    CAST([] AS VARCHAR[]),
                    ARRAY['300']::VARCHAR[],
                    FALSE,
                    NULL::VARCHAR,
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
            address_tokens,
            peeled_tokens_list,
            numeric_tokens,
            has_flat_indicator,
            flat_positional,
            flat_letter,
            flat_number,
            non_traditional_address_type,
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
                    ARRAY['100', 'HIGH', 'STREET'],
                    CAST([] AS VARCHAR[]),
                    ARRAY['100']::VARCHAR[],
                    FALSE,
                    NULL::VARCHAR,
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
                    ARRAY['200', 'PARK', 'AVENUE'],
                    CAST([] AS VARCHAR[]),
                    ARRAY['200']::VARCHAR[],
                    FALSE,
                    NULL::VARCHAR,
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
                    ARRAY['50', 'MAIN', 'ROAD'],
                    CAST([] AS VARCHAR[]),
                    ARRAY['50']::VARCHAR[],
                    FALSE,
                    NULL::VARCHAR,
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
                    ARRAY['75', 'OAK', 'DRIVE'],
                    CAST([] AS VARCHAR[]),
                    ARRAY['75']::VARCHAR[],
                    FALSE,
                    NULL::VARCHAR,
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
                    ARRAY['10', 'TEST', 'LANE'],
                    CAST([] AS VARCHAR[]),
                    ARRAY['10']::VARCHAR[],
                    FALSE,
                    NULL::VARCHAR,
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
                    ARRAY['300', 'CHURCH', 'ROAD', 'MANCHESTER'],
                    ARRAY['MANCHESTER'],
                    ARRAY['300']::VARCHAR[],
                    FALSE,
                    NULL::VARCHAR,
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
                    ARRAY['100', 'HIGH', 'STREET'],
                    CAST([] AS VARCHAR[]),
                    ARRAY['100']::VARCHAR[],
                    FALSE,
                    NULL::VARCHAR,
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
            address_tokens,
            peeled_tokens_list,
            numeric_tokens,
            has_flat_indicator,
            flat_positional,
            flat_letter,
            flat_number,
            non_traditional_address_type,
            has_business_unit,
            business_unit_type,
            business_unit_id,
            ukam_address_id
        )
        """
    )

    return df_fuzzy, df_canonical


@pytest.mark.skip(reason="Peeling logic removed from cleaning steps")
def test_peeled_address_matching_finds_matches(duck_con, peeled_test_data):
    """Test that peeled address matching correctly finds matches after removing
    locality tokens."""
    df_fuzzy, df_canonical = peeled_test_data

    results = run_deterministic_match_pass(
        duck_con,
        df_fuzzy,
        df_canonical,
        enabled_stage_names=["peeled_address"],
    )

    # Convert to list of dicts for easier assertions
    results_df = results.fetchdf()

    # Check that we got all input rows back
    assert len(results_df) == 6, f"Expected 6 rows, got {len(results_df)}"

    # Check specific matches
    matched = results_df[results_df["resolved_canonical_id"].notna()]
    matched_dict = dict(
        zip(matched["ukam_address_id"], matched["resolved_canonical_id"])
    )

    # Case 1: '100 HIGH STREET LONDON' -> '100 HIGH STREET' (canonical 1001)
    assert matched_dict.get(1) == 1001, "Case 1 should match canonical 1001"

    # Case 2: '200 PARK AVENUE LONDON GREATER LONDON' -> '200 PARK AVENUE' (canonical 1002)
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


@pytest.mark.skip(reason="Peeling logic removed from cleaning steps")
def test_peeled_address_matching_preserves_row_count(duck_con, peeled_test_data):
    """Test that peeled address matching doesn't inflate or reduce row count."""
    df_fuzzy, df_canonical = peeled_test_data

    results = run_deterministic_match_pass(
        duck_con,
        df_fuzzy,
        df_canonical,
        enabled_stage_names=["peeled_address"],
    )

    input_row_count = df_fuzzy.count("*").fetchone()[0]
    output_row_count = results.count("*").fetchone()[0]

    assert output_row_count == input_row_count, (
        f"Row count changed: input={input_row_count}, output={output_row_count}"
    )


@pytest.mark.skip(reason="Peeling logic removed from cleaning steps")
def test_peeled_address_matching_match_reason(duck_con, peeled_test_data):
    """Test that peeled matches have the correct match_reason."""
    df_fuzzy, df_canonical = peeled_test_data

    results = run_deterministic_match_pass(
        duck_con,
        df_fuzzy,
        df_canonical,
        enabled_stage_names=["peeled_address"],
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


@pytest.mark.skip(reason="Peeling logic removed from cleaning steps")
def test_peeled_address_multi_word_token_handling(duck_con):
    """Test that multi-word peeled tokens like 'TUNBRIDGE WELLS' are handled correctly.

    The key challenge: peeled_tokens_list=['TUNBRIDGE WELLS'] has length 1,
    but we need to remove 2 words from address_tokens.
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
                    ARRAY['10', 'TEST', 'STREET', 'TUNBRIDGE', 'WELLS'],
                    ARRAY['TUNBRIDGE WELLS'],
                    ARRAY['10']::VARCHAR[],
                    FALSE,
                    NULL::VARCHAR,
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
            address_tokens,
            peeled_tokens_list,
            numeric_tokens,
            has_flat_indicator,
            flat_positional,
            flat_letter,
            flat_number,
            non_traditional_address_type,
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
                    ARRAY['10', 'TEST', 'STREET'],
                    CAST([] AS VARCHAR[]),
                    ARRAY['10']::VARCHAR[],
                    FALSE,
                    NULL::VARCHAR,
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
            address_tokens,
            peeled_tokens_list,
            numeric_tokens,
            has_flat_indicator,
            flat_positional,
            flat_letter,
            flat_number,
            non_traditional_address_type,
            has_business_unit,
            business_unit_type,
            business_unit_id,
            ukam_address_id
        )
        """
    )

    results = run_deterministic_match_pass(
        duck_con,
        df_fuzzy,
        df_canonical,
        enabled_stage_names=["peeled_address"],
    )

    results_df = results.fetchdf()
    assert results_df.iloc[0]["resolved_canonical_id"] == 1000, (
        "Multi-word token 'TUNBRIDGE WELLS' should be correctly counted as 2 words"
    )
