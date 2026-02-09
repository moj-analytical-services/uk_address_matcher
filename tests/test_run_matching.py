import pytest

from uk_address_matcher import SplinkStage, StageName, run_matching


@pytest.fixture
def matching_data(duck_con):
    """Test data for run_matching tests.

    Fuzzy dataset:
    - ID 1: '4 SAMPLE STREET CC3 3CC' -> exact match to canonical 1000
    - ID 2: '5 DEMO RD DD4 4DD' -> exact match to canonical 2000
    - ID 3: '999 MYSTERY LANE EE5 5EE' -> no match in canonical
    """
    df_fuzzy = duck_con.sql(
        """
        SELECT *
        FROM (
            VALUES
                (
                    '1'::VARCHAR,
                    '4 SAMPLE STREET',
                    '4 SAMPLE STREET',
                    'CC3 3CC',
                    ARRAY['4', 'SAMPLE', 'STREET'],
                    CAST([] AS VARCHAR[]),
                    CAST(NULL AS VARCHAR[]),
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    1::BIGINT
                ),
                (
                    '2'::VARCHAR,
                    '5 DEMO RD',
                    '5 DEMO RD',
                    'DD4 4DD',
                    ARRAY['5', 'DEMO', 'RD'],
                    CAST([] AS VARCHAR[]),
                    CAST(NULL AS VARCHAR[]),
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    2::BIGINT
                ),
                (
                    '3'::VARCHAR,
                    '999 MYSTERY LANE',
                    '999 MYSTERY LANE',
                    'EE5 5EE',
                    ARRAY['999', 'MYSTERY', 'LANE'],
                    CAST([] AS VARCHAR[]),
                    CAST(NULL AS VARCHAR[]),
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    3::BIGINT
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
                    '1000'::VARCHAR,
                    '4 SAMPLE STREET',
                    '4 SAMPLE STREET',
                    'CC3 3CC',
                    ARRAY['4', 'SAMPLE', 'STREET'],
                    CAST([] AS VARCHAR[]),
                    CAST(NULL AS VARCHAR[]),
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    100::BIGINT
                ),
                (
                    '2000'::VARCHAR,
                    '5 DEMO RD',
                    '5 DEMO RD',
                    'DD4 4DD',
                    ARRAY['5', 'DEMO', 'RD'],
                    CAST([] AS VARCHAR[]),
                    CAST(NULL AS VARCHAR[]),
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    FALSE,
                    NULL::VARCHAR,
                    NULL::VARCHAR,
                    200::BIGINT
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
            has_business_unit,
            business_unit_type,
            business_unit_id,
            ukam_address_id
        )
        """
    )

    return df_fuzzy, df_canonical


@pytest.fixture
def all_match_data(duck_con):
    """Test data where every fuzzy record has an exact match."""
    df_fuzzy = duck_con.sql(
        """
        SELECT *
        FROM (
            VALUES
                (
                    '1'::VARCHAR, '4 SAMPLE STREET', '4 SAMPLE STREET', 'CC3 3CC',
                    ARRAY['4', 'SAMPLE', 'STREET'], CAST([] AS VARCHAR[]),
                    CAST(NULL AS VARCHAR[]),
                    FALSE, NULL::VARCHAR, NULL::VARCHAR, NULL::VARCHAR,
                    FALSE, NULL::VARCHAR, NULL::VARCHAR,
                    1::BIGINT
                )
        ) AS t(
            unique_id, original_address_concat, clean_full_address, postcode,
            address_tokens, peeled_tokens_list, numeric_tokens,
            has_flat_indicator, flat_positional, flat_letter, flat_number,
            has_business_unit, business_unit_type, business_unit_id,
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
                    '1000'::VARCHAR, '4 SAMPLE STREET', '4 SAMPLE STREET', 'CC3 3CC',
                    ARRAY['4', 'SAMPLE', 'STREET'], CAST([] AS VARCHAR[]),
                    CAST(NULL AS VARCHAR[]),
                    FALSE, NULL::VARCHAR, NULL::VARCHAR, NULL::VARCHAR,
                    FALSE, NULL::VARCHAR, NULL::VARCHAR,
                    100::BIGINT
                )
        ) AS t(
            unique_id, original_address_concat, clean_full_address, postcode,
            address_tokens, peeled_tokens_list, numeric_tokens,
            has_flat_indicator, flat_positional, flat_letter, flat_number,
            has_business_unit, business_unit_type, business_unit_id,
            ukam_address_id
        )
        """
    )

    return df_fuzzy, df_canonical


class TestRunMatchingDeterministicOnly:
    """Tests for run_matching with deterministic stages only (no Splink)."""

    def test_exact_match_finds_matches(self, duck_con, matching_data):
        df_fuzzy, df_canonical = matching_data

        results = run_matching(
            duck_con,
            df_fuzzy,
            df_canonical,
            stages=[StageName.EXACT_MATCHES],
        )

        results_df = results.fetchdf()

        # Should have all 3 fuzzy rows
        assert len(results_df) == 3

        # Check that IDs 1 and 2 matched
        matched = results_df[results_df["resolved_canonical_id"].notna()]
        assert len(matched) == 2

        matched_dict = dict(zip(matched["unique_id"], matched["resolved_canonical_id"]))
        assert matched_dict["1"] == "1000"
        assert matched_dict["2"] == "2000"

    def test_exact_match_sets_match_reason(self, duck_con, matching_data):
        df_fuzzy, df_canonical = matching_data

        results = run_matching(
            duck_con,
            df_fuzzy,
            df_canonical,
            stages=[StageName.EXACT_MATCHES],
        )

        results_df = results.fetchdf()
        matched = results_df[results_df["resolved_canonical_id"].notna()]

        for _, row in matched.iterrows():
            assert row["match_reason"] == "exact: full match"

    def test_unmatched_rows_have_nulls(self, duck_con, matching_data):
        import pandas as pd

        df_fuzzy, df_canonical = matching_data

        results = run_matching(
            duck_con,
            df_fuzzy,
            df_canonical,
            stages=[StageName.EXACT_MATCHES],
        )

        results_df = results.fetchdf()
        unmatched = results_df[results_df["unique_id"] == "3"]

        assert len(unmatched) == 1
        row = unmatched.iloc[0]
        assert pd.isna(row["resolved_canonical_id"])
        assert pd.isna(row["match_reason"])

    def test_all_records_matched_early_exit(self, duck_con, all_match_data):
        """When all records match in exact stage, subsequent stages are skipped."""
        df_fuzzy, df_canonical = all_match_data

        results = run_matching(
            duck_con,
            df_fuzzy,
            df_canonical,
            stages=[StageName.EXACT_MATCHES],
        )

        results_df = results.fetchdf()
        assert len(results_df) == 1
        assert results_df.iloc[0]["resolved_canonical_id"] == "1000"

    def test_no_splink_columns_when_splink_not_run(self, duck_con, matching_data):
        """When Splink stage is not in the stage list, match_weight and
        distinguishability columns should not appear."""
        df_fuzzy, df_canonical = matching_data

        results = run_matching(
            duck_con,
            df_fuzzy,
            df_canonical,
            stages=[StageName.EXACT_MATCHES],
        )

        assert "match_weight" not in results.columns
        assert "distinguishability" not in results.columns

    def test_canonical_details_joined(self, duck_con, matching_data):
        """Final output should include canonical address details."""
        df_fuzzy, df_canonical = matching_data

        results = run_matching(
            duck_con,
            df_fuzzy,
            df_canonical,
            stages=[StageName.EXACT_MATCHES],
        )

        assert "original_address_concat_canonical" in results.columns
        assert "postcode_canonical" in results.columns

        results_df = results.fetchdf()
        matched_1 = results_df[results_df["unique_id"] == "1"].iloc[0]
        assert matched_1["original_address_concat_canonical"] == "4 SAMPLE STREET"
        assert matched_1["postcode_canonical"] == "CC3 3CC"


class TestRunMatchingRowCount:
    """Verify __ukam_results always has the correct number of rows."""

    def test_row_count_preserved(self, duck_con, matching_data):
        df_fuzzy, df_canonical = matching_data

        results = run_matching(
            duck_con,
            df_fuzzy,
            df_canonical,
            stages=[StageName.EXACT_MATCHES],
        )

        input_count = df_fuzzy.count("*").fetchone()[0]
        output_count = results.count("*").fetchone()[0]
        assert output_count == input_count


class TestStageNameValidation:
    """Test stage name parsing and validation."""

    def test_string_stage_names_accepted(self, duck_con, matching_data):
        df_fuzzy, df_canonical = matching_data

        results = run_matching(
            duck_con,
            df_fuzzy,
            df_canonical,
            stages=["exact_matches"],
        )

        assert results.count("*").fetchone()[0] == 3

    def test_unknown_stage_name_raises(self, duck_con, matching_data):
        df_fuzzy, df_canonical = matching_data

        with pytest.raises(ValueError, match="Unknown matching stage"):
            run_matching(
                duck_con,
                df_fuzzy,
                df_canonical,
                stages=["nonexistent_stage"],
            )

    def test_duplicate_stage_raises(self, duck_con, matching_data):
        df_fuzzy, df_canonical = matching_data

        with pytest.raises(ValueError, match="Duplicate stage"):
            run_matching(
                duck_con,
                df_fuzzy,
                df_canonical,
                stages=["exact_matches", "exact_matches"],
            )

    def test_exact_matches_always_included(self, duck_con, matching_data):
        """Even if not explicitly listed, EXACT_MATCHES runs."""
        df_fuzzy, df_canonical = matching_data

        # Pass only unique_trigram; exact_matches should still run
        results = run_matching(
            duck_con,
            df_fuzzy,
            df_canonical,
            stages=[StageName.UNIQUE_TRIGRAM],
        )

        results_df = results.fetchdf()
        matched = results_df[results_df["resolved_canonical_id"].notna()]
        # Exact matches should still have found IDs 1 and 2
        assert len(matched) >= 2


class TestRunMatchingSplinkConfigValidation:
    def test_invalid_splink_blocking_config_raises(self, duck_con, matching_data):
        df_fuzzy, df_canonical = matching_data

        with pytest.raises(
            ValueError,
            match="At least one of 'include_full_postcode_block' or 'include_outside_postcode_block' must be True",
        ):
            run_matching(
                con=duck_con,
                df_messy_clean=df_fuzzy,
                df_canonical_clean=df_canonical,
                stages=[
                    SplinkStage(
                        include_full_postcode_block=False,
                        include_outside_postcode_block=False,
                    )
                ],
            )
