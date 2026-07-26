import math

import pytest

from uk_address_matcher import AddressMatcher
from uk_address_matcher.cleaning.chunking_strategies import prepare_data_for_matching
from uk_address_matcher.linking_model.splink_model import (
    _align_distinguishing_token_columns,
    _get_linker,
    _get_model_settings_dict,
    _sanitise_null_comparison_levels,
)
from uk_address_matcher.sql_pipeline.match_reasons import MatchReason


@pytest.fixture
def resolved_only_matches(duck_con):
    reason = MatchReason.EXACT.value
    return duck_con.sql(
        f"""
        SELECT *
        FROM (
            VALUES
                (
                    1::BIGINT,
                    'ADDRESS 1'::VARCHAR,
                    'POSTCODE 1'::VARCHAR,
                    '{reason}'::VARCHAR,
                    100::BIGINT,
                    NULL::BIGINT
                )
        ) AS t(
            unique_id,
            original_address_concat,
            postcode,
            match_reason,
            resolved_canonical_id,
            canonical_ukam_address_id
        )
        """
    )


@pytest.fixture
def unresolved_matches(duck_con):
    reason = MatchReason.EXACT.value
    return duck_con.sql(
        f"""
        SELECT *
        FROM (
            VALUES
                (
                    2::BIGINT,
                    'ADDRESS 2'::VARCHAR,
                    'POSTCODE 2'::VARCHAR,
                    '{reason}'::VARCHAR,
                    NULL::BIGINT,
                    NULL::BIGINT
                )
        ) AS t(
            unique_id,
            original_address_concat,
            postcode,
            match_reason,
            resolved_canonical_id,
            canonical_ukam_address_id
        )
        """
    )


@pytest.fixture
def canonical_non_empty(duck_con):
    return duck_con.sql(
        """
        SELECT *
        FROM (
            VALUES
                (100::BIGINT, 'CANONICAL 1'::VARCHAR, 'POSTCODE 1'::VARCHAR)
        ) AS t(unique_id, original_address_concat, postcode)
        """
    )


@pytest.fixture
def canonical_empty(duck_con):
    return duck_con.sql(
        """
        SELECT *
        FROM (
            SELECT
                CAST(NULL AS BIGINT) AS unique_id,
                CAST(NULL AS VARCHAR) AS original_address_concat,
                CAST(NULL AS VARCHAR) AS postcode
        )
        WHERE 1 = 0
        """
    )


def test_get_linker_raises_when_no_unresolved_rows(
    duck_con,
    resolved_only_matches,
    canonical_non_empty,
):
    with pytest.raises(ValueError, match="No unresolved records remain"):
        _get_linker(
            df_addresses_to_match=resolved_only_matches,
            df_addresses_to_search_within=canonical_non_empty,
            con=duck_con,
        )


def test_get_linker_raises_when_canonical_empty(
    duck_con,
    unresolved_matches,
    canonical_empty,
):
    with pytest.raises(ValueError, match="Canonical relation is empty"):
        _get_linker(
            df_addresses_to_match=unresolved_matches,
            df_addresses_to_search_within=canonical_empty,
            con=duck_con,
        )


def test_align_distinguishing_tokens_adds_typed_empty_and_preserves_values(duck_con):
    messy = duck_con.sql("SELECT 1 AS unique_id")
    canonical = duck_con.sql(
        "SELECT 2 AS unique_id, ['FLAT', 'A']::VARCHAR[] "
        "AS distinguishing_adj_start_tokens"
    )

    aligned_messy, aligned_canonical = _align_distinguishing_token_columns(
        messy,
        canonical,
    )

    assert aligned_messy.project("distinguishing_adj_start_tokens").fetchone() == ([],)
    assert str(aligned_messy.types[-1]) == "VARCHAR[]"
    assert aligned_canonical.project("distinguishing_adj_start_tokens").fetchone() == (
        ["FLAT", "A"],
    )


def test_packaged_distinguishing_token_comparison_has_exact_fixed_weights():
    settings = _get_model_settings_dict()
    comparison = next(
        comparison
        for comparison in settings["comparisons"]
        if comparison["output_column_name"] == "neighbour_distinguishing_tokens"
    )
    levels = comparison["comparison_levels"]

    assert [level["label_for_charts"] for level in levels] == [
        "No distinguishing tokens",
        "All distinguishing tokens present",
        "Some distinguishing tokens present",
        "No distinguishing tokens present",
    ]
    assert levels[0] == {
        "sql_condition": (
            "distinguishing_adj_start_tokens_l IS NULL OR "
            "len(distinguishing_adj_start_tokens_l) = 0"
        ),
        "label_for_charts": "No distinguishing tokens",
        "is_null_level": True,
    }
    assert [
        math.log2(level["m_probability"] / level["u_probability"]) for level in levels[1:]
    ] == [1.0, 0.5000000000000001, -10.0]
    assert all(
        level["fix_m_probability"] and level["fix_u_probability"] for level in levels[1:]
    )


def test_sanitise_null_comparison_level_removes_probabilities():
    settings = {
        "comparisons": [
            {
                "comparison_levels": [
                    {
                        "is_null_level": True,
                        "m_probability": 1.0,
                        "u_probability": 1.0,
                    }
                ]
            }
        ]
    }

    sanitised = _sanitise_null_comparison_levels(settings)

    assert sanitised["comparisons"][0]["comparison_levels"][0] == {"is_null_level": True}


def test_distinguishing_token_comparison_contributes_expected_match_weights(duck_con):
    canonical = duck_con.sql(
        """
        SELECT * FROM (VALUES
            ('c_all', 'FLAT A 1 HIGH STREET CAMDEN LONDON', 'N1 1AA'),
            ('c_all_base', '1 HIGH STREET CAMDEN LONDON', 'N1 1AA'),
            (
                'c_some',
                'OLD STATION HOUSE RAINBOW LANE TAUNTON',
                'TA1 1AA'
            ),
            ('c_some_neighbour', 'NEW RAINBOW LANE TAUNTON', 'TA1 1AA')
        ) AS t(unique_id, address_concat, postcode)
        """
    )
    messy = duck_con.sql(
        """
        SELECT * FROM (VALUES
            ('m_all', 'FLAT A 1 HIGH STREET CAMDEN LONDON', 'N1 1AA'),
            ('m_some', 'OLD RAINBOW LANE TAUNTON', 'TA1 1AA'),
            ('m_none', 'RAINBOW LANE TAUNTON', 'TA1 1AA')
        ) AS t(unique_id, address_concat, postcode)
        """
    )
    canonical_clean = prepare_data_for_matching(
        canonical,
        con=duck_con,
        num_of_chunks=1,
        derive_distinguishing_wrt_adjacent_records=True,
        dataset_role="canonical",
        show_progress=False,
    )
    messy_clean = prepare_data_for_matching(
        messy,
        con=duck_con,
        num_of_chunks=1,
        dataset_role="messy",
        show_progress=False,
    )

    assert canonical_clean.filter("unique_id = 'c_some'").project(
        "distinguishing_adj_start_tokens"
    ).fetchone() == (["OLD", "STATION", "HOUSE"],)

    linker = _get_linker(
        messy_clean,
        canonical_clean,
        con=duck_con,
        include_full_postcode_block=True,
        include_outside_postcode_block=False,
        retain_intermediate_calculation_columns=True,
    )
    predictions = linker.inference.predict(threshold_match_weight=-100)
    prediction_rows = predictions.as_pandas_dataframe()

    expected_weights = {
        frozenset(("c_all", "m_all")): 1.0,
        frozenset(("c_some", "m_some")): 0.5,
        frozenset(("c_some", "m_none")): -10.0,
    }
    actual_weights = {}
    for _, row in prediction_rows.iterrows():
        pair = frozenset((row["unique_id_l"], row["unique_id_r"]))
        if pair in expected_weights:
            actual_weights[pair] = math.log2(
                float(row["bf_neighbour_distinguishing_tokens"])
            )

    assert actual_weights == pytest.approx(expected_weights)


def test_address_matcher_derives_distinguishing_tokens_only_for_canonical(duck_con):
    canonical = duck_con.sql(
        """
        SELECT * FROM (VALUES
            ('c_flat', 'FLAT A 1 HIGH STREET CAMDEN LONDON', 'N1 1AA'),
            ('c_base', '1 HIGH STREET CAMDEN LONDON', 'N1 1AA')
        ) AS t(unique_id, address_concat, postcode)
        """
    )
    messy = duck_con.sql(
        """
        SELECT * FROM (VALUES
            ('m_flat', 'FLAT A 1 HIGH STREET CAMDEN LONDON', 'N1 1AA')
        ) AS t(unique_id, address_concat, postcode)
        """
    )
    matcher = AddressMatcher(
        canonical_addresses=canonical,
        addresses_to_match=messy,
        con=duck_con,
        show_progress=False,
    )

    matcher._resolve_canonical_data()
    matcher._resolve_messy_data()

    assert "distinguishing_adj_start_tokens" in matcher._canonical_clean.columns
    assert matcher._canonical_clean.filter("unique_id = 'c_flat'").project(
        "distinguishing_adj_start_tokens"
    ).fetchone() == (["FLAT", "A"],)
    assert "distinguishing_adj_start_tokens" not in matcher._messy_clean.columns
