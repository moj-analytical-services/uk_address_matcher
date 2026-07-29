import duckdb
import pytest

from uk_address_matcher.post_linkage.analyse_results import (
    _calculate_match_metrics,
    best_matches_with_distinguishability,
)


def test_calculate_exact_match_metrics_basic_counts():
    con = duckdb.connect(database=":memory:")
    relation = con.sql(
        """
        SELECT *
        FROM (VALUES
            ('method_a'),
            ('method_b'),
            ('method_b')
        ) AS t(match_reason)
        """
    )

    result_df = _calculate_match_metrics(relation).df()

    assert set(result_df.columns) == {
        "match_reason",
        "match_count",
        "match_percentage",
    }
    assert list(result_df["match_reason"]) == ["method_b", "method_a"]
    counts = dict(zip(result_df["match_reason"], result_df["match_count"]))
    assert counts == {"method_b": 2, "method_a": 1}

    percentages = dict(zip(result_df["match_reason"], result_df["match_percentage"]))
    assert pytest.approx(percentages["method_b"], rel=1e-6) == "66.67%"
    assert pytest.approx(percentages["method_a"], rel=1e-6) == "33.33%"


def test_calculate_exact_match_metrics_supports_ascending_order():
    con = duckdb.connect(database=":memory:")
    relation = con.sql(
        """
        SELECT *
        FROM (VALUES
            ('method_a'),
            ('method_b'),
            ('method_b')
        ) AS t(match_reason)
        """
    )

    result_df = _calculate_match_metrics(relation, order="ascending").df()

    assert list(result_df["match_reason"]) == ["method_a", "method_b"]


def test_calculate_exact_match_metrics_accepts_match_reason_column():
    con = duckdb.connect(database=":memory:")
    relation = con.sql(
        """
        SELECT *
        FROM (VALUES
            ('exact: postcode'),
            ('splink: probabilistic'),
            ('exact: postcode')
        ) AS t(match_reason)
        """
    )

    result_df = _calculate_match_metrics(relation).df()

    assert set(result_df["match_reason"]) == {
        "exact: postcode",
        "splink: probabilistic",
    }
    counts = dict(zip(result_df["match_reason"], result_df["match_count"]))
    assert counts == {"exact: postcode": 2, "splink: probabilistic": 1}


def test_calculate_exact_match_metrics_requires_column():
    con = duckdb.connect(database=":memory:")
    relation = con.sql("SELECT 1 AS different_column")

    with pytest.raises(ValueError):
        _calculate_match_metrics(relation)


def test_best_matches_with_distinguishability_uses_distinct_canonical_candidates():
    con = duckdb.connect(database=":memory:")
    df_predict = con.sql(
        """
        SELECT *
        FROM (
            VALUES
                (
                    'U1', 'M1', 101, 1,
                    '10 HIGH STREET', 'AA1 1AA',
                    10.0, 0.0, NULL, 'source-1'
                ),
                (
                    'U1', 'M1', 102, 1,
                    '10 HIGH STREET ANNEX', 'AA1 1AA',
                    9.8, 0.0, NULL, 'source-1'
                ),
                (
                    'U2', 'M1', 201, 1,
                    '12 HIGH STREET', 'AA1 1AA',
                    8.0, 0.0, NULL, 'source-1'
                )
        ) AS t(
            unique_id_l,
            unique_id_r,
            ukam_address_id_l,
            ukam_address_id_r,
            original_address_concat_l,
            postcode_l,
            match_weight,
            mw_adjustment,
            ukam_reranker_audit_source_unique_id_l,
            ukam_reranker_audit_source_unique_id_r
        )
        """
    )
    df_addresses_to_match = con.sql(
        """
        SELECT *
        FROM (
            VALUES
                ('M1', 1, '10 High Street, Sampletown AA1 1AA', 'AA1 1AA')
        ) AS t(
            unique_id,
            ukam_address_id,
            original_address_concat,
            postcode
        )
        """
    )

    result = best_matches_with_distinguishability(
        df_predict=df_predict,
        df_addresses_to_match=df_addresses_to_match,
        con=con,
        additional_columns_to_retain=["ukam_reranker_audit_source_unique_id"],
    ).df()

    assert len(result) == 1
    assert result.loc[0, "unique_id_l"] == "U1"
    assert result.loc[0, "ukam_address_id_l"] == 101
    assert result.loc[0, "match_weight"] == pytest.approx(10.0)
    assert result.loc[0, "distinguishability"] == pytest.approx(2.0)
    assert result.loc[0, "candidate_rank"] == 1
    assert result.loc[0, "ukam_reranker_audit_source_unique_id_r"] == "source-1"


def test_best_matches_with_distinguishability_uses_consistent_top_row_when_tied():
    con = duckdb.connect(database=":memory:")
    df_predict = con.sql(
        """
        SELECT *
        FROM (
            VALUES
                (
                    'U2', 'M1', 201, 1,
                    'FIRST FLOOR FRONT FLAT', 'AA1 1AA',
                    13.91, 0.0
                ),
                (
                    'U3', 'M1', 301, 1,
                    'FIRST FLOOR REAR FLAT', 'AA1 1AA',
                    13.91, 0.0
                ),
                (
                    'U1', 'M1', 101, 1,
                    'GROUND FLOOR FLAT', 'AA1 1AA',
                    -2.56, 0.0
                )
        ) AS t(
            unique_id_l,
            unique_id_r,
            ukam_address_id_l,
            ukam_address_id_r,
            original_address_concat_l,
            postcode_l,
            match_weight,
            mw_adjustment
        )
        """
    )
    df_addresses_to_match = con.sql(
        """
        SELECT *
        FROM (
            VALUES
                ('M1', 1, 'Flat 1st Flr RR 81 Mount Pleasant Lane', 'AA1 1AA')
        ) AS t(
            unique_id,
            ukam_address_id,
            original_address_concat,
            postcode
        )
        """
    )

    best_only = best_matches_with_distinguishability(
        df_predict=df_predict,
        df_addresses_to_match=df_addresses_to_match,
        con=con,
        best_match_only=True,
    ).df()
    all_candidates = best_matches_with_distinguishability(
        df_predict=df_predict,
        df_addresses_to_match=df_addresses_to_match,
        con=con,
        best_match_only=False,
    ).df()

    top_from_all = all_candidates.loc[all_candidates["candidate_rank"] == 1].reset_index(
        drop=True
    )

    assert len(best_only) == 1
    assert len(top_from_all) == 1
    assert best_only.loc[0, "unique_id_l"] == top_from_all.loc[0, "unique_id_l"]
    assert (
        best_only.loc[0, "ukam_address_id_l"] == top_from_all.loc[0, "ukam_address_id_l"]
    )
    assert best_only.loc[0, "candidate_rank"] == 1
    assert top_from_all.loc[0, "candidate_rank"] == 1
    assert best_only.loc[0, "distinguishability"] == pytest.approx(0.0)
    assert top_from_all.loc[0, "distinguishability"] == pytest.approx(0.0)
