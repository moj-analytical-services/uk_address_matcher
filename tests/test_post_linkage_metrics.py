import duckdb
import pytest

from uk_address_matcher.post_linkage.analyse_results import (
    calculate_match_metrics,
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

    result_df = calculate_match_metrics(relation).df()

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

    result_df = calculate_match_metrics(relation, order="ascending").df()

    assert list(result_df["match_reason"]) == ["method_a", "method_b"]


def test_calculate_exact_match_metrics_accepts_match_reason_column():
    con = duckdb.connect(database=":memory:")
    relation = con.sql(
        """
        SELECT *
        FROM (VALUES
            ('exact: postcode'),
            ('trie: fallback'),
            ('exact: postcode')
        ) AS t(match_reason)
        """
    )

    result_df = calculate_match_metrics(relation).df()

    assert set(result_df["match_reason"]) == {
        "exact: postcode",
        "trie: fallback",
    }
    counts = dict(zip(result_df["match_reason"], result_df["match_count"]))
    assert counts == {"exact: postcode": 2, "trie: fallback": 1}


def test_calculate_exact_match_metrics_requires_column():
    con = duckdb.connect(database=":memory:")
    relation = con.sql("SELECT 1 AS different_column")

    with pytest.raises(ValueError):
        calculate_match_metrics(relation)
