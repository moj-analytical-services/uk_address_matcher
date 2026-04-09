import duckdb

from uk_address_matcher import prepare_data_for_matching
from uk_address_matcher.linking_model.matching.stages.splink_integrations import (
    ngram_jaccard as ngram_jaccard_integration,
)
from uk_address_matcher.linking_model.splink_model import _get_linker


def _sql_literal(value: str | None) -> str:
    if value is None:
        return "NULL::VARCHAR"
    escaped_value = value.replace("'", "''")
    return f"'{escaped_value}'::VARCHAR"


def _clean_addresses(
    con: duckdb.DuckDBPyConnection,
    *,
    canonical_rows: list[tuple[str, str | None, str | None]],
    messy_rows: list[tuple[str, str | None, str | None]],
):
    canonical_values = ", ".join(
        f"({_sql_literal(unique_id)}, {_sql_literal(address)}, {_sql_literal(postcode)})"
        for unique_id, address, postcode in canonical_rows
    )
    messy_values = ", ".join(
        f"({_sql_literal(unique_id)}, {_sql_literal(address)}, {_sql_literal(postcode)})"
        for unique_id, address, postcode in messy_rows
    )

    canonical = con.sql(f"""
        SELECT *
        FROM (VALUES {canonical_values}) AS t(unique_id, address_concat, postcode)
    """)
    messy = con.sql(f"""
        SELECT *
        FROM (VALUES {messy_values}) AS t(unique_id, address_concat, postcode)
    """)

    return (
        prepare_data_for_matching(canonical, con=con),
        prepare_data_for_matching(messy, con=con),
    )


def test_predict_retains_only_ngram_score_by_default():
    con = duckdb.connect(":memory:")
    canonical_clean, messy_clean = _clean_addresses(
        con,
        canonical_rows=[("C1", "10 HIGH STREET LONDON", "SW1A 1AA")],
        messy_rows=[("M1", "10 HIGH ST LONDON", "SW1A 1AA")],
    )

    linker = _get_linker(
        df_addresses_to_match=messy_clean,
        df_addresses_to_search_within=canonical_clean,
        con=con,
        include_full_postcode_block=True,
        include_outside_postcode_block=False,
    )
    predictions = linker.inference.predict(threshold_match_weight=-100)
    result = predictions.as_pandas_dataframe()

    assert "ngram_final_score" in result.columns
    assert "ngram_shared_unusual_token_count" not in result.columns
    assert "ngram_shared_very_unusual_token_count" not in result.columns
    assert "ngram_intersection_count" not in result.columns
    assert "ngram_union_count" not in result.columns
    assert "ngram_trigram_jaccard" not in result.columns
    assert "postcode_exact" not in result.columns

    row = result.iloc[0]
    assert row["unique_id_l"] == "C1"
    assert row["unique_id_r"] == "M1"
    assert float(row["ngram_final_score"]) > 0.0


def test_shared_unusual_token_overlap_increases_ngram_final_score():
    con = duckdb.connect(":memory:")
    canonical_clean, messy_clean = _clean_addresses(
        con,
        canonical_rows=[("C1", "10 OLD BAKERY MEWS LONDON", "SW1A 1AA")],
        messy_rows=[
            ("M1", "12 OLD BAKERY ROAD LONDON", "SW1A 1AA"),
            ("M2", "12 OLD SMITH ROAD LONDON", "SW1A 1AA"),
        ],
    )

    linker = _get_linker(
        df_addresses_to_match=messy_clean,
        df_addresses_to_search_within=canonical_clean,
        con=con,
        include_full_postcode_block=True,
        include_outside_postcode_block=False,
    )
    predictions = linker.inference.predict(threshold_match_weight=-100)
    result = predictions.as_pandas_dataframe()

    score_by_messy_id = {
        row["unique_id_r"]: float(row["ngram_final_score"])
        for _, row in result.iterrows()
    }

    assert score_by_messy_id["M1"] > score_by_messy_id["M2"]


def test_non_blocked_rows_do_not_change_ngram_final_score():
    def run_prediction(canonical_rows: list[tuple[str, str, str]]):
        con = duckdb.connect(":memory:")
        canonical_clean, messy_clean = _clean_addresses(
            con,
            canonical_rows=canonical_rows,
            messy_rows=[("M1", "10 HIGH STREET LONDON", "SW1A 1AA")],
        )

        linker = _get_linker(
            df_addresses_to_match=messy_clean,
            df_addresses_to_search_within=canonical_clean,
            con=con,
            include_full_postcode_block=True,
            include_outside_postcode_block=False,
        )
        predictions = linker.inference.predict(threshold_match_weight=-100)
        result = predictions.as_pandas_dataframe()

        assert len(result) == 1
        row = result.iloc[0]
        assert row["unique_id_l"] == "C1"
        assert row["unique_id_r"] == "M1"
        return row

    baseline_row = run_prediction([("C1", "10 HIGH STREET LONDON", "SW1A 1AA")])

    distractor_rows = [
        (
            f"CD{i}",
            f"10 HIGH STREET LONDON SUITE {i}",
            "ZZ1 1ZZ",
        )
        for i in range(801)
    ]
    with_distractors_row = run_prediction(
        [("C1", "10 HIGH STREET LONDON", "SW1A 1AA"), *distractor_rows]
    )

    assert float(with_distractors_row["ngram_final_score"]) == float(
        baseline_row["ngram_final_score"]
    )


def test_null_and_empty_addresses_score_the_same_as_missing_inputs():
    con = duckdb.connect(":memory:")
    canonical_clean, messy_clean = _clean_addresses(
        con,
        canonical_rows=[
            ("C_NULL", None, "SW1A 1AA"),
            ("C_EMPTY", "", "SW1A 1AA"),
            ("C_FULL", "10 HIGH STREET LONDON", "SW1A 1AA"),
        ],
        messy_rows=[
            ("M_NULL", None, "SW1A 1AA"),
            ("M_EMPTY", "", "SW1A 1AA"),
            ("M_FULL", "10 HIGH STREET LONDON", "SW1A 1AA"),
        ],
    )

    linker = _get_linker(
        df_addresses_to_match=messy_clean,
        df_addresses_to_search_within=canonical_clean,
        con=con,
        include_full_postcode_block=True,
        include_outside_postcode_block=False,
    )
    result = linker.inference.predict(threshold_match_weight=-100).as_pandas_dataframe()

    score_by_pair = {
        (row["unique_id_l"], row["unique_id_r"]): float(row["ngram_final_score"])
        for _, row in result.iterrows()
    }

    assert score_by_pair[("C_FULL", "M_FULL")] > score_by_pair[("C_FULL", "M_EMPTY")]
    assert score_by_pair[("C_FULL", "M_FULL")] > score_by_pair[("C_FULL", "M_NULL")]
    assert score_by_pair[("C_FULL", "M_EMPTY")] == score_by_pair[("C_FULL", "M_NULL")]
    assert score_by_pair[("C_EMPTY", "M_FULL")] == score_by_pair[("C_NULL", "M_FULL")]


def test_precomputed_rare_token_arrays_drive_overlap_ratios():
    con = duckdb.connect(":memory:")
    blocked = con.sql("""
        SELECT *
        FROM (
            VALUES
                (
                    'C1',
                    'M1',
                    101,
                    201,
                    0,
                    'SW1A 1AA',
                    'SW1A 1AA',
                    ['10'],
                    ['10'],
                    ['MEWS'],
                    ['MEWS'],
                    [],
                    []
                ),
                (
                    'C1',
                    'M2',
                    101,
                    202,
                    0,
                    'SW1A 1AA',
                    'SW1A 1AA',
                    ['10'],
                    ['10'],
                    ['BAKERY'],
                    ['SMITH'],
                    [],
                    []
                ),
                (
                    'C1',
                    'M3',
                    101,
                    203,
                    0,
                    'SW1A 1AA',
                    'SW1A 1AA',
                    ['10'],
                    ['10'],
                    [],
                    [],
                    ['BAKERY'],
                    ['BAKERY']
                ),
                (
                    'C1',
                    'M4',
                    101,
                    204,
                    0,
                    'SW1A 1AA',
                    'SW1A 1AA',
                    ['10'],
                    ['10'],
                    [],
                    [],
                    [],
                    []
                ),
                (
                    'C1',
                    'M5',
                    101,
                    205,
                    0,
                    'SW1A 1AA',
                    'SW1A 1AA',
                    ['10'],
                    ['10'],
                    ['MEWS', 'COURT'],
                    ['MEWS', 'LANE'],
                    [],
                    []
                )
        ) AS t(
            unique_id_l,
            unique_id_r,
            ukam_address_id_l,
            ukam_address_id_r,
            match_key,
            postcode_l,
            postcode_r,
            numeric_tokens_l,
            numeric_tokens_r,
            unusual_tokens_arr_l,
            unusual_tokens_arr_r,
            very_unusual_tokens_arr_l,
            very_unusual_tokens_arr_r
        )
    """)
    nodes = con.sql("""
        SELECT *
        FROM (
            VALUES
                ('C1', 101, 'c_', '10 OLD BAKERY MEWS LONDON'),
                ('M1', 201, 'm_', '10 OLD BAKERY MEWS LONDON'),
                ('M2', 202, 'm_', '10 OLD BAKERY MEWS LONDON'),
                ('M3', 203, 'm_', '10 OLD BAKERY MEWS LONDON'),
                ('M5', 205, 'm_', '10 OLD BAKERY MEWS LONDON'),
                ('M4', 204, 'm_', '10 OLD BAKERY MEWS LONDON')
        ) AS t(unique_id, ukam_address_id, source_dataset, clean_full_address)
    """)

    con.register("blocked_with_cols_test", blocked)
    con.register("ngram_nodes_test", nodes)

    result = con.sql(
        f"""
        SELECT
            unique_id_l,
            unique_id_r,
            ngram_unusual_token_overlap_ratio,
            ngram_very_unusual_token_overlap_ratio,
            ngram_trigram_jaccard,
            ngram_final_score
        FROM (
            {
            ngram_jaccard_integration.build_blocked_pair_ngram_feature_sql(
                blocked_with_cols_table="blocked_with_cols_test",
                nodes_table="ngram_nodes_test",
            )
        }
        )
        """
    ).fetchall()

    rows_by_pair = {
        (row[0], row[1]): {
            "unusual_ratio": float(row[2]),
            "very_unusual_ratio": float(row[3]),
            "trigram_jaccard": float(row[4]),
            "final_score": float(row[5]),
        }
        for row in result
    }

    assert rows_by_pair[("C1", "M1")]["unusual_ratio"] == 1.0
    assert rows_by_pair[("C1", "M1")]["very_unusual_ratio"] == 0.0
    assert rows_by_pair[("C1", "M2")]["unusual_ratio"] == 0.0
    assert rows_by_pair[("C1", "M2")]["very_unusual_ratio"] == 0.0
    assert rows_by_pair[("C1", "M3")]["unusual_ratio"] == 0.0
    assert rows_by_pair[("C1", "M3")]["very_unusual_ratio"] == 1.0
    assert rows_by_pair[("C1", "M5")]["unusual_ratio"] == 1.0 / 3.0
    assert rows_by_pair[("C1", "M5")]["very_unusual_ratio"] == 0.0
    assert rows_by_pair[("C1", "M4")]["unusual_ratio"] == 0.0
    assert rows_by_pair[("C1", "M4")]["very_unusual_ratio"] == 0.0
    assert rows_by_pair[("C1", "M1")]["trigram_jaccard"] == 1.0
    assert rows_by_pair[("C1", "M2")]["trigram_jaccard"] == 1.0
    assert rows_by_pair[("C1", "M3")]["trigram_jaccard"] == 1.0
    assert rows_by_pair[("C1", "M4")]["trigram_jaccard"] == 1.0
    assert rows_by_pair[("C1", "M5")]["trigram_jaccard"] == 1.0
    assert (
        rows_by_pair[("C1", "M1")]["final_score"]
        > rows_by_pair[("C1", "M3")]["final_score"]
        > rows_by_pair[("C1", "M5")]["final_score"]
        > rows_by_pair[("C1", "M4")]["final_score"]
    )
