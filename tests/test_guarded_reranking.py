from __future__ import annotations

from itertools import count
from typing import Literal

import duckdb
import pytest

from uk_address_matcher import AddressMatcher, SplinkStage
from uk_address_matcher.post_linkage.analyse_results import (
    best_matches_with_distinguishability,
)
from uk_address_matcher.post_linkage.distinguishing_features.learned_reranking import (
    learned_reranker_relation,
)
from uk_address_matcher.post_linkage.identify_distinguishing_tokens import (
    _improve_predictions_using_distinguishing_tokens,
    improve_predictions_using_distinguishing_tokens,
)

_LEARNED_TABLES = count()


def _prediction_rows(
    *,
    source_postcode: str | None = "AB1 2CD",
    other_score: float = 9.0,
    representative_flat: str | None = None,
    alternate_aliases: tuple[tuple[int, float, str, str | None], ...] = (
        (102, 18.0, "FLAT B", "FLAT B"),
    ),
) -> list[tuple]:
    source_address = "FLAT B SCHOOL ROAD"
    rows = [
        (
            "uprn1",
            "source",
            101,
            1,
            20.0,
            0.9,
            "SCHOOL ROAD",
            source_address,
            "AB1 2CD",
            source_postcode,
            representative_flat,
            "FLAT B",
        )
    ]
    rows.extend(
        (
            "uprn1",
            "source",
            alias_id,
            1,
            score,
            0.8,
            address,
            source_address,
            "AB1 2CD",
            source_postcode,
            flat_identity,
            "FLAT B",
        )
        for alias_id, score, address, flat_identity in alternate_aliases
    )
    rows.extend(
        (
            unique_id,
            "source",
            address_id,
            1,
            score,
            0.8,
            address,
            source_address,
            postcode,
            source_postcode,
            None,
            "FLAT B",
        )
        for unique_id, address_id, score, address, postcode in (
            ("uprn2", 201, 18.0, "SCHOOL ROAD", "AB1 2CD"),
            ("uprn3", 301, other_score, "OTHER ROAD", "XY1 1ZZ"),
        )
    )
    return rows


def _prediction_relation(
    con: duckdb.DuckDBPyConnection,
    rows: list[tuple],
) -> duckdb.DuckDBPyRelation:
    con.execute("""
        CREATE TEMP TABLE df_predict (
            unique_id_l VARCHAR,
            unique_id_r VARCHAR,
            ukam_address_id_l BIGINT,
            ukam_address_id_r BIGINT,
            match_weight DOUBLE,
            match_probability DOUBLE,
            clean_full_address_l VARCHAR,
            clean_full_address_r VARCHAR,
            postcode_l VARCHAR,
            postcode_r VARCHAR,
            flat_identity_l VARCHAR,
            flat_identity_r VARCHAR
        )
    """)
    con.executemany(
        "INSERT INTO df_predict VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    return con.table("df_predict")


def _run_token_reranker(
    con: duckdb.DuckDBPyConnection,
    rows: list[tuple],
    *,
    projection: Literal["full", "lean"] = "full",
) -> duckdb.DuckDBPyRelation:
    predictions = _prediction_relation(con, rows)
    return _improve_predictions_using_distinguishing_tokens(
        df_predict=predictions,
        con=con,
        match_weight_threshold=-100,
        _projection=projection,
    )


def _as_mapping(value: object) -> dict:
    if isinstance(value, tuple) and len(value) == 1:
        value = value[0]
    if isinstance(value, dict):
        if "key" in value and "value" in value:
            pairs = zip(value["key"], value["value"], strict=True)
        else:
            pairs = value.items()
    else:
        pairs = value
    return {
        tuple(key) if isinstance(key, (list, tuple)) else key: count
        for key, count in pairs
    }


def _candidate_maps(
    con: duckdb.DuckDBPyConnection,
    relation: duckdb.DuckDBPyRelation,
    unique_id: str = "uprn1",
) -> tuple[dict, dict, dict]:
    row = con.sql(f"""
        SELECT overlapping_tokens_this_l_and_r,
               overlapping_bigrams_this_l_and_r,
               hist_all_bigrams_in_block_l
        FROM ({relation.sql_query()}) AS reranked
        WHERE unique_id_l = '{unique_id}'
    """).fetchall()[0]
    return tuple(_as_mapping(value) for value in row)


def test_alias_evidence_is_deduplicated_and_never_joins_across_aliases() -> None:
    with duckdb.connect() as con:
        result = _run_token_reranker(con, _prediction_rows())
        shortlist_rows, distinct_candidates = con.sql(f"""
            SELECT count(*),
                   count(DISTINCT (ukam_address_id_r, unique_id_l))
            FROM ({result.sql_query()}) AS reranked
        """).fetchall()[0]
        token_map, candidate_bigrams, block_bigrams = _candidate_maps(con, result)

    assert shortlist_rows == distinct_candidates == 3
    assert token_map["FLAT"] == 1
    assert token_map["B"] == 1
    assert candidate_bigrams[("FLAT", "B")] == 1
    assert block_bigrams[("SCHOOL", "ROAD")] == 2
    assert ("B", "SCHOOL") not in block_bigrams


def test_alternate_alias_ties_use_descending_alias_id() -> None:
    rows = _prediction_rows(
        alternate_aliases=(
            (102, 18.0, "FLAT C", "FLAT C"),
            (103, 18.0, "FLAT B", "FLAT B"),
        )
    )
    with duckdb.connect() as con:
        result = _run_token_reranker(con, rows)
        token_map, _, _ = _candidate_maps(con, result)

    assert "B" in token_map
    assert "C" not in token_map


@pytest.mark.parametrize(
    ("alternate_score", "alternate_used"),
    [(14.0, True), (13.99, False)],
)
def test_alternate_alias_must_be_within_six_raw_weight_points(
    alternate_score: float,
    alternate_used: bool,
) -> None:
    rows = _prediction_rows(
        alternate_aliases=((102, alternate_score, "FLAT B", "FLAT B"),)
    )
    with duckdb.connect() as con:
        result = _run_token_reranker(con, rows)
        token_map, _, _ = _candidate_maps(con, result)

    assert ("FLAT" in token_map) is alternate_used


def test_incompatible_populated_flat_identities_do_not_supply_alias_evidence() -> None:
    rows = _prediction_rows(representative_flat="FLAT A")
    with duckdb.connect() as con:
        result = _run_token_reranker(con, rows)
        token_map, _, _ = _candidate_maps(con, result)

    assert "FLAT" not in token_map


@pytest.mark.parametrize(
    ("source_postcode", "other_score"),
    [(None, 9.0), ("AB1 2CD", 10.0)],
)
def test_alias_guard_requires_postcode_and_a_strict_ten_point_lead(
    source_postcode: str | None,
    other_score: float,
) -> None:
    rows = _prediction_rows(source_postcode=source_postcode, other_score=other_score)
    with duckdb.connect() as con:
        result = _run_token_reranker(con, rows)
        token_map, _, _ = _candidate_maps(con, result)

    assert "FLAT" not in token_map


def test_full_and_compact_projections_keep_scores_winners_and_margins_equal() -> None:
    with duckdb.connect() as con:
        predictions = _prediction_relation(con, _prediction_rows())
        full = improve_predictions_using_distinguishing_tokens(
            df_predict=predictions,
            con=con,
            match_weight_threshold=-100,
        )
        lean = _improve_predictions_using_distinguishing_tokens(
            df_predict=predictions,
            con=con,
            match_weight_threshold=-100,
            _projection="lean",
        )
        assert {
            "overlapping_tokens_this_l_and_r",
            "hist_all_tokens_in_block_l",
            "bigrams_l",
        }.issubset(full.columns)
        assert {
            "overlapping_tokens_this_l_and_r",
            "hist_all_tokens_in_block_l",
            "bigrams_l",
        }.isdisjoint(lean.columns)

        score_columns = (
            "unique_id_l, match_weight_original, match_weight, token_reward, "
            "token_absence_penalty, bigram_reward, bigram_absence_penalty, "
            "missing_token_penalty, positional_conflict_penalty, mw_adjustment"
        )
        full_scores = con.sql(
            f"SELECT {score_columns} FROM ({full.sql_query()}) ORDER BY unique_id_l"
        ).fetchall()
        lean_scores = con.sql(
            f"SELECT {score_columns} FROM ({lean.sql_query()}) ORDER BY unique_id_l"
        ).fetchall()
        assert full_scores == lean_scores

        source = con.sql("""
            SELECT 'source' AS unique_id, 1::BIGINT AS ukam_address_id,
                   'FLAT B SCHOOL ROAD' AS original_address_concat,
                   'AB1 2CD' AS postcode
        """)
        full_best = best_matches_with_distinguishability(
            full, source, con, best_match_only=False
        )
        lean_best = best_matches_with_distinguishability(
            lean, source, con, best_match_only=False
        )
        best_columns = "unique_id_l, match_weight, distinguishability, candidate_rank"
        full_winners = con.sql(
            f"SELECT {best_columns} FROM ({full_best.sql_query()}) "
            "ORDER BY candidate_rank"
        ).fetchall()
        lean_winners = con.sql(
            f"SELECT {best_columns} FROM ({lean_best.sql_query()}) "
            "ORDER BY candidate_rank"
        ).fetchall()

    assert full_winners == lean_winners


def _learned_input(
    con: duckdb.DuckDBPyConnection,
    rows: list[tuple],
    *,
    with_range: bool = False,
    with_range_guard: bool = False,
) -> duckdb.DuckDBPyRelation:
    columns = [
        "ukam_address_id_r BIGINT",
        "ukam_address_id_l BIGINT",
        "unique_id_l VARCHAR",
        "match_weight_original DOUBLE",
        "match_weight DOUBLE",
        "clean_full_address_l VARCHAR",
        "clean_full_address_r VARCHAR",
        "postcode_l VARCHAR",
        "postcode_r VARCHAR",
        "mw_adjustment DOUBLE",
        "token_reward DOUBLE",
        "token_absence_penalty DOUBLE",
        "bigram_reward DOUBLE",
        "bigram_absence_penalty DOUBLE",
    ]
    if with_range:
        columns.append("numeric_range_adjustment DOUBLE")
    if with_range_guard:
        columns.append("numeric_range_guard_passed BOOLEAN")
    table_name = f"learned_candidates_{next(_LEARNED_TABLES)}"
    con.execute(f"CREATE TEMP TABLE {table_name} ({', '.join(columns)})")
    placeholders = ", ".join("?" for _ in columns)
    con.executemany(
        f"INSERT INTO {table_name} VALUES ({placeholders})",
        rows,
    )
    return con.table(table_name)


def _learned_rows(
    con: duckdb.DuckDBPyConnection,
    relation: duckdb.DuckDBPyRelation,
) -> dict[str, tuple[float, float]]:
    rows = con.sql(f"""
        SELECT unique_id_l, match_weight, learned_reranker_adjustment
        FROM ({relation.sql_query()}) AS learned
        ORDER BY unique_id_l
    """).fetchall()
    return {unique_id: (score, delta) for unique_id, score, delta in rows}


def _candidate(
    unique_id: str,
    address_id: int,
    raw_weight: float,
    *,
    current_weight: float | None = None,
    postcode: str | None = "AB1 2CD",
    adjustment: float = 0.0,
    token_reward: float = 1.0,
    range_adjustment: float | None = None,
    range_guard_passed: bool | None = None,
) -> tuple:
    row = (
        1,
        address_id,
        unique_id,
        raw_weight,
        raw_weight if current_weight is None else current_weight,
        f"{address_id} HIGH STREET",
        "10 HIGH STREET",
        postcode,
        "AB1 2CD",
        adjustment,
        token_reward,
        1.0,
        0.5,
        0.0,
    )
    if range_adjustment is not None:
        row += (range_adjustment,)
    if range_guard_passed is not None:
        row += (range_guard_passed,)
    return row


def test_learned_score_only_updates_guarded_same_postcode_candidates() -> None:
    with duckdb.connect() as con:
        candidates = _learned_input(
            con,
            [
                _candidate("best", 101, 20.0),
                _candidate("second", 102, 18.0),
                _candidate("edge", 103, 11.0),
                _candidate("other_postcode", 104, 9.0, postcode="XY1 1ZZ"),
            ],
        )
        learned = learned_reranker_relation(con, candidates)
        rows = _learned_rows(con, learned)

    assert rows["other_postcode"][1] == 0.0
    assert all(abs(rows[unique_id][1]) <= 2.5 for unique_id in ("best", "second", "edge"))
    assert any(rows[unique_id][1] != 0.0 for unique_id in ("best", "second", "edge"))
    assert all(
        rows[unique_id][0] == pytest.approx(raw_weight + float(rows[unique_id][1]))
        for unique_id, raw_weight in (
            ("best", 20.0),
            ("second", 18.0),
            ("edge", 11.0),
            ("other_postcode", 9.0),
        )
    )


def test_learned_score_keeps_the_strict_raw_and_final_postcode_guards() -> None:
    with duckdb.connect() as con:
        raw_boundary = _learned_input(
            con,
            [
                _candidate("best", 101, 20.0),
                _candidate("second", 102, 18.0),
                _candidate("raw_boundary", 103, 10.0, postcode=None),
            ],
        )
        final_boundary = _learned_input(
            con,
            [
                _candidate("best", 101, 20.0),
                _candidate("second", 102, 18.0),
                _candidate(
                    "final_boundary",
                    103,
                    9.0,
                    current_weight=17.0,
                    postcode="XY1 1ZZ",
                    adjustment=8.0,
                ),
            ],
        )
        raw_result = learned_reranker_relation(con, raw_boundary)
        final_result = learned_reranker_relation(con, final_boundary)
        raw_rows = _learned_rows(con, raw_result)
        final_rows = _learned_rows(con, final_result)

    assert all(delta == 0.0 for _, delta in raw_rows.values())
    assert all(delta == 0.0 for _, delta in final_rows.values())


@pytest.mark.parametrize("protected_by", ["range", "postcode_promotion"])
def test_learned_score_protects_range_and_existing_postcode_promotions(
    protected_by: str,
) -> None:
    with duckdb.connect() as con:
        if protected_by == "range":
            candidates = _learned_input(
                con,
                [
                    _candidate(
                        "best",
                        101,
                        20.0,
                        current_weight=21.0,
                        range_adjustment=1.0,
                    ),
                    _candidate("second", 102, 18.0, range_adjustment=0.0),
                    _candidate(
                        "other_postcode",
                        103,
                        9.0,
                        postcode="XY1 1ZZ",
                        range_adjustment=0.0,
                    ),
                ],
                with_range=True,
            )
        else:
            candidates = _learned_input(
                con,
                [
                    _candidate("best", 101, 20.0, current_weight=22.0, adjustment=1.0),
                    _candidate("second", 102, 18.0),
                    _candidate("other_postcode", 103, 9.0, postcode="XY1 1ZZ"),
                ],
            )
        learned = learned_reranker_relation(con, candidates)
        rows = _learned_rows(con, learned)

    assert all(delta == 0.0 for _, delta in rows.values())


def test_zero_range_adjustment_guard_flag_does_not_protect_the_query() -> None:
    with duckdb.connect() as con:
        candidates = _learned_input(
            con,
            [
                _candidate(
                    "best",
                    101,
                    20.0,
                    range_adjustment=0.0,
                    range_guard_passed=True,
                ),
                _candidate(
                    "second",
                    102,
                    18.0,
                    range_adjustment=0.0,
                    range_guard_passed=False,
                ),
                _candidate(
                    "other_postcode",
                    103,
                    9.0,
                    postcode="XY1 1ZZ",
                    range_adjustment=0.0,
                    range_guard_passed=False,
                ),
            ],
            with_range=True,
            with_range_guard=True,
        )
        learned = learned_reranker_relation(con, candidates)
        rows = _learned_rows(con, learned)

    assert any(rows[unique_id][1] != 0.0 for unique_id in ("best", "second"))


@pytest.mark.parametrize("retain_diagnostics", [False, True])
def test_splink_stage_materialises_learned_scores_for_acceptance(
    retain_diagnostics: bool,
) -> None:
    with duckdb.connect() as con:
        canonical = con.sql("""
            SELECT * FROM (VALUES
                ('c1', '10 HIGH STREET', 'AB1 2CD'),
                ('c2', '10 HIGH STREET FLAT B', 'AB1 2CD'),
                ('c3', '10 HIGH STREET FLAT C', 'AB1 2CD')
            ) AS addresses(unique_id, address_concat, postcode)
        """)
        messy = con.sql("""
            SELECT * FROM (VALUES
                ('m1', '10 HIGH STREET FLAT B', 'AB1 2CD')
            ) AS addresses(unique_id, address_concat, postcode)
        """)
        stage = SplinkStage(
            minimum_match_weight=-50,
            minimum_distinguishability=None,
            retain_intermediate_calculation_columns=retain_diagnostics,
        )
        AddressMatcher(
            canonical_addresses=canonical,
            addresses_to_match=messy,
            con=con,
            stages=[stage],
            show_progress=False,
        ).match()

        assert stage.improved_predictions_table is not None
        assert stage.best_matches_table is not None
        improved = con.table(stage.improved_predictions_table)
        assert "learned_reranker_adjustment" in improved.columns
        if retain_diagnostics:
            assert "overlapping_tokens_this_l_and_r" in improved.columns
        else:
            assert "overlapping_tokens_this_l_and_r" not in improved.columns

        best_count, score_mismatches = con.sql(f"""
            SELECT count(*),
                   count_if(best.match_weight IS DISTINCT FROM improved.match_weight)
            FROM {stage.best_matches_table} AS best
            JOIN {stage.improved_predictions_table} AS improved
              USING (ukam_address_id_r, unique_id_l, ukam_address_id_l)
            WHERE best.candidate_rank = 1
        """).fetchall()[0]

    assert best_count > 0
    assert score_mismatches == 0
