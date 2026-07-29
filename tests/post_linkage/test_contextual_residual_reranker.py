from __future__ import annotations

from dataclasses import replace

import duckdb
import pytest

from uk_address_matcher.linking_model.matching.stages.splink import (
    _contextual_acceptance_lift_filter,
)
from uk_address_matcher.post_linkage.contextual_residual_reranker import (
    PRECISION_K3_CONFIG,
    improve_predictions_using_contextual_residuals,
)


def _sql_list(tokens: list[str]) -> str:
    return "[" + ", ".join(f"'{token}'" for token in tokens) + "]"


def _predictions(
    con: duckdb.DuckDBPyConnection,
    *,
    query_tokens: list[str],
    candidates: list[tuple[str, list[str], float]],
    phase1_scores: dict[str, float] | None = None,
) -> duckdb.DuckDBPyRelation:
    candidate_address_ids = {
        candidate_id: index
        for index, candidate_id in enumerate(
            sorted({item[0] for item in candidates}), start=1
        )
    }
    rows = ", ".join(
        "("
        + ", ".join(
            (
                f"'{candidate_id}'",
                "'source-1'",
                str(candidate_address_ids[candidate_id]),
                "10",
                _sql_list(tokens),
                _sql_list(query_tokens),
                str(splink_match_weight),
                "0.0",
                str(
                    phase1_scores.get(candidate_id, splink_match_weight)
                    if phase1_scores is not None
                    else splink_match_weight
                ),
            )
        )
        + ")"
        for candidate_id, tokens, splink_match_weight in candidates
    )
    return con.sql(f"""
        SELECT *
        FROM (VALUES {rows}) AS pairs(
            unique_id_l,
            unique_id_r,
            ukam_address_id_l,
            ukam_address_id_r,
            tokens_l,
            tokens_r,
            splink_match_weight,
            mw_adjustment,
            phase1_score
        )
    """)


def _result(
    *,
    query_tokens: list[str],
    candidates: list[tuple[str, list[str], float]],
    **options: object,
) -> list[dict[str, object]]:
    con = duckdb.connect(database=":memory:")
    try:
        config = options.get("config", PRECISION_K3_CONFIG)
        assert isinstance(config, type(PRECISION_K3_CONFIG))
        options["config"] = replace(config, include_diagnostics=True)
        relation = _predictions(
            con,
            query_tokens=query_tokens,
            candidates=candidates,
        )
        result = improve_predictions_using_contextual_residuals(
            df_predict=relation,
            con=con,
            **options,
        )
        return [dict(zip(result.columns, row)) for row in result.fetchall()]
    finally:
        con.close()


def _by_candidate(rows: list[dict[str, object]]) -> dict[str, dict[str, object]]:
    return {str(row["unique_id_l"]): row for row in rows}


def test_consensus_tokens_are_removed_and_unit_is_contextual_winner() -> None:
    rows = _by_candidate(
        _result(
            query_tokens=["UNIT", "3", "WESTGATE", "BUSINESS", "PARK"],
            candidates=[
                ("unit-2", ["UNIT", "2", "WESTGATE", "BUSINESS", "PARK"], 10.0),
                ("unit-3", ["UNIT", "3", "WESTGATE", "BUSINESS", "PARK"], 10.0),
                ("unit-4", ["UNIT", "4", "WESTGATE", "BUSINESS", "PARK"], 10.0),
            ],
        )
    )

    assert (
        rows["unit-3"]["contextual_match_weight"]
        > rows["unit-2"]["contextual_match_weight"]
    )
    assert (
        rows["unit-3"]["contextual_match_weight"]
        > rows["unit-4"]["contextual_match_weight"]
    )
    assert rows["unit-3"]["consensus_query_tokens"] == [
        "UNIT",
        "WESTGATE",
        "BUSINESS",
        "PARK",
    ]
    assert rows["unit-3"]["matched_residual_tokens"] == ["3"]


def test_commercial_noise_is_unaddressed_and_excluded_from_evidence() -> None:
    rows = _by_candidate(
        _result(
            query_tokens=[
                "ACME",
                "FACILITIES",
                "ACCOUNTS",
                "PAYABLE",
                "UNIT",
                "3",
                "WESTGATE",
                "BUSINESS",
                "PARK",
            ],
            candidates=[
                ("unit-2", ["UNIT", "2", "WESTGATE", "BUSINESS", "PARK"], 10.0),
                ("unit-3", ["UNIT", "3", "WESTGATE", "BUSINESS", "PARK"], 10.0),
                ("unit-4", ["UNIT", "4", "WESTGATE", "BUSINESS", "PARK"], 10.0),
            ],
        )
    )

    winner = rows["unit-3"]
    assert winner["unaddressed_query_tokens"] == [
        "ACME",
        "FACILITIES",
        "ACCOUNTS",
        "PAYABLE",
    ]
    assert winner["contextual_match_weight"] > rows["unit-2"]["contextual_match_weight"]


def test_missing_distinguishing_evidence_has_zero_contextual_adjustment() -> None:
    rows = _result(
        query_tokens=["ROSE", "HOUSE", "KING", "STREET"],
        candidates=[
            ("flat-1", ["FLAT", "1", "ROSE", "HOUSE", "KING", "STREET"], 10.0),
            ("flat-2", ["FLAT", "2", "ROSE", "HOUSE", "KING", "STREET"], 10.0),
            ("flat-3", ["FLAT", "3", "ROSE", "HOUSE", "KING", "STREET"], 10.0),
        ],
    )

    for row in rows:
        assert row["addressable_distinguishing_weight"] == pytest.approx(0.0)
        assert row["evidence_scale"] == pytest.approx(0.0)
        assert row["contextual_adjustment"] == pytest.approx(0.0)


def test_split_join_can_be_ablated_without_numeric_split() -> None:
    split_rows = _by_candidate(
        _result(
            query_tokens=["UNIT3", "WESTGATE"],
            candidates=[
                ("joined", ["UNIT", "3", "WEST", "GATE"], 10.0),
                ("other", ["UNIT", "2", "WEST", "GATE"], 10.0),
            ],
        )
    )
    no_split_rows = _by_candidate(
        _result(
            query_tokens=["UNIT3", "WESTGATE"],
            candidates=[
                ("joined", ["UNIT", "3", "WEST", "GATE"], 10.0),
                ("other", ["UNIT", "2", "WEST", "GATE"], 10.0),
            ],
            config=replace(PRECISION_K3_CONFIG, use_split_join=False),
        )
    )
    numeric_rows = _by_candidate(
        _result(
            query_tokens=["12"],
            candidates=[
                ("split-number", ["1", "2"], 10.0),
                ("other", ["3"], 10.0),
            ],
        )
    )

    assert split_rows["joined"]["split_join_rate"] > 0.0
    assert no_split_rows["joined"]["split_join_rate"] == pytest.approx(0.0)
    assert numeric_rows["split-number"]["split_join_rate"] == pytest.approx(0.0)


def test_soft_matching_is_conservative_and_can_be_disabled() -> None:
    soft_rows = _by_candidate(
        _result(
            query_tokens=["WILOUGHBY"],
            candidates=[
                ("near", ["WILLOUGHBY"], 10.0),
                ("other", ["WILDFLOWER"], 10.0),
            ],
        )
    )
    no_soft_rows = _by_candidate(
        _result(
            query_tokens=["WILOUGHBY"],
            candidates=[
                ("near", ["WILLOUGHBY"], 10.0),
                ("other", ["WILDFLOWER"], 10.0),
            ],
            config=replace(PRECISION_K3_CONFIG, use_soft_matching=False),
        )
    )
    excluded_rows = _result(
        query_tokens=["ABCD", "12345", "A12BC"],
        candidates=[
            ("candidate", ["ABCE", "12346", "A12BD"], 10.0),
            ("other", ["OTHER"], 10.0),
        ],
    )

    assert soft_rows["near"]["soft_match_rate"] > 0.0
    assert (
        soft_rows["near"]["contextual_match_weight"]
        > soft_rows["other"]["contextual_match_weight"]
    )
    assert no_soft_rows["near"]["soft_match_rate"] == pytest.approx(0.0)
    assert all(row["soft_match_rate"] == pytest.approx(0.0) for row in excluded_rows)


def test_ineligible_candidates_cannot_receive_positive_adjustment() -> None:
    rows = _by_candidate(
        _result(
            query_tokens=["UNIT", "3"],
            candidates=[
                ("winner", ["UNIT", "3"], 10.0),
                ("ineligible", ["UNIT", "2"], -3.0),
            ],
        )
    )

    assert rows["winner"]["eligible_candidate_count"] == 1
    assert rows["winner"]["contextual_adjustment"] == pytest.approx(0.0)
    assert not rows["ineligible"]["is_contextually_eligible"]
    assert rows["ineligible"]["contextual_adjustment"] <= 0.0


def test_support_advantage_blocks_positive_lift_for_non_b_candidate() -> None:
    ungated_rows = _by_candidate(
        _result(
            query_tokens=["UNIT", "3", "WESTGATE"],
            candidates=[
                ("b-winner", ["UNIT", "2", "WESTGATE"], 11.0),
                ("contextual-rival", ["UNIT", "3", "WESTGATE"], 10.0),
            ],
        )
    )
    gated_rows = _by_candidate(
        _result(
            query_tokens=["UNIT", "3", "WESTGATE"],
            candidates=[
                ("b-winner", ["UNIT", "2", "WESTGATE"], 11.0),
                ("contextual-rival", ["UNIT", "3", "WESTGATE"], 10.0),
            ],
            config=replace(PRECISION_K3_CONFIG, minimum_support_advantage=1.1),
        )
    )

    assert ungated_rows["contextual-rival"]["contextual_adjustment"] > 0.0
    assert gated_rows["contextual-rival"]["contextual_adjustment"] == pytest.approx(0.0)


def test_soft_matching_telemetry_counts_each_workload_stage() -> None:
    telemetry: dict[str, float | int] = {}

    _result(
        query_tokens=["WESGATE"],
        candidates=[("near", ["WESTGATE"], 10.0)],
        telemetry=telemetry,
    )

    assert telemetry["soft_residual_pairs_considered"] == 1
    assert telemetry["soft_jaro_winkler_pairs_evaluated"] == 1
    assert telemetry["soft_threshold_pairs"] == 1


def test_pair_deduplication_and_candidate_order_do_not_change_scores() -> None:
    candidates = [
        ("unit-2", ["UNIT", "2", "WESTGATE"], 10.0),
        ("unit-3", ["UNIT", "3", "WESTGATE"], 10.0),
    ]
    ordered = _by_candidate(
        _result(query_tokens=["UNIT", "3", "WESTGATE"], candidates=candidates)
    )
    reordered = _by_candidate(
        _result(
            query_tokens=["UNIT", "3", "WESTGATE"],
            candidates=list(reversed(candidates)),
        )
    )
    duplicated = _by_candidate(
        _result(
            query_tokens=["UNIT", "3", "3", "WESTGATE"],
            candidates=candidates + [candidates[1]],
        )
    )

    for candidate_id in ordered:
        assert ordered[candidate_id]["contextual_match_weight"] == pytest.approx(
            reordered[candidate_id]["contextual_match_weight"]
        )
        assert ordered[candidate_id]["contextual_match_weight"] == pytest.approx(
            duplicated[candidate_id]["contextual_match_weight"]
        )
        assert duplicated[candidate_id]["phase1_score"] == pytest.approx(
            duplicated[candidate_id]["splink_match_weight"]
            + duplicated[candidate_id]["mw_adjustment"]
        )


def test_contextual_base_score_selects_combined_or_splink_only_mode() -> None:
    con = duckdb.connect(database=":memory:")
    try:
        relation = _predictions(
            con,
            query_tokens=["UNIT", "3"],
            candidates=[
                ("phase1-winner", ["UNIT", "2"], 10.0),
                ("splink-winner", ["UNIT", "3"], 11.0),
            ],
            phase1_scores={"phase1-winner": 12.0, "splink-winner": 11.0},
        )
        combined_relation = improve_predictions_using_contextual_residuals(
            df_predict=relation,
            con=con,
            config=PRECISION_K3_CONFIG,
        )
        combined = combined_relation.fetchall()
        contextual_only_relation = improve_predictions_using_contextual_residuals(
            df_predict=relation,
            con=con,
            config=PRECISION_K3_CONFIG,
            contextual_base_score="splink_match_weight",
        )
        contextual_only = contextual_only_relation.fetchall()

        combined_rows = [dict(zip(combined_relation.columns, row)) for row in combined]
        contextual_only_rows = [
            dict(zip(contextual_only_relation.columns, row)) for row in contextual_only
        ]
        for row in combined_rows:
            assert row["contextual_match_weight"] - row[
                "contextual_adjustment"
            ] == pytest.approx(row["phase1_score"])
        for row in contextual_only_rows:
            assert row["contextual_match_weight"] - row[
                "contextual_adjustment"
            ] == pytest.approx(row["splink_match_weight"])
    finally:
        con.close()


def test_production_mode_omits_diagnostics_and_cleans_intermediate_tables() -> None:
    con = duckdb.connect(database=":memory:")
    try:
        relation = _predictions(
            con,
            query_tokens=["UNIT", "3"],
            candidates=[
                ("unit-2", ["UNIT", "2"], 10.0),
                ("unit-3", ["UNIT", "3"], 10.0),
            ],
        )
        result = improve_predictions_using_contextual_residuals(
            df_predict=relation,
            con=con,
        )

        assert "consensus_query_tokens" not in result.columns
        remaining_tables = con.sql("""
            SELECT table_name
            FROM duckdb_tables()
            WHERE table_name LIKE '__ukam__contextual_%'
        """).fetchall()
        assert remaining_tables == [("__ukam__contextual_final",)]
    finally:
        con.close()


def test_maximum_contextual_acceptance_lift_requires_both_thresholds() -> None:
    con = duckdb.connect(database=":memory:")
    try:
        acceptance_lift_filter = _contextual_acceptance_lift_filter(
            final_match_weight_threshold=10.0,
            maximum_acceptance_lift=2.0,
        )
        accepted = con.sql(f"""
            SELECT candidate_id
            FROM (
                VALUES
                    ('allowed-lift', 8.5, 10.1),
                    ('excessive-lift', 7.9, 11.0),
                    ('below-final-threshold', 8.5, 9.9)
            ) AS best_match(candidate_id, phase1_score, match_weight)
            WHERE best_match.match_weight >= 10.0
            {acceptance_lift_filter}
        """).fetchall()

        assert accepted == [("allowed-lift",)]
    finally:
        con.close()
