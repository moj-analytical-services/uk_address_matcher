from __future__ import annotations

import duckdb
import numpy as np
import pandas as pd
import pytest
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

from uk_address_matcher.model_training.additive_pairwise_road_assignment import (
    ADDITIVE_FEATURE_COLUMNS,
    _pairwise_training_frame as _additive_pairwise_training_frame,
    _select_precision_policy,
    _threshold_curve,
    additive_feature_sql,
)
from uk_address_matcher.model_training.all_sector_model import FEATURE_COLUMNS
from uk_address_matcher.model_training.linear_road_assignment import (
    FoldedLogisticModel,
    _top_one_metrics,
    fold_logistic_model,
    logistic_logit_sql,
)
from uk_address_matcher.model_training.tree_rule_scorecard import (
    TreeRule,
    TreeRuleCondition,
    rule_feature_definitions,
)


def test_folded_logistic_model_matches_standardised_sklearn_logits() -> None:
    features = np.arange(45, dtype=np.float64).reshape(3, len(FEATURE_COLUMNS))
    labels = np.array([0, 1, 1])
    scaler = StandardScaler().fit(features)
    model = LogisticRegression(C=1.0, solver="lbfgs").fit(
        scaler.transform(features), labels
    )

    folded = fold_logistic_model(model, scaler)
    expected = model.decision_function(scaler.transform(features))
    actual = features @ np.array(
        [folded.coefficients[feature] for feature in FEATURE_COLUMNS]
    )
    actual += folded.intercept

    assert actual == pytest.approx(expected)


def test_logistic_logit_sql_uses_folded_feature_weights() -> None:
    model = FoldedLogisticModel(
        intercept=-1.5,
        coefficients={
            feature: float(index) for index, feature in enumerate(FEATURE_COLUMNS)
        },
    )
    con = duckdb.connect()
    boolean_features = {
        "ends_at_tail",
        "road_syntax_terminal",
        "contains_residence_token",
        "contains_business_token",
    }
    columns = ", ".join(
        f"{feature} {'BOOLEAN' if feature in boolean_features else 'DOUBLE'}"
        for feature in FEATURE_COLUMNS
    )
    con.execute(f"CREATE TABLE features ({columns})")
    values = ", ".join(
        "true" if feature in boolean_features else str(index)
        for index, feature in enumerate(FEATURE_COLUMNS)
    )
    con.execute(f"INSERT INTO features VALUES ({values})")

    actual = con.execute(f"SELECT {logistic_logit_sql(model)} FROM features").fetchone()[
        0
    ]
    expected = -1.5 + sum(
        index * (1 if feature in boolean_features else index)
        for index, feature in enumerate(FEATURE_COLUMNS)
    )

    assert actual == expected


def test_top_one_metrics_uses_one_deterministic_winner_per_address() -> None:
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE scores AS SELECT * FROM (VALUES "
        "('a', 'BETA ROAD', 2, 1, true, true, 0.7), "
        "('a', 'ALPHA ROAD', 1, 0, false, true, 0.7), "
        "('b', 'GAMMA ROAD', 1, 1, true, true, 0.9), "
        "('c', 'DELTA ROAD', 1, NULL, false, false, 0.8)) "
        "AS rows(unique_id, candidate_phrase, candidate_start_position, candidate_label, "
        "candidate_exact, target_eligible, ranker_logit)"
    )

    actual = _top_one_metrics(con, score_table="scores")

    assert actual == {
        "reachable_addresses": 2,
        "top_one_correct": 1,
        "safe_core_top_one": 0.5,
        "safe_core_precision": 0.5,
        "safe_core_recall": 0.5,
        "safe_core_f1": 0.5,
        "exact_top_one": 0.5,
    }


def test_additive_pairwise_training_weights_each_address_equally() -> None:
    con = duckdb.connect()
    feature_values = {feature: [0.0, 0.0, 0.0] for feature in FEATURE_COLUMNS}
    frame = pd.DataFrame(
        {
            "unique_id": ["a", "a", "a"],
            "experiment_split": ["train", "train", "train"],
            "target_eligible": [True, True, True],
            "candidate_label": [1, 0, 0],
            **feature_values,
        }
    )
    con.register("feature_rows", frame)
    con.execute(
        "CREATE TABLE all_sector_candidate_features AS SELECT * FROM feature_rows"
    )

    actual, details = _additive_pairwise_training_frame(con, max_addresses=10)

    assert details["training_pairs"] == 2
    assert actual["pair_weight"].tolist() == [0.5, 0.5, 0.5, 0.5]


def test_additive_feature_sql_exposes_tree_thresholds_and_interactions() -> None:
    con = duckdb.connect()
    feature_values = {feature: 0.0 for feature in FEATURE_COLUMNS}
    feature_values.update(
        {
            "tail_length": 5.0,
            "start_tail_fraction": 0.6,
            "end_tail_fraction": 0.8,
            "road_syntax_terminal": True,
            "terminal_right_context_diversity": 2_000.0,
        }
    )
    columns = ", ".join(
        f"{name} {'BOOLEAN' if isinstance(value, bool) else 'DOUBLE'}"
        for name, value in feature_values.items()
    )
    values = ", ".join(str(value).lower() for value in feature_values.values())
    con.execute(f"CREATE TABLE candidates ({columns})")
    con.execute(f"INSERT INTO candidates VALUES ({values})")

    actual = con.execute(
        f"SELECT {additive_feature_sql('candidates')} FROM candidates"
    ).df()

    assert tuple(actual.columns) == ADDITIVE_FEATURE_COLUMNS
    assert actual.loc[0, "tail_length_ge_4_5"] == 1.0
    assert actual.loc[0, "end_tail_fraction_ge_0_8619"] == 0.0
    assert actual.loc[0, "road_terminal_x_tail_length_ge_4_5"] == 1.0


def test_precision_policy_selects_maximum_recall_at_requested_precision() -> None:
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE winners AS SELECT * FROM (VALUES "
        "(3.0, true, 1), (2.0, true, 1), (1.0, true, 0), (0.0, false, NULL)) "
        "AS rows(ranker_logit, target_eligible, winner_label)"
    )

    curve = _threshold_curve(con, winner_table="winners")
    actual = _select_precision_policy(curve, minimum_precision=1.0)

    assert actual == {
        "minimum_ranker_logit": 2.0,
        "precision": 1.0,
        "recall": 2 / 3,
        "f1": 0.8,
        "accepted_correct": 2,
        "accepted_incorrect": 0,
        "reachable_addresses": 3,
    }


def test_tree_rule_feature_definition_evaluates_its_full_conjunction() -> None:
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE candidates AS SELECT * FROM (VALUES (true, 5.0, 0.7), "
        "(true, 5.0, 0.8)) AS rows(road_syntax_terminal, tail_length, end_tail_fraction)"
    )
    rules = (
        TreeRule(
            conditions=(
                TreeRuleCondition("road_syntax_terminal", ">", 0.5),
                TreeRuleCondition("tail_length", ">", 4.5),
                TreeRuleCondition("end_tail_fraction", "<=", 0.75),
            ),
            training_gain=1.0,
        ),
    )
    name, expression = rule_feature_definitions(rules)[0]

    actual = con.execute(
        f"SELECT {expression.format(alias='candidates')} AS {name} FROM candidates"
    ).fetchall()

    assert actual == [(1.0,), (0.0,)]
