"""Distil shallow high-gain HGB paths into a DuckDB-native pairwise scorecard."""

from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

import duckdb
import joblib
import pandas as pd

from uk_address_matcher.cleaning.steps.road_resources import sql_text

from .additive_pairwise_road_assignment import (
    ADDITIVE_FEATURES,
    _pairwise_training_frame,
    _score_with_timing,
)
from .all_sector_model import FEATURE_COLUMNS
from .linear_road_assignment import (
    FoldedLogisticModel,
    _fit_model,
    _top_one_metrics,
    fold_logistic_model,
)

RULE_SCORECARD_VERSION = 1
RULE_COUNTS = (32, 64)
RULE_REGULARIZATIONS = (0.01, 0.03)


@dataclass(frozen=True, order=True)
class TreeRuleCondition:
    feature: str
    operator: str
    threshold: float


@dataclass(frozen=True)
class TreeRule:
    conditions: tuple[TreeRuleCondition, ...]
    training_gain: float


def _condition_sql(condition: TreeRuleCondition, alias: str) -> str:
    return (
        f"{alias}.{condition.feature}::DOUBLE "
        f"{condition.operator} {condition.threshold!r}"
    )


def rule_feature_definitions(rules: Sequence[TreeRule]) -> tuple[tuple[str, str], ...]:
    """Return fixed CASE expressions for selected shallow HGB path rules."""
    return tuple(
        (
            f"tree_rule_{index:03d}",
            "CASE WHEN "
            + " AND ".join(
                _condition_sql(condition, "{alias}") for condition in rule.conditions
            )
            + " THEN 1.0 ELSE 0.0 END",
        )
        for index, rule in enumerate(rules, start=1)
    )


def extract_high_gain_rules(model_path: Path, max_rules: int) -> tuple[TreeRule, ...]:
    """Extract de-duplicated HGB path predicates using training-only split gains."""
    model = joblib.load(model_path)
    gains: dict[tuple[TreeRuleCondition, ...], float] = {}

    def walk(
        nodes: Any,
        node_index: int,
        conditions: tuple[TreeRuleCondition, ...],
    ) -> None:
        node = nodes[node_index]
        if node["is_leaf"]:
            return
        feature = FEATURE_COLUMNS[int(node["feature_idx"])]
        threshold = float(node["num_threshold"])
        children = ((int(node["left"]), "<="), (int(node["right"]), ">"))
        for child_index, operator in children:
            rule_conditions = conditions + (
                TreeRuleCondition(feature, operator, threshold),
            )
            if len(rule_conditions) <= 3:
                gains[rule_conditions] = gains.get(rule_conditions, 0.0) + float(
                    node["gain"]
                )
                walk(nodes, child_index, rule_conditions)

    for predictors in model._predictors:
        for predictor in predictors:
            walk(predictor.nodes, 0, ())

    ranked = sorted(gains.items(), key=lambda item: (-item[1], len(item[0]), item[0]))
    return tuple(
        TreeRule(conditions=conditions, training_gain=gain)
        for conditions, gain in ranked[:max_rules]
    )


def _rule_metadata(rules: Sequence[TreeRule]) -> list[dict[str, Any]]:
    return [
        {
            "name": f"tree_rule_{index:03d}",
            "training_gain": rule.training_gain,
            "conditions": [
                {
                    "feature": condition.feature,
                    "operator": condition.operator,
                    "threshold": condition.threshold,
                }
                for condition in rule.conditions
            ],
        }
        for index, rule in enumerate(rules, start=1)
    ]


def run_tree_rule_scorecard(
    *,
    source_database_path: Path,
    teacher_model_path: Path,
    output_dir: Path,
    max_training_addresses: int = 500_000,
    threads: int | None = None,
) -> dict[str, Any]:
    """Select an additive pairwise model enriched by high-gain shallow HGB rules."""
    output_dir.mkdir(parents=True, exist_ok=True)
    rules = extract_high_gain_rules(teacher_model_path, max(RULE_COUNTS))
    con = duckdb.connect(str(output_dir / "tree_rule_scorecard_results.duckdb"))
    if threads is not None:
        con.execute(f"SET threads TO {threads}")
    con.execute(
        f"ATTACH {sql_text(str(source_database_path.resolve()))} AS source (READ_ONLY)"
    )
    con.execute(
        "CREATE OR REPLACE VIEW all_sector_candidate_features AS "
        "SELECT * FROM source.all_sector_candidate_features"
    )
    trial_rows: list[dict[str, float | int]] = []
    selected: (
        tuple[
            int, float, FoldedLogisticModel, tuple[TreeRule, ...], dict[str, float | int]
        ]
        | None
    ) = None

    for rule_count in RULE_COUNTS:
        selected_rules = rules[:rule_count]
        feature_definitions = ADDITIVE_FEATURES + rule_feature_definitions(selected_rules)
        feature_columns = tuple(name for name, _ in feature_definitions)
        construction_started = time.perf_counter()
        training, training_sample = _pairwise_training_frame(
            con,
            max_training_addresses,
            feature_definitions=feature_definitions,
        )
        construction_seconds = time.perf_counter() - construction_started
        for regularization in RULE_REGULARIZATIONS:
            fit_started = time.perf_counter()
            sklearn_model, scaler = _fit_model(
                training,
                regularization,
                feature_columns=feature_columns,
                sample_weight=training["pair_weight"],
            )
            fit_seconds = time.perf_counter() - fit_started
            folded = fold_logistic_model(
                sklearn_model,
                scaler,
                feature_columns=feature_columns,
            )
            score_table = (
                f"tree_rule_calibration_{rule_count}_"
                f"{str(regularization).replace('.', '_')}"
            )
            scoring = _score_with_timing(
                con,
                model=folded,
                split="calibration",
                output_table=score_table,
                feature_definitions=feature_definitions,
            )
            evaluation_started = time.perf_counter()
            metrics = _top_one_metrics(con, score_table=score_table)
            evaluation_seconds = time.perf_counter() - evaluation_started
            trial_rows.append(
                {
                    "rule_count": rule_count,
                    "regularization_c": regularization,
                    "pair_construction_seconds": construction_seconds,
                    "fit_seconds": fit_seconds,
                    "calibration_evaluation_seconds": evaluation_seconds,
                    **scoring,
                    **metrics,
                }
            )
            candidate = (rule_count, regularization, folded, selected_rules, metrics)
            if (
                selected is None
                or metrics["safe_core_top_one"] > selected[4]["safe_core_top_one"]
            ):
                selected = candidate
            con.execute(f"DROP TABLE {score_table}")

    if selected is None:
        raise ValueError("No tree-rule scorecard trials completed")
    rule_count, regularization, folded, selected_rules, calibration = selected
    feature_definitions = ADDITIVE_FEATURES + rule_feature_definitions(selected_rules)
    test_scoring = _score_with_timing(
        con,
        model=folded,
        split="test",
        output_table="tree_rule_scorecard_test_scores",
        feature_definitions=feature_definitions,
    )
    evaluation_started = time.perf_counter()
    test = _top_one_metrics(con, score_table="tree_rule_scorecard_test_scores")
    test_evaluation_seconds = time.perf_counter() - evaluation_started
    artifact = {
        "version": RULE_SCORECARD_VERSION,
        "model_type": "tree_rule_distilled_additive_pairwise_ranker",
        "feature_columns": list(folded.coefficients),
        "coefficients": folded.coefficients,
        "intercept": folded.intercept,
        "rule_count": rule_count,
        "rules": _rule_metadata(selected_rules),
        "regularization_c": regularization,
        "selection_split": "experiment_split = 'calibration'",
        "frozen_test_split": "experiment_split = 'test'",
        "calibration": calibration,
        "frozen_test": test,
        "training": {**training_sample, "address_balanced": True},
        "runtime": {
            "frozen_test_scoring": test_scoring,
            "frozen_test_evaluation_seconds": test_evaluation_seconds,
        },
    }
    pd.DataFrame(trial_rows).to_parquet(
        output_dir / "tree_rule_scorecard_trials.parquet", index=False
    )
    (output_dir / "tree_rule_scorecard.json").write_text(
        json.dumps(artifact, indent=2, sort_keys=True), encoding="utf-8"
    )
    return artifact


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Trial a high-gain HGB-rule DuckDB scorecard"
    )
    parser.add_argument("--source-database-path", type=Path, required=True)
    parser.add_argument("--teacher-model-path", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--max-training-addresses", type=int, default=500_000)
    parser.add_argument("--threads", type=int, default=os.cpu_count())
    arguments = parser.parse_args()
    artifact = run_tree_rule_scorecard(
        source_database_path=arguments.source_database_path,
        teacher_model_path=arguments.teacher_model_path,
        output_dir=arguments.output_dir,
        max_training_addresses=arguments.max_training_addresses,
        threads=arguments.threads,
    )
    print(json.dumps(artifact["frozen_test"], sort_keys=True))  # noqa: T201


if __name__ == "__main__":
    main()
