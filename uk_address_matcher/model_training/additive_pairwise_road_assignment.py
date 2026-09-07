"""Trial a compact, DuckDB-native additive pairwise road ranker."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any, Sequence

import duckdb
import pandas as pd

from uk_address_matcher.cleaning.steps.road_resources import sql_text

from .all_sector_model import FEATURE_COLUMNS
from .linear_road_assignment import (
    FoldedLogisticModel,
    _fit_model,
    _top_one_metrics,
    fold_logistic_model,
    logistic_logit_sql,
)

ADDITIVE_PAIRWISE_VERSION = 2
ADDITIVE_PAIRWISE_TRIALS = (0.003, 0.01, 0.03, 0.1)


def _threshold_feature(name: str, feature: str, threshold: float) -> tuple[str, str]:
    return (
        name,
        f"CASE WHEN {{alias}}.{feature}::DOUBLE >= {threshold} THEN 1.0 ELSE 0.0 END",
    )


_RAW_FEATURES = tuple(
    (feature, f"{{alias}}.{feature}::DOUBLE") for feature in FEATURE_COLUMNS
)
_TREE_THRESHOLD_FEATURES = (
    *(
        _threshold_feature(
            f"tail_length_ge_{threshold}".replace(".", "_"), "tail_length", threshold
        )
        for threshold in (2.5, 3.5, 4.5, 5.5)
    ),
    *(
        _threshold_feature(
            f"start_tail_fraction_ge_{threshold}".replace(".", "_"),
            "start_tail_fraction",
            threshold,
        )
        for threshold in (0.2566, 0.5119, 0.5420, 0.5635)
    ),
    *(
        _threshold_feature(
            f"end_tail_fraction_ge_{threshold}".replace(".", "_"),
            "end_tail_fraction",
            threshold,
        )
        for threshold in (0.4189, 0.6754, 0.7208, 0.8619)
    ),
    (
        "log_terminal_right_context_diversity",
        "ln(1.0 + {alias}.terminal_right_context_diversity::DOUBLE)",
    ),
    *(
        _threshold_feature(
            f"terminal_right_context_diversity_ge_{threshold}".replace(".", "_"),
            "terminal_right_context_diversity",
            threshold,
        )
        for threshold in (1707.0, 3628.0, 19793.0)
    ),
    *(
        _threshold_feature(
            f"log_terminal_support_ge_{threshold}".replace(".", "_"),
            "log_terminal_support",
            threshold,
        )
        for threshold in (11.0794, 12.5114, 13.3338, 13.8957, 14.6659)
    ),
)
_TREE_INTERACTION_FEATURES = (
    (
        "road_terminal_x_tail_length_ge_4_5",
        "{alias}.road_syntax_terminal::DOUBLE "
        "* CASE WHEN {alias}.tail_length::DOUBLE >= 4.5 THEN 1.0 ELSE 0.0 END",
    ),
    (
        "road_terminal_x_start_tail_fraction_ge_0_5119",
        "{alias}.road_syntax_terminal::DOUBLE "
        "* CASE WHEN {alias}.start_tail_fraction::DOUBLE >= 0.5119 THEN 1.0 ELSE 0.0 END",
    ),
    (
        "road_terminal_x_end_tail_fraction_ge_0_6754",
        "{alias}.road_syntax_terminal::DOUBLE "
        "* CASE WHEN {alias}.end_tail_fraction::DOUBLE >= 0.6754 THEN 1.0 ELSE 0.0 END",
    ),
    (
        "tail_length_ge_4_5_x_end_tail_fraction_ge_0_6754",
        "CASE WHEN {alias}.tail_length::DOUBLE >= 4.5 "
        "AND {alias}.end_tail_fraction::DOUBLE >= 0.6754 THEN 1.0 ELSE 0.0 END",
    ),
    (
        "tail_length_ge_3_5_x_start_tail_fraction_ge_0_5119",
        "CASE WHEN {alias}.tail_length::DOUBLE >= 3.5 "
        "AND {alias}.start_tail_fraction::DOUBLE >= 0.5119 THEN 1.0 ELSE 0.0 END",
    ),
    (
        "residence_x_start_tail_fraction_ge_0_5119",
        "{alias}.contains_residence_token::DOUBLE "
        "* CASE WHEN {alias}.start_tail_fraction::DOUBLE >= 0.5119 THEN 1.0 ELSE 0.0 END",
    ),
    (
        "road_terminal_x_terminal_right_context_diversity_ge_1707",
        "{alias}.road_syntax_terminal::DOUBLE "
        "* CASE WHEN {alias}.terminal_right_context_diversity::DOUBLE >= 1707.0 "
        "THEN 1.0 ELSE 0.0 END",
    ),
    (
        "tail_length_ge_4_5_x_terminal_right_context_diversity_ge_1707",
        "CASE WHEN {alias}.tail_length::DOUBLE >= 4.5 "
        "AND {alias}.terminal_right_context_diversity::DOUBLE >= 1707.0 "
        "THEN 1.0 ELSE 0.0 END",
    ),
    (
        "end_tail_fraction_ge_0_6754_x_terminal_right_context_diversity_ge_1707",
        "CASE WHEN {alias}.end_tail_fraction::DOUBLE >= 0.6754 "
        "AND {alias}.terminal_right_context_diversity::DOUBLE >= 1707.0 "
        "THEN 1.0 ELSE 0.0 END",
    ),
    (
        "terminal_right_context_diversity_ge_1707_x_log_terminal_support_ge_13_3338",
        "CASE WHEN {alias}.terminal_right_context_diversity::DOUBLE >= 1707.0 "
        "AND {alias}.log_terminal_support::DOUBLE >= 13.3338 THEN 1.0 ELSE 0.0 END",
    ),
    (
        "road_terminal_x_width_tail_fraction_ge_0_3875",
        "{alias}.road_syntax_terminal::DOUBLE "
        "* CASE WHEN {alias}.width_tail_fraction::DOUBLE >= 0.3875 "
        "THEN 1.0 ELSE 0.0 END",
    ),
)
_EXTENDED_TREE_THRESHOLD_FEATURES = (
    *(
        _threshold_feature(
            f"width_tail_fraction_ge_{threshold}".replace(".", "_"),
            "width_tail_fraction",
            threshold,
        )
        for threshold in (0.2792, 0.3542, 0.3875, 0.4143, 0.4643, 0.5500, 0.6333, 0.8750)
    ),
    *(
        _threshold_feature(
            f"log_postcode_support_ge_{threshold}".replace(".", "_"),
            "log_postcode_support",
            threshold,
        )
        for threshold in (0.6931, 1.3863, 1.6094, 2.4849, 3.4012, 4.4308, 4.7791)
    ),
    *(
        _threshold_feature(
            f"district_count_ge_{threshold}".replace(".", "_"),
            "district_count",
            threshold,
        )
        for threshold in (1.0, 2.0, 3.0, 7.0, 15.0, 42.0, 79.0)
    ),
    *(
        _threshold_feature(
            f"log_phrase_support_ge_{threshold}".replace(".", "_"),
            "log_phrase_support",
            threshold,
        )
        for threshold in (1.7918, 2.4849, 6.1159, 7.0648, 8.6155, 9.9640, 10.8718)
    ),
    *(
        _threshold_feature(
            f"number_diversity_ratio_ge_{threshold}".replace(".", "_"),
            "number_diversity_ratio",
            threshold,
        )
        for threshold in (0.0557, 0.1081, 0.1481, 0.2072, 0.2174, 0.2429, 0.3779, 0.4805)
    ),
    *(
        _threshold_feature(
            f"tail_length_ge_{threshold}".replace(".", "_"), "tail_length", threshold
        )
        for threshold in (6.5, 7.5)
    ),
)
ADDITIVE_FEATURES = (
    _RAW_FEATURES
    + _TREE_THRESHOLD_FEATURES
    + _EXTENDED_TREE_THRESHOLD_FEATURES
    + _TREE_INTERACTION_FEATURES
)
ADDITIVE_FEATURE_COLUMNS = tuple(name for name, _ in ADDITIVE_FEATURES)


def additive_feature_sql(
    alias: str,
    *,
    feature_definitions: Sequence[tuple[str, str]] = ADDITIVE_FEATURES,
) -> str:
    """Project the fixed, HGB-derived scorecard feature basis for a table alias."""
    return ",\n            ".join(
        f"{expression.format(alias=alias)} AS {name}"
        for name, expression in feature_definitions
    )


def _pairwise_training_frame(
    con: duckdb.DuckDBPyConnection,
    max_addresses: int,
    *,
    feature_definitions: Sequence[tuple[str, str]] = ADDITIVE_FEATURES,
) -> tuple[pd.DataFrame, dict[str, int]]:
    address_count = int(
        con.execute(
            """
            SELECT count(DISTINCT unique_id)
            FROM all_sector_candidate_features
            WHERE experiment_split = 'train'
              AND target_eligible
              AND candidate_label = 1
            """
        ).fetchone()[0]
    )
    if address_count == 0:
        raise ValueError("Pairwise training requires reachable training addresses")
    modulus = 1_000_000
    cutoff = min(modulus, int(modulus * max_addresses / address_count))
    feature_columns = tuple(name for name, _ in feature_definitions)
    feature_differences = ",\n                ".join(
        "("
        f"{expression.format(alias='positive')} "
        f"- {expression.format(alias='negative')}"
        f") AS {name}"
        for name, expression in feature_definitions
    )
    reversed_differences = ", ".join(
        f"-{feature} AS {feature}" for feature in feature_columns
    )
    frame = con.execute(
        f"""
        WITH sampled_addresses AS (
            SELECT DISTINCT unique_id
            FROM all_sector_candidate_features
            WHERE experiment_split = 'train'
              AND target_eligible
              AND candidate_label = 1
              AND hash(unique_id) % {modulus} < {cutoff}
        ), forward_pairs AS (
            SELECT
                {feature_differences},
                1.0 / count(*) OVER (PARTITION BY positive.unique_id) AS pair_weight
            FROM all_sector_candidate_features AS positive
            JOIN all_sector_candidate_features AS negative
              ON positive.unique_id = negative.unique_id
            JOIN sampled_addresses
              ON positive.unique_id = sampled_addresses.unique_id
            WHERE positive.experiment_split = 'train'
              AND positive.target_eligible
              AND positive.candidate_label = 1
              AND negative.candidate_label = 0
        )
        SELECT {", ".join(feature_columns)}, 1 AS candidate_label, pair_weight
        FROM forward_pairs
        UNION ALL
        SELECT {reversed_differences}, 0 AS candidate_label, pair_weight
        FROM forward_pairs
        """
    ).df()
    return frame, {
        "reachable_address_population": address_count,
        "sampled_address_cutoff": cutoff,
        "training_pairs": len(frame) // 2,
        "training_rows": len(frame),
    }


def _score_with_timing(
    con: duckdb.DuckDBPyConnection,
    *,
    model: FoldedLogisticModel,
    split: str,
    output_table: str,
    feature_definitions: Sequence[tuple[str, str]] = ADDITIVE_FEATURES,
) -> dict[str, float | int]:
    started = time.perf_counter()
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {output_table} AS
        WITH scorecard_features AS (
            SELECT
                unique_id,
                candidate_phrase,
                candidate_start_position,
                candidate_end_position,
                candidate_label,
                candidate_exact,
                target_eligible,
                {
            additive_feature_sql("candidates", feature_definitions=feature_definitions)
        }
            FROM all_sector_candidate_features AS candidates
            WHERE experiment_split = {sql_text(split)}
        )
        SELECT
            unique_id,
            candidate_phrase,
            candidate_start_position,
            candidate_end_position,
            candidate_label,
            candidate_exact,
            target_eligible,
            {logistic_logit_sql(model)} AS ranker_logit
        FROM scorecard_features
        """
    )
    seconds = time.perf_counter() - started
    candidate_count = int(
        con.execute(f"SELECT count(*) FROM {output_table}").fetchone()[0]
    )
    return {
        "seconds": seconds,
        "candidate_count": candidate_count,
        "candidates_per_second": candidate_count / seconds,
    }


def _create_winners(
    con: duckdb.DuckDBPyConnection, *, score_table: str, output_table: str
) -> None:
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {output_table} AS
        WITH ranked AS (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY unique_id
                    ORDER BY ranker_logit DESC, candidate_phrase, candidate_start_position
                ) AS candidate_rank
            FROM {score_table}
        )
        SELECT
            unique_id,
            target_eligible,
            candidate_label AS winner_label,
            ranker_logit
        FROM ranked
        WHERE candidate_rank = 1
        """
    )


def _threshold_curve(
    con: duckdb.DuckDBPyConnection, *, winner_table: str
) -> pd.DataFrame:
    winners = con.execute(
        f"SELECT ranker_logit, target_eligible, winner_label FROM {winner_table}"
    ).df()
    winners["accepted_correct"] = (
        winners["target_eligible"] & (winners["winner_label"] == 1)
    ).astype(int)
    winners["accepted_incorrect"] = 1 - winners["accepted_correct"]
    curve = (
        winners.groupby("ranker_logit", as_index=False, sort=False)
        .agg(
            accepted_correct=("accepted_correct", "sum"),
            accepted_incorrect=("accepted_incorrect", "sum"),
        )
        .sort_values("ranker_logit", ascending=False, kind="stable")
    )
    curve["accepted_correct"] = curve["accepted_correct"].cumsum()
    curve["accepted_incorrect"] = curve["accepted_incorrect"].cumsum()
    reachable_addresses = int(winners["target_eligible"].sum())
    curve["reachable_addresses"] = reachable_addresses
    curve["precision"] = curve["accepted_correct"] / (
        curve["accepted_correct"] + curve["accepted_incorrect"]
    )
    curve["recall"] = curve["accepted_correct"] / reachable_addresses
    curve["f1"] = (
        2 * curve["precision"] * curve["recall"] / (curve["precision"] + curve["recall"])
    )
    return curve


def _select_precision_policy(
    curve: pd.DataFrame, minimum_precision: float
) -> dict[str, float | int] | None:
    candidates = curve.loc[curve["precision"] >= minimum_precision]
    if candidates.empty:
        return None
    selected = candidates.sort_values(
        ["recall", "precision", "f1"], ascending=False, kind="stable"
    ).iloc[0]
    return {
        "minimum_ranker_logit": float(selected["ranker_logit"]),
        "precision": float(selected["precision"]),
        "recall": float(selected["recall"]),
        "f1": float(selected["f1"]),
        "accepted_correct": int(selected["accepted_correct"]),
        "accepted_incorrect": int(selected["accepted_incorrect"]),
        "reachable_addresses": int(selected["reachable_addresses"]),
    }


def _evaluate_precision_policy(
    con: duckdb.DuckDBPyConnection,
    *,
    winner_table: str,
    policy: dict[str, float | int] | None,
) -> dict[str, float | int] | None:
    if policy is None:
        return None
    threshold = float(policy["minimum_ranker_logit"])
    row = con.execute(
        f"""
        SELECT
            count(*) FILTER (
                WHERE target_eligible AND winner_label = 1 AND ranker_logit >= ?
            ) AS accepted_correct,
            count(*) FILTER (
                WHERE NOT (target_eligible AND winner_label = 1) AND ranker_logit >= ?
            ) AS accepted_incorrect,
            count(*) FILTER (WHERE target_eligible) AS reachable_addresses
        FROM {winner_table}
        """,
        [threshold, threshold],
    ).fetchone()
    accepted_correct = int(row[0])
    accepted_incorrect = int(row[1])
    reachable_addresses = int(row[2])
    precision = accepted_correct / (accepted_correct + accepted_incorrect)
    recall = accepted_correct / reachable_addresses
    return {
        "minimum_ranker_logit": threshold,
        "precision": precision,
        "recall": recall,
        "f1": 2 * precision * recall / (precision + recall),
        "accepted_correct": accepted_correct,
        "accepted_incorrect": accepted_incorrect,
        "reachable_addresses": reachable_addresses,
    }


def run_additive_pairwise_ranker(
    *,
    source_database_path: Path,
    output_dir: Path,
    max_training_addresses: int = 500_000,
    address_balanced: bool = False,
) -> dict[str, Any]:
    """Select a gain-guided additive pairwise ranker and evaluate frozen test."""
    output_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(output_dir / "additive_pairwise_ranker_results.duckdb"))
    con.execute(
        f"ATTACH {sql_text(str(source_database_path.resolve()))} AS source (READ_ONLY)"
    )
    con.execute(
        "CREATE OR REPLACE VIEW all_sector_candidate_features AS "
        "SELECT * FROM source.all_sector_candidate_features"
    )
    started = time.perf_counter()
    training, training_sample = _pairwise_training_frame(con, max_training_addresses)
    pair_construction_seconds = time.perf_counter() - started
    trial_rows: list[dict[str, float | int]] = []
    selected: tuple[float, FoldedLogisticModel, dict[str, float | int]] | None = None

    for regularization in ADDITIVE_PAIRWISE_TRIALS:
        fit_started = time.perf_counter()
        sklearn_model, scaler = _fit_model(
            training,
            regularization,
            feature_columns=ADDITIVE_FEATURE_COLUMNS,
            sample_weight=training["pair_weight"] if address_balanced else None,
        )
        fit_seconds = time.perf_counter() - fit_started
        folded = fold_logistic_model(
            sklearn_model,
            scaler,
            feature_columns=ADDITIVE_FEATURE_COLUMNS,
        )
        score_table = (
            f"additive_pairwise_calibration_{str(regularization).replace('.', '_')}"
        )
        scoring = _score_with_timing(
            con,
            model=folded,
            split="calibration",
            output_table=score_table,
        )
        evaluation_started = time.perf_counter()
        metrics = _top_one_metrics(con, score_table=score_table)
        evaluation_seconds = time.perf_counter() - evaluation_started
        trial_rows.append(
            {
                "regularization_c": regularization,
                "fit_seconds": fit_seconds,
                "calibration_evaluation_seconds": evaluation_seconds,
                **scoring,
                **metrics,
            }
        )
        candidate = (regularization, folded, metrics)
        if (
            selected is None
            or metrics["safe_core_top_one"] > selected[2]["safe_core_top_one"]
        ):
            selected = candidate
        con.execute(f"DROP TABLE {score_table}")

    if selected is None:
        raise ValueError("No additive pairwise ranker trials completed")
    regularization, folded, calibration = selected
    calibration_scoring = _score_with_timing(
        con,
        model=folded,
        split="calibration",
        output_table="additive_pairwise_ranker_calibration_scores",
    )
    _create_winners(
        con,
        score_table="additive_pairwise_ranker_calibration_scores",
        output_table="additive_pairwise_ranker_calibration_winners",
    )
    precision_curve = _threshold_curve(
        con, winner_table="additive_pairwise_ranker_calibration_winners"
    )
    precision_policy = _select_precision_policy(precision_curve, minimum_precision=0.995)
    test_scoring = _score_with_timing(
        con,
        model=folded,
        split="test",
        output_table="additive_pairwise_ranker_test_scores",
    )
    _create_winners(
        con,
        score_table="additive_pairwise_ranker_test_scores",
        output_table="additive_pairwise_ranker_test_winners",
    )
    test_evaluation_started = time.perf_counter()
    test = _top_one_metrics(con, score_table="additive_pairwise_ranker_test_scores")
    test_evaluation_seconds = time.perf_counter() - test_evaluation_started
    frozen_precision_policy = _evaluate_precision_policy(
        con,
        winner_table="additive_pairwise_ranker_test_winners",
        policy=precision_policy,
    )
    artifact = {
        "version": ADDITIVE_PAIRWISE_VERSION,
        "model_type": "additive_pairwise_logistic_candidate_ranker",
        "feature_columns": list(ADDITIVE_FEATURE_COLUMNS),
        "intercept": folded.intercept,
        "coefficients": folded.coefficients,
        "regularization_c": regularization,
        "selection_split": "experiment_split = 'calibration'",
        "frozen_test_split": "experiment_split = 'test'",
        "calibration": calibration,
        "frozen_test": test,
        "training": {
            "pair_construction_seconds": pair_construction_seconds,
            "address_balanced": address_balanced,
            **training_sample,
        },
        "runtime": {
            "calibration_scoring": calibration_scoring,
            "frozen_test_scoring": test_scoring,
            "frozen_test_evaluation_seconds": test_evaluation_seconds,
        },
        "raw_ranker_gate": {
            "minimum_precision": 0.995,
            "selection": precision_policy,
            "frozen_test": frozen_precision_policy,
            "note": (
                "Ranker-logit threshold only; raw-text abstention and "
                "learned gate excluded."
            ),
        },
    }
    pd.DataFrame(trial_rows).to_parquet(
        output_dir / "additive_pairwise_ranker_trials.parquet", index=False
    )
    precision_curve.to_parquet(
        output_dir / "additive_pairwise_ranker_precision_curve.parquet", index=False
    )
    (output_dir / "additive_pairwise_ranker.json").write_text(
        json.dumps(artifact, indent=2, sort_keys=True), encoding="utf-8"
    )
    return artifact


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Trial a gain-guided additive pairwise road ranker"
    )
    parser.add_argument("--source-database-path", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--max-training-addresses", type=int, default=500_000)
    parser.add_argument("--address-balanced", action="store_true")
    arguments = parser.parse_args()
    artifact = run_additive_pairwise_ranker(
        source_database_path=arguments.source_database_path,
        output_dir=arguments.output_dir,
        max_training_addresses=arguments.max_training_addresses,
        address_balanced=arguments.address_balanced,
    )
    print(json.dumps(artifact["frozen_test"], sort_keys=True))  # noqa: T201


if __name__ == "__main__":
    main()
