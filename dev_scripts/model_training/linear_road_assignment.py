"""Train and export a DuckDB-compatible logistic road-candidate ranker."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

import duckdb
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

from uk_address_matcher.cleaning.steps.road_resources import sql_text

from .all_sector_model import FEATURE_COLUMNS, _sample_training_frame

LINEAR_RANKER_VERSION = 1
LINEAR_RANKER_TRIALS = (0.03, 0.1, 0.3, 1.0)


@dataclass(frozen=True)
class FoldedLogisticModel:
    intercept: float
    coefficients: dict[str, float]


def _fit_model(
    training: pd.DataFrame,
    regularization: float,
    *,
    feature_columns: Sequence[str] = FEATURE_COLUMNS,
    sample_weight: np.ndarray | pd.Series | None = None,
) -> tuple[LogisticRegression, StandardScaler]:
    scaler = StandardScaler()
    raw_features = training.loc[:, feature_columns].to_numpy(dtype=np.float64)
    scaler.fit(raw_features, sample_weight=sample_weight)
    features = scaler.transform(raw_features)
    model = LogisticRegression(
        C=regularization,
        max_iter=500,
        random_state=20260830,
        solver="lbfgs",
    )
    model.fit(features, training["candidate_label"], sample_weight=sample_weight)
    return model, scaler


def fold_logistic_model(
    model: LogisticRegression,
    scaler: StandardScaler,
    *,
    feature_columns: Sequence[str] = FEATURE_COLUMNS,
) -> FoldedLogisticModel:
    """Fold standardisation into coefficients so serving needs only raw features."""
    coefficients = model.coef_[0] / scaler.scale_
    intercept = model.intercept_[0] - float(np.dot(coefficients, scaler.mean_))
    return FoldedLogisticModel(
        intercept=float(intercept),
        coefficients={
            feature: float(coefficient)
            for feature, coefficient in zip(feature_columns, coefficients, strict=True)
        },
    )


def logistic_logit_sql(model: FoldedLogisticModel) -> str:
    """Return the raw logistic score expression evaluated directly by DuckDB."""
    terms = [repr(model.intercept)]
    terms.extend(
        f"({coefficient!r} * {feature}::DOUBLE)"
        for feature, coefficient in model.coefficients.items()
    )
    return " + ".join(terms)


def _score_relation(
    con: duckdb.DuckDBPyConnection,
    *,
    model: FoldedLogisticModel,
    split: str,
    output_table: str,
) -> None:
    logit = logistic_logit_sql(model)
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {output_table} AS
        SELECT
            unique_id,
            candidate_phrase,
            candidate_start_position,
            candidate_end_position,
            candidate_label,
            candidate_exact,
            target_eligible,
            {logit} AS ranker_logit
        FROM all_sector_candidate_features
        WHERE experiment_split = {sql_text(split)}
        """
    )


def _top_one_metrics(
    con: duckdb.DuckDBPyConnection, *, score_table: str
) -> dict[str, float | int]:
    row = con.execute(
        f"""
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
            count(*) FILTER (
                WHERE target_eligible AND candidate_rank = 1
            ) AS reachable_addresses,
            count(*) FILTER (
                WHERE target_eligible AND candidate_rank = 1 AND candidate_label = 1
            ) AS top_one_correct,
            count(*) FILTER (
                WHERE target_eligible AND candidate_rank = 1 AND candidate_exact
            ) AS exact_top_one_correct
        FROM ranked
        """
    ).fetchone()
    reachable_addresses = int(row[0])
    top_one_correct = int(row[1])
    exact_top_one_correct = int(row[2])
    safe_core_top_one = top_one_correct / reachable_addresses
    return {
        "reachable_addresses": reachable_addresses,
        "top_one_correct": top_one_correct,
        "safe_core_top_one": safe_core_top_one,
        "safe_core_precision": safe_core_top_one,
        "safe_core_recall": safe_core_top_one,
        "safe_core_f1": safe_core_top_one,
        "exact_top_one": exact_top_one_correct / reachable_addresses,
    }


def _artifact(
    *,
    model: FoldedLogisticModel,
    regularization: float,
    calibration: dict[str, float | int],
    test: dict[str, float | int],
) -> dict[str, Any]:
    return {
        "version": LINEAR_RANKER_VERSION,
        "model_type": "logistic_candidate_ranker",
        "feature_columns": list(FEATURE_COLUMNS),
        "intercept": model.intercept,
        "coefficients": model.coefficients,
        "regularization_c": regularization,
        "selection_split": "experiment_split = 'calibration'",
        "frozen_test_split": "experiment_split = 'test'",
        "calibration": calibration,
        "frozen_test": test,
    }


def run_linear_ranker(
    *,
    source_database_path: Path,
    output_dir: Path,
    max_training_per_class: int = 2_000_000,
) -> dict[str, Any]:
    """Select a logistic candidate ranker on calibration, then evaluate frozen test."""
    output_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(output_dir / "linear_ranker_results.duckdb"))
    con.execute(
        f"ATTACH {sql_text(str(source_database_path.resolve()))} AS source (READ_ONLY)"
    )
    con.execute(
        "CREATE OR REPLACE VIEW all_sector_candidate_features AS "
        "SELECT * FROM source.all_sector_candidate_features"
    )
    training, training_sample = _sample_training_frame(con, max_training_per_class)
    trial_rows: list[dict[str, float | int]] = []
    selected: tuple[float, FoldedLogisticModel, dict[str, float | int]] | None = None

    for regularization in LINEAR_RANKER_TRIALS:
        sklearn_model, scaler = _fit_model(training, regularization)
        folded = fold_logistic_model(sklearn_model, scaler)
        table_name = f"linear_ranker_calibration_{str(regularization).replace('.', '_')}"
        _score_relation(
            con,
            model=folded,
            split="calibration",
            output_table=table_name,
        )
        metrics = _top_one_metrics(con, score_table=table_name)
        trial_rows.append({"regularization_c": regularization, **metrics})
        candidate = (regularization, folded, metrics)
        if (
            selected is None
            or metrics["safe_core_top_one"] > selected[2]["safe_core_top_one"]
        ):
            selected = candidate

    if selected is None:
        raise ValueError("No logistic ranker trials completed")
    regularization, folded, calibration = selected
    _score_relation(
        con,
        model=folded,
        split="test",
        output_table="linear_ranker_test_scores",
    )
    test = _top_one_metrics(con, score_table="linear_ranker_test_scores")
    artifact = _artifact(
        model=folded,
        regularization=regularization,
        calibration=calibration,
        test=test,
    )
    pd.DataFrame(trial_rows).to_parquet(
        output_dir / "linear_ranker_trials.parquet", index=False
    )
    (output_dir / "linear_ranker.json").write_text(
        json.dumps(artifact, indent=2, sort_keys=True), encoding="utf-8"
    )
    (output_dir / "linear_ranker_training_sample.json").write_text(
        json.dumps(training_sample, indent=2, sort_keys=True), encoding="utf-8"
    )
    return artifact


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Train a DuckDB-compatible logistic road ranker"
    )
    parser.add_argument("--source-database-path", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--max-training-per-class", type=int, default=2_000_000)
    arguments = parser.parse_args()
    artifact = run_linear_ranker(
        source_database_path=arguments.source_database_path,
        output_dir=arguments.output_dir,
        max_training_per_class=arguments.max_training_per_class,
    )
    print(json.dumps(artifact["frozen_test"], sort_keys=True))  # noqa: T201


if __name__ == "__main__":
    main()
