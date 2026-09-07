"""Export a fitted sklearn histogram gradient booster as portable JSON."""

from __future__ import annotations

import argparse
import json
import math
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

MODEL_TYPE = "hist_gradient_boosting_candidate_ranker"
FORMAT_VERSION = 1


def hist_gradient_boosting_to_dict(
    model: Any,
    *,
    feature_columns: Sequence[str],
    metadata: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Return an inference-complete JSON representation of a fitted binary model."""
    if int(model.n_trees_per_iteration_) != 1:
        raise ValueError(
            "Only binary classifiers with one tree per iteration are supported"
        )
    if len(feature_columns) != int(model.n_features_in_):
        raise ValueError("feature_columns must match the fitted model feature count")

    trees: list[list[dict[str, object]]] = []
    for predictors in model._predictors:
        nodes = predictors[0].nodes
        if any(bool(node["is_categorical"]) for node in nodes):
            raise ValueError("Categorical histogram splits are not supported")
        trees.append(
            [
                (
                    {"value": float(node["value"])}
                    if bool(node["is_leaf"])
                    else {
                        "feature": int(node["feature_idx"]),
                        "threshold": float(node["num_threshold"]),
                        "missing_left": bool(node["missing_go_to_left"]),
                        "left": int(node["left"]),
                        "right": int(node["right"]),
                    }
                )
                for node in nodes
            ]
        )

    artifact: dict[str, object] = {
        "format_version": FORMAT_VERSION,
        "model_type": MODEL_TYPE,
        "feature_columns": list(feature_columns),
        "classes": [int(value) for value in model.classes_],
        "baseline_prediction": float(model._baseline_prediction[0, 0]),
        "tree_count": len(trees),
        "node_count": sum(len(tree) for tree in trees),
        "trees": trees,
    }
    if metadata is not None:
        artifact["metadata"] = dict(metadata)
    return artifact


def raw_score_from_artifact(
    artifact: Mapping[str, object],
    feature_values: Sequence[float | int | None],
) -> float:
    """Evaluate one row from an exported binary classifier artifact."""
    feature_columns = artifact["feature_columns"]
    if not isinstance(feature_columns, list) or len(feature_values) != len(
        feature_columns
    ):
        raise ValueError("feature_values must match the artifact feature count")

    score = float(artifact["baseline_prediction"])
    trees = artifact["trees"]
    if not isinstance(trees, list):
        raise ValueError("artifact trees must be a list")
    for tree in trees:
        node_index = 0
        while True:
            node = tree[node_index]
            if "value" in node:
                score += float(node["value"])
                break
            value = feature_values[int(node["feature"])]
            missing = value is None or (isinstance(value, float) and math.isnan(value))
            go_left = (
                bool(node["missing_left"])
                if missing
                else float(value) <= float(node["threshold"])
            )
            node_index = int(node["left"] if go_left else node["right"])
    return score


def export_hist_gradient_boosting_json(
    model: Any,
    output_path: Path,
    *,
    feature_columns: Sequence[str],
    metadata: Mapping[str, object] | None = None,
) -> None:
    """Write a fitted binary classifier as deterministic portable JSON."""
    artifact = hist_gradient_boosting_to_dict(
        model,
        feature_columns=feature_columns,
        metadata=metadata,
    )
    output_path.write_text(
        json.dumps(artifact, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("model_path", type=Path)
    parser.add_argument("metadata_path", type=Path)
    parser.add_argument("output_path", type=Path)
    args = parser.parse_args()

    import joblib

    model = joblib.load(args.model_path)
    source_metadata = json.loads(args.metadata_path.read_text(encoding="utf-8"))
    export_hist_gradient_boosting_json(
        model,
        args.output_path,
        feature_columns=source_metadata["feature_columns"],
        metadata=source_metadata,
    )


if __name__ == "__main__":
    main()
