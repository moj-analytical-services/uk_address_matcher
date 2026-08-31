from __future__ import annotations

import json

import numpy as np
import pytest
from sklearn.ensemble import HistGradientBoostingClassifier

from uk_address_matcher.model_training.hist_gradient_boosting_json import (
    hist_gradient_boosting_to_dict,
    raw_score_from_artifact,
)


def test_exported_hist_gradient_booster_preserves_raw_scores() -> None:
    features = np.array(
        [
            [0.0, 1.0],
            [0.2, 0.8],
            [0.8, 0.2],
            [1.0, 0.0],
            [np.nan, 0.5],
        ]
    )
    labels = np.array([0, 0, 1, 1, 0])
    model = HistGradientBoostingClassifier(
        max_iter=4,
        max_leaf_nodes=3,
        min_samples_leaf=1,
        random_state=20260830,
    ).fit(features, labels)

    artifact = hist_gradient_boosting_to_dict(
        model,
        feature_columns=("first", "second"),
    )
    round_tripped = json.loads(json.dumps(artifact))
    actual = [raw_score_from_artifact(round_tripped, row) for row in features]

    assert actual == pytest.approx(model.decision_function(features), abs=1e-15)
    assert artifact["tree_count"] == model.n_iter_
    assert artifact["node_count"] == sum(
        len(predictor.nodes)
        for predictors in model._predictors
        for predictor in predictors
    )
