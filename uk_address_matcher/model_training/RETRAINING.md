# Road Identifier Retraining Guide

This is the operational contract for reproducing or refining the selected road ranker. Keep training data and sklearn dependencies outside production serving.

## Inputs

| Field | Use |
| --- | --- |
| `source_unique_id` | Deterministic UPRN-level split and deduplication key |
| `cohort` | Prefer `built` before `royalmail` for duplicate IDs |
| `full_address_raw` | Candidate source text |
| `postcode_raw` | Normalisation and outward-district context |
| `street_raw` | Evaluation oracle only |
| `candidate_status` | Target-eligibility conditions |

Do not commit the cache, generated DuckDB databases, joblib models, or rows that contain real addresses.

## Candidate Contract

1. Uppercase the full address and replace non-alphanumeric runs with one space.
2. Normalise and remove an embedded supplied postcode.
3. Remove the established suffix chain from `data/common_uk_end_tokens.json`.
4. Find the rightmost token matching `123`, `123A`, or `123-125A`.
5. Generate every width-two and width-three span after that anchor.
6. Apply the packaged `data/road_candidate_token_policy.json` guardrails.

Inputs without a numeric anchor produce no candidate. Do not add one-token, longer-span, fuzzy, or forced-road fallbacks without a controlled reachability and false-positive study.

## Labels and Leakage Controls

A candidate is positive when it exactly matches the normalised oracle street or is its safe left-prefix. It is exact only when it matches the complete oracle.

- Split by `hash(unique_id) % 10`: `0..7` train, `8` calibration, `9` final test.
- Build recurrence topology from train rows only.
- Select hyperparameters against calibration only.
- Freeze the model before reading final-test metrics.
- Never expose oracle text, labels, or source-status-derived fields to inference.

## Selected Model

The complete reference is:

```text
HistGradientBoostingClassifier(
    max_iter=250,
    max_leaf_nodes=63,
    learning_rate=0.06,
    l2_regularization=4.0,
    early_stopping=True,
    validation_fraction=0.1,
    random_state=20260826,
)
```

Its 15 deployable inputs cover candidate position and shape, terminal syntax, train-only phrase recurrence, postcode/district breadth, and terminal recurrence. The trainer emits both joblib and inference-complete JSON forms.

Production uses the separately evaluated tree-rule-distilled additive scorecard. Do not replace `road_assignment_scorecard_v1.json` merely because a new HGB has better offline figures: rerun distillation, exact SQL replay, terminal-first evaluation, blocker-volume checks, and full canonical timing first.

## Run

Start with a bounded smoke run:

```bash
uv run python -m uk_address_matcher.model_training.all_sector_model \
  --cache-path /secure/path/cache.parquet \
  --output-dir /secure/path/road_identifier_smoke \
  --row-limit 5000
```

Then use a new output directory for the complete corpus:

```bash
uv run python -m uk_address_matcher.model_training.all_sector_model \
  --cache-path /secure/path/cache.parquet \
  --output-dir /secure/path/road_identifier \
  --threads 4 \
  --memory-limit 16GB
```

## Acceptance Checklist

1. Record candidate reachability and safe-core/exact recall at ranks 1, 2, and 3.
2. Report postcode-district slices and high-confidence wrong winners.
3. Verify the exported HGB JSON reproduces sklearn raw scores, including missing feature values.
4. Distil and evaluate the deployable scorecard on the untouched final split.
5. Compare terminal-first and all-candidate routing.
6. Run focused SQL artefact and road-stage tests.
7. Measure full canonical road time, output equality, blocker size, and candidate volume before changing the packaged live model.

Preserve experiment metadata and a concise decision, but do not retain rejected training branches in the production source tree.
