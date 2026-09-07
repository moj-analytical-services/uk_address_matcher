# Road Identifier Model Training

This package contains offline training, evaluation, distillation, and artefact
verification for the roadlike-span ranker. Production road extraction does not
import this package: it loads a folded JSON scorecard and executes DuckDB SQL.

## Selected Workflow

1. `all_sector_model.py` prepares candidates, builds train-only recurrence
  topology, trains the all-sector `HistGradientBoostingClassifier`, and evaluates
  the frozen test split.
2. `hist_gradient_boosting_json.py` exports every fitted tree and node to a
  deterministic, inference-complete JSON representation.
3. `additive_pairwise_road_assignment.py` trains the address-balanced additive
  scorecard used as the basis for distillation.
4. `tree_rule_scorecard.py` distils useful HGB paths into additive SQL predicates.
5. `road_assignment_artifacts.py` validates folded scorecards and compiles their
  coefficients and rules into DuckDB SQL.
6. `benchmark_road_assignment_artifacts.py` compares deployable scorecards on a
  common candidate relation.

`linear_road_assignment.py` remains because the selected additive trainer shares
its folded-logistic model and evaluation helpers. Rejected gate, listwise,
pairwise-linear, and example-scoring branches have been removed.

Read [RESULTS.md](RESULTS.md) for the selected measurements and
[RETRAINING.md](RETRAINING.md) before changing features, labels, splits, or the
candidate policy.

## Setup

From the `uk_address_matcher` repository:

```bash
uv sync --group model-training --group dev
```

The experiment requires a secure canonical residential/commercial Parquet cache
with `source_unique_id`, `cohort`, `full_address_raw`, `postcode_raw`,
`street_raw`, and `candidate_status`. Do not commit source addresses, experiment
databases, or joblib files.

## Train

```bash
uv run python -m uk_address_matcher.model_training.all_sector_model \
  --cache-path /secure/path/canonical_addresses.parquet \
  --output-dir /secure/path/all_sector_road_identifier \
  --threads 4 \
  --memory-limit 16GB
```

Use `--row-limit 5000` for a smoke run. The complete run writes the experiment
database, joblib model, metadata, reports, and
`models/_road_ranker_hist_gradient_boosting_v1.json`.

## Packaged Models

| Artefact | Role | Runtime dependency |
| --- | --- | --- |
| `data/road_assignment_scorecard_v1.json` | Live tree-rule-distilled ranker | Loaded by production; DuckDB-only scoring |
| `data/_road_ranker_hist_gradient_boosting_v1.json` | Private, complete HGB reference | Not loaded by production |

The private JSON contains the baseline prediction, ordered feature names,
classes, and all 250 trees with 31,250 nodes. It is suitable for exact offline
replay without unpickling the original estimator. The leading underscore marks it
as package-private, not a supported runtime model.

Results are road-oracle experimental evidence, not end-to-end address-matching
accuracy.
# Road Identifier Model Training

This package contains the reproducible, offline experiment that identifies roadlike spans in UK address strings. It is not part of the production address-matching path and does not alter Splink, canonical preparation, or matcher outputs.

## What It Does

1. Normalises a full address and removes an embedded postcode.
2. Removes the established UKAM common suffix chain.
3. Finds the rightmost numeric token.
4. Generates every contiguous 2- and 3-token span after that anchor.
5. Uses training-only recurrence and context evidence to rank the spans.
6. Optionally applies a second tree model that estimates whether the top-ranked span should be emitted.

The emitted candidates retain descending ranker probability. The gate is an abstention decision, not a different ranking model.

## Modules

- `all_sector_model.py`: all-sector candidate generation, topology, ranker training, calibration, test evaluation, and audit tables.
- `all_sector_reachability_gate.py`: optional correct-top-1 acceptance gate trained from ranker outputs.
- `listwise_reranker.py`: calibration-selected top-three reranker over frozen ranker scores.
- `listwise_reranker_gate.py`: calibrated high-precision acceptance gate for the selected reranker.
- `gated_top_k_report.py`: top-1, top-2, and top-3 gate-aware metrics.
- `road_example_scoring.py`: in-memory scoring of supplied address examples against frozen ranker topology and models.
- `_resources.py`: packaged candidate policy and common-end-token resource access.
- `candidate_token_policy.json`: candidate syntax and residence/non-road token policy used by the experiment.

Read [RETRAINING.md](RETRAINING.md) before changing a feature, label, split, or gate policy. Headline outcomes and their limitations are in [RESULTS.md](RESULTS.md).

## Setup

From the `uk_address_matcher` repository:

```bash
uv sync --group model-training --group dev
```

The experiment expects a canonical residential/commercial cache Parquet with at least `source_unique_id`, `cohort`, `full_address_raw`, `postcode_raw`, `street_raw`, and `candidate_status`. The source cache is deliberately external: do not commit it, model databases, or real addresses.

## Run Order

Run the ranker first, then the optional gate:

```bash
uv run python -m uk_address_matcher.model_training.all_sector_model \
  --cache-path /secure/path/canonical_residential_commercial_addresses.parquet \
  --output-dir /secure/path/all_sector_road_identifier \
  --threads 4 \
  --memory-limit 16GB

uv run python -m uk_address_matcher.model_training.all_sector_reachability_gate \
  --database-path /secure/path/all_sector_road_identifier/all_sector_road_identifier.duckdb \
  --output-dir /secure/path/all_sector_reachability_gate

uv run python -m uk_address_matcher.model_training.listwise_reranker \
  --database-path /secure/path/all_sector_road_identifier/all_sector_road_identifier.duckdb \
  --output-dir /secure/path/all_sector_listwise_reranker_v1

uv run python -m uk_address_matcher.model_training.listwise_reranker_gate \
  --source-database-path /secure/path/all_sector_road_identifier/all_sector_road_identifier.duckdb \
  --reranker-model-path /secure/path/all_sector_listwise_reranker_v1/models/listwise_reranker.joblib \
  --output-dir /secure/path/all_sector_listwise_reranker_gate_v1
```

The ranker supports `--row-limit` for a smoke run. The gate uses durable 25,000-address checkpoints and can safely resume against the same model version and schema.

## Outputs

The ranker writes a DuckDB database, selected `HistGradientBoostingClassifier` model, model metadata, calibration/test metrics, top-three candidate audit, district metrics, and a Markdown report. The gate writes an independent model, probability curves, calibration-selected policies, final-half metrics, and feature importance.

Results are oracle-evaluated experimental evidence. They must not be represented as current production matching accuracy. The selected reranker gate reached 99.528% held-out operational precision at 95.586% oracle-reachable recall; it requires shadow-mode review before any production trial.