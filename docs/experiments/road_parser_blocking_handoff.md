# Road Parser and Blocking Performance Handoff

## Purpose and Current Constraint

This document describes the deployed road finder as of 2026-08-29 for a performance-focused review. Roads are **blocking keys only**. They must not change Splink comparison features, match scores, thresholds, reranking, or final match selection.

The deployment target is a DuckDB-only runtime. Training can use scikit-learn, but serving must use the folded JSON scorecard and DuckDB SQL only. The main objective is to reduce full canonical precomputation from the verified 43.342 seconds towards 20 seconds, with 30 seconds an acceptable deployment ceiling, while retaining useful blocking recall. The current code uses the top-one inferred road (`road_1_norm`) for blocking; it does not yet use a top-$k$ road list.

## Production Artefacts and Paths

| Artefact | Exact path | Purpose |
| --- | --- | --- |
| Runtime scorecard | `uk_address_matcher/data/road_assignment_scorecard_v1.json` | Selected tree-rule-distilled additive pairwise ranker: intercept, 86 folded additive feature coefficients, and 64 tree-path rules. |
| Runtime phrase catalogue | `uk_address_matcher/data/roadlike_places.parquet` | Phrase-support and terminal-context lookup used by the road finder. |
| Serving SQL implementation | `uk_address_matcher/cleaning/steps/roadlike_places.py` | Candidate generation, feature SQL, runtime scorecard validation and compilation, top-one and top-two outputs. |
| Offline artefact verifier | `uk_address_matcher/model_training/road_assignment_artifacts.py` | Replays deployable scorecards for training and benchmark checks; production does not import it. |
| Canonical precomputation call site | `uk_address_matcher/cleaning/chunking_strategies.py` | Adds `road_1_norm` and cached blocker cardinalities before canonical parquet output. |
| Canonical pipeline call site | `uk_address_matcher/prepare_canonical.py` | Invokes canonical road-key precomputation before writing prepared canonical parquet. |
| Existing production road blockers | `uk_address_matcher/linking_model/matching/stages/splink.py` | Candidate-generation-only rules; no road-derived comparison or scoring changes. |

The JSON artefact is about 40 KB and the phrase catalogue is about 15 MB. No sklearn, joblib, or model-server dependency is required at serving time.

## What `road_1_norm` Means

`road_1_norm` is the highest-scoring roadlike phrase extracted from `clean_full_address`. It is not copied from an input street field and is not a calibrated probability. The score is a ranking logit used to choose a road candidate within one address.

The road finder requires already cleaned inputs with `clean_full_address`, `postcode`, and `numeric_tokens`. It:

1. Removes facility-clause and trailing-unit noise using the shared token policy.
2. Finds the rightmost numeric anchor in the cleaned address.
3. Enumerates width-two and width-three token spans after that anchor.
4. Prefers spans with recognised terminal road syntax; only addresses with no such span fall back to all candidate spans.
5. Left joins phrase and terminal recurrence statistics from `roadlike_places.parquet`.
6. Computes 15 base features, folded additive interactions, and 64 tree-path `CASE` features.
7. Computes `ranker_logit = intercept + sum(coefficient * feature)` and ranks candidates per address.
8. Emits the rank-one phrase as `road_1_norm`, plus confidence, margin, token count, and distinctive tokens.

Ambiguous inputs matching `CARAVAN`, `HOUSE BOAT`, `HOUSEBOAT`, `BEACH HUT`, selected `TENNIS ... UNNAMED ROAD` patterns, or `REAR OF` are excluded from road-candidate generation. This behaviour is in `AMBIGUOUS_ADDRESS_PATTERN` in `roadlike_places.py`.

## Exact SQL Ownership

The SQL that creates the production column is generated and executed by `add_top_1_road_features()` in `uk_address_matcher/cleaning/steps/roadlike_places.py`. The owning SQL builders are:

```python
prepared = roadlike_place_prepared_input_sql(source_relation)
candidates = roadlike_place_prepared_candidate_sql(source_relation)
catalog = roadlike_place_catalog_sql(candidate_relation)
features = _road_candidate_feature_sql(candidate_relation, catalog_table)
scored = _score_road_candidates(con, feature_relation, model)
```

The final projection that produces the needed blocking column is:

```sql
SELECT
    input.*,
    ranked.candidate_phrase AS road_1_norm,
    ranked.ranker_logit AS road_1_confidence,
    ranked.candidate_width AS road_1_token_count,
    ranked.ranker_logit - ranked.next_ranker_logit AS road_1_margin,
    ranked.distinctive_tokens AS road_1_distinctive_tokens
FROM input
LEFT JOIN ranked USING (address_id)
WHERE ranked.candidate_rank = 1 OR ranked.candidate_rank IS NULL
```

The complete SQL is intentionally generated from the source functions above rather than maintained as a copied static query. The exact model expression comes from `_score_road_candidates()`, which validates and compiles the packaged scorecard without importing offline training code.

### Canonical Precomputation SQL

`_add_canonical_road_blocking_keys()` in `uk_address_matcher/cleaning/chunking_strategies.py` calls the top-one finder, then persists `road_1_norm` and two cheap eligibility flags alongside the canonical data:

```sql
SELECT
    source_columns,
    road_1_norm,
    COUNT(*) FILTER (WHERE road_1_norm IS NOT NULL)
        OVER (PARTITION BY road_1_norm) <= 1000 AS road_frequency_lte_1000,
    COUNT(*) FILTER (
        WHERE road_1_norm IS NOT NULL AND numeric_token_1 IS NOT NULL
    ) OVER (PARTITION BY road_1_norm, numeric_token_1) <= 32
        AS road_n1_block_size_lte_32
FROM road_features
```

Those flags are precomputed only for candidate eligibility; they never affect linkage scoring.

## Current Accuracy Evidence

All road-ranker quality figures below are held-out road-oracle experiment results, not end-to-end address-matcher precision/recall. The explicit `street_raw` source field is used only after inference as an evaluation oracle. A safe-core prediction is correct when it equals the source street or is a valid left-prefix; exact recall requires the whole source street phrase.

### Corpus and Split

- Input cache: 51,538,832 canonical residential/commercial rows.
- Preferred canonical records: 33,575,026, one per UPRN.
- Candidate stream before terminal-first filtering: 108,843,568 width-two/three spans.
- Geographic coverage: 5,077 outward postcode districts.
- Deterministic UPRN hash split: 80% training, 10% calibration, 10% untouched final test.
- Frozen oracle-reachable test population: 2,952,500 addresses.
- Train-only topology rule: phrase/terminal recurrence never incorporates oracle text, labels, calibration rows, or test rows.

### Frozen-Test Ranking Metrics

| Ranker | Safe-core P/R/F1 | Exact top-one recall | Top-two list recall | Top-three list recall |
| --- | ---: | ---: | ---: | ---: |
| Pointwise logistic, 15 raw features | 89.658% | 85.168% | 97.770% | 98.787% |
| Pairwise logistic, 15 raw features | 93.863% | 90.397% | 97.936% | 98.918% |
| Pairwise additive scorecard v1, 42 features | 96.384% | 90.004% | 98.630% | 99.477% |
| Pairwise additive scorecard v2, 86 features | 96.591% | 90.478% | 98.821% | 99.531% |
| Address-balanced additive scorecard v2, 86 features | 96.772% | 90.958% | 98.831% | 99.543% |
| Tree-rule-distilled scorecard v1, all candidates | 96.7726% | 91.8248% | Not measured | Not measured |
| **Selected: tree-rule-distilled v1, terminal-first** | **96.7523%** | **93.7707%** | Not measured | Not measured |

The selected configuration has 2,856,612 safe-core-correct top-one winners from 2,952,500 reachable test addresses. Relative to the all-candidate tree model, terminal-first loses 646 safe-core winners ($-0.0249$ percentage points) but gains 57,452 exact top-one winners ($+1.9460$ percentage points).

### Important Interpretation for Blocking

The selected top-one road finder already exceeds the requested 0.90 precision floor on the oracle-reachable frozen test. For blocking, list recall is more relevant than rank-one precision: an exact road candidate may be at rank 2 or 3 even where the rank-one phrase is broader or a prefix. The historical native-model top-$k$ measurements were 98.266% exact recall at top 2 and 98.974% at top 3, but these are not yet measured for the selected terminal-first folded scorecard. Do not substitute these numbers without a fresh folded-scorecard top-$k$ benchmark.

The current system will miss exact equality when an address yields a broad phrase such as `MY STREET` and the canonical road is `MY STREET EAST`. A top-two or top-three **candidate-generation-only** experiment is therefore justified, provided every emitted road remains an exact scalar equality key and candidate volume is measured separately.

## Verified Performance Baseline

This is the closest verified answer to the remembered "five million records per second" figure. It is a macOS run with 14 logical CPUs against 33,575,026 pre-cleaned canonical rows. It excludes raw canonical cleaning and any learned acceptance gate.

| Stage | Work | Time | Throughput |
| --- | --- | ---: | ---: |
| Candidate generation | 47,676,776 candidates | 30.752s | 1.55M candidates/s |
| Phrase catalogue construction | 990,119 phrases | 4.844s | N/A |
| SQL scoring | 47,676,776 candidates | 5.973s | 7.98M candidates/s |
| Winner selection | 30,477,812 winners | 1.773s | 17.19M winners/s |
| **Total** | **33,575,026 input rows** | **43.342s** | **0.775M input rows/s** |

So the claim is only true for candidate scoring: 7.98M candidates/s. It is not true for end-to-end road finding, which is approximately $33{,}575{,}026 / 43.342 = 774{,}638$ input rows/s. Candidate generation is 71.0% of total time and is the principal optimisation target. The terminal-first change already reduced total prepared-input time from 159.363s to 43.342s.

## Available Data and Test Surfaces

| Resource | Path | What it contains | Appropriate use |
| --- | --- | --- | --- |
| Full inferred canonical output | `benchmarking/results/road_scoring_experiment/canonical_with_inferred_road/ukam_canonical_addresses.parquet` | 71,438,939 rows, including cleaned fields and `road_1_*` columns | Subset-first runtime profiling and output-stability checks. It does **not** contain `street_raw`; it cannot validate road-oracle accuracy. |
| Chunked top-two output | `benchmarking/results/road_scoring_experiment/canonical_with_top_two_roads/ukam_canonical_addresses_chunks/*.parquet` | Canonical records with experimental `road_top_2_norms` | Top-$k$ blocking-volume prototyping. |
| Phrase catalogue | `uk_address_matcher/data/roadlike_places.parquet` | Candidate phrase topology | Runtime dependency; do not use source oracle labels. |
| Matcher baseline outputs | `benchmarking/results/road_scoring_matrix/V0_derivative_no_road_comparison_matches.parquet` | 63 MB of labelled matcher outcomes | Blocking-recall and candidate-volume validation, not road-parser oracle accuracy. |
| Road fixture tests | `tests/test_roadlike_place_stage.py` and `tests/test_road_assignment_artifacts.py` | Small deterministic SQL/artefact checks | Fast correctness checks after a local change. |
| Held-out road-oracle evaluation | `benchmarking/results/all_sector_road_identifier/all_sector_road_identifier.duckdb` | `all_sector_prepared_addresses`, candidates, features, winners, and frozen-test metrics when the experiment output is retained | Authoritative road accuracy rerun. This database is not present in this checkout. |

The source contract for recreating the held-out database is documented in `uk_address_matcher/model_training/RETRAINING.md`: `unique_id`/UPRN, `clean_full_address`, `postcode`, `numeric_tokens`, classification data, and `street_raw` as evaluation-only oracle. The training pipeline is `uk_address_matcher/model_training/all_sector_model.py`.

## Reproducible Review Plan

### 1. Start with a Deterministic Subset

Use the full inferred-road parquet to profile without reading all 6.1 GB. This checks runtime and output stability, not accuracy:

```sql
CREATE OR REPLACE TEMP VIEW sample AS
SELECT *
FROM read_parquet(
  'benchmarking/results/road_scoring_experiment/canonical_with_inferred_road/ukam_canonical_addresses.parquet'
)
WHERE hash(ukam_address_id) % 100 = 0;
```

Run `add_top_1_road_features()` on this same deterministic subset. Record row count, candidate count, and `road_1_norm` equality versus the persisted output. Begin with 1%, then 5%, then a full prepared-canonical run only after an optimisation preserves subset outputs and oracle accuracy.

### 2. Capture Plans for Every Material Stage

Use a new DuckDB connection, set the same thread count as the target laptop, and run `EXPLAIN ANALYZE` separately for:

1. `roadlike_place_prepared_input_sql(sample)`.
2. `roadlike_place_prepared_candidate_sql(prepared_input)`.
3. `roadlike_place_catalog_sql(candidates)`.
4. `_road_candidate_feature_sql(candidates, catalogue)`.
5. `_score_road_candidates(...)`.
6. The rank-one window query.

Inspect actual row counts, time per operator, materialised intermediates, regex costs, `UNNEST`/`range` expansion, window sorts, and phrase-catalogue hash join build/probe time. Do not profile only the final monolithic query: the 30.752-second candidate-generation stage hides the useful operator-level signal.

### 3. Test Simplification Candidates

Evaluate each proposal against the frozen oracle test before deployment. Keep training-only fields out of runtime SQL.

| Candidate change | Reason to test | Required guardrail |
| --- | --- | --- |
| Reduce span enumeration before scoring | Candidate generation is the 71% bottleneck | Preserve safe-core and exact top-one metrics; inspect reachability loss. |
| Remove low-value feature terms | 150 scorecard terms are much cheaper than generation, but may still help | Ablate one feature family at a time; retain at least 0.90 safe-core precision and measure exact recall. |
| Precompute reused token/anchor fields | Avoid repeating cleaning/anchor extraction in reruns | Ensure cached columns are versioned with the token policy. |
| Reuse the phrase catalogue | Avoid rebuilding topology when canonical source is unchanged | Preserve train-only topology provenance. |
| Emit top 2/3 road candidates for blocking | Recovers exact-superstring cases such as `MY STREET EAST` | Measure exact incremental pairs, truth capture, and `HASH_JOIN`; do not use substring or list-overlap joins. |
| Precompute blocking cardinalities | Avoid runtime counts by road or road/number | Keep all optional blockers bounded and scalar equality-only. |

### 4. Blocking-Specific Acceptance Criteria

For every blocker, report normal and postcode-omitted cohorts separately:

- raw comparison count;
- exact increment beyond the baseline candidate union;
- incremental labelled true physical pairs;
- newly rescued messy inputs;
- pairs per rescued input;
- true-pair capture and record-capture rate;
- physical plan contains `HASH_JOIN` and no `NESTED_LOOP`;
- preprocessing seconds and candidate-generation seconds.

Do not use `OR`, fuzzy matching, substring containment, list overlap, or postcode-derived fields in experimental road blockers. Candidate counts alone are insufficient because a low count may add no truth coverage.

## Known Implementation and Evaluation Caveats

- The raw score/logit and score margin are ranking utilities, not calibrated acceptance probabilities.
- The deployment path is the folded tree-rule model, not the legacy 15-feature logistic model. Continue using the logreg-compatible folded architecture: it can be exported to SQL and served with DuckDB alone.
- Raw canonical cleaning is excluded from the 43.342-second metric. A production claim must report it separately.
- The top-two output function exists (`add_top_2_road_features()`), but it is not yet integrated into the production canonical preparation path or blocking rules.
- The full oracle-labelled road test database is not checked into this workspace. Do not claim fresh road-parser accuracy from the inferred-road parquet alone.
- Current scalar blocker sweeps are still in progress. Their partial checkpoint files must not be treated as a production recommendation until both normal and postcode-omitted cohorts and cumulative union metrics complete.

## Recommended Starting Point for the Reviewer

Profile `roadlike_place_prepared_candidate_sql()` first. It consumes most of the verified wall time and is where span enumeration, road-terminal filtering, and fallback handling interact. Make one small change at a time on the deterministic subset, compare row-for-row top-one output against the baseline, then run the frozen all-sector oracle evaluation. Only after quality holds should a full-canonical timing run and an exact scalar blocking-volume sweep be repeated.

Primary supporting documents:

- `uk_address_matcher/model_training/RESULTS.md`
- `uk_address_matcher/model_training/RETRAINING.md`
- `docs/experiments/all_sector_road_identifier_listwise_reranker.md`
- `benchmarking/results/road_blocking/candidate_volume_summary.md`
- `benchmarking/road_blocking_audit.py`
- `benchmarking/selective_road_blocking_audit.py`