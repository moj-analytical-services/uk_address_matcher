# Road Identifier Results

## Decision

Production uses `road_assignment_scorecard_v1.json`, a compact tree-rule-distilled additive ranker. It preserves DuckDB-only serving and is used with terminal-first candidate selection. The complete HGB is retained privately as `_road_ranker_hist_gradient_boosting_v1.json` for audit, replay, and future distillation. It is not loaded during matching or canonical preparation.

## Frozen Corpus

- 51,538,832 residential/commercial source rows.
- 33,575,026 preferred canonical records, one per UPRN.
- 5,077 outward postcode districts.
- Deterministic UPRN split: 80% train, 10% calibration, 10% final test.
- 2,952,500 oracle-reachable addresses in the frozen test set.
- Phrase and terminal recurrence topology built from train rows only.

`street_raw` is used only as an evaluation oracle. It is never a candidate feature or runtime input.

## Selected Models

| Model | Safe-core top one | Exact top one | Safe-core top two | Exact top two |
| --- | ---: | ---: | ---: | ---: |
| Complete HGB reference | about 98.22% | about 92.37% | 99.322% | 98.266% |
| Live distilled scorecard, terminal-first | 96.7523% | 93.7707% | Not measured | Not measured |

Safe-core counts a source street or valid left-prefix. Exact requires the complete source street phrase. These are road-ranker measurements, not matcher precision or recall.

The live artefact has 86 additive coefficients and 64 tree-rule predicates, 150 score terms before exactly-zero terms are omitted by the compiler. Terminal-first selection retains familiar road-ending candidates and falls back to all valid candidates only when no terminal candidate exists.

## Runtime

On the frozen preferred-canonical workload with 14 logical CPUs:

| Stage | Work | Time |
| --- | --- | ---: |
| Candidate generation | 47,676,776 candidates | 30.752s |
| Phrase catalogue | 990,119 phrases | 4.844s |
| SQL scoring | 47,676,776 candidates | 5.973s |
| Winner selection | 30,477,812 winners | 1.773s |
| **Total** | **33,575,026 inputs** | **43.342s** |

SQL scoring reached 7.98 million candidates per second. Candidate generation, not score arithmetic, was the principal cost.

The optimised canonical blocking path now selects one preferred row per UPRN, scores equivalent post-number tails once per chunk, rejoins winners to all source variants, and computes grouped blocker cardinalities. Its full 71,438,939-row road stage took 26.954s at the 16GB DuckDB limit, with no top-one differences in the fixed 1/20 old/new comparison.

The complete canonical preparation run, including cleaning, term frequencies, indexes, road keys, and eight output shards, took 537.531s. Roads added only 1.16% to output size in the matched earlier comparison.

## Artefact Inventory

| File | Contents | Approximate size |
| --- | --- | ---: |
| `road_assignment_scorecard_v1.json` | Folded additive weights and 64 SQL rules | 40KB |
| `_road_ranker_hist_gradient_boosting_v1.json` | Baseline plus 250 trees and 31,250 nodes | 3.14MB |
| Original joblib, experiment output only | sklearn estimator used to create the private JSON | 1.82MB |

The 729-byte JSON beside the original joblib is metadata only. It cannot score a row. The packaged private JSON is inference-complete: on 100 random rows, including a missing value, its reconstructed raw scores matched sklearn exactly with a maximum absolute difference of `0.0`.

## Limits

- Candidate generation considers only width-two and width-three spans after the rightmost numeric anchor.
- One-token and four-token source roads are therefore unreachable.
- The score and margin rank candidates within an address; they are not calibrated acceptance probabilities.
- Top-two blocking remains an experimental opportunity and must be evaluated for candidate volume as well as truth capture.
- Production must continue to use scalar equality blockers and DuckDB-only scoring unless a separately reviewed deployment changes that contract.
