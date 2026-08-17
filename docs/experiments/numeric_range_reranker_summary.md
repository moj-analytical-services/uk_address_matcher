# Numeric Range Reranker Summary

## Intended Purpose

The reranker is an always-on post-Splink repair for already-shortlisted
candidate pairs where exactly one side contains one typed range and the other
side contains scalar numerics. It does not create candidates and it never reads
postcode fields. A `SplinkStage` cannot disable it; passing `None` is rejected.

It scores only:

- endpoint: `20-23` with `20` or `23`;
- interior: `20-23` with `21` or `22`.

Partial overlap, exact range equality, multiple ranges, invalid ranges,
reference ranges, suffix-incompatible endpoints, suffix-bearing interiors, and
flat-identity conflicts receive no adjustment.

## Configuration

The default configuration is:

```python
NumericRangeRerankerConfig(
    maximum_adjustment_bits=20.0,
    minimum_non_numeric_bits=-100.0,
)
```

No numeric-range score settings are read from environment variables. The
configuration remains explicit so thresholds can be tuned without making the
reranking stage optional.

## Pipeline Shape

The matching stage:

1. Packs nullable valid range attributes into one internal
    `numeric_range_metadata` struct; `flat_identity` remains separate because it
    is also a direct Splink comparison input.
2. Retains Splink factors internally long enough to create the ordinary
    shortlist; the debug retention flag does not control reranking.
3. Filters the shortlist to exactly-one-range-versus-scalar rows.
4. Calculates `legacy_numeric_bits` only for those filtered rows.
5. Projects gamma, BF, and individual numeric TF columns away from the normal
    prediction relation; explicit debugging can expose the raw factors without
    calculating `legacy_numeric_bits` for every row.
6. Hash-joins the narrow range adjustment relation back to the shortlist before
    the existing token and relation-marker adjustments continue.

The range SQL first filters to exactly-one-range-versus-scalar rows, extracts
only the first typed range attribute, and returns one narrow adjustment row per
four-column pair key. That relation is hash-joined back to the shortlist before
the existing token and relation-marker adjustments continue.

## Adjustment

```text
non_numeric_bits = match_weight - legacy_numeric_bits
range_tf_bits = lower_endpoint_tf_weight * log2(1 / lower_endpoint_tf)
candidate_range_bits = relationship_base_bits + range_tf_bits
numeric_range_adjustment = min(
    maximum_adjustment_bits,
    max(0, candidate_range_bits - legacy_numeric_bits),
)
```

A null, zero, or unusable lower-endpoint TF contributes zero TF bits. The guard
requires `non_numeric_bits > minimum_non_numeric_bits`; the selected experiment
therefore uses the permissive `-100.0` threshold while preserving the guard in
the model.

## Memory And Storage

The prepared canonical address parquet was measured with DuckDB's
`parquet_metadata` on 71,438,939 rows:

| Variant | Compressed size | Uncompressed column size |
| --- | ---: | ---: |
| Without numeric-range columns | 1,600,859,857 bytes | 8,792,246,411 bytes |
| Valid-range attributes only | 1,614,508,261 bytes | 8,879,834,950 bytes |
| Numeric-range overhead | 13,648,404 bytes | 87,588,539 bytes |

The valid-range attributes therefore add approximately 13.0 MiB of compressed
column payload, an 0.85% increase over the no-range variant. The actual parquet
files increased by 15,017,605 bytes, approximately 14.3 MiB. The uncompressed
column footprint is approximately 83.5 MiB; this is an upper-bound indicator
for materialised in-memory column storage, not a direct process-RSS measurement.

The normal prediction relation does not materialise `legacy_numeric_bits` for
all Splink rows. Only the already-shortlisted numeric range/scalar candidates
receive that calculated value. Older prepared canonical data can still be
adapted when `numeric_range_attributes` and `numeric_tokens` are present;
prepared data with no numeric-range metadata now fails clearly because
reranking is mandatory.

## Latest Persisted Benchmark

The latest `run_benchmarking.py` pass used the valid-range-only canonical
variant, the Hackney dataset, threshold `8.0`, and baseline run
`d141fb4f525014b2`. The fresh run is `cb5320a263c7a10f`.

| Metric | Baseline | Always-on reranker | Change |
| --- | ---: | ---: | ---: |
| Correct matches | 112,876 | 112,899 | +23 |
| Matched rows | 113,567 | 113,587 | +20 |
| Precision | 99.3915% | 99.3943% | +0.0028 pp |
| Recall | 98.8701% | 98.8902% | +0.0201 pp |
| F1 | 99.1301% | 99.1416% | +0.0115 pp |
| PR-AUC | 0.586019 | 0.586221 | +0.000202 |
| Total runtime | 31.58s | 32.53s | +0.95s |

The valid-range-only canonical reduced runtime by approximately 1.69s versus
the previous shortlist-only pass and by 1.13s versus the earlier wide-schema
always-on pass. It preserved all 112,899 correct matches while removing one
false positive relative to the wide-schema run. The persisted manifest records
the repository `HEAD` commit; these measurements were run against the current
working tree as well.

Artefacts:

- [manifest](../../benchmarking/results/hackney/2026-08-14/cb5320a263c7a10f/manifest.json)
- [comparison report](../../benchmarking/results/hackney/2026-08-14/cb5320a263c7a10f/comparison_report_d141fb4f525014b2_vs_cb5320a263c7a10f.md)
- [comparison summary](../../benchmarking/results/hackney/2026-08-14/cb5320a263c7a10f/comparison_summary_d141fb4f525014b2_vs_cb5320a263c7a10f.json)
- [precision-recall overlay](../../benchmarking/results/hackney/2026-08-14/cb5320a263c7a10f/charts/precision_recall_overlay_d141fb4f525014b2_vs_cb5320a263c7a10f.html)

## JSON Range Blocking Trial

The single symmetric postcode/range-signature rule was trialled on a frozen
pooled-councils slice: 5,000 messy rows and 20,000 canonical rows, with one
warm-up run followed by three measured runs. This is candidate recovery
evidence, not proof that every labelled target is correct.

| Metric | Existing rules | Exact range rule | Change |
| --- | ---: | ---: | ---: |
| Distinct candidate pairs | 16,066 | 16,066 | 0 |
| Messy records gaining a candidate | 0 | 0 | 0 |
| Candidate SQL audit median | 0.059s | 0.061s | +0.002s |
| Matcher/prediction median | 1.447s | 1.400s | -0.047s |
| Total matching median | 1.596s | 1.551s | -0.045s |
| Correct matches at MW8 | 3,726 | 3,726 | 0 |

The earlier hard-to-reach audit contains 143 labelled rows representing 80
unique messy/target ID pairs. Those rows are a separate review population: the
exact-range rule targets rows with equal postcode, lower bound, and upper bound;
it does not apply flag, role, suffix, width, or range-count guards. The pooled
slice produced no incremental candidates. The remaining audit rows are:

- 62 one-sided/numeric-mismatch rows outside this exact-range block;
- 32 postcode-disagreeing rows.

Full review extract, including every messy address beside its labelled target
address, postcode status, classification, and recovery expectation:

- [hard-to-reach Markdown review](../../benchmarking/outputs/numeric_window_audit_pooled_20260814/hard_to_reach_records.md)
- [hard-to-reach CSV extract](../../benchmarking/outputs/numeric_window_audit_pooled_20260814/hard_to_reach_records.csv)
- [machine-readable exact-range benchmark](../../benchmarking/outputs/numeric_range_json_blocking_20260816_exact_range/summary.json)

## Current Version Results

For these tables:

- `TP` is `correct_matches`;
- `FP` is `matched_rows - correct_matches`; and
- `unmatched` is `total_rows - matched_rows`.

The following R0/R1 tables are historical experiments from before the
always-on contract. R0 used the then-optional feature-off path; R1 used
`NumericRangeRerankerConfig(maximum_adjustment_bits=20.0,
minimum_non_numeric_bits=-100.0)`. They remain useful for context but should
not be read as evidence that the current implementation can disable reranking.

### Historical Frozen Hackney Cohort

| Arm | Input | TP | FP | Unmatched | Precision | Recall | F1 | Runtime |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| R0 off | 113,836 | 112,845 | 490 | 501 | 99.5677% | 99.1294% | 99.3481% | 30.60s |
| R1 configured | 113,836 | 112,868 | 486 | 482 | 99.5713% | 99.1497% | 99.3600% | 29.88s |

R1 gained 23 true positives, reduced false positives by 4, and reduced
unmatched rows by 19 in this frozen cohort.

### Historical Frozen Pooled Cohort

The pooled cohort contains 348,687 input rows and 340,796 distinct IDs. Three
measured repetitions were run per arm; the table reports each repetition so
runtime variation remains visible.

| Arm | Rep | Input | TP | FP | Unmatched | Precision | Recall | F1 | Runtime |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| R0 off | 1 | 348,687 | 338,268 | 3,925 | 6,494 | 98.8530% | 97.0119% | 97.9238% | 48.62s |
| R0 off | 2 | 348,687 | 338,271 | 3,922 | 6,494 | 98.8539% | 97.0128% | 97.9247% | 52.15s |
| R0 off | 3 | 348,687 | 338,268 | 3,925 | 6,494 | 98.8530% | 97.0119% | 97.9238% | 48.64s |
| R1 configured | 1 | 348,687 | 338,297 | 3,919 | 6,471 | 98.8548% | 97.0203% | 97.9289% | 45.15s |
| R1 configured | 2 | 348,687 | 338,298 | 3,918 | 6,471 | 98.8551% | 97.0205% | 97.9292% | 43.01s |
| R1 configured | 3 | 348,687 | 338,298 | 3,918 | 6,471 | 98.8551% | 97.0205% | 97.9292% | 44.54s |

Median runtime was 48.64s for R0 and 44.54s for R1, a 4.10s reduction.
R1 gained 27 to 30 true positives across repetitions, reduced false positives
by 6 to 7, and reduced unmatched rows by 23.

These results are current-version evidence, not the earlier fixed-bonus Hackney
prototype matrix. Postcode-status stratification and changed-record exports
remain required before enabling the configuration by default.

### Historical Hackney Threshold Matrix

The latest frozen Hackney cohort contains 113,836 rows. Each threshold was run
in a fresh process against the same typed canonical variant. R1 used:

```python
NumericRangeRerankerConfig(
    maximum_adjustment_bits=20.0,
    minimum_non_numeric_bits=-100.0,
)
```

`TP` is the number of correct matches, `FP` is matched minus correct, and
`unmatched` is input minus matched.

| MW | Arm | Matched | TP | FP | Unmatched | Precision | Recall | F1 | Runtime |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 6 | R0 off | 113,506 | 112,985 | 521 | 330 | 99.5410% | 99.2524% | 99.3965% | 30.15s |
| 6 | R1 configured | 113,513 | 113,003 | 510 | 323 | 99.5507% | 99.2682% | 99.4093% | 30.99s |
| 8 | R0 off | 113,335 | 112,845 | 490 | 501 | 99.5677% | 99.1294% | 99.3481% | 28.50s |
| 8 | R1 configured | 113,354 | 112,868 | 486 | 482 | 99.5713% | 99.1497% | 99.3600% | 30.41s |
| 10 | R0 off | 112,954 | 112,518 | 436 | 882 | 99.6140% | 98.8422% | 99.2266% | 33.30s |
| 10 | R1 configured | 112,983 | 112,550 | 433 | 853 | 99.6168% | 98.8703% | 99.2421% | 32.91s |
| 12 | R0 off | 112,325 | 111,952 | 373 | 1,511 | 99.6679% | 98.3450% | 99.0020% | 32.62s |
| 12 | R1 configured | 112,366 | 111,994 | 372 | 1,470 | 99.6689% | 98.3819% | 99.0212% | 36.26s |

#### R1 Minus R0

| MW | TP change | FP change | Unmatched change | Precision change | Recall change | F1 change | Runtime change |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 6 | +18 | -11 | -7 | +0.0097pp | +0.0158pp | +0.0128pp | +0.84s |
| 8 | +23 | -4 | -19 | +0.0036pp | +0.0202pp | +0.0119pp | +1.91s |
| 10 | +32 | -3 | -29 | +0.0028pp | +0.0281pp | +0.0155pp | -0.39s |
| 12 | +42 | -1 | -41 | +0.0010pp | +0.0369pp | +0.0192pp | +3.65s |

The current formulation therefore recovers 18 to 42 additional true positives
across these MW thresholds, rather than the approximately 70-record recovery
seen in the older prototype. The comparison is current-version evidence on the
frozen 113,836-row cohort; it is not directly comparable to the older 113,829-
row matrix without replaying that exact cohort and canonical preparation.

## Benchmarking Note

The original Hackney matrix used a fixed-bonus prototype and should not be used
as proof of the corrected implementation. The historical R0/R1 tables above
also predate the mandatory always-on contract and shortlist-only legacy-bit
projection. The latest persisted comparison is the current reference for this
working tree; broader rollout still benefits from pooled runs on an identical
frozen messy cohort and canonical dataset, with postcode-status stratification
and candidate-count parity.
