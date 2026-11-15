# Name Agreement: Residential and Commercial Benchmark

## Question

Does grading leading address-name similarity, with postcode-local-name support,
improve matching quality on residential and commercial data?

## Runs Compared

Runs were made on 2026-11-12. Hackney council tax and Mid Sussex business rates
share baseline run `c1b4b6c577a8cb29` and variant run `4eb363646d89bcbf`;
pooled councils uses baseline `4c71a5ae2e2dbdf6` and variant
`84c833725539d17a`. The final projected-column pooled rerun is
`bff28e66555c7907`.

| Dataset | Run | Rows | Runtime | PR-AUC | Precision | Recall | F1 | Correct | Wrong | Wrong Δ | Unmatched | Unmatched Δ |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Hackney | Baseline | 114,173 | 41.79s | 0.480894 | 99.4063% | 98.9849% | 99.1951% | 113,014 | 675 | - | 484 | - |
| Hackney | Variant | 114,173 | 40.18s | 0.480426 | 99.4515% | 98.9323% | 99.1912% | 112,954 | 623 | -52 | 596 | +112 |
| Mid Sussex | Baseline | 4,284 | 23.01s | 0.644555 | 88.4014% | 75.2568% | 81.3012% | 3,224 | 423 | - | 637 | - |
| Mid Sussex | Variant | 4,284 | 25.94s | 0.639610 | 88.9136% | 74.5098% | 81.0770% | 3,192 | 398 | -25 | 694 | +57 |
| Pooled councils | Baseline | 348,284 | 128.00s | 0.313123 | 98.9077% | 98.0697% | 98.4869% | 341,561 | 3,772 | - | 2,951 | - |
| Pooled councils | Variant | 348,284 | 75.06s | 0.312879 | 98.9590% | 98.0430% | 98.4989% | 341,468 | 3,592 | -180 | 3,224 | +273 |
| Pooled councils | Projected rerun | 348,284 | 95.92s | 0.312879 | 98.9590% | 98.0430% | 98.4989% | 341,468 | 3,592 | -180 | 3,224 | +273 |

Deltas are variant minus baseline; a negative wrong delta means fewer wrong
matches, while a positive unmatched delta means more records left unmatched.

## Exact Changes

- `benchmarking/run_benchmarking.py`: selected `hackney`, `mid_sussex`, and `pooled_councils`; disabled the residential-only canonical filter for all runs.
- `uk_address_matcher/data/splink_model.json`: added the supplied `name_agreement` comparison levels and probabilities. The shifted-prefix lambda uses DuckDB's supported `lambda i: ...` syntax. The final implementation uses projected `name_words_l/r` references; the three different-or-missing-postcode levels also handle two NULL postcodes correctly.
- `uk_address_matcher/linking_model/splink_model.py`: restored `_align_name_words_columns` after comparing inline and projected expressions. It lazily projects `list_slice(clean_full_address_tokens, 1, 6)` for each input that lacks `name_words`. No prepared-canonical rebuild or new prepared-data column is required.
- `tests/test_linker.py`: five comparison cases exercise the projection helper and cover shifted-prefix/local support, empty/NULL tokens, both-NULL postcodes, and the six-token boundary.

## Controls

Each pair used the same prepared canonical, dataset, `ExactMatchStage`,
`PeeledAddressStage`, Splink threshold `10.0`, and missing-UPRN filter. No
residential-only canonical filter was applied. The baseline has no
`name_agreement` comparison; the variant uses the transient token slice for
that comparison. For the pooled pair, the token derivation was also present in
the baseline run but unused by its model. Each dataset was run once per arm;
the pooled run retained 348,284 of 348,867 source rows after canonical-label
filtering.

## Review and Guarded Performance Check

A Claude Opus 5.5 review confirmed the model diff only appends `name_agreement`;
existing comparisons, blocking rules, and probabilities were not changed. The
review identified redundant storage: the original lazy projection could be
materialised on candidate records and included in the full canonical parquet
cache for chunked matching. Inline slices reuse an already-required token column,
so neither a new prepared column nor a compatibility helper is required.

Fresh-process Hackney checks used the same national prepared canonical, stages,
threshold, label filter, and one matching chunk. Each child process had an
independent 70-second limit that kills its process group. Neither hit the limit.
Persistence/charts were disabled; pipeline time includes loading and matching,
not the later prediction-hash audit. Peak RSS was captured before that audit
using macOS `resource.getrusage(RUSAGE_SELF).ru_maxrss` (bytes).

| Comparison implementation | Pipeline runtime | Pipeline peak RSS | Whole guarded process |
| --- | ---: | ---: | ---: |
| Projected six-token column | 38.52s | 7.54 GiB | 42.92s |
| Inline six-token slices | 39.09s | 7.67 GiB | 43.10s |

Both implementations returned 112,954 correct, 623 wrong, and 596 unmatched
records. The sorted stable-ID prediction hashes agree for all 1,542,400
predictions, rounding weights to eight decimal places. Before the postcode
correction, a direct SQL check also found identical gamma levels across 2,916
paired token/postcode/local-support cases for column versus inline expressions.

Inline evaluation is comfortably below 70 seconds, so there is no evidence here
that pregeneration is needed. It was 0.57s slower and approximately 137 MiB higher
in peak RSS in this pair; these single-run differences do not establish an
overhead or a memory saving. System load varied during both measurements. The
earlier pooled runtime drop cannot be attributed to this feature from the
available timing evidence.

The both-NULL-postcode correction is a separate behavioural fix: previously even
strong names fell through to the weak-name ELSE level. The historical headline
table predates this correction; the guarded Hackney checks include it and retain
the same counts. Mid Sussex has not been rerun with the correction; the pooled
rerun below includes it.

## Final Projected-Column Pooled Rerun

The final choice is the projected-column implementation for its shorter, clearer
comparison SQL. The helper is restored, and the NULL-postcode correction is
retained. The possible duplicate array in chunked canonical caches remains a
known trade-off; this rerun uses one matching chunk and does not measure it.

Run `bff28e66555c7907` uses the normal benchmark entrypoint with only the dataset
selection and baseline ID overridden in memory. The tracked runner still selects
Hackney, Mid Sussex, and pooled councils. Canonical, stages, filters, threshold,
and chunk settings are unchanged. Results are compared with the original pooled
baseline `4c71a5ae2e2dbdf6`.

| Measurement | Final projected rerun |
| --- | ---: |
| Eligible rows | 348,284 |
| Data load | 0.66s |
| Match pipeline | 94.92s |
| Splink stage | 85.12s |
| Total benchmark runtime | 95.92s |
| Including persistence and reporting | 98.69s |
| Peak RSS before persistence | 9.94 GiB |
| Correct / wrong / unmatched | 341,468 / 3,592 / 3,224 |
| PR-AUC | 0.312879 |

Counts and headline quality metrics match the earlier variant. The overlay still
has 300 positive, 93 negative, and 1 unchanged false-positive reductions at 394
aligned recall points. Runtime is 20.85s above the earlier variant's 75.06s and
32.08s below the original baseline's 128.00s. The system's one-minute load average
rose from 40.04 to 78.01 during the rerun, so these runs cannot isolate the
implementation's timing effect from resource contention.

## Paired Comparison-On and Comparison-Off Timing

To isolate the added comparison, fresh sequential processes ran pooled councils
without `name_agreement` (`65aa533e0aefb8d1`) and with the current projected-column
comparison (`e9e817eebe8894fa`). The standard persisted benchmark entrypoint was
used. Only that comparison was removed in memory for the first arm; the helper,
canonical, remaining model, stages, filters, threshold, and chunk settings were
unchanged. No production scoring changes were made for this measurement.

| Measurement | Without name comparison | With current name comparison | Difference |
| --- | ---: | ---: | ---: |
| Total benchmark runtime | 58.25s | 62.31s | +4.06s |
| Splink stage | 47.85s | 51.87s | +4.02s |
| Process CPU before persistence | 481.06s | 537.07s | +56.01s |
| Peak RSS before persistence | 12.49 GiB | 13.76 GiB | +1.27 GiB |
| Correct | 341,561 | 341,468 | -93 |
| Wrong | 3,772 | 3,592 | -180 |
| Unmatched | 2,951 | 3,224 | +273 |

The observed total-runtime increase is about 7%; 95.92s is not a reproducible
fixed runtime for this dataset. CPU time is summed across threads, not elapsed
wall time. These are one measurement per arm, not a statistically controlled
overhead estimate: one-minute load average rose from 2.18 to 13.04 during the
first arm and from 13.04 to 25.82 during the second, on a 14-logical-CPU machine.

The with-name PR-AUC is 0.312879. Its overlay against the new without-name run
reproduces the earlier mixed-but-mostly-positive pattern: 300 positive, 93
negative, and 1 unchanged false-positive reductions at 394 aligned recall points.

The requested string-only follow-up should remove both explicit postcode guards
and postcode-derived local-key support from this comparison, reducing it to four
scored name-strength bands plus the null level. Each band currently has three
different conditional weights; an unconditional weight cannot be recovered by
simply deleting guards or averaging those values. That scoring decision remains
open; the timings above measure the existing contextual model, not an uncalibrated
string-only replacement. Other postcode comparisons and blocking remain intact.

## Proposed-Level Runtime and Necessity Analysis

Question: Which of the 12 scored `name_agreement` branches consume the comparison
runtime, how often does ordered CASE evaluation reach each branch, and which
branches appear worth a later quality ablation? The current comparison uses four
name-strength bands (strong, shifted-prefix, partial, weak), each split into
three postcode/local-key contexts, plus the no-name null level.

Plan:

1. Use the same pooled-councils source, prepared canonical, deterministic stages,
	filters, threshold, and comparison settings as the paired runs above. Do not
	change or recalibrate model weights in this timing pass.
2. Capture the actual blocked-pair relation immediately before Splink
	`predict(threshold_match_weight=-20)`. Do not substitute accepted matches or
	the predictions table: both omit blocked pairs below that threshold.
3. Join blocked IDs to only the input fields required by the 12 predicates. Keep
	the profiling relation in memory or ignored scratch storage; export aggregate
	counts and timings only, never address values.
4. Record the count of candidate pairs and the ordered residual/hit count for
	each branch. Then time each exact branch predicate independently over the
	same deterministic candidate sample, including a scan-only control; warm up
	once, repeat five times, and report median wall time and time per 100,000
	candidate pairs. Time the complete ordered CASE separately.
5. Report the 12 branch timings and hit counts individually, then by the four
	string-strength bands. Standalone predicate times are diagnostic rankings,
	not additive shares of full CASE time, because ordered evaluation can skip
	later branches and the optimiser may reuse expression work.
6. Use branch reach/hit counts to identify candidates for follow-up ablation,
	not as proof that a branch is unnecessary. Any quality ablation must preserve
	the comparison-on/off input and report precision, recall, F1, PR-AUC, wrong,
	and unmatched counts at the same threshold.

The timing pass also separates the four postcode-independent string-strength
predicates from the postcode and local-key context checks. It does not choose
replacement m/u weights; that remains a separate scoring decision because each
current string band has three context-specific likelihood ratios.

### Run and Controls

The guarded pooled-councils run retained 348,284 of 348,867 labelled source
rows and captured the blocked-pair relation before `predict(threshold_match_weight=-20)`.
That relation contained 6,881,264 rows. A deterministic hash sample of 100,000
distinct pairs was profiled; all 100,000 joined to their input features. Each
query had one warm-up followed by five repetitions; values below are median
wall time in milliseconds per 100,000 pairs. No prediction threshold, model
weight, or production comparison was changed.

### Branch Results

Standalone predicate timings evaluate each full explicit level over the whole
sample. Ordered reach is the number of rows still entering that level after
earlier CASE branches; ordered hits are its first-match count.

| Level | Branch | Standalone hits | Standalone ms/100k | Ordered reach | Ordered hits |
| ---: | --- | ---: | ---: | ---: | ---: |
| 0 | No usable name words (null level) | 0 | 8.49 | 100,000 | 0 |
| 1 | Strong; local-name support | 57 | 145.25 | 100,000 | 57 |
| 2 | Strong; same postcode, no local-name support | 926 | 145.54 | 99,943 | 926 |
| 3 | Strong; different or missing postcode | 341 | 145.43 | 99,017 | 341 |
| 4 | Shifted prefix; local-name support | 236 | 95.50 | 98,676 | 180 |
| 5 | Shifted prefix; same postcode, no local-name support | 12,011 | 96.09 | 98,496 | 11,256 |
| 6 | Shifted prefix; different or missing postcode | 8,062 | 96.23 | 87,240 | 7,724 |
| 7 | Partial; local-name support | 348 | 44.01 | 79,516 | 167 |
| 8 | Partial; same postcode, no local-name support | 13,183 | 44.58 | 79,349 | 10,606 |
| 9 | Partial; different or missing postcode | 11,825 | 43.96 | 68,743 | 6,698 |
| 10 | Weak; local-name support | 474 | 1.57 | 62,045 | 70 |
| 11 | Weak; same postcode, no local-name support | 46,549 | 1.77 | 61,975 | 23,761 |
| 12 | Weak; different or missing postcode (`ELSE`) | n/a | n/a | 38,214 | 38,214 |

The `ELSE` level has no standalone SQL predicate; its hit count is the residual
after the preceding levels. The null level matched no rows in this sample.

### String and Context Costs

The string-only timings exclude all postcode and local-key checks. The context
timings evaluate each unique context gate independently of name similarity.

| String predicate | Standalone hits | Median ms/100k |
| --- | ---: | ---: |
| Exact or strong fuzzy | 1,324 | 143.83 |
| Shifted prefix | 20,309 | 94.68 |
| Partial | 25,356 | 43.05 |
| Weak (`TRUE`) | 100,000 | 0.23 |

| Context predicate | Standalone hits | Median ms/100k |
| --- | ---: | ---: |
| Local-name support | 474 | 1.49 |
| Same postcode, no local-name support | 46,549 | 1.82 |
| Different or missing postcode | 52,977 | 1.65 |

The scan-only control took 10.00 ms/100k; the complete ordered Splink CASE took
459.29 ms/100k. Standalone costs are not additive: each explicit predicate is
evaluated over the whole sample, while the ordered CASE short-circuits and the
engine may reuse expression work.

### Interpretation

String similarity dominates the isolated context cost: strong-name evaluation
took 143.83 ms/100k, shifted-prefix 94.68 ms/100k, and partial agreement 43.05
ms/100k, versus 1.49-1.82 ms/100k for the three context gates. The weak-name
string predicate is `TRUE` and negligible. This points to string comparison,
not postcode checks, as the main per-predicate cost in this sample.

Every scored branch received at least 57 first-match rows; the rare local-support
branches are low-frequency, not unreachable. The sample therefore does not
justify removing any level on frequency alone. The null level's zero hits here
also do not make it redundant for empty or null token inputs. Use these counts
to prioritize a separate quality ablation, preserving the existing comparison
weights unless that calibration is explicitly requested.

The process peak RSS at capture was 7,102,988,288 bytes (about 6.62 GiB), which
includes pipeline setup and profiling and is not an incremental comparison
memory estimate. The 100,000-pair sample stayed in in-memory DuckDB temporary
tables; only the 15,969-byte aggregate receipt at
`.ai_scratch/name_level_profile.json` was written, with no address or pair-ID
rows. No persistent comparison storage was added.

## Interpretation

- Hackney: precision rose `0.0452` percentage points while recall fell `0.0526` points; PR-AUC fell `0.000468`. The overlay is modestly above baseline at high recall: precision gains are about `0.007` to `0.045` points around recall `0.75` to `0.98`, and 369 of 395 recall-aligned points reduce false positives.
- Mid Sussex: precision rose `0.5122` points while recall fell `0.7470` points; PR-AUC fell `0.004945` and runtime increased `2.92s`. The overlay is mixed: 320 of 379 recall-aligned points reduce false positives, 43 increase them, and 16 are unchanged. At the variant's maximum recall (`0.744`), precision is `0.242` points higher, but overall ranking quality is lower.
- Pooled councils: wrong matches fell by `180` and unmatched rows rose by `273`; precision improved `0.0513` points and F1 `0.0120` points, while recall fell `0.0267` points and PR-AUC fell `0.000244`. Runtime was `52.94s` lower in this single run. The overlay is mixed but mostly favorable at aligned recall: 300 of 394 points reduce false positives, 93 increase them, and 1 is unchanged.
- Across the three slices, the feature is more conservative rather than a broad ranking improvement: wrong matches fall, but unmatched rows rise and PR-AUC is slightly lower in every slice. Treat the runtime results as indicative because each arm ran once; do not promote it as a default without further calibration.

## Limitations

The prepared canonical was built with `uk_address_matcher v2.0.0.dev1`; both
benchmark arms ran with `v2.0.0.dev2`. The runs are internally comparable, but
the version warning remains. The worktree is based on local `build/ukam_2.0`
commit `0c5ba7d`, which was 43 commits behind `origin/build/ukam_2.0` when
created. No per-record transition matrix or changed-address review was
generated; the benchmark artifacts contain aggregate metrics only.

## Artifacts

- [Hackney comparison report](../../benchmarking/results/hackney/2026-11-12/4eb363646d89bcbf/comparison_report_c1b4b6c577a8cb29_vs_4eb363646d89bcbf.md)
- [Hackney precision-recall overlay](../../benchmarking/results/hackney/2026-11-12/4eb363646d89bcbf/charts/precision_recall_overlay_c1b4b6c577a8cb29_vs_4eb363646d89bcbf.html)
- [Mid Sussex comparison report](../../benchmarking/results/mid_sussex/2026-11-12/4eb363646d89bcbf/comparison_report_c1b4b6c577a8cb29_vs_4eb363646d89bcbf.md)
- [Mid Sussex precision-recall overlay](../../benchmarking/results/mid_sussex/2026-11-12/4eb363646d89bcbf/charts/precision_recall_overlay_c1b4b6c577a8cb29_vs_4eb363646d89bcbf.html)
- [Pooled councils comparison report](../../benchmarking/results/pooled_councils/2026-11-12/84c833725539d17a/comparison_report_4c71a5ae2e2dbdf6_vs_84c833725539d17a.md)
- [Pooled councils precision-recall overlay](../../benchmarking/results/pooled_councils/2026-11-12/84c833725539d17a/charts/precision_recall_overlay_4c71a5ae2e2dbdf6_vs_84c833725539d17a.html)
- [Final projected pooled precision-recall overlay](../../benchmarking/results/pooled_councils/2026-11-12/bff28e66555c7907/charts/precision_recall_overlay_4c71a5ae2e2dbdf6_vs_bff28e66555c7907.html)
- [Final projected pooled comparison report](../../benchmarking/results/pooled_councils/2026-11-12/bff28e66555c7907/comparison_report_4c71a5ae2e2dbdf6_vs_bff28e66555c7907.md)
- [Final projected pooled runtime and RSS receipt](../../benchmarking/results/pooled_councils/2026-11-12/bff28e66555c7907/projected_column_runtime.json)
- [Paired on/off precision-recall overlay](../../benchmarking/results/pooled_councils/2026-11-12/e9e817eebe8894fa/charts/precision_recall_overlay_65aa533e0aefb8d1_vs_e9e817eebe8894fa.html)
- [Paired on/off comparison report](../../benchmarking/results/pooled_councils/2026-11-12/e9e817eebe8894fa/comparison_report_65aa533e0aefb8d1_vs_e9e817eebe8894fa.md)
- [Without-name timing receipt](../../benchmarking/results/pooled_councils/2026-11-12/65aa533e0aefb8d1/paired_name_runtime.json)
- [With-name timing receipt](../../benchmarking/results/pooled_councils/2026-11-12/e9e817eebe8894fa/paired_name_runtime.json)

Validation: `uv run --frozen pytest tests/test_linker.py -q --tb=short` passed
(38 tests after review). Guarded performance receipts and exploratory probes are
local ignored files under `.ai_scratch/`, not committed address data.