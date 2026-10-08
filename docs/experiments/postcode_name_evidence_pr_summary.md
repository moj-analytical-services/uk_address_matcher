# Postcode-Wide Name Evidence and Guarded Spacing

## Overview

Adds default postcode-wide evidence for property names without house numbers.
Cleaning caches word and phrase rarity across all canonical properties; Splink
uses it to retrieve and score eligible names, including guarded joined/split
spacing, while preserving literal anchors. Source evidence is computed once in a
narrow temporary lookup, rather than repeated in prediction and reranker rows.
No new arguments or feature switches.

## Final Verification

Three serial fresh-process pairs on 348,687 pooled council sources show
**440 more correct matches and 124 fewer wrong matches in every pair**, at the
unchanged match-weight threshold 10. Median matching time is
**51.888 -> 53.659 s: +1.771 s (+3.41%)**. Paired peak-RSS changes range from
**-0.824 to +0.402 GiB**; no national RSS increase or fixed memory saving is claimed.
All six runs report zero post-stage spill.

The version-4 cache adds **12.27 MiB** to the matched canonical Parquet export,
down from the original 48.36 MiB. Postcode-validation metadata is stored once per
postcode, and name maps are not copied into default prediction/reranker rows.
The final focused suite has **226 passing tests**, with exact real-data subset
parity. Historical measurements and their separate scopes are retained below.

## Worked Example

All addresses and identifiers below are fictional. Suppose postcode `ZZ1 1ZZ`
contains four distinct properties:

| Alias | Property | Canonical address |
| --- | --- | --- |
| A1 | 001 | MEADOW COTTAGE 12 TEST ROAD |
| B1 | 002 | MEADOW HOUSE 14 TEST ROAD |
| C1 | 003 | ROSEBANK 16 TEST ROAD |
| D1 | 004 | LAUREL GROVE 18 TEST ROAD |

For source `MEADOW COTTAGE`, extraction retains `MEADOW` at position 1,
`COTTAGE` at position 2 and adjacent phrase `MEADOW COTTAGE` at position 1.
Canonical rarity counts distinct properties, not aliases, including numbered
addresses: `MEADOW` occurs at 2/4 properties; `MEADOW COTTAGE` at 1/4.

| Source | Derived evidence | Name contribution |
| --- | --- | --- |
| MEADOW COTTAGE | Alias levels `{A1: 6, B1: 3}`; literal anchor `001` | A1: BF16, +4 weight bits. B1: BF0.25, -2 bits, because the anchor identifies a different property |
| ROSE BANK | Alias level `{C1: 2}`; no literal anchor | C1: BF2, +1 bit, because phrase `ROSE BANK` and word `ROSEBANK` have the same unique compact key |

Only the strongest literal evidence per alias is used; word and phrase rewards
are not added together. These are contributions to the full model, not automatic
acceptances.

Precision guards:
- If another property has canonical address `22 ROSE BANK`, compact key
  `ROSEBANK` is no longer unique. Spacing support for C1 is suppressed even
  though that competing address is numbered.
- Sources must be digit-free, unresolved and known not to be flats/business
  units. Literal canonical keys must start within the first three tokens;
  spacing keys must start within the first three tokens on both sides and have
  at least six characters after removing spaces.
- Literal rarity requires at least two properties, frequency at most three and
  frequency/population at most 0.5. A singleton literal anchor suppresses
  spacing to other properties. Within its property, spacing alone cannot
  displace a better literal alias; the guard uses the actual model contribution.

## Real-World Results

### Literal-Name Evidence Against Main

Historical paired runs `local-keys-main-control-20261006` and
`local-keys-main-leading-20261006`, at unchanged match-weight threshold 10.
Both use the same residential canonical population: 1,058,228 aliases and
366,023 properties, with stable source-row IDs and unchanged non-target settings.
Cells show **control -> name evidence**.

| Dataset | Source rows | Correct | Wrong | Precision | Recall |
| --- | ---: | --- | --- | --- | --- |
| Hackney | 114,166 | 112,863 -> 112,863 | 601 -> 601 | 99.4703% -> 99.4703% | 98.8587% -> 98.8587% |
| Rhondda | 111,552 | 108,795 -> 108,917 | 1,130 -> 1,133 | 98.9720% -> 98.9705% | 97.5285% -> 97.6379% |
| Aberdeenshire | 122,969 | 118,555 -> 118,862 | 1,686 -> 1,559 | 98.5978% -> 98.7054% | 96.4105% -> 96.6601% |
| Pooled councils | 348,687 | 340,213 -> 340,644 | 3,417 -> 3,291 | 99.0056% -> 99.0431% | 97.5697% -> 97.6933% |

The separately executed pooled run gains **431 correct matches and removes
126 wrong matches**: precision +0.0375 percentage points, recall +0.1236 points.
It is not a sum of council outcomes or an untouched holdout. Rhondda gains 122
correct matches but adds three wrong matches; Hackney has no MW10 change.

| Dataset | Exact covered-residual PR-AUC | Exact numericless PR-AUC |
| --- | --- | --- |
| Hackney | 0.989842 -> 0.989857 | 0.809003 -> 0.842956 |
| Rhondda | 0.953648 -> 0.956990 | 0.747492 -> 0.795653 |
| Aberdeenshire | 0.878912 -> 0.908488 | 0.801783 -> 0.857487 |
| Pooled councils | 0.960805 -> 0.966781 | 0.790962 -> 0.845199 |

These are trapezoidal areas, not average precision. Repository overall pooled
PR-AUC separately rises from 0.346017 to 0.347258; its scope differs because the
exported curve includes fixed-stage matches.

Primary charts and supporting reports:
- [Pooled precision-recall overlay](../../benchmarking/results/pooled_councils/2026-10-06/local-keys-main-leading-20261006/charts/precision_recall_overlay_local-keys-main-control-20261006_vs_local-keys-main-leading-20261006.html)
- [Hackney overlay](../../benchmarking/results/hackney/2026-10-06/local-keys-main-leading-20261006/charts/precision_recall_overlay_local-keys-main-control-20261006_vs_local-keys-main-leading-20261006.html)
- [Rhondda overlay](../../benchmarking/results/rhondda/2026-10-06/local-keys-main-leading-20261006/charts/precision_recall_overlay_local-keys-main-control-20261006_vs_local-keys-main-leading-20261006.html)
- [Aberdeenshire overlay](../../benchmarking/results/aberdeenshire/2026-10-06/local-keys-main-leading-20261006/charts/precision_recall_overlay_local-keys-main-control-20261006_vs_local-keys-main-leading-20261006.html)
- [Main-model comparison and follow-up measurements](local_keys_vs_main_2026-10-06.md)
- [Earlier frozen-multicouncil experiment](local_keys_2026-10-05.md)

The pooled curve improves mainly towards higher recall, not at every threshold;
some lower/intermediate bands add false positives. Aberdeenshire drives the
largest gain. Rhondda's broader residual frontier is mixed despite improved
numericless evidence. The historical table is literal-only evidence, not a fresh
four-dataset certification of the final spacing and simplification commits.
Later full-pipeline runs also show small fixed-stage assignment variations;
those are not credited to this feature.

### Guarded Spacing Evidence

The later Rhondda/Aberdeenshire pooled ablation contains 234,521 sources and
712,062 canonical aliases. `budget-C1-spacing-r1-20261007` versus
`budget-C1-literal-r1-20261007` adds **11 net correct covered-residual matches**
with **zero net increase in wrong matches**: 54,213 -> 54,224 correct and 1,640
wrong in both. Zero net increase does not mean no individual wrong transition.
There is no equivalent Hackney or separate-council spacing certification.

C1's evidence matches the current implementation exactly on all 60,343
unresolved sources. This supports the spacing mechanism, but is not additional
uplift over current production. The experimental prefix-cache implementation
failed its runtime gate and was not promoted; its separate prefix file is not
part of this PR's persisted cache.

- [Spacing overlay against literal-only control](../../benchmarking/results/numericless_budget_2026-10-07/overlay_C1.html)
- [Spacing experiment report and rejected optimisation gates](../../benchmarking/results/numericless_budget_2026-10-07/report.md)

## Runtime and Memory

| Measurement | Control | Feature/variant | Difference | Scope |
| --- | ---: | ---: | ---: | --- |
| Final simplified median matching pipeline | 51.888 s | 53.659 s | **+1.771 s, +3.41%** | 348,687 sources; three serial fresh-process pairs; version-4 sparse cache |
| Final simplified median process peak RSS | 10.954 GiB | 10.574 GiB | -0.380 GiB observed | Same final pairs; paired changes include +0.402 GiB, not a guaranteed saving |
| Earlier lookup-based median matching pipeline | 55.275 s | 56.901 s | +1.625 s, +2.94% | Before sparse layout and code simplification; three serial pairs |
| Initial full-feature median matching pipeline | 52.852 s | 67.881 s | **+15.029 s, +28.44%** | Before runtime lookup optimisation; 348,687 sources; three serial fresh-process pairs |
| Initial full-feature median process peak RSS | 12.096 GiB | 12.929 GiB | **+0.833 GiB, +6.88%** | Same initial pairs; high-water mark immediately after Splink, before reporting |
| Historical no-flag literal matching pipeline | 49.201 s | 51.011 s | +1.810 s, +3.68% | 348,687 sources; one pair; includes source cleaning, excludes later audits/persistence |
| Spacing ablation median matching pipeline | 35.152 s | 35.965 s | +0.813 s, +2.31% | 234,521 sources; three fresh-process pairs; experimental prefix-cache variant |
| Spacing ablation median process peak RSS | 10.36 GiB | 11.22 GiB | +0.86 GiB, +8.32% | Same three pairs; not the complete feature versus feature-free main |
| Compact-cache smoke timing | 3.787 s | 3.465 s | -0.322 s | 1,000 balanced sources / 54,675 aliases; one pair, previous full cache versus compact code |

### Final Lookup Implementation

The final implementation stops copying name maps/anchors into default prediction
and reranker outputs. It computes eligible source evidence once in a narrow
temporary lookup, then materialises only excluded alias-ID pairs for the literal
anchor guard. Scoring, spacing gates, model weights and thresholds are unchanged.
The simplified code uses one cache-validation query for normalised legacy and
sparse records, a direct relation join to attach source evidence, a two-CTE alias
guard and one owned-table cleanup prefix. No new abstractions or options.

Six fresh processes run serially in feature/control, control/feature,
feature/control order, on identical base canonical rows and auxiliary files.
The final runs use the version-4 sparse cache. Median overhead is
**1.771 s (+3.41%)**, versus the initial **15.029 s (+28.44%)**. These are
within-batch differences of medians, not an isolated cross-batch speedup claim.
Paired timing changes range from **-0.054 s to +1.861 s** (-0.10% to +3.59%);
the result is not a hard per-run latency cap.

All six runs report zero post-stage spill. Peak-RSS changes are **-0.824 to
+0.402 GiB**; the lower observed median does not establish a general memory
reduction. Name preparation takes 0.870 s and the alias guard 0.074 s at their
function-call medians. Reranking is 6.021 s control versus 6.121 s feature;
lazy work elsewhere can be attributed to later calls, so phase medians are not
an additive accounting of total overhead.

Every final pair gains **440 correct matches and removes 124 wrong matches**.
Feature results are 340,654-340,655 correct and 3,291-3,292 wrong;
the raw candidate population remains 3,032,149 -> 3,039,139 (+0.23%). Small
fixed-stage/tie variations remain. The latest overlay against historical main
is higher at sampled common recall bands 0.85-0.979, with larger false-positive
reductions towards higher recall; this is not a new untouched validation set.

- [Final pooled overlay against historical main](../../benchmarking/results/pooled_councils/2026-10-08/simplified-name-final-compact-r4-20261008/charts/precision_recall_overlay_local-keys-main-control-20261006_vs_simplified-name-final-compact-r4-20261008.html)
- [Final runtime report and reproduction command](../../benchmarking/results/pooled_performance_2026-10-08/simplified_final/report.md)
- [Final six-run aggregate measurements](../../benchmarking/results/pooled_performance_2026-10-08/simplified_final/summary.json)
- [Earlier lookup-based comparison](../../benchmarking/results/pooled_performance_2026-10-08/lean_predictions_cached_sources/report.md)

### Earlier Measurements

The initial full-feature comparison uses the same 1,058,228 canonical aliases,
366,023 properties, auxiliary files, residential filter, stable source-row IDs,
MW10 and non-name settings. Its control omits the name cache, source-name
extraction, name retrieval/scoring and retained name maps. The saved non-name
settings were checked against current full-precision settings, allowing only
their historical numeric rounding. Run order is control/feature, feature/control,
control/feature, with no overlapping benchmark children. Matching time includes
canonical loading/initialisation, source cleaning, matching and final-match
materialisation; both variants include the same canonical-population count check.
It excludes cache construction and later accuracy, audit, persistence and charts.

The initial full feature was materially more expensive than the historical
**+3.68% literal-only** estimate. Paired slowdowns are **24.63%-37.00%**
(13.42-19.27 s); the table differences are between variant medians, not medians
of paired differences. Peak-RSS paired changes range from approximately zero
(-0.0006 GiB) to +1.522 GiB, so +0.833 GiB is not a guaranteed memory bound.
Initial matching takes 67.881 s versus the old literal run's 51.011 s, but that
cross-date difference also includes host/code effects: today's control takes
52.852 s rather than the historical 49.201 s.

All three pairs gain **439-440 correct matches and remove 123-124 wrong matches**.
Initial results are 340,654-340,655 correct and 3,291-3,292 wrong; fixed-stage/tie
assignment variation remains small. This measures the complete initial feature,
not the matching-time effect of compaction alone.

- [Full pooled performance report and reproduction command](../../benchmarking/results/pooled_performance_2026-10-08/report.md)
- [Six-run aggregate measurements](../../benchmarking/results/pooled_performance_2026-10-08/summary.json)
- [Current pooled overlay against historical main](../../benchmarking/results/pooled_councils/2026-10-08/compact-pooled-serial-compact-r3-20261008/charts/precision_recall_overlay_local-keys-main-control-20261006_vs_compact-pooled-serial-compact-r3-20261008.html)

The historical spacing RSS row measures an experimental implementation, not the
complete current feature. The subset timing remains a smoke measurement, not a
certified speedup.

The spacing experiment's median cold-wall growth was **2.221 s**, including
1.465 s incremental prefix load/materialisation; its worst paired cold growth
was 4.842 s, exceeding the requested 1 s cap. Individual RSS ratios ranged from
0.862 to 1.213. No spill was detected, but sampling cannot exclude brief spills.
These results are not a passing performance gate for a new extension.

The current version-4 canonical cache occupies **50,368,592 bytes (48.04 MiB)**
as DuckDB-exported Arrow logical buffers across 1,058,228 aliases. The preceding
version-3 cache occupied 71,340,765 bytes (68.04 MiB), and the original full cache
745,374,429 bytes (710.84 MiB). The latest change removes another **29.40%** of
the version-3 logical footprint. This is neither compressed disk size nor process
peak RSS. Single cache builds from already cleaned rows took **2.80 s** for
version 4 and 3.10 s for version 3, excluding raw cleaning, loading, sorting,
writes and comparisons; these are not controlled repeated preparation timings.

Version 4 stores one postcode-validation record per postcode, not per alias:
**18,746 records instead of 1,058,228** in this population. It keeps the same
column name but uses a typed list containing one record on the lowest alias ID
and empty lists elsewhere. A missing or duplicate carrier, changed text or a
filtered population invalidates the cache. A nullable fixed-width struct would
still carry per-row child buffers in Arrow, so merely replacing repeated values
with NULL would not remove this expanded-memory duplication.

Reading both Parquet exports with the same Arrow reader confirms **81.73% less
background metadata** and **29.67% less total cache payload**. All columns other
than background representation and cache version are identical, including the
complete matching index. Linear scaling to the national file's 71,438,939 aliases
would change the expanded cache estimate from about **4.44 to 3.13 GiB**; this is
not measured national RSS and does not account for differing address composition.
The final +1.771 s pooled timing includes the sparse layout and simplified code.

**Remaining limits:** both comparisons cover their working-tree code on one
host, not cold-disk behaviour, repeated canonical rebuilding or national
scale. A full pooled compact-versus-previous-full-cache timing comparison has not
been recorded. No general matching speedup or national-scale memory bound is
claimed; the final implementation reduces, rather than eliminates, matching
overhead. Cache construction is excluded from both full-pipeline comparisons.

## Persisted Storage

Fresh measurement of the **current** six-column cache on 1,058,228 aliases /
366,023 properties. Both files contain identical base rows and stored columns,
sorted by alias ID, using ZSTD and 122,880-row groups. Auxiliary canonical files
are unchanged. Non-cache row equality was checked before export.

| Canonical address Parquet | Bytes | MiB |
| --- | ---: | ---: |
| Without name cache | 46,836,992 | 44.67 |
| With original full name cache | 97,542,335 | 93.02 |
| With version-3 compact name cache | 62,267,364 | 59.38 |
| With current version-4 sparse background | 59,707,303 | 56.94 |
| Current additional storage | **12,870,311** | **12.27** |

That is **27.48% growth for the address file**, or **12.16 bytes per canonical
alias**, not 27.48% growth for the complete prepared folder. The latest change
removes **16.59% of version 3's added disk overhead**. The original full cache
added 48.36 MiB / 108.26%. This is a controlled re-export, not a comparison of
unrelated historical file layouts.

The increase is not one column: `local_key_index` contributes 3,926,272 compressed
column-chunk bytes (3.74 MiB), token hash 8,466,391 (8.07 MiB), and postcode
background 288,737 (0.28 MiB), down from 2,592,536 bytes. Duplicate canonical `local_key_tokens` are no
longer persisted; source token extraction remains unchanged. Two
version columns and the extraction-mode flag contribute another 1,629 bytes.
Column-chunk totals exclude Parquet metadata, so they do not equal whole-file
growth exactly. Runtime `lk_level_by_alias`, anchors and eligibility are temporary
matching features, not additional columns in the prepared canonical files. The
source lookup materialises only eligible alias IDs, maps and anchors once.
Default predictions and reranker outputs do not retain these maps; the alias
guard caches only excluded canonical/source alias-ID pairs. Stage-owned lookup
tables and input views are released after matching.

- [Sparse-background report](../../benchmarking/results/postcode_name_pr_2026-10-08/sparse_background/report.md)
- [Current storage measurements](../../benchmarking/results/postcode_name_pr_2026-10-08/sparse_background/storage.json)
- [Matched-reader payload comparison](../../benchmarking/results/postcode_name_pr_2026-10-08/sparse_background/comparison.json)
- [Previous version-3 storage measurements](../../benchmarking/results/postcode_name_pr_2026-10-08/compact/storage.json)
- [Original full-cache storage measurement](../../benchmarking/results/postcode_name_pr_2026-10-08/storage.json)
- [Private storage measurement script](../../benchmarking/results/postcode_name_pr_2026-10-08/measure_storage.py)

## Validation and Caveats

- 226 tests pass across local keys, linker, canonical preparation, address matcher
  and full examples; lint, formatting and editor diagnostics are clean.
- The final real-data simplification check has zero evidence, raw pair/score or
  final-match differences across 1,000 sources and all 14,933 raw pairs.
  [Subset report](../../benchmarking/results/local_key_simplification_2026-10-08/subset_comparison.json).
  Raw comparisons use supplied source-row IDs and canonical alias IDs;
  automatically assigned source alias IDs can vary with execution order.
- Canonical statistics use the complete supplied postcode population, including
  numbered properties and all aliases. Filtered/changed populations invalidate
  the cache. Version-4 canonical caches retain only early evidential keys after
  calculating full-population statistics, with validation metadata stored once
  per postcode. Valid version-2/3 caches with spacing statistics upgrade without
  recomputing frequencies; stale, older or literal-only schemas rebuild.
  No new user arguments or feature switches are introduced.
- Weights are heuristic, not newly calibrated probabilities. Results are not an
  untouched validation set, and urban transfer is not established by Hackney.
- The full suite was not rerun for these changes. An earlier full-suite run
  records an unrelated flat-number/letter Bayes-factor failure.

Benchmark artefacts are local and gitignored. Their HTML overlays and aggregate
reports must be shared separately to make these evidence links available to PR
reviewers; do not upload private record-level address data.