# ART-index blocking for realtime linkage

Status: validated experiment (2026-06-15) · Owner: address-matching · Reproducible end-to-end

This document records an experiment that replaces the most expensive part of the
stock Splink matching path — the `__splink__df_concat_with_tf_filtered` scan —
with cheap, index-driven candidate generation backed by a DuckDB ART (Adaptive
Radix Tree) index, **without changing a single match weight**. It explains the
problem, the design, the precise stages it supersedes, the measured results at
both small (Hackney residential, 346K) and full (71M) canonical scale, how to
reproduce everything, the failure modes we hit (and fixed), and a recommended
path towards a more efficient realtime pipeline.

---

## 1. TL;DR / recommendations

- **The blocking step is essentially free when DuckDB really uses the ART
  index.** On the full 71M-row canonical a single-record candidate lookup took
  **~10.5 ms** versus a stock `predict()` fixed cost of **~10.6–11.7 s**.
- **The recommended realtime default for the inverted-index-only path is now the
  prefiltered ART-scored path.** On the full canonical it runs in
  **0.024 s / 0.045 s / 0.487 s** for row limits **1 / 10 / 100**, i.e.
  **445x / 251x / 24x** faster than stock `predict()`, with **identical match
  weights** (`max |Δ| = 0.00e+00`) on all shared pairs.
- **The recommended faithful realtime default is the prefiltered full-block
  path.** It reproduces **all** blocking rules, keeps **100% coverage** of
  baseline prediction pairs, preserves **identical match weights**, and ran in
  **0.039 s / 0.077 s / 3.396 s** at row limits **1 / 10 / 100**
  (**270x / 145x / 3.4x** faster than stock).
- **A stricter “reduced canonical + stock `predict()`” path is valid, but not
  the latency winner.** Reproducing all blocking rules, materialising the
  blocked canonical subset, rebuilding a fresh linker, and then calling normal
  Splink `predict()` remained behaviourally lossless and ran in
  **0.897 s / 0.914 s / 4.350 s** at row limits **1 / 10 / 100**
  (**12.8x / 13.4x / 2.6x** faster than stock), but it is still materially
  slower than the prefiltered full-block scorer because it pays linker rebuild
  and residual stock-`predict()` overhead on every request.
- **`EXPLAIN ANALYZE` shows a real planner flip in the inverted-index lookup.**
  At row-limit 1 DuckDB used an index scan; at row-limits 10 and 100 it fell
  back to a sequential scan over ~57–61M rows. The candidate-generation path is
  therefore not yet at its floor for multi-row batches.
- **Recommended direction for a realtime pipeline:**
  1. Persist an ART-indexed canonical database once (build cost ≈ 100 s for the
     71M canonical materialise + ≈ 35 s for the inverted indexes + a one-off
     **~10.8 s** `canonical_ukam_lookup` build on existing databases upgraded to
     the new prefiltered full-block path).
  2. At query time, reproduce all blocking rules directly against the indexed
     canonical (no `concat_with_tf`) and union the ART inverted-index candidates.
  3. Prefilter the canonical to the candidate ids before running Splink's stock
     comparison/predict SQL.
- **Next efficiency frontier:** the remaining cost has moved. For the
  inverted-index-only path it is now the planner fallback from index probes to a
  sequential scan at 10+ rows. For the faithful full-block path, the remaining
  cost at larger batches is mostly blocking / candidate assembly, not scoring.

---

## 1.1 Findings summary

This section consolidates the main findings and the recommended decisions so a
reader does not need to reconstruct them from the detailed tables later.

### Core findings

| Finding | Evidence | Practical meaning |
| --- | --- | --- |
| Stock Splink small-batch latency is dominated by a fixed canonical-side cost. | Stock `predict()` stayed at about 10.6–11.7 s across row limits 1/10/100 on the full 71M canonical. | Realtime latency will not improve meaningfully unless the canonical-side query shape changes. |
| Blocking rules do not require TF columns. | The blocking rules are all expressed on raw columns such as `postcode`, `numeric_token_*`, token arrays, and `exploding_unique_ids`. | Blocking can be regenerated directly against the raw indexed canonical without materialising `__splink__df_concat_with_tf_filtered`. |
| The old ~2.5 s residual was mostly canonical rescoring against the full `c_` view. | Legacy ART-scored path stayed around 2.43–2.53 s even after candidate generation had been made cheap. | Prefiltering the canonical before scoring is the key change that unlocks the large latency drop. |
| Canonical prefiltering works and is lossless. | Prefiltered ART-scored path ran in 0.0239 s / 0.0445 s / 0.4870 s with `max |Δ match_weight| = 0.00e+00`. | The recommended inverted-index-only path is now fast enough for realtime use while preserving exact scores on shared pairs. |
| Full-block prefiltering works and remains faithful. | Prefiltered full-block path ran in 0.0394 s / 0.0767 s / 3.3959 s with 100% shared-pair coverage and `max |Δ| = 0.00e+00`. | The recommended faithful drop-in path now has sub-100 ms latency for 1–10 row batches and remains materially faster at 100 rows. |
| Reduced-canonical stock `predict()` is viable but leaves performance on the table. | Strict reduced-canonical stock `predict()` ran in 0.8972 s / 0.9140 s / 4.3498 s with 100% shared-pair coverage and `max |Δ| = 0.00e+00`. | This is a valid lower-intrusion optimisation if keeping the normal Splink `predict()` call matters more than absolute latency, but it is not the best realtime path. |
| Tiny reduced-canonical linker setup has a real fixed floor, but it is not explained by accidental table copying. | Follow-up probes on canonical subsets of 100 / 1,000 / 5,000 / 10,000 rows kept `_get_linker()` at ~0.39–0.43 s; a manual breakdown attributed about ~0.27 s to Splink `Linker(...)` initialisation itself. | Some residual cost is simply Splink startup overhead; optimising around it helps only at the margins unless the full-canonical query shape is removed. |
| Materialising the tiny reduced canonical helps slightly; materialising the full canonical hurts badly. | A 47-row filtered canonical improved from 0.400 s -> 0.278 s linker build and 0.632 s -> 0.560 s `predict()` when materialised, but full Hackney vs full 71M canonical degraded from 14.922 s stock `predict()` to 34.174 s after materialising the full canonical table first. | Materialisation is worth promoting only for the tiny candidate-prefiltered subset, not for the full canonical fed back into stock Splink. |
| The candidate lookup itself still has a planner problem. | `EXPLAIN ANALYZE` showed an index scan at row-limit 1, but sequential scans over ~56.9M / ~61.3M rows at row-limits 10 / 100. | The inverted-index lookup is still not at its floor for multi-row batches; this is now the highest-leverage remaining optimisation in the inverted-index-only path. |
| Small-canonical behaviour differs from full-canonical behaviour. | On Hackney-scale runs the prefiltered variants can be only modestly better, or occasionally slightly worse, than the legacy paths because the full-canonical scan problem is absent. | The prefilter changes are justified by full-canonical realtime behaviour, not by small-canonical microbenchmarks alone. |

### Recommended decisions

1. Treat the **prefiltered ART-scored path** as the default for the
  inverted-index-only experiment when the goal is fastest possible realtime
  scoring over ART-generated candidates.
2. Treat the **prefiltered full-block path** as the default faithful realtime
  path when recall must match stock Splink blocking behaviour.
3. Treat the **reduced-canonical stock `predict()` path** as a valid fallback
  when lower implementation intrusion matters more than best-in-class latency.
4. Keep the legacy unfiltered scoring paths only as diagnostics. Their main
  value now is proving what cost the canonical prefilter removes.
5. Persist the full ART database as a deployment artefact, including the narrow
  `canonical_ukam_lookup` table, so the one-off build cost is amortised.
6. Focus the next round of optimisation work on the candidate lookup planner
  fallback, not on the Bayes-factor / match-weight computation. The scoring
  maths is no longer the dominant cost.

### What has been superseded

- The earlier conclusion that the best achievable end-to-end gain was only
  about **4–5x** is now outdated. That figure described the legacy unfiltered
  scoring paths, not the true floor of the ART approach.
- The weak plain-text `EXPLAIN` heuristic for index use is superseded by real
  `EXPLAIN ANALYZE` plan capture.
- For faithful realtime scoring, the non-prefiltered full-block path is no
  longer the recommended implementation; it has been replaced by the
  prefiltered full-block path.
- The idea that simply reducing the canonical and then calling stock Splink
  `predict()` would necessarily reach the same latency floor as the helper-based
  scorer is now superseded. The stricter path is faster than stock, but the
  rebuilt-linker and residual `predict()` costs remain significant.

---

## 2. The problem this supersedes

`uk_address_matcher` matches messy UK addresses against a canonical gazetteer
using Splink (`link_type = link_only`). For realtime / small-batch linkage the
dominant cost is **not** scoring — it is a fixed cost Splink pays on every
`predict()` call regardless of batch size.

In `two_dataset_link_only` mode Splink materialises
`__splink__df_concat_with_tf_filtered`, which scans the full cached
`__splink__df_concat_with_tf` table (all **71,438,939** canonical rows) to attach
term-frequency columns — even when only **one** messy record is being matched.

Measured fixed cost (from `findings/small_batch_matching_findings.md` and this
experiment):

| Batch size | Stock `predict()` total | Of which concat-with-tf scan |
| ---: | ---: | ---: |
| 1 messy row | ~11.5 s | dominant |
| 10 messy rows | ~11.4 s | ~3.35 s (full-canonical earlier probe) |
| 100 messy rows | ~11.8 s | dominant |

The cost is effectively **constant** in batch size — it is a property of the
canonical, not the query. For realtime linkage (one record, sub-second budget)
this is fatal.

**Key structural insight that unlocks the fix:** Splink's blocking rules
reference only *raw blocking columns* (`postcode`, `numeric_token_1/2`,
`unusual_tokens_arr`, `extremely_unusual_tokens_arr`, `split_part(postcode,...)`,
`exploding_unique_ids`). **They never reference TF columns.** Therefore the
entire blocking table can be regenerated directly against the raw, indexed
canonical — skipping the TF-concat scan completely — and TF columns are only
attached for the (small) set of candidate pairs during scoring.

### Stages superseded

| Stock Splink stage | Status in the ART path | Replacement |
| --- | --- | --- |
| `__splink__df_concat_with_tf` (full canonical materialise) | **eliminated at query time** | Raw indexed `canonical_addresses` table built once |
| `__splink__df_concat_with_tf_filtered` (per-query full scan) | **eliminated** | Blocking joins on raw columns + ART index lookups |
| Exploding `exploding_unique_ids` blocking rule (rule 9) | **replaced** | ART point lookups over a hashed bigram/trigram inverted index |
| 8 non-exploding blocking rules | **reproduced as-is** | `block_using_rules_sqls(...)` run directly against `c_`/`m_` raw tables |
| Comparison-vector + predict SQL | **unchanged** | Scored on the candidate set with distinct `l`/`r` inputs |

The match weights are therefore *provably identical* on every shared pair — the
only thing that changes is *how candidate pairs are produced*, not *how they are
scored*.

An additional 2026-06-15 experiment tested a stricter integration shape: build
the blocked canonical subset first, rebuild a fresh linker on that reduced
canonical, then call normal Splink `predict()`. That path is also behaviourally
lossless, but it retains more fixed overhead than the direct comparison/predict
SQL path used elsewhere in this document.

---

## 3. Design

### 3.1 Components

```
                       ┌─────────────────────────────────────────┐
                       │  Persisted ART database (built once)     │
                       │  ─ canonical_addresses (71M, raw cols)   │
                       │      + ART index on unique_id            │
                       │      + ART indexes on postcode,          │
                       │        numeric_token_1, numeric_token_2  │
                       │  ─ term_frequencies                      │
                       │  ─ inverted_index_string (key -> ids[])  │
                       │  ─ inverted_index_hashed                 │
                       │      hash(key) UBIGINT + ART index       │
                       └─────────────────────────────────────────┘
                                         │
     messy batch ──► clean ──► derive bigram/trigram keys ──► hash(key)
                                         │
                ┌────────────────────────┴────────────────────────┐
                │                                                  │
   (A) ART inverted-index lookup                    (B) non-exploding blocking rules
   point-lookup hash(key) -> unique_ids[]           block_using_rules_sqls against
   = "exploding rule" candidates                    raw indexed canonical (c_) x messy (m_)
                │                                                  │
                └──────────────► UNION ALL + GROUP BY join_key ◄──┘
                                         │
                          __splink__blocked_id_pairs
                                         │
              Splink comparison-vector + predict SQL (distinct l/r inputs)
              → __splink__df_predict  (identical match weights)
```

### 3.2 The scoring paths we benchmarked

- **Option 1 — prefiltered ART-scored path** (`run_art_scored_prefiltered_path`):
  inject *only* the ART inverted-index candidates as `__splink__blocked_id_pairs`,
  then prefilter the canonical with `WHERE unique_id IN (...)` before scoring.
  This reproduces only the exploding rule, so coverage of baseline predictions is
  partial (candidate recall gap), but it is now the **recommended realtime
  default** for the inverted-index-only experiment and proves weight fidelity.
- **Option 1 legacy — unfiltered ART-scored path** (`run_art_scored_path`):
  retained only as a diagnostic to show the old full-canonical scoring scan.
- **Option 3 — prefiltered full-block path** (`run_full_block_path(...,
  prefilter_canonical=True)`): reproduce **all** blocking rules (8
  non-exploding via Splink's own SQL + ART candidates for the exploding rule),
  union, de-duplicate, prefilter the canonical to the blocked ids, then score.
  This achieves **100% coverage** of baseline prediction pairs with identical
  weights — it is the **faithful realtime default**.
- **Option 3 legacy — unfiltered full-block path**
  (`run_full_block_path(..., prefilter_canonical=False)`): retained only to show
  the cost of rescoring against the full canonical after blocking has already
  been narrowed.
- **Option 4 — reduced-canonical stock `predict()` path**
  (`run_reduced_canonical_predict_path(...)`): reproduce all blocking rules,
  materialise the blocked canonical subset, rebuild a fresh linker on that
  subset, then call normal Splink `predict()`. This achieves **100% coverage**
  of baseline prediction pairs with identical weights, but it is slower than
  the prefiltered full-block scorer because it pays linker rebuild and residual
  stock-`predict()` overhead on every query.

(Option 2 was the original exploratory candidate-generation-only pass —
`run_art_path` — that produces candidates and assembles a comparison-ready table
but does *not* score. It is retained because its timing breakdown is what yields
the dramatic blocking-only ratios.)

### 3.3 Why distinct `l`/`r` inputs matter

Passing distinct projections for the two sides:

```sql
input_tablename_l = "(select *, 'c_' as source_dataset from c_)"   -- canonical
input_tablename_r = "(select *, 'm_' as source_dataset from m_)"   -- messy
```

makes Splink treat the run as a genuine two-dataset link and **skip**
`__splink__df_concat_with_tf_filtered` entirely. TF columns are attached only for
the candidate pairs, not the whole canonical.

### 3.4 Join-key format

Composite join keys use Splink's separator `-__-`:

```
<source_dataset>-__-<ukam_address_id>      e.g.  c_-__-12345   /   m_-__--7
```

The unique-id column is `ukam_address_id` (not `unique_id`). Messy rows are
assigned a **negative** `ukam_address_id` so they can never collide with the
positive canonical ids, and so each side is identifiable later by sign.

---

## 4. Results

The core ART and full-block numbers below are from the validated 2026-06-08
runs. The reduced-canonical stock-`predict()` path was added and validated on
2026-06-15. Match weights are identical (`max |Δ match_weight| = 0.00e+00`) on
every shared pair in every configuration.

### 4.1 Full unfiltered canonical — 71,438,939 rows

Build (one-off):

| Phase | Seconds |
| --- | ---: |
| materialise_canonical (71.4M rows + 4 ART indexes via incremental insert) | ~100 s |
| build_string_inverted_index (reused from prepared parquet, 61.5M keys) | ~4.5 s |
| build_hashed_inverted_index (61.5M rows + ART index via incremental insert) | ~30.8 s |
| build_ukam_lookup (one-off on existing databases upgraded to the new path) | ~10.8 s |

Per-query latency vs stock `predict()`:

| Row limit | ART candidate-gen | ART-scored prefiltered | ART-scored legacy | Full-block prefiltered | Reduced-canonical stock `predict()` | Full-block legacy | Stock `predict()` |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 0.0105 s (1016x) | 0.0239 s (445x) | 2.48 s (4.3x) | 0.0394 s (270x) | 0.8972 s (12.8x) | 2.52 s (4.2x) | 11.45 s |
| 10 | 0.0976 s (114x) | 0.0445 s (251x) | 2.43 s (4.6x) | 0.0767 s (145x) | 0.9140 s (13.4x) | 2.46 s (4.5x) | 12.26 s |
| 100 | 1.7169 s (6.8x) | 0.4870 s (23.9x) | 2.53 s (4.6x) | 3.3959 s (3.4x) | 4.3498 s (2.6x) | 5.43 s (2.1x) | 11.42 s |

`EXPLAIN ANALYZE` on the candidate lookup showed the planner behaviour directly:

| Row limit | Index scan | Sequential scan | Max reported rows |
| ---: | --- | --- | ---: |
| 1 | Yes | Yes | 9 |
| 10 | No | Yes | 56,887,704 |
| 100 | No | Yes | 61,312,633 |

That explains the timing shape: the inverted-index lookup is genuinely tiny at a
single row, but DuckDB flips to a large sequential scan once the probe side gets
slightly larger.

### 4.2 Hackney residential canonical — 346,168 rows

Here the stock baseline is already only ~1.0 s (small canonical), but ART still
dominates and the full-block path is a faithful drop-in:

| Row limit | ART candidate-gen | ART-scored (opt 1) | Full-block (opt 3) | Stock `predict()` | Coverage |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | ~0.016–0.023 s (43–62x) | ~0.045–0.10 s (10–23x) | ~0.21 s* | ~1.0 s | 100% (4/4) |
| 10 | ~0.016–0.023 s | ~0.045–0.10 s | ~0.08–0.13 s | ~1.0 s | 100% (26/26) |
| 100 | ~0.016–0.023 s | ~0.045–0.10 s | ~0.08–0.13 s | ~1.07 s | 100% (442/442) |

\* row-limit-1 full-block total includes a one-off ART index build on the anchor
columns the first time the database is touched; steady-state runs reuse them.

Artefact: `benchmarking/results/art_candidate_assembly_probe/2026-06-08/e0683f8a1be05610/summary.md`.

### 4.3 How to read the new speed-ups

There are now three distinct comparisons worth reading differently:

- **Blocking-only / candidate-generation** can exceed **1000x** for a single
  record because the stock fixed cost is paid in full while the ART lookup is
  about ten milliseconds.
- **Prefiltered ART-scored** is the honest end-to-end figure for the
  inverted-index-only experiment: **24x–445x**, with identical weights on the
  overlapping pairs.
- **Prefiltered full-block** is the honest faithful drop-in figure: **3.4x–270x**
  with **100% coverage** and identical weights.

The old **~4–5x** end-to-end figure is still visible in the legacy scoring
paths; it is now clearly understood as the cost of rescoring against the full
canonical rather than a true floor for the ART approach.

### 4.1.1 Strict reduced-canonical stock `predict()` path

This follow-up tested the closest thing to a low-intrusion production
integration: keep the full-block candidate generation, materialise the blocked
canonical subset, rebuild a fresh linker on that subset, and then call normal
Splink `predict()`.

Results on the full canonical:

| Row limit | Reduced-canonical total | Baseline `predict()` total | Speed-up | Filtered canonical rows | Shared pairs | Max |Δ match_weight| |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 0.8972 s | 11.4543 s | 12.8x | 7 | 4 | 0.00e+00 |
| 10 | 0.9140 s | 12.2631 s | 13.4x | 47 | 26 | 0.00e+00 |
| 100 | 4.3498 s | 11.4229 s | 2.6x | 1,004 | 551 | 0.00e+00 |

Interpretation:

- This path is **behaviourally lossless**: 100% shared-pair coverage on the
  full-block path and identical match weights on every shared pair.
- It is a **real optimisation** over stock full-canonical `predict()`.
- It is **not** the best latency path. It remains slower than the prefiltered
  full-block scorer because each query still pays about **0.26 s** to rebuild
  the linker and about **0.59–0.64 s** to run stock `predict()` even on a tiny
  filtered canonical.
- At row-limit 100 the dominant cost is no longer stock full-canonical scoring.
  It is mostly full-block candidate assembly (`combine_and_dedupe` ≈ 2.85 s)
  plus canonical prefilter (`prefilter_canonical` ≈ 0.43 s), with stock
  `predict()` then adding another ≈ 0.64 s.

### 4.1.2 Follow-up: what should actually be materialised?

On 2026-06-17 we ran two follow-up probes to answer the narrower integration
question behind the reduced-canonical path: are we paying extra because we hand
Splink a lazy reduced relation instead of a real table, and should we therefore
promote canonical materialisation more broadly?

#### ART-filtered subset: yes, but only modestly and not uniformly

The most relevant follow-up is the realtime ART path itself: after candidate
`unique_id`s have already been found from the ART-index lookup, should the
filtered canonical subset be passed to `_get_linker()` as a lazy relation or
materialised to a temp table first?

Using the full-canonical ART database and the actual ART-filtered subsets:

| Row limit | Filtered rows | Lazy subset total | Temp-table subset total | Effect |
| ---: | ---: | ---: | ---: | --- |
| 1 | 7 | 0.845 s | 0.779 s | 1.08x faster |
| 10 | 47 | 0.810 s | 0.821 s | slightly worse |
| 100 | 997 | 1.410 s | 1.190 s | 1.18x faster |

More detail on where the gain appeared:

- **row-limit 1:** linker **0.279 s -> 0.234 s**, `predict()`
  **0.565 s -> 0.535 s**
- **row-limit 10:** linker was flat/slightly worse, `predict()`
  **0.570 s -> 0.549 s**, but the temp-table build cost cancelled that out
- **row-limit 100:** linker was effectively unchanged, but `predict()`
  dropped from **1.170 s -> 0.555 s**

Prediction row counts were identical in all three cases.

So there is a **real but modest** benefit from materialising the
**ART-filtered candidate subset** before `_get_linker()`, with the strongest
signal once the filtered subset is no longer trivial. It is not a large enough
win to treat as automatically beneficial in every tiny batch.

#### Tiny reduced canonical: the linker still has a genuine fixed floor

We also measured `_get_linker()` directly on 1-row messy input with canonical
subsets of **100 / 1,000 / 5,000 / 10,000** rows. It stayed at about
**0.39–0.43 s** across all four sizes. A manual breakdown put roughly:

- **~0.27 s** in Splink `Linker(...)` initialisation itself
- **~0.03–0.04 s** in canonical counting
- **~0.03–0.04 s** in `register_table_input_nodes_concat_with_tf(...)`
- only small extra cost in our settings prep and TF registration

This follow-up found **no evidence** that our reduced-canonical path was
accidentally copying those tiny subsets into full physical tables. The residual
floor is mostly Splink startup work, not a hidden canonical copy in
`uk_address_matcher`.

#### Full canonical: no, do not materialise the whole thing before stock `predict()`

The same idea becomes actively harmful if promoted to the full-canonical case.
Using the full Hackney dataset (**114,166** messy rows) against the full
**71,438,939**-row prepared canonical:

| Variant | Linker build | `predict()` | One-off canonical materialise | Total |
| --- | ---: | ---: | ---: | ---: |
| stock prepared relation | 0.466 s | 14.922 s | n/a | 18.969 s |
| materialised full canonical table | 0.313 s | 34.174 s | 14.904 s | 52.474 s |

A reverse-order confirmation still showed **29.839 s** materialised
`predict()` versus **13.433 s** stock `predict()`, with identical prediction row
counts (**3,417,643**).

The correct promotion target is therefore narrow:

- **promote materialisation only for the ART-filtered candidate-prefiltered
  subset, and treat it as a measured realtime optimisation rather than a
  universal rule**
- **do not materialise the full canonical and then feed that full table back into stock Splink `predict()`**

---

## 5. Reproduce it

### 5.1 Prerequisites

- A prepared canonical folder (default `benchmarking.settings.CANONICAL_PATH`)
  containing `ukam_canonical_addresses.parquet`, `ukam_inverted_index.parquet`,
  `ukam_term_frequencies.parquet`, `ukam_manifest.json`.
- Run everything with `uv` and the repo root on `PYTHONPATH` (the `benchmarking`
  package is not installed; `uk_address_matcher` is editable-installed).

### 5.2 Commands

Hackney residential (fast, good for iterating):

```bash
cd /path/to/uk_address_matcher
PYTHONPATH=. uv run python scripts/experiments/art_index_candidate_assembly_probe.py \
  --overwrite-database
```

Full unfiltered canonical (realtime-scale benchmark):

```bash
cd /path/to/uk_address_matcher
PYTHONPATH=. PYTHONUNBUFFERED=1 uv run python \
  scripts/experiments/art_index_candidate_assembly_probe.py \
  --full-canonical \
  --database-path /path/to/ukam_prepared_canonical/ukam_full_canonical_art.duckdb
```

`--full-canonical` is a convenience flag that:

- flips the canonical filter to a no-op (`1 = 1`) unless you passed an explicit
  `--canonical-filter`;
- reuses the prepared inverted-index parquet directly
  (`--reuse-prepared-inverted-index`) instead of recomputing the ngram GROUP BY;
- caps DuckDB at `memory_limit = 8GB` and `threads = 4` (see §6 — these caps are
  required to survive the build at full scale).

Useful flags: `--row-limits 1,10,100`, `--canonical-filter "<sql>"`,
`--threads`, `--memory-limit`, `--output-dir`, `--overwrite-database`.

### 5.3 Output

Each run writes a Markdown summary and a JSON results file under
`benchmarking/results/art_candidate_assembly_probe/<date>/<run_id>/`. The
Markdown has Headline comparison, ART path step breakdown, prefiltered and
legacy scoring comparisons, full-block comparisons, and Interpretation
sections; the printed `Markdown summary:` and `JSON results:` lines give the
exact paths.

---

## 6. Failure modes and fixes (essential for reproduction at scale)

### 6.1 DuckDB ART index bulk-build OOM (critical)

**Symptom.** Building the full-canonical database crashed the Python process with
`EXIT=133` (SIGTRAP / `EXC_BREAKPOINT`) and a `std::bad_alloc`
(`libsystem_malloc → operator new` inside `_duckdb...so`). It crashed at the same
point every time — db size ~11.8–12.6 GB, right after the canonical CTAS, during
a bulk `CREATE INDEX` over the 71M-row table.

**Root cause.** A single bulk `CREATE INDEX` over tens of millions of rows does
**one giant key-sort allocation that is NOT spillable** and is **immune to
`memory_limit`** (it crashed identically at 24 GB and 8 GB). It is *not* an OS
jetsam OOM kill (confirmed via `log show`).

**Fix — incremental index population.** Create the table **empty**, build the ART
indexes on the empty table, then `INSERT` the data so the ART populates per-batch
at far lower peak RSS:

```sql
CREATE TABLE canonical_addresses AS SELECT * FROM (<prepared>) LIMIT 0;
CREATE INDEX canonical_unique_id_idx   ON canonical_addresses(unique_id);
CREATE INDEX canonical_postcode_idx    ON canonical_addresses(postcode);
CREATE INDEX canonical_numeric_token_1_idx ON canonical_addresses(numeric_token_1);
CREATE INDEX canonical_numeric_token_2_idx ON canonical_addresses(numeric_token_2);
INSERT INTO canonical_addresses SELECT * FROM (<prepared>);   -- ARTs fill incrementally
CHECKPOINT;
```

The same pattern is applied to `inverted_index_hashed`. With this fix the
full-canonical build RSS climbed gently to ~8.5 GB and completed cleanly.

### 6.2 Connection configuration for full scale

```sql
SET enable_progress_bar = false;
SET threads = 4;                              -- fewer concurrent allocations
SET memory_limit = '8GB';                     -- leave headroom for untracked allocs
SET preserve_insertion_order = false;
SET temp_directory = '.../ukam_art_duckdb_tmp';
SET max_temp_directory_size = '200GB';
-- CHECKPOINT after large inserts to flush dirty buffers off resident memory
```

The `memory_limit` must sit **well below physical RAM** (36 GB on the test
machine) because the ART build and multi-GB checkpoint allocate **outside**
DuckDB's spillable buffer pool.

### 6.3 macOS crash diagnosis

Parse the `.ips` JSON in `~/Library/Logs/DiagnosticReports/` (first line is a
header, the remainder is JSON). Read `faultingThread`, `threads`, and
`usedImages` to confirm the allocation site.

---

## 7. Code map

Single self-contained script:
[`scripts/experiments/art_index_candidate_assembly_probe.py`](../../scripts/experiments/art_index_candidate_assembly_probe.py).

| Function | Role |
| --- | --- |
| `build_hackney_art_database(...)` | Build the persisted ART database (incremental indexes; reuses prepared inverted-index parquet for full canonical). |
| `_configure_connection(...)` | Apply the DuckDB resource caps from §6.2. |
| `_explain_analyse(...)` / `_explain_flags(...)` | Capture and summarise `EXPLAIN ANALYZE` plans so index use is measured, not inferred. |
| `_ensure_canonical_ukam_lookup(...)` | Build a persistent narrow `ukam_address_id -> unique_id` lookup table used by the prefiltered full-block path. |
| `_materialise_filtered_canonical(...)` | Materialise a candidate-only canonical slice via `WHERE unique_id IN (...)`. |
| `run_art_path(...)` | Option 2 — derive keys → ART lookup → assemble comparison table (no scoring). Produces the blocking-only timings. |
| `run_baseline_splink(...)` | Stock `predict()` baseline; assigns negative `ukam_address_id` to messy rows; returns the linker for reuse. |
| `_score_existing_blocked_pairs(...)` | Shared fast scorer: comparison-vector + predict SQL over a pre-built `__splink__blocked_id_pairs` with distinct `l`/`r` inputs. |
| `run_art_scored_path(...)` | Legacy option 1 — inject ART candidates as blocked pairs, then score against the full canonical. |
| `run_art_scored_prefiltered_path(...)` | Recommended option 1 — inject ART candidates, prefilter the canonical, then score. |
| `run_full_block_path(...)` | Option 3 — reproduce all blocking rules (`block_using_rules_sqls` for non-exploding + ART for exploding), union/dedupe, and optionally prefilter the canonical before scoring. |
| `run_reduced_canonical_predict_path(...)` | Option 4 — reproduce all blocking rules, materialise the blocked canonical subset, rebuild a fresh linker on that subset, and call normal Splink `predict()`. |
| `ensure_blocking_indexes(...)` | Idempotently ensure the anchor-column ART indexes exist (no-op after build). |
| `compare_scores(...)` | Equivalence check: normalise both prediction sets and compute `max |Δ match_weight|` and mismatch count. |
| `run_probe(...)` / `main(...)` | Orchestrate build → per-row-limit paths → equivalence → Markdown/JSON artefacts. |

Relevant Splink internals used:

- `splink.internals.blocking.block_using_rules_sqls`,
  `ExplodingBlockingRule`
- `splink.internals.comparison_vector_values.compute_comparison_vector_values_from_id_pairs_sqls`
- `splink.internals.predict.predict_from_comparison_vectors_sqls_using_settings`
- `splink.internals.pipeline.CTEPipeline`

Inverted-index strategies:
`uk_address_matcher.cleaning.steps.inverted_index` (`BIGRAM_STRATEGY`,
`TRIGRAM_STRATEGY`). Linker construction:
`uk_address_matcher.linking_model.splink_model._get_linker`.

Related findings:
[`findings/small_batch_matching_findings.md`](../../findings/small_batch_matching_findings.md),
[`findings/splink_canonical_cache_findings.md`](../../findings/splink_canonical_cache_findings.md),
[`findings/splink_stage_sql_map_findings.md`](../../findings/splink_stage_sql_map_findings.md).

---

## 8. Towards a more efficient pipeline

1. **Adopt the prefiltered full-block path as the faithful realtime matching
  mechanism.** It is the current best drop-in replacement: 100% coverage,
  identical weights, and sub-100 ms latency for 1–10 row batches in the
  measured full-canonical run.
2. **Keep the reduced-canonical stock `predict()` path as the lower-intrusion
  fallback.** It is a valid optimisation when preserving the normal Splink
  `predict()` call matters more than achieving the lowest latency, but it is
  not the recommended default if the helper-based scorer is acceptable.
3. **Build the ART database as a deployment artefact**, not per-process. Build
  cost (~135 s for 71M) amortises across all realtime queries; the database is
  ~13 GB on disk, and the narrow `canonical_ukam_lookup` table should be part
  of that artefact.
4. **Attack the candidate lookup planner fallback next.** `EXPLAIN ANALYZE`
  shows the inverted-index lookup switches from an index scan at row-limit 1 to
  a large sequential scan at row-limits 10 and 100. Re-expressing that step so
  DuckDB keeps using ART probes is now the highest-leverage optimisation for
  the inverted-index-only path.
5. **For the faithful full-block path, profile blocking / candidate assembly at
  larger batches.** Once canonical rescoring is prefiltered, the remaining
  100-row latency is no longer dominated by scoring; it is mostly the blocking
  work itself.
6. **Validate weight equivalence on every change.** The experiment's
   `compare_scores` equivalence check (`max |Δ match_weight| == 0`) is the guard
   that keeps the optimisation lossless — keep it in any productionisation.
7. **Watch candidate recall, not weight fidelity.** Weight fidelity is solved
   (always identical). The only quality risk is the ART candidate recall for the
   exploding rule; the full-block path covers this because the non-exploding
   rules backstop it, but monitor coverage if the inverted-index parameters
   (`MAX_UNIQUE_IDS_PER_KEY`, ngram strategies) change.

---

## 9. Provenance

- Machine: macOS, 36 GB RAM.
- Prepared canonical: 71,438,939 rows; inverted index 61,559,861 keys.
- Persisted auxiliary lookup: `canonical_ukam_lookup` with 71,438,939 rows.
- Validated artefacts:
  - Full canonical: `benchmarking/results/art_candidate_assembly_probe/2026-06-08/67f90a482222e45e/`
  - Full canonical reduced-canonical stock `predict()`: `benchmarking/results/art_candidate_assembly_probe/2026-06-15/strict_reduced_canonical_full/`
  - Hackney residential: `benchmarking/results/art_candidate_assembly_probe/2026-06-08/e0683f8a1be05610/`
- Match-weight equivalence: `max |Δ| = 0.00e+00` in all configurations.
