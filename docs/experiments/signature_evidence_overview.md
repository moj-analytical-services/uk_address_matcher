# Signature Evidence (Inverted Index): Final Overview

This document is the single consolidated record of the inverted-index
"signature evidence" work. It replaces the earlier pair of notes
(`signature_evidence_findings.md` and
`signature_evidence_inverted_index_summary.md`) and is the place to return to if
we pick this work back up.

It covers:

- what the feature is and how it works end to end,
- the key design decision (why we did **not** materialise extra index columns),
- what we tested and what the results were,
- which existing model fields overlap with this signal,
- the approach we are currently pursuing and the open questions.

---

## 1. What the feature is

We reuse the existing bigram/trigram inverted index to produce a per-candidate
overlap score for each messy → canonical pair, then expose that score to Splink
as a new comparison called `signature_evidence`.

Critically, we did **not** switch the persisted index to a richer multi-column
scoring table. The on-disk index is still fundamentally a
`key -> candidate unique_ids` structure. What changed lives in the runtime
lookup and the model layer:

1. the lookup stage derives an additional runtime score map from the same index,
2. Splink reads that score map as a new comparison (`signature_evidence`),
3. no new persisted scoring columns are required.

---

## 2. How it works end to end

### 2.1 The persisted inverted index

After the rebuild, the on-disk file
[ukam_inverted_index.parquet](/Users/thomas.hepworth/data/address_matcher/secret_data/os/ukam_prepared_canonical/ukam_inverted_index.parquet)
has three columns:

- `key` — one bigram or trigram derived from `clean_full_address`
- `unique_ids` — the list of canonical `unique_id` values containing that key
- `index_strategy` — lightweight provenance / grouping metadata

Build rules:

- bigram and trigram keys only,
- a key is kept only if it maps to at most `20` canonical ids (more common keys
  are dropped as too common to be useful for blocking).

There is deliberately **no** stored column for key frequency, IDF, capped
score, within-address occurrence count, or any extra n-gram payload.

`index_strategy` is not required by the scoring logic. We kept it because it
compresses well when the parquet is written ordered by `index_strategy, key`,
which actually makes the file smaller (see Section 6).

### 2.2 The runtime scoring step

During messy-side cleaning, in
[inverted_index.py](/Users/thomas.hepworth/work_projects/address_matching/inverted_index_matching/uk_address_matcher/cleaning/steps/inverted_index.py),
we derive a `signature_score_map` column. For each messy record:

1. generate its bigram/trigram keys,
2. deduplicate the keys per record (a repeated n-gram counts once),
3. join the keys to the inverted index,
4. compute a per-key inverse-document-frequency weight:

$$
\mathrm{key\_idf} = \log_2\!\left(\frac{N}{\text{posting\_list\_size}}\right)
$$

where `N` is the canonical row count and
`posting_list_size = len(unique_ids)` for that key,

5. unnest the matched candidate ids and add the key's IDF to every candidate it
   points to,
6. emit `signature_score_map : MAP<canonical_id VARCHAR -> summed IDF DOUBLE>`.

`N` is provided at match time via a small `__ukam_index_meta` table registered
in
[pipelines.py](/Users/thomas.hepworth/work_projects/address_matching/inverted_index_matching/uk_address_matcher/cleaning/pipelines.py),
and the canonical row count is threaded through
[address_matcher.py](/Users/thomas.hepworth/work_projects/address_matching/inverted_index_matching/uk_address_matcher/address_matcher.py).

The resulting map is keyed by canonical `unique_id` and holds the summed IDF
evidence for that messy row against each candidate, e.g.:

```text
{
  "123456789": 42.7,
  "987654321": 18.4,
  "555555555": 7.9
}
```

### 2.3 The Splink comparison

In
[splink_model.json](/Users/thomas.hepworth/work_projects/address_matching/inverted_index_matching/uk_address_matcher/data/splink_model.json)
the `signature_evidence` comparison reads the score for the specific candidate
pair:

```sql
list_extract(map_extract(signature_score_map_r, CAST(unique_id_l AS VARCHAR)), 1)
```

Because Splink evaluates one pair at a time with the canonical row on the left
(`_l`) and the messy row on the right (`_r`), this expression:

1. takes the current canonical candidate id (`unique_id_l`),
2. casts it to `VARCHAR` (the map keys are strings),
3. looks it up in the messy row's `signature_score_map_r`,
4. returns the summed IDF score for that exact pair.

So the map is not a global table — it is a per-messy-row lookup answering "if
Splink is currently evaluating candidate X against me, here is the overlap
score for X".

Current production buckets (Bayes factors):

| Level | Condition | m |
| --- | --- | --- |
| very strong | summed IDF `>= 40` | 8 |
| strong | summed IDF `>= 20` | 3 |
| weak | summed IDF `>= 8` | 1.5 |
| none / else | otherwise | neutral |

The new "level" therefore lives in the model layer, not in the parquet schema.

### 2.4 Two jobs from one index

| Use | Question it answers |
| --- | --- |
| candidate generation (`exploding_unique_ids`) | which canonical ids share at least one bigram/trigram with this messy row? |
| candidate scoring (`signature_score_map`) | for each of those ids, how much rare shared n-gram evidence is there? |

The index was already powering `exploding_unique_ids` before this change. The
new work added the scoring layer on top of the same structure.

### 2.5 Worked example

Messy address:

```text
flat 2 10 high street aberdeen
```

Generated keys include `flat 2`, `2 10`, `10 high`, `high street`,
`flat 2 10`, `2 10 high`, `10 high street`. If:

- candidate A shares `flat 2 10`, `10 high street`, `high street`
- candidate B shares only `high street`
- candidate C shares `10 high`

then with rarer keys carrying higher IDF the map might be:

```text
{ "A": 41.6, "B": 4.3, "C": 9.8 }
```

Splink buckets these as A → very strong (`>= 40`), C → weak (`>= 8`), B →
neutral. The feature says A is much stronger because it shares rarer, more
distinctive fragments.

---

## 3. Why we did NOT precompute extra index fields

A reasonable expectation was that we would expand the index to store per-key
counts, capped scores, or precomputed weights. We deliberately did not, for
these reasons:

- **The score is pair-specific, not key-specific.** The useful number is "how
  much shared rare-ngram evidence does *this messy row* have for *this
  candidate*". That is a function of the messy row's key set and the candidate
  id, summed across keys — not a property of any single key, so per-key scalars
  would not save the join-and-sum we still do at match time.
- **The expensive part is already cheap.** The IDF is just
  `log2(N / len(unique_ids))`. `len(unique_ids)` is already in the index and `N`
  is a single number, so precomputing IDF would only avoid one cheap `log2` per
  key while enlarging the index and coupling it to `N`.
- **We want the index small and reusable.** Adding per-key payloads grows disk
  and match-time memory. Keeping it as `key -> unique_ids` keeps the same
  structure powering both blocking and scoring.
- **Repeated-ngram counts were intentionally dropped.** Keys are deduplicated
  per record, so this is set-based overlap evidence, not repeated-occurrence
  frequency. This avoids inflating scores for long or repetitive addresses,
  which would hurt precision.
- **The cap is structural, not a stored score.** The only cap is the build-time
  posting-list cap (`<= 20` ids per key). The Splink thresholds (40/20/8) do the
  bucketing at match time, where it is easy to tune.

---

## 4. What we tested and what worked

### 4.1 Adding the scoring path (worked)

Adding `signature_score_map` plus the `signature_evidence` comparison improved
recall with negligible precision impact across all three benchmark datasets.

Per-dataset deltas (signature ON vs OFF, current weights):

| Dataset | Precision Δ | Recall Δ | F1 Δ |
| --- | --- | --- | --- |
| hackney | −0.023 pp | +0.354 pp | +0.171 pp |
| rhondda | +0.012 pp | +0.161 pp | +0.088 pp |
| aberdeenshire | −0.002 pp | +0.205 pp | +0.106 pp |

Headline result: more correct matches, roughly flat precision.

### 4.2 Schema-alignment fix for older prepared canonicals (worked)

Live messy cleaning always emits `signature_score_map`, but a canonical
prepared before this change does not. The linker concatenates the column set
from both sides, which caused a binder error. Fix: in
[splink_model.py](/Users/thomas.hepworth/work_projects/address_matching/inverted_index_matching/uk_address_matcher/linking_model/splink_model.py)
we add an empty, type-compatible map to whichever side lacks the column, so we
can run without rebuilding the full index.

### 4.3 Experiment toggles during development (since removed)

While developing the feature we used environment toggles to switch the
signature path on/off and to sweep m-weights without editing JSON. Those
experiment modes have since been **removed**: the production code now always
runs the single summed-IDF path, and the corresponding tests assert the
production behaviour. The findings below were gathered while those toggles
still existed.

### 4.4 m-weight tuning (mixed)

We swept several weightings. On hackney, concentrating weight on the strongest
bucket (`10/1/1`) was best, but it did not generalise to the other datasets.

| Variant | hackney F1 | rhondda F1 | aberdeenshire F1 |
| --- | --- | --- | --- |
| baseline (off) | 0.97951 | 0.97622 | 0.96468 |
| prod 8/3/1.5 | 0.98122 | 0.97708 | 0.96574 |
| strong_only 10/1/1 | 0.98227 | 0.97706 | 0.96529 |

Conclusion: we kept `8/3/1.5` in production. This is the clearest evidence of
the competing-signal problem (Section 5): the medium/weak buckets add little
once other token-overlap comparisons already fire.

### 4.5 What is not working / not yet done

- The runtime "memory cost" win is small; this feature is recall-oriented, not
  a performance optimisation.
- Early wall-clock "faster" numbers were confounded by OS file-cache warmth, so
  timing comparisons should be treated cautiously.
- We have not yet jointly retuned `signature_evidence` together with the
  overlapping token comparison (Section 5), which is the principled next step.

---

## 5. Overlap with existing model fields

Splink's Fellegi-Sunter scoring assumes comparisons are conditionally
independent. Signature evidence (shared rare bigrams/trigrams) is **not**
independent of several comparisons that also reward shared tokens. When two
correlated comparisons both fire, their Bayes factors multiply and we
double-count evidence — the "competing signals" effect.

Ranked by overlap with signature evidence:

- **`token_rel_freq_arr_hist` — highest overlap.** The dominant token-overlap
  comparison (Bayes factors up to ~78,000:1), scoring shared tokens weighted by
  rarity. Signature evidence rewards the same rare-token sharing at
  bigram/trigram granularity. This is the strongest competitor and the main
  reason the medium/weak signature buckets add little.
- **`clean_full_address` (exact match) — high overlap at the top end.** When the
  whole cleaned address matches exactly, signature overlap is also maximal;
  the two are almost perfectly redundant for exact-match pairs.
- **`address_without_numbers` (incl. Jaccard levels) — high overlap.** Jaccard
  over tokens is conceptually close to bigram/trigram overlap, especially
  mid-band.
- **`common_end_tokens` — moderate overlap.** Shared trailing tokens, though the
  posting-list cap removes the most common ones.
- **Low overlap (mostly independent):** `flat_identity`,
  `sub_premise_location`, `numeric_token_1/2/3`, `postcode`.

**Should we remove any fields?** Not yet. Each currently earns its place, and
signature evidence is a net positive on top of them. The right fix for the
correlation is coordinated tuning, not deletion: treat
`token_rel_freq_arr_hist` and `signature_evidence` as a correlated pair, and
either down-weight signature evidence where it co-fires with strong
token-histogram evidence, or fold the bigram/trigram signal into a single
combined token comparison. Any removal should be driven by a benchmark showing
no recall loss at fixed precision.

---

## 6. Persisted file state

The rebuilt prepared inverted index file has:

- schema: `key`, `unique_ids`, `index_strategy`
- size: `696,133,480` bytes

Compared with an earlier rebuilt file that omitted `index_strategy`
(`955,366,890` bytes), the ordered write with `index_strategy` retained is
smaller by:

- `259,233,410` bytes (≈ `247.22 MiB`, ≈ `27.13%`).

This is why we kept `index_strategy`: ordering by `index_strategy, key`
compresses better than dropping the column.

---

## 7. Current approach and open questions

The approach we are initially pursuing:

- keep the persisted index structurally simple (`key -> unique_ids`, plus
  `index_strategy` for compression),
- derive a pair-specific summed-IDF overlap score at lookup time,
- let Splink bucket that derived score into the `signature_evidence` comparison
  with production weights `8/3/1.5`,
- run a single, always-on production path (no experiment toggles).

Open questions to revisit later:

1. Joint tuning of `signature_evidence` with `token_rel_freq_arr_hist` (and
   possibly `address_without_numbers` Jaccard) to remove double-counting,
   rather than tuning signature evidence in isolation.
2. Whether `signature_evidence` and `address_without_numbers` Jaccard levels do
   meaningfully different work; if not, one could be trimmed.
3. Whether a more explicit rarity feature for numeric/structured tokens would
   let the inverted index carry more of the scoring load (related to the
   separate question of whether inverted-index evidence could reduce reliance
   on numeric TF adjustment — see the Splink stage notes).

---

## 8. What is true now

- The inverted index still stores only key-to-candidate-list mappings (plus
  `index_strategy` metadata).
- We derive a candidate-specific summed-IDF overlap score at lookup time.
- Splink consumes that derived score as the `signature_evidence` comparison.
- We did **not** add persisted per-key score columns, n-gram count columns, or a
  stored capped score.
- The feature is a consistent recall/F1 win across all three datasets at
  roughly flat precision.
- The main open problem is correlation with existing token-overlap comparisons
  (`token_rel_freq_arr_hist` most of all), which is why aggressive signature
  weights do not generalise.
