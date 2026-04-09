# Matching Stages

The matching pipeline is usually ordered as:

1. exact matching,
2. peeled address matching,
3. unique trigram matching,
4. probabilistic Splink matching.

Splink is the last supported stage. It consumes the harder residual cases that
earlier deterministic stages leave behind.

---

## 1. Exact Matching

**File:** [annotate_exact_matches.py](annotate_exact_matches.py)

**Stage name:** `StageName.EXACT_MATCHES`

**Match reason:** `MatchReason.EXACT` → `"exact: full match"`

Straightforward hash-join on cleaned address strings.

**Steps:**
- Join fuzzy addresses to canonical on `clean_full_address` + `postcode`
- If multiple canonical matches exist (duplicates), take the first
- Annotate matched records with `EXACT` match reason

**Example:**
```
Fuzzy:     10 HIGH STREET SW1A 1AA
Canonical: 10 HIGH STREET SW1A 1AA
→ Match (identical after cleaning)
```

---

## 2. Peeled Address Matching

**File:** [peeled_address_matching.py](peeled_address_matching.py)

**Stage name:** `StageName.PEELED_ADDRESS`

**Match reason:** `MatchReason.PEELED_ADDRESS` → `"peeled_address: match after removing common UK end tokens"`

Matches addresses after removing common UK locality tokens from the end.

**What is "peeling"?**
Iteratively strips trailing tokens like cities (LONDON, MANCHESTER), counties (HERTFORDSHIRE), boroughs (HACKNEY, LAMBETH), and regions (GREATER LONDON).

**Steps:**
- Compute `peeled_address` for fuzzy and canonical by slicing off peeled tokens
- Join on `postcode` + `peeled_address` (exact match on the peeled result)
- Require at least one side to have peeled something (avoids duplicating exact matches)
- Deduplicate by fuzzy address ID, taking the match with fewest peeled tokens

**Example:**
```
Fuzzy:     100 TEST STREET HACKNEY LONDON SW1A 1AA
           → peeled to: 100 TEST STREET
Canonical: 100 TEST STREET SW1A 1AA
           → peeled to: 100 TEST STREET
→ Match (peeled addresses identical)
```

---

## 3. Unique Trigram Matching

**File:** [resolve_with_trigrams.py](resolve_with_trigrams.py)

**Stage name:** `StageName.UNIQUE_TRIGRAM`

**Match reason:** `MatchReason.UNIQUE_TRIGRAM` → `"unique_trigram: unique trigram match"`

Matches based on trigrams (3-token sequences) that uniquely identify a single canonical address.

**Steps:**
- Generate all trigrams from canonical address tokens
- Build an index of trigrams that appear in exactly one canonical address (per postcode + numeric tokens + unit indicators)
- Generate trigrams from fuzzy addresses
- Join fuzzy trigrams to the unique index on:
  - `postcode`
  - `numeric_tokens`
  - `trigram_hash`
  - Flat/unit indicators (NULL-safe equality)
- Keep only fuzzy addresses where all matched trigrams point to the same canonical address
- Require at least `min_unique_hits` (default: 1) supporting trigrams

**Key constraints:**
- Flat indicators must match (prevents "12 HIGH ST" matching "FLAT A 12 HIGH ST")
- Business unit type/ID must match (prevents "UNIT C" matching "UNIT F")
- Non-traditional address types must match

**Example:**
```
Fuzzy:     FIRST FLOOR 25 ACACIA AVENUE SW1A 1AA
Trigrams:  [FIRST, FLOOR, 25], [FLOOR, 25, ACACIA], [25, ACACIA, AVENUE]

If [25, ACACIA, AVENUE] uniquely identifies one canonical address at SW1A 1AA
with matching flat indicators → Match
```

---

## 4. Splink Probabilistic Matching

**File:** [splink.py](splink.py)

**Stage class:** `SplinkStage`

**Match reason:** `MatchReason.SPLINK` → `"splink: probabilistic match"`

This stage is the final matcher in the supported pipeline. It is designed for
records that remain unresolved after the deterministic stages.

**Runtime flow:**
- Build a Splink linker with the bundled settings from `uk_address_matcher/data/splink_model.json`
- Generate blocked candidate pairs
- Compute comparison-vector evidence for each blocked pair
- Run the repo-owned upstream ngram feature step before final Bayesian scoring
- Call `linker.inference.predict()` to get candidate match weights
- Apply the distinguishing-token improvement pass to the top Splink candidates
- Compute `distinguishability` and keep the best candidate per messy record
- Apply final `match_weight` and `distinguishability` thresholds before emitting matches

**Key outputs:**
- `match_weight`: overall strength of evidence for the chosen candidate
- `distinguishability`: gap to the next-best candidate for the same messy record

**Important thresholds on the stage itself:**
- `predict_threshold_match_weight`: how permissive the initial Splink predict call is
- `improve_threshold_match_weight`: which candidates are eligible for token-based rescoring
- `final_match_weight_threshold`: minimum final match weight to emit a match
- `final_distinguishability_threshold`: minimum winner-vs-runner-up gap required to emit a match

**Blocking behaviour:**
- The bundled model uses postcode pieces, numeric tokens, unusual tokens, and `exploding_unique_ids` to generate candidate pairs
- `include_full_postcode_block` can force a strict same-postcode rule
- `include_outside_postcode_block` controls whether broader cross-postcode blocking rules stay enabled

**Bundled model summary:**
- Exact `clean_full_address` agreement remains a strong signal
- `ngram_final_score` is an upstream pairwise feature bucketed into score bands: `>= 0.92`, `>= 0.86`, `>= 0.78`, `>= 0.62`, else
- Structural evidence from `flat_identity` remains a major discriminator for flats and units
- Numeric-token comparisons (`numeric_token_1/2/3`) capture house/unit numbering consistency
- Token-frequency and common-ending comparisons help distinguish informative vs generic address overlap
- Postcode agreement remains an explicit comparison rather than being baked only into blocking

## 5. Upstream Ngram Features In Splink

The old standalone `NgramJaccardStage` has been removed from the supported
stage API. Its pairwise lexical scorer now runs inside the Splink prediction
path as an internal feature-engineering step.

Implementation layout:
- pre-Splink integrations now live under `uk_address_matcher/linking_model/matching/stages/splink_integrations/`
- each integration can add blocked-pair feature SQL after Splink blocking and before final Bayesian scoring
- this is intended as the slot-in point for future algorithms beyond ngram
- integrations now distinguish core feature columns needed by the model from optional debug columns retained only for development or inspection

Current behaviour:
- Splink still owns blocking and final `match_weight` computation
- The repo computes blocked-pair trigram overlap, shared rare phrase-token evidence, and a composite `ngram_final_score`
- Numeric disagreements are now treated as weighted evidence inside the score and the wider Splink model rather than as a separate hard upstream reject
- The bundled Splink settings use the resulting `ngram_final_score` as one comparison among the wider probabilistic evidence set
- by default only `ngram_final_score` is propagated into later Splink output; richer ngram diagnostics are opt-in via `SplinkStage(retain_pre_splink_debug_columns=True)`

---

## Usage

The public API is stage-class based. A typical pipeline looks like:

```python
from uk_address_matcher import (
  ExactMatchStage,
  PeeledAddressStage,
  UniqueTrigramStage,
  SplinkStage,
)

stages = [
  ExactMatchStage(),
  PeeledAddressStage(),
  UniqueTrigramStage(),
  SplinkStage(
    predict_threshold_match_weight=-50,
    final_match_weight_threshold=-20.0,
    final_distinguishability_threshold=0.0,
  ),
]
```
