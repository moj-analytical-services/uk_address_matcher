# Exact Matching Strategies

Four matching strategies that run before probabilistic (Splink) matching.

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

## 4. Ngram Jaccard Matching

**File:** [ngram_jaccard.py](ngram_jaccard.py)

**Class:** `NgramJaccardStage`

**Match reason:** `MatchReason.NGRAM_JACCARD` → `"ngram_jaccard: rare-token indexed trigram shortlist match"`

Two-round fuzzy shortlist + scoring stage for residual unmatched rows.

**Lightweight summary:**
- Runs after deterministic stages for unresolved rows.
- Uses postcode-restricted rare-token retrieval to build candidates.
- Applies character trigram Jaccard plus structure-aware reranking.
- Enforces strict number-conflict guards and optional score-gap ambiguity checks.
- Supports optional postcode fallback and optional chunking for operational resilience.

For full internal detail, including CTE-by-CTE behaviour, parameter guidance, and code line references, see:
- [docs/internal/stages/ngram_jaccard.md](../../../../../docs/internal/stages/ngram_jaccard.md)

Code reference:
- [ngram_jaccard.py](ngram_jaccard.py)

Paper reference:
- https://arxiv.org/abs/1708.01402

---

## Usage

Stages are registered in [matching_stages.py](matching_stages.py) and can be enabled via `StageName`:

```python
from uk_address_matcher.linking_model.exact_matching.matching_stages import StageName

enabled_stages = [
    StageName.PEELED_ADDRESS,
    StageName.UNIQUE_TRIGRAM,
]
```
