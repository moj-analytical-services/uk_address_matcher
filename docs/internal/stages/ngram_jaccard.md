# NgramJaccardStage internal notes

This approach is loosely based off:

Reference paper:
- Exploiting Redundancy, Recurrence and Parallelism: How to Link Millions of Addresses with Ten Lines of Code in Ten Minutes (arXiv:1708.01402)
- Original arXiv article: https://arxiv.org/abs/1708.01402

We are using the core ideas highlighted in the paper, but with a bespoke reranker and additional guards to suit our specific use case and precision requirements. This solution may be better off included as another signal in the Splink modelling stage, but for now, we've implemented it as a standalone stage.

## Purpose

`NgramJaccardStage` is a deterministic residual matcher used after earlier exact style stages.

Code entry points:
- stage class: [uk_address_matcher/linking_model/matching/stages/ngram_jaccard.py](uk_address_matcher/linking_model/matching/stages/ngram_jaccard.py#L21)
- SQL stage builder: [uk_address_matcher/linking_model/matching/stages/ngram_jaccard.py](uk_address_matcher/linking_model/matching/stages/ngram_jaccard.py)


## SQL pipeline shape

The stage is assembled as CTE steps and executed in order.

Core CTE groups:
1. rare phrase-token retrieval
- builds ordered adjacent 2-word phrase tokens from messy and canonical addresses
- filters phrase tokens by frequency using `max_token_frequency`

2. candidate shortlist
- computes shared rare phrase-token counts per messy/canonical pair
- applies `min_shared_rare_tokens`
- applies `candidate_cap_per_messy` with `ROW_NUMBER()`

3. trigram similarity scoring
- builds character trigrams for shortlisted ids only
- computes intersection and union to derive `jaccard_similarity`
- adds structural features (primary/secondary numbers, flat indicators, conflicts)

### Jaccard similarity formula

For trigram sets $A$ (messy) and $B$ (canonical), the stage computes:

$$
J(A, B) = \frac{|A \cap B|}{|A \cup B|}
$$

In SQL terms used by the stage:
- `intersection_count = |A ∩ B|`
- `union_count = messy_ngram_count + canonical_ngram_count - intersection_count`
- `jaccard_similarity = intersection_count / union_count` (with zero-division guard)

4. structural reranking and hard guards
- computes `final_score` from lexical plus structural features
- applies strict reject flag for high-similarity numeric conflicts
- applies parent-like near-tie ambiguity guard

5. ranking and output
- ranks candidates per messy row
- keeps winner (`rn = 1`) subject to `min_final_score`
- applies optional `min_score_gap`
- outputs resolved canonical id plus diagnostic fields and blocking strategy

### Example output (single matched row)

The final CTE emits rows with a shape like:

```text
ukam_address_id: 483920
canonical_ukam_address_id: 7711023
resolved_canonical_id: 10002123456
shared_rare_token_count: 3
intersection_count: 21
union_count: 24
jaccard_similarity: 0.875000
final_score: 0.913400
score_gap_to_second: 0.081200
candidate_looks_parent_like: 0
blocking_strategy: postcode_exact
match_reason: ngram_jaccard: rare-token indexed trigram shortlist match
```

Exact field names come from the stage output CTE in
[uk_address_matcher/linking_model/matching/stages/ngram_jaccard.py](uk_address_matcher/linking_model/matching/stages/ngram_jaccard.py).

## Match reason and stage naming

- emitted match reason enum is `MatchReason.NGRAM_JACCARD`
- accuracy-table stage key is `ngram_jaccard`
- downstream subset logic should key off the stage prefix (split before `:`) rather than brittle free-text contains checks

## Practical parameter notes

- `min_final_score` gates acceptance on the computed final score.
- `max_token_frequency` controls how strict rare phrase-token retrieval is within each pass.
- `candidate_cap_per_messy` limits retrieval fan-out before trigram scoring.
- `min_shared_rare_tokens` controls minimum shared rare evidence in retrieval.
- `min_score_gap` is an ambiguity guard on winner versus runner-up.
- `use_postcode_fallback` enables a second postcode strategy for unresolved rows.
- `num_of_chunks` enables chunked execution for large unresolved sets.
