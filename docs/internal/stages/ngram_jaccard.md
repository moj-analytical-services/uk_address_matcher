# NgramJaccardStage internal notes

Reference paper:
- Exploiting Redundancy, Recurrence and Parallelism: How to Link Millions of Addresses with Ten Lines of Code in Ten Minutes (arXiv:1708.01402)
- https://arxiv.org/abs/1708.01402

## Purpose

`NgramJaccardStage` is a deterministic residual matcher used after earlier exact style stages.

Code entry points:
- stage class: [uk_address_matcher/linking_model/matching/stages/ngram_jaccard.py](uk_address_matcher/linking_model/matching/stages/ngram_jaccard.py#L21)
- SQL stage builder: [uk_address_matcher/linking_model/matching/stages/ngram_jaccard.py](uk_address_matcher/linking_model/matching/stages/ngram_jaccard.py#L312)

## Runtime flow

1. Stage orchestration
- `find_matches(...)` runs exact postcode pass first, then optional drop-last-character fallback for unresolved rows.
- references:
  - [uk_address_matcher/linking_model/matching/stages/ngram_jaccard.py](uk_address_matcher/linking_model/matching/stages/ngram_jaccard.py#L220)
  - [uk_address_matcher/linking_model/matching/stages/ngram_jaccard.py](uk_address_matcher/linking_model/matching/stages/ngram_jaccard.py#L265)

2. Optional chunking behaviour
- `_run_pass(...)` supports `num_of_chunks` as an operational fallback when unresolved volume is large.
- chunking key is postcode-derived for determinism.
- references:
  - [uk_address_matcher/linking_model/matching/stages/ngram_jaccard.py](uk_address_matcher/linking_model/matching/stages/ngram_jaccard.py#L123)
  - [uk_address_matcher/linking_model/matching/stages/ngram_jaccard.py](uk_address_matcher/linking_model/matching/stages/ngram_jaccard.py#L154)

## SQL pipeline shape

The stage is assembled as CTE steps and executed in order.

Core CTEs to understand:
1. rare-token retrieval
- token frequencies from messy and canonical token sets
- rare-token filter using `max_token_frequency`
- references:
  - [uk_address_matcher/linking_model/matching/stages/ngram_jaccard.py](uk_address_matcher/linking_model/matching/stages/ngram_jaccard.py#L402)
  - [uk_address_matcher/linking_model/matching/stages/ngram_jaccard.py](uk_address_matcher/linking_model/matching/stages/ngram_jaccard.py#L406)

2. candidate shortlist
- `round1_candidate_pairs` counts shared rare tokens
- `candidates_sql` filters by `min_shared_rare_tokens` and applies `candidate_cap_per_messy` with `ROW_NUMBER()`
- references:
  - [uk_address_matcher/linking_model/matching/stages/ngram_jaccard.py](uk_address_matcher/linking_model/matching/stages/ngram_jaccard.py#L424)
  - [uk_address_matcher/linking_model/matching/stages/ngram_jaccard.py](uk_address_matcher/linking_model/matching/stages/ngram_jaccard.py#L436)

3. trigram similarity scoring
- character trigram sets are built for shortlisted ids
- pair intersections and union counts produce `jaccard_similarity`
- references:
  - [uk_address_matcher/linking_model/matching/stages/ngram_jaccard.py](uk_address_matcher/linking_model/matching/stages/ngram_jaccard.py#L466)
  - [uk_address_matcher/linking_model/matching/stages/ngram_jaccard.py](uk_address_matcher/linking_model/matching/stages/ngram_jaccard.py#L558)

4. structural reranking and guards
- `final_score_expr` combines Jaccard, shared-token signal, and structure features
- strict primary and secondary number reject guard removes high-similarity sibling conflicts
- references:
  - [uk_address_matcher/linking_model/matching/stages/ngram_jaccard.py](uk_address_matcher/linking_model/matching/stages/ngram_jaccard.py#L820)
  - [uk_address_matcher/linking_model/matching/stages/ngram_jaccard.py](uk_address_matcher/linking_model/matching/stages/ngram_jaccard.py#L857)

5. ranking and output
- `ranked_pairs_sql` computes winner and runner-up
- optional `min_score_gap` is applied on final score difference
- final output is the selected canonical winner plus diagnostics
- references:
  - [uk_address_matcher/linking_model/matching/stages/ngram_jaccard.py](uk_address_matcher/linking_model/matching/stages/ngram_jaccard.py#L898)
  - [uk_address_matcher/linking_model/matching/stages/ngram_jaccard.py](uk_address_matcher/linking_model/matching/stages/ngram_jaccard.py#L982)

## Practical parameter notes

- `min_final_score` currently gates acceptance on the computed final score.
- `min_jaccard` is retained as a compatibility alias.
- `max_token_frequency` controls how strict rare-token retrieval is within each pass.
- `candidate_cap_per_messy` limits retrieval fan-out before trigram scoring.
- `min_shared_rare_tokens` controls minimum shared rare evidence in retrieval.
- `min_score_gap` is an ambiguity guard on winner versus runner-up.
