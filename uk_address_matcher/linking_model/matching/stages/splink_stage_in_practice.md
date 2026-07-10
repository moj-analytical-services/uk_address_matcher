# Splink Stage In Practice

This note summarises the full Splink stage as it runs today. It is intended to
be read alongside [splink.py](splink.py), which orchestrates the steps described
here.

There are really two separate pieces:

1. the Splink model itself, which scores pairwise candidates using the cleaned
   address fields and a set of fixed comparison rules,
2. a second pass which looks only at the candidate matches already generated
   for one messy record and adjusts the scores using within-group token rarity.

The code path starts in [splink.py](splink.py), builds the linker in [../../splink_model.py](../../splink_model.py), applies the local re-ranking in [../../../post_linkage/identify_distinguishing_tokens.py](../../../post_linkage/identify_distinguishing_tokens.py), and finally computes distinguishability in [../../../post_linkage/analyse_results.py](../../../post_linkage/analyse_results.py).

## 1. High-level flow

For one Splink stage pass, the runtime flow is:

1. take only the still-unresolved messy rows,
2. build pairwise candidate rows using the configured blocking rules,
3. score those pairs with the Splink model,
4. keep only the stronger candidate pairs,
5. re-rank those pairs using token rarity inside each messy-row candidate group,
6. compute the gap between the best and second-best candidate,
7. emit only the top candidate per messy row if it clears the final thresholds.

In code terms inside [splink.py](splink.py):

| Runtime step | Implementation |
| --- | --- |
| build linker | `_get_linker(...)` |
| pairwise scoring | `linker.inference.predict(...)` |
| local re-ranking | `improve_predictions_using_distinguishing_tokens(...)` |
| best-candidate selection | `best_matches_with_distinguishability(...)` |
| final filter | `final_match_weight_threshold` and `final_distinguishability_threshold` |

### Side convention

Throughout the Splink stage the canonical (gazetteer) record is the left side
and the messy input record is the right side:

| Side | Suffix | Meaning |
| --- | --- | --- |
| left | `_l` | canonical candidate record (`unique_id_l`) |
| right | `_r` | messy input record being matched (`unique_id_r`) |

This is why [splink.py](splink.py) projects `unique_id_l` to
`resolved_canonical_id` and `ukam_address_id_r` to the messy `ukam_address_id`.

## 2. Stage 1: the Splink model itself

### 2.1 What Stage 1 is doing

Stage 1 is a pairwise probabilistic scorer.

For each candidate pair `(canonical_l, messy_r)`, Splink evaluates a fixed set
of comparisons from [../../../data/splink_model.json](../../../data/splink_model.json). Each comparison contributes positive, neutral, or negative evidence to the final `match_weight`.

Conceptually:

$$
\text{match\_weight} \approx \sum_i \log_2\left(\frac{m_i}{u_i}\right)
$$

where each comparison level contributes a Bayes-factor-style term.

### 2.2 Candidate generation before scoring

The model is not run across every possible pair. Candidate rows are generated
through blocking rules in [../../../data/splink_model.json](../../../data/splink_model.json).

Today those rules mix:

- postcode plus numeric-token rules,
- postcode plus unusual-token rules,
- a special rule based on `exploding_unique_ids` coming from the inverted index.

That means the inverted index already plays a major role before scoring even
starts. It is part of candidate generation, not just a later feature.

### 2.3 Main Stage 1 evidence sources

The current model combines several kinds of evidence.

| Evidence type | Practical purpose |
| --- | --- |
| exact cleaned-address agreement | strong positive evidence when the normalised strings line up exactly |
| address-without-numbers similarity | rewards near matches once house numbers are stripped |
| flat identity and sub-premise logic | protects against false matches across flats and sub-units |
| numeric token comparisons | checks house, flat, and secondary numbers explicitly |
| postcode structure | rewards district or unit agreement and penalises disagreement |
| signature evidence | adds positive evidence when the messy row shares rare bigram or trigram keys with the candidate canonical row |

### 2.4 Where TF adjustment fits

There is a specific term-frequency adjustment path for numeric tokens.

The linker registers term-frequency lookup tables for `numeric_token_1`,
`numeric_token_2`, and `numeric_token_3` in [../../splink_model.py](../../splink_model.py), and the comparison definitions in [../../training.py](../../training.py) use `tf_adjustment_column` on the exact numeric-match levels.

Importantly, these numeric frequencies are not recomputed from the current
canonical set at runtime. They are read from a pre-baked, package-bundled
`numeric_token_frequencies.parquet` (loaded in [../../../cleaning/pipelines.py](../../../cleaning/pipelines.py) and re-registered with Splink in [../../splink_model.py](../../splink_model.py)).

This means:

- a match on a rare number is worth more than a match on a very common number,
- the adjustment uses a fixed, global frequency table rather than per-run
  canonical counts,
- it applies during Stage 1 scoring and independently of the local candidate
  group used by Stage 2.

### 2.5 Where signature evidence fits

The current production model also consumes `signature_score_map` through the
`signature_evidence` comparison.

That score is built from the inverted index and measures rare shared bigram and
trigram overlap between the messy row and each candidate canonical id.

The levels in [../../../data/splink_model.json](../../../data/splink_model.json) are:

| Condition | Effect |
| --- | --- |
| no map entry | neutral |
| score `>= 40` | very strong positive evidence |
| score `>= 20` | strong positive evidence |
| score `>= 8` | weak positive evidence |
| score `< 8` | neutral |

So Stage 1 already contains both:

- global numeric rarity from TF adjustment,
- pair-specific lexical rarity from the inverted index.

## 3. Stage 2: local re-ranking using distinguishing tokens

### 3.1 Why this second pass exists

Splink alone scores each pair independently.

That is useful, but it does not directly ask a crucial ranking question:

> among the small set of plausible candidates for this one messy row, which
> candidate contains the tokens that are most specific to this row?

The second pass answers that question.

It is not another global model. It is a local competition inside each messy
row's candidate group.

### 3.2 Which candidate rows Stage 2 sees

In [../../../post_linkage/identify_distinguishing_tokens.py](../../../post_linkage/identify_distinguishing_tokens.py), the re-ranker first narrows to:

1. pairs with `match_weight > improve_threshold_match_weight`,
2. one row per `(unique_id_r, unique_id_l)` pair,
3. at most `improve_top_n_matches` candidates per messy record.

With the current `SplinkStage` defaults, that means:

| Parameter | Default |
| --- | --- |
| `improve_threshold_match_weight` | `-20` |
| `improve_top_n_matches` | `5` |
| `improve_use_bigrams` | `True` |

So Stage 2 is intentionally focused on a short list of plausible candidates,
not the full candidate universe.

### 3.3 Token preparation

For each retained candidate set, Stage 2:

1. removes some common trailing tokens,
2. tokenises the messy row and candidate rows,
3. optionally builds bigrams,
4. measures token and bigram frequencies inside the candidate block for that
   single messy row.

The important detail is that these counts are local.

They are not corpus-wide TF values. They are frequencies inside the current
messy row's candidate group.

### 3.4 What gets rewarded and penalised

The re-ranker computes three unigram-derived structures for each candidate:

| Structure | Meaning |
| --- | --- |
| `overlapping_tokens_this_l_and_r` | tokens shared by this candidate and the messy row |
| `tokens_elsewhere_in_block_but_not_this` | messy-row tokens found in rival candidates but not this candidate |
| `missing_tokens` | tokens present in the candidate but absent from the messy row |

If bigrams are enabled, it builds the same pattern for bigrams and then filters
some bigram evidence to avoid double-counting cases already fully explained by
unigrams.

### 3.5 The actual adjustment formula

The code applies the following score adjustment.

With defaults:

- `REWARD_MULTIPLIER = 3`
- `PUNISHMENT_MULTIPLIER = 1.5`
- `BIGRAM_REWARD_MULTIPLIER = 3`
- `BIGRAM_PUNISHMENT_MULTIPLIER = 1.5`
- `MISSING_TOKEN_PENALTY = 0.1`

the adjustment is:

$$
\begin{aligned}
\text{mw\_adjustment} = {} & 3 \cdot \sum_{t \in \text{shared tokens}} \frac{1}{c(t)^2} \\
& - 1.5 \cdot \left|\text{tokens elsewhere but not here}\right| \\
& - 0.1 \cdot |\text{missing tokens}| \\
& + 3 \cdot \sum_{b \in \text{shared bigrams}} \frac{1}{c(b)^2} \\
& - 1.5 \cdot \left|\text{bigrams elsewhere but not here}\right|
\end{aligned}
$$

where `c(token)` and `c(bigram)` are the counts of that token or bigram among
the canonical candidates in the current messy-row block. The reward terms are
frequency-weighted (rarer shared tokens count for more), while the penalty
terms are simple counts of distinguishing tokens or bigrams that other rival
candidates carry but this one does not.

The updated score is simply:

$$
\text{match\_weight}_{\text{new}} = \text{match\_weight}_{\text{original}} + \text{mw\_adjustment}
$$

### 3.6 Practical interpretation

In practice this means:

- shared tokens are rewarded more when they are rare among rival candidates,
- if the messy row contains a token that other candidates have but this
  candidate does not, this candidate is penalised,
- candidates with extra unmatched tokens are slightly penalised,
- shared rare bigrams receive the same kind of reward,
- common evidence inside the candidate set contributes little.

This is why the second pass is often helpful for same-street or same-building
competitions where multiple candidates already look plausible under the main
Splink model.

## 4. Final best-match selection

After the local re-ranking, [../../../post_linkage/analyse_results.py](../../../post_linkage/analyse_results.py) computes:

| Output | Meaning |
| --- | --- |
| `candidate_rank` | rank of each candidate within one messy row |
| `distinguishability` | `top match_weight - next best match_weight` |
| `distinguishability_category` | bucketed human-readable label |

If there is only one plausible candidate left, `distinguishability` is `NULL`.

The final stage then keeps only `candidate_rank = 1` and applies:

- `final_match_weight_threshold`
- `final_distinguishability_threshold`

before emitting the final match. A `NULL` distinguishability passes the
distinguishability filter, so a single-candidate match is not rejected purely
for lacking a runner-up to compare against.

## 5. The three rarity signals in the current system

This is the key architectural point.

The current system has three distinct rarity mechanisms, and they are not doing
the same job.

| Mechanism | Scope | What it measures |
| --- | --- | --- |
| numeric TF adjustment | global, pre-baked table | whether a matched number is common or rare across a fixed global frequency table |
| signature evidence from inverted index | pair-specific, pre-ranking | rare shared bigram and trigram overlap between messy and candidate |
| distinguishing-token re-ranking | local per messy row | whether a token or bigram distinguishes one candidate from its direct rivals |

This is why removing one of them is not automatically safe.

## 6. Could inverted-index evidence replace TF adjustment?

Short answer: not cleanly, and not yet.

### 6.1 Why a full replacement is risky

The numeric TF adjustment and the inverted-index signal operate on different
information.

Numeric TF adjustment is especially useful when the decisive evidence is in the
house, flat, or secondary numeric tokens. It tells the model that matching on a
rare number is more informative than matching on `1`, `2`, `10`, or another
very common value.

The inverted-index signal does not directly model that same question.

It rewards rare shared bigrams and trigrams in the cleaned address string. That
is powerful lexical evidence, but it is not a direct substitute for global
number rarity.

### 6.2 Where the inverted index is already the main signal

In one sense, it already is a main signal.

- it drives candidate generation through `exploding_unique_ids`,
- it adds a direct `signature_evidence` comparison inside the Splink model.

So the inverted index is already doing real work in both recall and ranking.

What it is not doing is replacing the specific numeric-frequency calibration
currently provided by TF adjustment.

### 6.3 What would probably happen if TF adjustment were removed today

The most likely effect is:

- little change on cases dominated by rare lexical tokens,
- weaker calibration on rows where the main agreement is numeric,
- more confusion among nearby addresses sharing the same street and postcode,
- the biggest risk on flatted and dense urban addresses where numeric tokens
  carry a lot of the discriminative load.

### 6.4 A more realistic approach

If you want to simplify this area, the safer path is:

1. keep the negative numeric mismatch comparisons,
2. benchmark removing only the TF adjustment terms from the numeric exact-match
   levels,
3. compare PR-AUC, fixed-threshold accuracy, and the overlay charts,
4. only remove TF fully if the regression is genuinely negligible.

If the long-term goal is to make the inverted index carry more of the scoring,
the better replacement would be a more explicit rarity feature for numeric or
structured tokens, not just a blanket assumption that bigram and trigram
signature evidence covers the same ground.

## 7. Practical conclusion

The current Splink stage is best understood as:

- a global probabilistic model over cleaned address fields,
- plus a local candidate-group re-ranker,
- plus a final best-versus-next-best confidence filter.

The inverted index is already an important part of that design, but it is not a
drop-in replacement for numeric TF adjustment.

If we want to test that hypothesis properly, we should do it as a narrow
benchmark change rather than as an immediate code simplification.
