# 6. Match weight and distinguishability acceptance

Date: 2026-11-18

## Status

Proposed

## Context

Splink ranks canonical address candidates using match weight (MW). The leading
candidate can still be wrong when its score is close to another candidate's.
Distinguishability is the gap between the top candidate and the next-best
candidate; a small gap means the result depends on weak separation between
plausible addresses.

For example, a fictional input such as
`LOWER GREEN FARM TREORCH MID GLAM` could produce two candidates:

| Candidate | MW |
| --- | ---: |
| `GREEN COTTAGE, LOWER GREEN FARM, TREORCH` | 15 |
| `FARMHOUSE, LOWER GREEN FARM, TREORCH` | 13 |

The top score is above a nominal MW floor of 10, but the gap is only 2. The
input does not contain enough information to decide reliably between a cottage
and a farmhouse. Conversely, `BASEMENT FLAT 7E EXAMPLE STREET` may score 10
against `FLAT 7 EXAMPLE STREET` with no credible alternative candidate. A gap
above 10 makes acceptance more defensible despite a possible input error.

A threshold on MW alone therefore ignores useful evidence. Hard-coded staircase
rules express this idea, but are difficult to explain and expose as a public
setting. Experiments on the pooled Hackney, Rhondda and Aberdeenshire extracts
support a continuous MW/distinguishability boundary, with separate profiles for
balanced and precision-focused outcomes.

## Decision

Add an acceptance profile to `SplinkStage` for the two user outcomes that
are meaningfully different:

| Mode | Behaviour |
| --- | --- |
| `"balanced"` | "I want the best overall trade-off": require the versioned linear boundary currently proposed as `MW + 1.7 * distinguishability >= 13.2`, selected from the fixed-MW benchmark as the F1-oriented profile. |
| `"precision"` | "I care most about avoiding false matches": apply the conservative decaying requirement currently proposed as `distinguishability >= max(0, 8 - 0.2 * MW)`. |

Do not add a third mode for returning every available top-1 candidate. Its
behaviour is sufficiently similar to the balanced profile once the
distinguishability minimum excludes ties, and realised recall still depends on
candidate generation, the MW floor and the correctness of those winners.

Retain two independent, user-facing controls:

```python
SplinkStage(
    acceptance_profile="balanced",
    minimum_match_weight=10,
    minimum_distinguishability=0.1,
)
```

The profile selects the formula shape. A match must also satisfy
`minimum_match_weight` and `minimum_distinguishability`. The defaults are
`balanced` and `0.1` respectively, alongside the existing permissive default MW
minimum.

The corresponding refined user stories are:

- As a user who wants the best overall operating point, I want an
  automatically benchmarked MW/distinguishability trade-off so that F1 is
  prioritised for my chosen minimums. The coefficients are selected during
  release benchmarking, not recomputed for every match.
- As a user for whom false matches are costly, I want a precision-first
  policy so that ambiguous winners are left unmatched.

The mode-specific coefficients are implementation detail and should be
versioned and benchmarked. They are not calibrated match probabilities. A
single numeric "strictness" setting is not adopted because interpolation
between these formulae does not have a validated monotonic meaning.

Candidate generation, retention and reranking remain unchanged. Acceptance is
applied only after the winner and distinguishability have been calculated.
The API should use `balanced` and `precision` rather than `None`, `more_recall`,
`maximum_precision` or `coverage`.

## Consequences

- `balanced` provides a mild false-positive reduction without changing the
  user-controlled MW floor.
- `precision` provides a stricter option when avoiding a false match is
  materially more costly than leaving a record unmatched.
- Stricter profiles remove records from circulation. They can improve precision
  while reducing recall, so profile choice must reflect the cost of false
  matches relative to unmatched records.
- Coefficients and defaults must be revalidated when the model, reranker,
  canonical data or labelled holdouts change. Council-level and overall
  precision, recall, F1, TP, FP and unmatched counts should be recorded.
- NULL or single-candidate distinguishability needs explicit, tested semantics.

This decision is an acceptance-policy change, not a claim that MW and
distinguishability alone produce a calibrated probability. See
[#671](https://github.com/moj-analytical-services/uk_address_matcher/issues/671)
and the
[pooled acceptance-policy trial](../findings/mw_distinguishability_acceptance_policy_trial.md).
