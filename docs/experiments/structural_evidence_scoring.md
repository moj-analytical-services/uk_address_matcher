# Structural evidence scoring

The four adjustments are applied in
`post_linkage/distinguishing_features/structural_evidence.py`, after the existing
distinguishing-token reranker and before relation-marker scoring, distinguishability,
thresholding, and final ranking. Each adjustment is retained as a named score component
in log2 Bayes-factor units.

1. The distinguishing-token reranker identifies the positive bigram reward attributable
   only to numeric, unit, floor, or layout tokens. The structural-evidence stage removes
   that component only when no canonical variant for the candidate UPRN has the source's
   exact normalised full postcode. Negative and mixed substantive bigram evidence is not
   changed.
2. The stage subtracts eight bits when every canonical variant lacks both exact full
   postcode evidence and substantive token overlap.
3. The stage subtracts three bits for an unambiguous explicit flat-letter contradiction
   when a competing UPRN supports the source letter.
4. The stage subtracts three bits when the candidate has no exact-postcode variant,
   `gamma_postcode <= 2`, and `gamma_address_without_numbers = 0`.

Postcode, substantive-token, and flat-letter evidence are grouped by source record and
canonical `unique_id` before scoring, so all variants of a UPRN contribute. The shared
token taxonomy lives in `post_linkage/token_classification.py` and is used by both the
reranker and the structural-evidence stage.

No Splink comparison configuration changed, so `data/splink_model.json` did not require
regeneration. Candidate generation, the top-five reranker limit, final score threshold,
and deterministic tie-breaking remain unchanged.

Run the four-authority labelled validation with:

```bash
uv run python -m benchmarking.validate_structural_evidence
```

It writes an ID-only changed-winner audit and JSON/Markdown summaries under
`benchmarking/results_structural_evidence_scoring/`.

## Clean-main benchmark

### Question

Do the four adjustments improve precision-recall performance in each labelled
authority relative to a clean checkout of `main`?

### Runs compared

- Baseline: `main-1fbaa19`, built from commit `1fbaa19` in a clean worktree.
- Current: `feature-structural-evidence-scoring`.
- Authorities: Hackney, Rhondda Cynon Taf, Aberdeenshire, and Mid Sussex.
- Date: 2026-08-13.

The comparison used the same labelled inputs, prepared canonical data, canonical
filters, production stage order, top-five reranker limit, and final match-weight
threshold. The prepared canonical data was not rebuilt because the change only
affects post-linkage scoring. No Splink comparison or threshold changed.

### Headline metrics

| Authority | PR-AUC baseline | PR-AUC current | PR-AUC delta | Correct-match delta | Precision delta | Recall delta |
|---|---:|---:|---:|---:|---:|---:|
| Hackney | 0.572984 | 0.573523 | +0.000539 | +59 | +0.0741 pp | +0.0517 pp |
| Rhondda | 0.310175 | 0.310112 | -0.000063 | -8 | +0.0026 pp | -0.0071 pp |
| Aberdeenshire | 0.141238 | 0.141238 | 0.000000 | 0 | 0.0000 pp | 0.0000 pp |
| Mid Sussex | 0.665033 | 0.665282 | +0.000249 | +1 | +0.1609 pp | +0.0221 pp |

The result is mixed and does not establish unambiguous improvement in all four
authorities. Hackney and Mid Sussex improve, Aberdeenshire is unchanged, and
Rhondda has a small PR-AUC and recall regression. In particular, the Rhondda
curve crosses the baseline curve rather than dominating it.

The fixed-threshold Rhondda precision increase comes from greater abstention,
not better ranking throughout the curve: the feature run emits 11 fewer Splink
matches, while recall falls by 0.0071 percentage points. One additional
`peeled_address_stripped` difference occurred outside the changed scoring stage
and is treated as run-to-run noise in the end-to-end comparison.

### Rule attribution

Sequential replay across 130,153 probabilistic candidate groups found 163 winner
changes, comprising 121 label-agreement fixes and 18 harms. The first three rules
accounted for 106 fixes and four harms. The weak address-and-postcode rule
accounted for 15 fixes and 14 harms.

All 14 Rhondda harms came from the weak address-and-postcode rule and form one
internally conflicting block. The source street text and labelled UPRNs identify
Sandybank Road, while the source postcodes exactly identify same-number rivals on
Pontrhondda Road. Every Sandybank canonical variant has the bottom
`address_without_numbers` comparison level and a non-matching postcode, so UPRN
variant aggregation does not change the rule outcome. All 14 candidates remain
below the configured production threshold before and after scoring; they affect
the raw-label precision-recall curve but do not create emitted wrong matches.

This replay used ordinary production blocking and source labels. The separate
4,740-row consensus-adjudication dataset used to review probabilistic cases was
not available, so its conclusions should not be conflated with this
whole-population raw-label curve.

### Artefacts

- Per-area comparison reports and primary precision-recall overlays:
   `benchmarking/results_structural_evidence_main_vs_pr/`.
- Rule-by-rule summary:
   `benchmarking/results_structural_evidence_scoring/validation_summary.md`.
- Machine-readable summary:
   `benchmarking/results_structural_evidence_scoring/validation_summary.json`.
- ID-only changed-winner audit:
   `benchmarking/results_structural_evidence_scoring/changed_winner_label_agreements.csv`.