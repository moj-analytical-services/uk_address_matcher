# Quantised Shared-Token Information

## Summary

This experiment tests a smaller representation of the rarity-overlap signal used
by Splink when comparing shared address tokens. The existing feature stores a
histogram keyed by a token and its floating-point relative frequency:

```text
MAP(STRUCT(tok VARCHAR, rel_freq DOUBLE), UBIGINT)
```

The proposed representation stores the token as the key and a quantised inverse
frequency plus token multiplicity as the value:

```text
MAP(
    VARCHAR,
    STRUCT(
        idf_q USMALLINT,
        token_count UTINYINT
    )
)
```

The quantised inverse frequency is calculated only at the final histogram stage
of canonical cleaning. Earlier cleaning decisions continue to use the precise
relative frequency, so unusual-token bands, common-end-token handling and
numeric-token fallbacks are unchanged.

## What It Does In Practice

For each candidate pair, the new comparison:

1. Intersects the token keys from the two `token_idf_q_hist` maps.
2. Uses the lower quantised IDF when the two sides disagree.
3. Uses the lower token count from the two sides.
4. Multiplies IDF by the shared count for each token.
5. Sums those contributions into an unsigned integer score.
6. Assigns the score to ordered Splink comparison levels.

The score is measured in units of $1/256$ of a decimal inverse-frequency order.
The human-readable value is therefore `shared_token_information_q / 256.0`.

The experiment includes these controls:

- the unchanged floating-point baseline;
- a fine-grained quantised control, which tests the representation and arithmetic
  while retaining the existing reachable-level weights;
- the proposed reduced-band quantised comparison;
- complete removal of the shared-token comparison.

The fine-grained control is important because it separates quantisation effects
from the effects of merging many old levels into eight broad bands.

## End-To-End Results So Far

The benchmark used Hackney with the existing deterministic stages and production
match-weight threshold. The rebuilt canonical contained 71,438,939 rows.

| Variant | Precision | Recall | Correct matches |
| --- | ---: | ---: | ---: |
| Existing canonical baseline | 0.994565 | 0.985845 | 112,550 |
| Rebuilt canonical baseline | 0.994565 | 0.985828 | 112,548 |
| Fine-grained quantised control | 0.994565 | 0.985828 | 112,548 |
| Reduced-band quantised candidate | 0.994671 | 0.984172 | 112,359 |
| Complete ablation | 0.996398 | 0.947349 | 108,155 |

The rebuilt baseline is effectively equivalent to the existing baseline. It has
the same precision and loses only two correct matches, with recall decreasing by
0.0017 percentage points.

The fine-grained quantised control has the same aggregate precision, recall and
correct-match count as the rebuilt baseline. This indicates that the observed
movement in the reduced-band candidate comes from the broad comparison bands,
not from quantisation itself.

The reduced-band candidate is faster and slightly more precise, but recall falls
by 0.1656 percentage points and 189 correct matches are lost relative to the
rebuilt baseline. It should not yet be promoted under the proposed no-material-
regression recall gate.

The ablation loses 4,393 correct matches and 3.8479 percentage points of recall
relative to the rebuilt baseline. This shows that the rarity-overlap feature
provides useful independent evidence, even though the first reduced-band weight
configuration is too aggressive.

## Storage Result

The experiment was expected to reduce the canonical file size by replacing
floating-point values with small integer values. The measured result does not
show a saving at this stage.

| Canonical file | Size in bytes | Approximate size |
| --- | ---: | ---: |
| Existing baseline | 1,658,350,807 | 1.5 GiB |
| Rebuilt file with `token_idf_q_hist` only | 1,797,677,742 | 1.7 GiB |

The new file is 139,326,935 bytes larger, an increase of 8.40%.

This is a measured result, not a projection. Parquet compression depends on the
physical encoding, map layout, token ordering and surrounding columns, so the
smaller logical integer types do not guarantee a smaller compressed file. The
experiment also showed that the intermediate file containing both histogram
columns was larger still, as expected.

The total directory sizes are not directly comparable because the old prepared
canonical directory contains additional unrelated artefacts. The canonical
address Parquet files are the valid comparison for this feature-size question.

## Implementation Boundary

The quantised map is produced in:

`uk_address_matcher/cleaning/steps/term_frequencies.py`

The shared-token SQL and reduced-band comparison are defined in:

`uk_address_matcher/linking_model/comparisons/shared_token_information.py`

The generated model variants are under:

`benchmarking/model_variants/`

The packaged production model remains unchanged. The next useful work is to
review the reduced-band threshold and weight design, while retaining the fine
control as the arithmetic and representation control.