# Choosing a matching threshold

Fuzzy matches found by `uk_address_matcher` in the `matcher.match().matches()` table have two scores associated with them: `match_weight` and `distinguishability`.[^splink-only]

By default, `SplinkStage` uses a permissive MW minimum of `-20.0`, a
distinguishability minimum of `0.1`, and the `balanced` profile. The user
should tune the acceptance policy to the quality of their input data and the
cost of a false match.

Once you have decided on a threshold, you can set it in the `SplinkStage` parameters:

```python
matcher = AddressMatcher(
    canonical_addresses=df_canonical,
    addresses_to_match=df_messy,
    con=con,
    stages=[
        ExactMatchStage(),
        PeeledAddressStage(),
        UniqueTrigramStage(),
        SplinkStage(
            acceptance_profile="balanced",
            minimum_match_weight=10.0,
            minimum_distinguishability=1.0,
        ),
    ],
)
```

## What do these scores mean?

### Match weight

`match_weight` is Splink's score for the selected candidate.

Higher values mean stronger evidence that the messy address and canonical address are the same place. Lower values are weaker matches. Negative values are often a sign that the candidate was retained only because the stage is in a very permissive mode. Raising `minimum_match_weight` filters out weak matches.

### Distinguishability

`distinguishability` is the gap in `match_weight` between the best candidate and the next best candidate for the same messy address. Higher values mean the winner is clearly ahead. Low values mean the top two candidates look similar, which is often where false positives come from. Raising `minimum_distinguishability` filters out ambiguous matches.

[^splink-only]: These scores are only calculated for `SplinkStage` matches and do not apply, for example, to `ExactMatchStage` matches.


To illustrate why we need both scores, consider the following examples.


#### Example 1:

##### Messy address:
```
Flat 165 Block 3 Rose Square, Hammersmith And Fulham, EG1 2EG
```

##### Candidates
```
Flat 165 A Block 3 Rose Square, Hammersmith And Fulham, EG1 2EG
Flat 165 B Block 3 Rose Square, Hammersmith And Fulham, EG1 2EG
```

These two candidates will both get a high match score due to the large amount of matching information, but will have zero distinguishability.

#### Example 2:

##### Messy address:
```
1 High St, AB1 2CD
```

##### Candidates
```
1 High Street, AB1 2CD
1 High Street, WX1 2YZ
```

Neither of these candidate will have a high match score because they are very short, but they will have high distinguishability due to the different postcodes.

## Choosing thresholds

Unfortunately, the best thresholds to use depend on various factors such as the quality and type of your input data (e.g. businesses vs residential), and whether you are matching to a small geographical area or the whole country.  There's therefore no one-size-fits-all answer to what thresholds to use.

As a starting point we recommend trying the following settings:

```python
SplinkStage(
    minimum_match_weight=10.0,
    minimum_distinguishability=1.0,
    acceptance_profile="balanced",
)
```

`acceptance_profile` determines how sensitive matching is to false positives:

- `balanced` requires `MW + 2.5 * distinguishability >= 11.35`. This is the
  F1-oriented decision boundary on the current release benchmark.
- `precision` also requires
  `distinguishability >= max(0, 12.85 - 0.12 * MW)`. This boundary achieved
  99.70% precision on the current pooled release benchmark.

The profile does not replace the minimum values. They still need tuning for the
input data and use case. A sole candidate with `NULL` distinguishability passes
the profile formula because there is no competing top-two gap.

To pick optimal minimums for your use case, use labelled data and a
precision-recall curve. See [performance and benchmarking](./performance.md)
for more details.

The previous `final_match_weight_threshold` and
`final_distinguishability_threshold` arguments remain as compatibility aliases,
but new code should use the minimum names.