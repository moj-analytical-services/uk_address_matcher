# Optimising accuracy

`uk_address_matcher` has a variety of settings that can be tuned to optimise accuracy dependnig on your use case.

In 'biggest wins' we describe the most important settings that are likely to result in unambiguously better accuracy.

In 'optimising match stages' we describe settings which are harder to make recommendations about because the best settings depend on the input data.

## Biggest wins

### If matching to Ordnance Survey data, use `ukam-os-builder` to prepare it for matching

In its [typical form](https://docs.os.uk/osngd/data-structure/address/gb-address/built-address), Ordnance Survey address data contains one address [UPRN](https://www.ordnancesurvey.co.uk/public/unique-property-reference-numbers) per row.

However, if processed in a specific way, Ordnance Survey contains variants on an address that provides a greater 'target area' to match to.  Our `ukam-os-builder` tool does this processing for you to increase the chance of a match. Use of this tool is documented in [this guide](ordnance_survey.md).

To illustrate why this is important, an single address could have two variants:

1. `Basement Flat, 10 Demo Road, Townton`
2. `Flat A, Example Court, 10 Demo Road, Townton`

By providing `uk_address_matcher` with a canonical address dataset built by `ukam-os-builder`, you will match against all these variants, giving more options for a high scoring match.

### Filter your input data to the smallest possible dataset

The smaller the number of addresses to match to, the more accurate your results are likely to be, because there is less chance of multiple similar candidates (such as two different '1 High Street' addresses in two different geographical locations).

Matching will also be faster, because there are fewer candidates to compare against.

There are two primary ways to filter your input data:

- **Geographically**.  If your input data comes from a known geographical area such as a local authority, use an extract of Ordnance Survey data for that area only.
- **By classification or other metadata**.  Ordnance Survey data contains rich metadata about each address, such as its [classification](https://docs.os.uk/osngd/code-lists/code-lists-overview/addressclassificationcodevalue).  For example, if you know your messy data is residential only, filter out non-residential addresses from the canonical dataset.

For Ordnance Survey data, see [Working with Ordnance Survey data](ordnance_survey.md#filtering-before-canonical-preparation) for guidance on filtering by `classificationcode`, excluding parent shells, and deciding whether a rule belongs in canonical preparation or only at match time.

In practice, the biggest wins usually come from classification first. A simple
residential filter already removes most clearly non-residential oddities such as
`ADVERTISING` and `TELEPHONE BOX`, while smaller residential ancillary groups
such as `RG`, `RB`, and `RC` are where garage, parking, and shared-space rows
tend to survive.

#### How to filter

The mechanism for filtering depends on whether you are [pre-processing your canonical dataset or processing it on the fly](https://moj-analytical-services.github.io/uk_address_matcher/get_started/#choose-whether-to-pre-process-your-canonical-dataset).

If you are processing data on-the-fly, then you can simply filter your data before passing it to `AddressMatcher`:


#### Filtering for on the fly processing

```python
import duckdb
from uk_address_matcher import AddressMatcher

con = duckdb.connect()

df_canonical = con.read_csv("path/to/canonical.csv")

# Filter to residential addresses only
df_canonical = df_canonical.filter("substr(classificationcode, 1, 1) = 'R'")
df_messy = con.read_csv("path/to/messy.csv")

matcher = AddressMatcher(
    canonical_addresses=df_canonical,
    addresses_to_match=df_messy,
    con=con,
)
result = matcher.match()

```

#### Filtering a pre-prepared datasets

If you are pre-processing your canonical dataset, first decide whether the rule is a stable policy or only an ad hoc subset.

Use this rule of thumb:

- If all users should inherit the exclusion, apply it before `prepare_canonical_folder()`.
- If different users need different subsets, keep a broader prepared folder and use `canonical_address_filter=` later.
- If the rule is about which records should be eligible to match, do not implement it by stripping text during cleaning.

If users of the preprocessed file will always want a filter applied, apply this filter before passing the data to `prepare_canonical_folder()`.

```python
import duckdb
import tempfile
from uk_address_matcher import prepare_canonical_folder

con = duckdb.connect()
df_canonical = con.read_csv("path/to/canonical.csv")
df_canonical = df_canonical.filter("substr(classificationcode, 1, 1) = 'R'")

prepare_canonical_folder(
    df_canonical,
    output_folder=tempfile.mkdtemp(),
    con=con,
)
```

For concrete recipe-style filters, including a household-style subset that
excludes `RG`, `PP`, `RB`, `RC`, and common parking prefixes, see [Working with
Ordnance Survey data](ordnance_survey.md#quick-filtering-recipes).

However, if different users will need different filters, you can also apply a filter _after_ pre-processing the whole dataset.  This will result in a small degradation in accuracy because indices and term frequencies will be computed globally, making them less discriminative.

```python

output_folder = "path_to_prepared_canonical_folder"
df_canonical = con.read_csv("path/to/canonical.csv")

prepare_canonical_folder(
    df_canonical,
    output_folder=output_folder,
    con=con
)

matcher = AddressMatcher(
    canonical_addresses=output_folder,
    addresses_to_match=df_messy,
    canonical_address_filter="substr(classificationcode, 1, 1) = 'R'",
    con=con,
)
result = matcher.match()
```

### Candidate-pool policy versus cleaning

Be careful with rules that look like cleaning rules but are really eligibility rules.

For example, if your organisation never wants to match to records such as car
park spaces, garages, or parent shells, that is usually best implemented as a
canonical filter, not as a string-cleaning step that edits address text.

As a general rule:

- use cleaning to normalise text
- use filtering to decide which canonical records may participate in matching

If you are considering a new heuristic exclusion such as `CAR PARK SPACE%`,
benchmark it first on a representative dataset and inspect the overlay
precision-recall chart before treating it as standard policy.



## Optimising match stages

`uk_address_matcher` uses an 'ensemble' methodology, whereby it sequentially applies a number of  matching strategies called 'stages'.

Stages run in order. Once a record is matched by one stage, later stages do not revisit it.  The results contain a column indicating which stage produced the match.

As a result they are intended to be ordered from highest precision to lowest precision.  For example, the first stage is usually the `ExactMatchStage`, since it makes sense to find all exact matches (full address string and postcode) prior to applying any more sophisticated/fuzzy matching strategies.


### Matching stages

The available stages are as follows:

| Stage | Type | What it is good at | Accuracy implication |
|---|---|---|---|
| `ExactMatchStage` | Deterministic | Cleaned address text is already the same on both sides | Very high precision, should usually run first |
| `DistinguishingTokenStage` | Deterministic | A locally unique canonical prefix is followed by two ordered address tokens, allowing up to two safe gaps | Substantially higher recall than peeled matching, with a small reduction in precision overall; requires an opt-in prepared canonical |
| `PeeledAddressStage` | Deterministic | One side has extra trailing locality words such as `LONDON` or `HACKNEY` | High precision, useful before probabilistic matching |
| `UniqueTrigramStage` | Deterministic | A distinctive phrase identifies one canonical address within the postcode | High precision, removes clear fuzzy cases before Splink |
| `SplinkStage` | Scored | Typos, abbreviations, partial matches, and other fuzzy cases | Precision and recall depend on threshold choice |


##### Summary recommendation

You almost always want to use the `ExactMatchStage`.  The `DistinguishingTokenStage`, `PeeledAddressStage` and `UniqueTrigramStage` produce high, but not perfect precision (i.e. there's a chance of a small number of false positives).

`DistinguishingTokenStage` requires additional canonical preparation. Enable it
when building the prepared canonical:

Across four benchmark areas, `ExactMatchStage` followed by
`DistinguishingTokenStage` achieved 76.1465% weighted recall, compared with
60.0428% for `ExactMatchStage` followed by `PeeledAddressStage`. Weighted
precision remained high but was slightly lower: 99.6010% compared with
99.6855%. The recall improvement was substantial in every benchmark area,
while the precision difference was small and varied slightly by area.

```python
prepare_canonical_folder(
    df_canonical,
    output_folder=output_folder,
    con=con,
    derive_distinguishing_wrt_adjacent_records=True,
)
```

You almost always want to use the `SplinkStage` last, to attempt to find any matches missed by the previous stages.  In some cases, it may produce higher accuracy than the `PeeledAddressStage` and `UniqueTrigramStage`, which is why you do not always want to use these stages.




```python
from uk_address_matcher import (
    AddressMatcher,
    DistinguishingTokenStage,
    ExactMatchStage,
    PeeledAddressStage,
    UniqueTrigramStage,
    SplinkStage,
)

matcher = AddressMatcher(
    canonical_addresses=df_canonical,
    addresses_to_match=df_messy,
    con=con,
    stages=[
        ExactMatchStage(),
        DistinguishingTokenStage(),
        PeeledAddressStage(),
        UniqueTrigramStage(),
        SplinkStage(
            final_match_weight_threshold=2.0,
            final_distinguishability_threshold=1.0,
        ),
    ],
)
```

## Tuning the Splink stage

For guidance on choosing Splink thresholds, see [here](choosing_a_matching_threshold.md).


## Stage API docs

### ExactMatchStage

::: uk_address_matcher.ExactMatchStage

### DistinguishingTokenStage

::: uk_address_matcher.DistinguishingTokenStage

### PeeledAddressStage

::: uk_address_matcher.PeeledAddressStage

### UniqueTrigramStage

::: uk_address_matcher.UniqueTrigramStage

### SplinkStage

::: uk_address_matcher.SplinkStage
