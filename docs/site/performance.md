# Performance overview

## Timings

Indicative timings for matching 100,000 messy addresses are as follows.

Note that runtimes depend on whether the canonical data covers a local council region or the whole UK.

| Task | Local council region | Full country |
|------|---------------------|--------------|
| 1. Create data package and API key | 5 minutes | 5 minutes |
| 2. Install Python, uv, and `uk_address_matcher` | 5 minutes | 5 minutes |
| 3. Download and process OS data into a flat file | 5 seconds[^1] | 4 minutes[^2] |
| 4. Pre-process indexes, features, road catalogue, and road keys | Not necessary | About 10 minutes[^3] |
| 5. Match 100,000 records | 18 seconds | 30 seconds |
| Total one-off setup (steps 1-4) | About 10 min 20 sec[^4] | About 42 minutes[^4] |

[^1]: Plus ~15 seconds to download the data.
[^2]: Plus ~18 minutes to download the data.
[^3]: See [Canonical preparation runtime](#canonical-preparation-runtime).
[^4]: Approximate total from the component timings above, including download time
    and excluding the matching step.

These timings are measured on a MacBook Pro M4 Max.

Steps 1-3 are one-off; subsequent matching runs only require step 5 (or steps 4-5 for the full UK dataset).

## Benchmarking

There many different address matching solutions and it can be hard to compare performance between them.

Luckily, there are several open datasets of labelled address data that can be used to benchmark accuracy.

In this section, we set out `uk_address_matcher`'s accuracy against these labelled datasets.

The results and operating-point curves below are from the
[v2.0.0.dev3 release validation](https://github.com/moj-analytical-services/uk_address_matcher/blob/build/ukam_2.0/docs/runtime/v2.0.0.dev3/report.md).

For each benchmark, note that we:

- Filter out rows from the messy dataset where the labelled UPRN does not exist
  in the selected canonical dataset. We could not be expected to match these.
- Filter the canonical dataset to the relevant local-authority region and
  property classification.

### Hackney Council data

The Hackney Council dataset is available [here](https://www.datadaptive.com/addr/.)

<details>
<summary>Expand to see the Hackney benchmarking script</summary>

```python
from benchmarking.config.datasets import (
    filter_dataset_to_canonical_labels,
    load_dataset,
)
from benchmarking.settings import CANONICAL_PATH
from benchmarking.utils.io import setup_connection
from uk_address_matcher import (
    AddressMatcher,
    ExactMatchStage,
    PeeledAddressStage,
    SplinkStage,
)
from uk_address_matcher.prepare_canonical import load_prepared_canonical_data

canonical_filter = (
    "lowertierlocalauthoritygsscode = 'E09000012' "
    "AND substr(classificationcode, 1, 1) = 'R'"
)
con = setup_connection()
messy = load_dataset(con, dataset_key="hackney")
prepared = load_prepared_canonical_data(
    CANONICAL_PATH, con, canonical_address_filter=canonical_filter
)
messy = filter_dataset_to_canonical_labels(con, messy, prepared.addresses)

result = AddressMatcher(
    canonical_addresses=CANONICAL_PATH,
    canonical_address_filter=canonical_filter,
    addresses_to_match=messy,
    con=con,
    stages=[
        ExactMatchStage(True),
        PeeledAddressStage(),
        SplinkStage(acceptance_profile="balanced", minimum_match_weight=10),
    ],
).match()
result.accuracy_analysis(output_type="table", add_metrics=["f1"])
```

</details>

It achieves:

- MW10: **99.80% precision and 99.69% recall**
- MW20: **99.86% precision and 98.79% recall**

The full precision-recall curves are shown below:

```vegalite
{
    "schema-url": "assets/charts/hackney_precision_recall.json"
}
```

### Suppressing the postcode from the Hackney data

This uses the same [Hackney Council dataset](https://www.datadaptive.com/addr/),
with the postcode set to `NULL` in the messy input before matching.

<details>
<summary>Expand to see the postcode-suppressed benchmarking script</summary>

```python
from benchmarking.config.datasets import (
    filter_dataset_to_canonical_labels,
    load_dataset,
)
from benchmarking.settings import CANONICAL_PATH
from benchmarking.utils.io import setup_connection
from uk_address_matcher import (
    AddressMatcher,
    ExactMatchStage,
    PeeledAddressStage,
    SplinkStage,
)
from uk_address_matcher.prepare_canonical import load_prepared_canonical_data

canonical_filter = (
    "lowertierlocalauthoritygsscode = 'E09000012' "
    "AND substr(classificationcode, 1, 1) = 'R'"
)
con = setup_connection()
hackney = load_dataset(con, dataset_key="hackney")
messy = con.sql("""
    SELECT
        unique_id,
        address_concat,
        ukam_label,
        CAST(NULL AS VARCHAR) AS postcode
    FROM hackney
""")
prepared = load_prepared_canonical_data(
    CANONICAL_PATH, con, canonical_address_filter=canonical_filter
)
messy = filter_dataset_to_canonical_labels(con, messy, prepared.addresses)

result = AddressMatcher(
    canonical_addresses=CANONICAL_PATH,
    canonical_address_filter=canonical_filter,
    addresses_to_match=messy,
    con=con,
    stages=[
        ExactMatchStage(True),
        PeeledAddressStage(),
        SplinkStage(acceptance_profile="balanced", minimum_match_weight=10),
    ],
).match()
result.accuracy_analysis(output_type="table", add_metrics=["f1"])
```

</details>

It achieves:

- MW10: **99.65% precision and 86.70% recall**
- MW20: **99.89% precision and 66.89% recall**

The full precision-recall curves are shown below:

```vegalite
{
    "schema-url": "assets/charts/hackney_precision_recall_postcode_suppressed.json"
}
```

### Mid Sussex District Council business rates data

The dataset is available [here](https://www.midsussex.gov.uk/housing-council-tax/council-tax-benefits-and-business-rates/business-rates/open-data-business-rates/).

<details>
<summary>Expand to see the Mid Sussex benchmarking script</summary>

```python
from benchmarking.config.datasets import (
    filter_dataset_to_canonical_labels,
    load_dataset,
)
from benchmarking.settings import CANONICAL_PATH
from benchmarking.utils.io import setup_connection
from uk_address_matcher import (
    AddressMatcher,
    ExactMatchStage,
    PeeledAddressStage,
    SplinkStage,
)
from uk_address_matcher.prepare_canonical import load_prepared_canonical_data

canonical_filter = (
    "lowertierlocalauthoritygsscode = 'E07000228' "
    "AND substr(classificationcode, 1, 1) = 'C'"
)
con = setup_connection()
messy = load_dataset(con, dataset_key="mid_sussex")
prepared = load_prepared_canonical_data(
    CANONICAL_PATH, con, canonical_address_filter=canonical_filter
)
messy = filter_dataset_to_canonical_labels(con, messy, prepared.addresses)

result = AddressMatcher(
    canonical_addresses=CANONICAL_PATH,
    canonical_address_filter=canonical_filter,
    addresses_to_match=messy,
    con=con,
    stages=[
        ExactMatchStage(True),
        PeeledAddressStage(),
        SplinkStage(acceptance_profile="balanced", minimum_match_weight=10),
    ],
).match()
result.accuracy_analysis(output_type="table", add_metrics=["f1"])
```

</details>

It achieves:

- MW10: **96.63% precision and 88.38% recall**
- MW20: **97.68% precision and 81.79% recall**

The full precision-recall curves are shown below:

```vegalite
{
    "schema-url": "assets/charts/mid_sussex_precision_recall.json"
}
```

### Rhondda Cynon Taf data

The raw council-tax file used for this benchmark is not publicly hosted here.

<details>
<summary>Expand to see the Rhondda Cynon Taf benchmarking script</summary>

```python
from benchmarking.config.datasets import (
    filter_dataset_to_canonical_labels,
    load_dataset,
)
from benchmarking.settings import CANONICAL_PATH
from benchmarking.utils.io import setup_connection
from uk_address_matcher import (
    AddressMatcher,
    ExactMatchStage,
    PeeledAddressStage,
    SplinkStage,
)
from uk_address_matcher.prepare_canonical import load_prepared_canonical_data

canonical_filter = (
    "lowertierlocalauthoritygsscode = 'W06000016' "
    "AND substr(classificationcode, 1, 1) = 'R'"
)
con = setup_connection()
messy = load_dataset(con, dataset_key="rhondda")
prepared = load_prepared_canonical_data(
    CANONICAL_PATH, con, canonical_address_filter=canonical_filter
)
messy = filter_dataset_to_canonical_labels(con, messy, prepared.addresses)

result = AddressMatcher(
    canonical_addresses=CANONICAL_PATH,
    canonical_address_filter=canonical_filter,
    addresses_to_match=messy,
    con=con,
    stages=[
        ExactMatchStage(True),
        PeeledAddressStage(),
        SplinkStage(acceptance_profile="balanced", minimum_match_weight=10),
    ],
).match()
result.accuracy_analysis(output_type="table", add_metrics=["f1"])
```

</details>

It achieves:

- MW10: **99.38% precision and 98.35% recall**
- MW20: **99.60% precision and 96.46% recall**

The full precision-recall curves are shown below:

```vegalite
{
    "schema-url": "assets/charts/rhondda_precision_recall.json"
}
```

### Aberdeenshire data

The raw council-tax file used for this benchmark is not publicly hosted here.

<details>
<summary>Expand to see the Aberdeenshire benchmarking script</summary>

```python
from benchmarking.config.datasets import (
    filter_dataset_to_canonical_labels,
    load_dataset,
)
from benchmarking.settings import CANONICAL_PATH
from benchmarking.utils.io import setup_connection
from uk_address_matcher import (
    AddressMatcher,
    ExactMatchStage,
    PeeledAddressStage,
    SplinkStage,
)
from uk_address_matcher.prepare_canonical import load_prepared_canonical_data

canonical_filter = (
    "lowertierlocalauthoritygsscode = 'S12000034' "
    "AND substr(classificationcode, 1, 1) = 'R'"
)
con = setup_connection()
messy = load_dataset(con, dataset_key="aberdeenshire")
prepared = load_prepared_canonical_data(
    CANONICAL_PATH, con, canonical_address_filter=canonical_filter
)
messy = filter_dataset_to_canonical_labels(con, messy, prepared.addresses)

result = AddressMatcher(
    canonical_addresses=CANONICAL_PATH,
    canonical_address_filter=canonical_filter,
    addresses_to_match=messy,
    con=con,
    stages=[
        ExactMatchStage(True),
        PeeledAddressStage(),
        SplinkStage(acceptance_profile="balanced", minimum_match_weight=10),
    ],
).match()
result.accuracy_analysis(output_type="table", add_metrics=["f1"])
```

</details>

It achieves:

- MW10: **99.30% precision and 98.43% recall**
- MW20: **99.51% precision and 97.54% recall**

The full precision-recall curves are shown below:

```vegalite
{
    "schema-url": "assets/charts/aberdeenshire_precision_recall.json"
}
```

## Canonical preparation runtime

The v2.0.0.dev3 release validation processed 73,058,999 canonical rows in
**about 10 minutes**.
