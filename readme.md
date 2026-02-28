# High performance address matcher

Fast, simple address matching (geocoding) in Python.

The key features are:
- **Simple**: Python only, set up in seconds on any laptop, no infrastructure needed
- **Fast**: Match 100,000 addresses in around 30 seconds*
- **Reproducible benchmarks**:  High accuracy, demonstrated with reproducible examples

\* Timings based on a Macbook M4 Max.

## Installation

```bash
pip install --pre uk_address_matcher
```

## Usage

`uk_address_matcher` assumes you have two tables in the following format:

| unique_id | address_concat                          |
|-----------|-----------------------------------------|
| 1         | 123 Fake Street, Faketown, FA1 2KE      |
| 2         | 456 Other Road, Otherville, NO1 3WY     |
| ...       | ...                                     |


Generally one dataset will be a dataset of 'messy addresses' which need matching, and the second will be a 'canonical dataset' of addresses to match to, such as Ordnance Survey Addressbase or NGD.




### Basic Matching

> [!NOTE]`
> Two runnable examples with live sample data are included for experimentation:
> - [`examples/example_matching.py`](./examples/example_matching.py): End-to-end matching example, including loading data, running the matcher, and previewing results.
> - [`examples/example_prepare_canonical.py`](./examples/example_prepare_canonical.py): Example of preparing a canonical dataset for repeated use, demonstrating how to persist prepared data to disk and load it for matching.
>
> Both use parquet files in [`example_data/`](./example_data/) so you can run and adapt them immediately. You will need to download the example data from the releases page to run them, or you can adapt the code to use your own data.

```python
import duckdb

from uk_address_matcher import AddressMatcher, ExactMatchStage, SplinkStage

con = duckdb.connect()

df_canonical = con.read_parquet("your_canonical_addresses.parquet")
df_messy = con.read_parquet("your_messy_addresses.parquet")

matcher = AddressMatcher(
    canonical_addresses=df_canonical,
    addresses_to_match=df_messy,
    con=con,
)

result = matcher.match()  # returns a DuckDBPyRelation
result.limit(10).show(max_width=500)
```

The default stages are `ExactMatchStage` followed by `SplinkStage`. You can
customise them by passing your own `stages` list:

```python
from uk_address_matcher import (
    AddressMatcher,
    ExactMatchStage,
    SplinkStage,
    UniqueTrigramStage,
)

matcher = AddressMatcher(
    canonical_addresses=df_canonical,
    addresses_to_match=df_messy,
    con=con,
    stages=[
        ExactMatchStage(),
        UniqueTrigramStage(),
        SplinkStage(
            final_match_weight_threshold=20.0,
            final_distinguishability_threshold=5.0,
        ),
    ],
)

result = matcher.match()
```

### Additional columns

You may also provide a separate column called `postcode`, which, if provided will take precidence over any postcode information provided in `address_concat`.

If you have labelled data (you know the ground truth), you may provide a column called `ukam_label`, if provided, this will propagate through your results for accuracy analysis.

### Pre-preparing canonical data

Cleaning a large canonical dataset (e.g. AddressBase) is expensive. Use
`prepare_canonical_folder` to do it once and write the artefacts to disk.
Subsequent runs load the prepared folder directly, skipping cleaning entirely.

```python
from uk_address_matcher import AddressMatcher, prepare_canonical_folder

# One-time preparation
prepare_canonical_folder(
    df_canonical,
    output_folder="./ukam_prepared_canonical",
    con=con,
    overwrite=True,
)

print("Prepared canonical data written to ./ukam_prepared_canonical/")

# Fast matching — pass the folder path instead of a relation
matcher = AddressMatcher(
    canonical_addresses="./ukam_prepared_canonical",
    addresses_to_match=df_messy,
    con=con,
)

result = matcher.match()
```

### Matching one or more AddressRecord entries

If you want to match a small number of addresses, or you have them in-memory as Python dictionaries, you can pass them directly as `addresses_to_match` without needing to create a DuckDB relation first.

You can pass a list of `AddressRecord` entries directly as
`addresses_to_match`. The matcher also accepts a list of dicts with
`address_concat`, `postcode`, and `unique_id`, or a DuckDB relation.

```python
import duckdb

from uk_address_matcher import AddressMatcher, AddressRecord

con = duckdb.connect()

df_canonical = con.read_parquet("your_canonical_addresses.parquet")

records = [
    AddressRecord(
        unique_id="m_1",
        address_concat="10 downing street westminster london",
        postcode="SW1A 2AA",
    ),
    AddressRecord(
        unique_id="m_2",
        address_concat="11 downing street westminster london",
        postcode="SW1A 2AB",
    ),
]

matcher = AddressMatcher(
    canonical_addresses=df_canonical,
    addresses_to_match=records,
    con=con,
)

result = matcher.match()
```


## Methodology

The Splink phase uses a two-pass approach to achieve high accuracy matching:

1. **First Pass**: A standard probabilistic linkage model using Splink generates candidate matches for each input address.

2. **Second Pass**: Within each candidate group, the model analyzes distinguishing tokens to refine matches:
   - Identifies tokens that uniquely distinguish addresses within a candidate group
   - Detects "punishment tokens" (tokens in the messy address that don't match the current candidate but do match other candidates)
   - Uses this contextual information to improve match scores

This approach is particularly effective when matching to a canonical (deduplicated) address list, as it can identify subtle differences between very similar addresses.



## Development

The scripts and tests will run better if you create .vscode/settings.json with the following:

```json
{
    "jupyter.notebookFileRoot": "${workspaceFolder}",
    "python.analysis.extraPaths": [
        "${workspaceFolder}"
    ],
    "python.testing.pytestEnabled": true,
    "python.testing.unittestEnabled": false,
    "python.testing.pytestArgs": [
        "-v",
        "--capture=tee-sys"
    ]
}
```

