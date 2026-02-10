# High performance UK addresses matcher (geocoder)

Extremely fast address matching using a pre-trained [Splink](https://github.com/moj-analytical-services/splink) model.

```
Full time taken: 11.05 seconds
to match 176,640 messy addresses to 273,832 canonical addresses
at a rate of 15,008 addresses per second

(On Macbook M4 Max)
```

## Installation

```bash
pip install --pre uk_address_matcher
```

## Usage

High performance address matching using a pre-trained [Splink](https://github.com/moj-analytical-services/splink) model.

Will match two datasets provided in this format:

| unique_id | address_concat                          |
|-----------|-----------------------------------------|
| 1         | 123 Fake Street, Faketown, FA1 2KE      |
| 2         | 456 Other Road, Otherville, NO1 3WY     |
| ...       | ...                                     |


- You may also provide a separate column called `postcode`, which, if provided will trump any postcode information provided in `address_concat`.
- If you have labelled data (you know the ground truth), you may provide a column called `ukam_label`, if provided, this will propagate through your results for accuracy analysis.


Generally one dataset will be a dataset of 'messy addresses' which need matching, and the second will be a 'canonical dataset' of addresses to match to.

## Preparing AddressBase for use in `uk_address_matcher`

`uk_address_matcher` can be used to any canonical list of addresses provided in the format above.

Many users will wish to link to Ordnance Survey address products.


### Simplest route (lower accuracy)

The simplest Ordnance Survey product to use for this purpose is [NGD Built Address](https://docs.os.uk/osngd/data-structure/address/gb-address/built-address).

You can use this 'out of the box' as your canonical list of addresses by selecting data from BuiltAddress as follows:

```
select uprn as unique_id, fulladdress as address_concat
from builtaddress
where {your_filter_here}
```

And providing the result output to `uk_address_matcher`.  You will generally improve accuracy if you filter the data down to the geographical region of interest, and filter the addresses down as much as possible to include only those of interest (e.g. residential only, if you're matching residential addresses)

### Full prep (higher accuracy)

Higher accuracy can be achieve by processing Ornance Survey data in a more sophisticated way.

For instance, Ordnance Survey provides multiple representations of a single address in Addressbase Premium and also in [NGD Address](https://docs.os.uk/osngd/data-structure/address/related-components/alternate-address).

By providing multiple addresses representations of each canonical address to `uk_adress_matcher`, you will have a better chance of higher precisison matching.

We provide a recommendation for automated build scripts for how to build such a file from Addressbase Premium and the NGD datasets here:
- [AddressBase Premium build script](https://github.com/moj-analytical-services/prepare_addressbase_for_address_matching)
- [NGD build script](https://github.com/moj-analytical-services/prepare_ngd_for_address_matching)


### Basic Matching

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

### Pre-preparing canonical data

Cleaning a large canonical dataset (e.g. AddressBase) is expensive. Use
`prepare_and_persist_canonical_data` to do it once and write the artefacts to
disk. Subsequent runs load the prepared folder directly, skipping cleaning
entirely.

```python
from uk_address_matcher import AddressMatcher, prepare_canonical_folder

# One-time preparation
prepare_canonical_folder(
    df_canonical,
    output_folder="./ukam_prepared_canonical",
    con=con,
    overwrite=True,
)

# Fast matching — pass the folder path instead of a relation
matcher = AddressMatcher(
    canonical_addresses="./ukam_prepared_canonical",
    addresses_to_match=df_messy,
    con=con,
)

result = matcher.match()
```

### Matching a single address

Use `match_one` with an `AddressRecord` or a plain dict for quick lookups:

```python
from uk_address_matcher import AddressMatcher, AddressRecord

result = matcher.match_one(
    AddressRecord(
        address_concat="10 downing street westminster london",
        postcode="SW1A 2AA",
    )
)

# Or with a dict:
result = matcher.match_one({
    "address_concat": "10 downing street westminster london",
    "postcode": "SW1A 2AA",
})
```


### Two-Pass Matching Approach

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

