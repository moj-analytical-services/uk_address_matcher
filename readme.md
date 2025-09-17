# High performance UK addresses matcher (geocoder)

Extremely fast address matching using a pre-trained [Splink](https://github.com/moj-analytical-services/splink) model.

```
Full time taken: 11.05 seconds
to match 176,640 messy addresses to 273,832 canonical addresses
at a rate of 15,008 addresses per second

(On Macbook M4 Max)
```

## Installation

At the moment this uses a branch of Splink only available on Github.
```bash
pip install --pre uk_address_matcher
```

## Usage

High performance address matching using a pre-trained [Splink](https://github.com/moj-analytical-services/splink) model.

Assuming you have two duckdb dataframes in this format:

| unique_id | address_concat               | postcode  |
|-----------|------------------------------|-----------|
| 1         | 123 Fake Street, Faketown    | FA1 2KE   |
| 2         | 456 Other Road, Otherville   | NO1 3WY   |
| ...       | ...                          | ...       |


### Basic Matching

Match them with:

```python
import duckdb

from uk_address_matcher import (
    clean_data_using_precomputed_rel_tok_freq,
    get_linker,
    best_matches_with_distinguishability,
    improve_predictions_using_distinguishing_tokens,
)

p_ch = "./example_data/companies_house_addresess_postcode_overlap.parquet"
p_fhrs = "./example_data/fhrs_addresses_sample.parquet"

con = duckdb.connect(database=":memory:")

df_ch = con.read_parquet(p_ch).order("postcode")
df_fhrs = con.read_parquet(p_fhrs).order("postcode")

df_ch_clean = clean_data_using_precomputed_rel_tok_freq(df_ch, con=con)
df_fhrs_clean = clean_data_using_precomputed_rel_tok_freq(df_fhrs, con=con)

linker = get_linker(
    df_addresses_to_match=df_fhrs_clean,
    df_addresses_to_search_within=df_ch_clean,
    con=con,
    include_full_postcode_block=True,
    additional_columns_to_retain=["original_address_concat"],
)

# First pass - standard probabilistic linkage
df_predict = linker.inference.predict(
    threshold_match_weight=-50
)
df_predict_ddb = df_predict.as_duckdbpyrelation()

# Second pass - improve predictions using distinguishing tokens

df_predict_improved = improve_predictions_using_distinguishing_tokens(
    df_predict=df_predict_ddb,
    con=con,
    match_weight_threshold=-20,
)

# Find best matches within group and compute distinguishability

best_matches = best_matches_with_distinguishability(
    df_predict=df_predict_improved,
    df_addresses_to_match=df_fhrs,
    con=con,
)

best_matches

```


### Two-Pass Matching Approach

The package uses a two-pass approach to achieve high accuracy matching:

1. **First Pass**: A standard probabilistic linkage model using Splink generates candidate matches for each input address.

2. **Second Pass**: Within each candidate group, the model analyzes distinguishing tokens to refine matches:
   - Identifies tokens that uniquely distinguish addresses within a candidate group
   - Detects "punishment tokens" (tokens in the messy address that don't match the current candidate but do match other candidates)
   - Uses this contextual information to improve match scores

This approach is particularly effective when matching to a canonical (deduplicated) address list, as it can identify subtle differences between very similar addresses.



Refer to [the example](example_matching.py), which has detailed comments, for how to match your data.

See [an example of comparing two addresses](example_compare_two.py) to get a sense of what it does/how it scores

Run an interactive example in your browser:

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/RobinL/uk_address_matcher/blob/main/match_example_data.ipynb)  Match 5,000 FHRS records to 21,952 companies house records in < 10 seconds.

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/RobinL/uk_address_matcher/blob/main/interactive_comparison.ipynb) Investigate and understand how the model works



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

