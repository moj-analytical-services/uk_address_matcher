## Matching your messy addresses to authoratitive UK address data

Ordnance Survey is the UK’s authoritative provider of address data.  Many public sector organisations are able to use this data for free under the [Public Sector Geospatial Agreement](https://www.ordnancesurvey.co.uk/customers/public-sector/public-sector-geospatial-agreement) (PSGA).

This guide describes our recommended end-to-end process for address matching to Ordnance Survey data, including downloading the raw data, all software installs and building a single authoritative input address file optimised for matching.

Supposing we have 100,000 messy addresses to match.  The steps and their respective timings are as follows.  Time taken dependes on whether the 100,000 are from around the whole country or a specific geographical region.  A local council area is used as an example.

| Task | Timing (Matching to local council region) | Timing (Matching to full country) |
|------|-------------------------------|----------------------|
| 1. Create a data package and corresponding API key in the [Ordnance Survey Data Hub](https://osdatahub.os.uk/data/downloads/data-packages) | 5 minutes | 5 minutes |
| 2. Install Python and Astral UV and the `uk_address_matcher` package | 5 minutes | 5 minutes |
| 3. Process Ordnance Survey data into a flatfile | 5 seconds* | 4 minutes** |
| 4. Derives indexes and other features for address matching  | Not necessary, can be done on the fly | 4 mins 50 seconds |
| 5. Use `uk_address_matcher` to match 100,000 records | 26 seconds | 46 seconds |


\* Plus 15 seconds to download the data

\** Plus 18 minutes to download the data

Timings data from processing on a Macbook Pro M4 Max.

Steps 1-3 are one-time-only jobs.  Subsequent data matching to the same geographic region only requires step 4.


## Step 1: Create a data package and obtain an API key

To download data from Ordnance Survey, you need three values:
- Data package `package_id`
- Its `version_id`; and
- An API key (the 'password' you use to download data)

Choose whether you want to use AddressBase or NGD.  Use whichever you're familiar with, but default to NGD if you've never used either.

Log in to `https://osdatahub.os.uk/` and create a [new recipe](https://osdatahub.os.uk/data/downloads/recipe-library) corresponding to the geographical area of interest.

Once created, navigate to [data packages](https://osdatahub.os.uk/data/downloads/data-packages/), and locate your data package, which will be at a URL like `https://osdatahub.os.uk/data/downloads/data-packages/18296`.

Use this URL to identify the data package ID, which in the above example is `18296`.  You also need the version id.  You can obtain this by hovering over any of the data downloads in the data packages.  The version ID is the number after the data package ID:

`https://osdatahub.os.uk/api/dataPackages/{data_package_id}/{version_id}/download?fileName=add_gb_builtaddress.zip`

Then obtain your API key and API secret from the [API Projects](https://osdatahub.os.uk/data/apis/projects) page in Data Hub.  Create a new project if one does not already exist.

## Step 2: Create a new directory for your project and install the required software

We will use `uv` to install the `uk_address_matcher` package.  Install it using the [official instructions](https://docs.astral.sh/uv/getting-started/installation/).

Create a new directory for your project, say `address_project`:

```bash
mkdir address_project
cd address_project
```

Initiate a new `uv` project:

```
uv init --bare
```

Then install `uk_address_matcher` into your project:

```
uv add uk_address_matcher
```

## Step 3: Build the optimised cannoical dataset of UK addresses


We have created a tool called `ukam-os-builder`to process Ordnance Survey data into a format optimised for address matching.  For more details of the tool, see the Github [homepage](https://github.com/moj-analytical-services/ukam_os_builder).

Using this tool is a two step process.  The first command provides a config wizard that helps you point the tool to your data package.  The second command downloads the data and builds the optimised flatfile.

```bash
# Run config wizard:
uvx --from ukam-os-builder ukam-os-setup
```

```bash
# Download the data and build the optimised flatfile:
uvx --from ukam-os-builder ukam-os-build
```

If you use the default settings, your data will now be built to: `data/output/`.  Note, unless you set `num_chunks=1`, this will be a folder containing multiple files representing a single table.  DuckDB will allow us to easily read this as a single table using a command like `con.read_parquet('data/output/*.parquet')`.


## Step 4: Pre-processing for matching (needed for whole UK dataset only)

When matching to address in a small region, using `uk_address_matcher` is simpler because all data processing can be done on the fly.  There is no need to pre-process any of the underlying tables such as features and inverted indices.

If you're matching to the whole UK dataset, you will want to use the following preprocessing step where you derive the features and inverted index explicitly.   The reason for this is twofold:
- If you attempt to process the full 60m records on the fly, you are likely to run into memory issues; and
- You can re-use these preprocessed files for subsequent matching runs.  Even on a high spec computer, it will take several minutes to derive these tables, so pre-processing them avoids repeated recomputation.

To prepare your indexed data:

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

# Fast matching — pass the folder path to the pre-processed data
matcher = AddressMatcher(
    canonical_addresses="./ukam_prepared_canonical",
    addresses_to_match=df_messy,
    con=con,
)

result = matcher.match()
```


## Step 5: Match the data using `uk_address_matcher`


### Option A: If your datapackage is for a local council region


Create your address matching script in a file called `script.py`

```python
import duckdb
from uk_address_matcher import AddressMatcher, ExactMatchStage, SplinkStage

con = duckdb.connect()

df_messy = con.read_parquet("messy_addresses.parquet")
df_canonical = con.read_parquet("data/output/*.parquet")


matcher = AddressMatcher(
    canonical_addresses=df_canonical,
    addresses_to_match=df_messy,
    con=con,
    stages=[
        ExactMatchStage(),
        SplinkStage(
            final_match_weight_threshold=2,
            final_distinguishability_threshold=1,
        ),
    ],
)

result = matcher.match()

# Your results
result.matches.show(max_width=10000)

# Summary metrics on how many were matched
result.match_metrics().show()
```

And run it using:

```
uv run script.py
```

### Option B: Using pre-processed data:


```python
from uk_address_matcher import AddressMatcher, prepare_canonical_folder
import duckdb

con = duckdb.connect()
df_messy  = con.read_parquet("path/to/messy.parquet")

# Fast matching — pass the folder path instead of a relation
matcher = AddressMatcher(
    canonical_addresses="./ukam_prepared_canonical",
    addresses_to_match=df_messy,
    con=con,
)

result = matcher.match()
```

