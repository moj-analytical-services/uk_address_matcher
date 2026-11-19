# Ordnance Survey data

This guide walks through the end-to-end process of matching messy data to Ordnance Survey using `uk_address_matcher`

The steps are:

| Step | Indicative timing[^timings-m4] |
|------|-------------------|
| Create a data package in the Ordnance Survey Data Hub and obtain an API key | about 5 mins |
| Install tooling | about 5 mins |
| Build the canonical dataset | about 4 mins for full UK |
| Pre-process for matching (national-scale only) | about 12 min 27 sec |
| Match | Less than 1 minute |

[^timings-m4]: Timings on a MacBook M4 Max.


## What is Ordnance Survey data?

Ordnance Survey is the UK's authoritative provider of address data. Many public sector organisations can use this data for free under the [Public Sector Geospatial Agreement](https://www.ordnancesurvey.co.uk/customers/public-sector/public-sector-geospatial-agreement)
(PSGA).


## Why is there a special process for Ordnance Survey data?

`uk_address_matcher` makes no assumptions about the canonical dataset provided by the user.

However, in recognition that many users will be matching to Ordnance Survey data, we provide a streamlined process for downloading and preparing it for matching.  There are two main reasons:

1. We provide a tool to easily download and extract the data into the required format
2. With careful processing of Ordnance Survey files, it's possible to achieve higher accuracy than simply using the raw [NGD Builtaddress](https://docs.os.uk/osngd/data-structure/address/gb-address/built-address).  Our tool does this processing for you.


## Step 1: Create a data package and obtain an API key

Create a 'data package' in the [OS Data Hub](https://osdatahub.os.uk/) containing the Ordnance Survey data you want to match to.

Choose NGD or AddressBase. Default to NGD if you have no preference.

!!! tip "Optimising accuracy when linking to a local area"
    If you are matching to a local area, restrict your data package to only that area to make matching faster and more accurate.

1. Log in and create a
   [new recipe](https://osdatahub.os.uk/data/downloads/recipe-library) for
   your geographical area.  Follow the process and create a new data package.
2. Navigate to [data packages](https://osdatahub.os.uk/data/downloads/data-packages/)
   and and crete a new data package corresponding to the area of interest.  Use 'load polygon' to load a pre-defined polygons, which exist for geographic areas such as local authorities.
4. Obtain your `package_id` and `version_id`.

    The `package_id` is in the URL for the data package - for instance it is `18296` in  `https://osdatahub.os.uk/data/downloads/data-packages/18296`

    Within the data package, The version ID can be retrieved by hovering over the download link for your data package, which is in the format `https://osdatahub.os.uk/api/dataPackages/{package_id}/{version_id}/file`.

4. Obtain your API key and secret from the
   [API Projects](https://osdatahub.os.uk/data/apis/projects) page.


You should now have the following values:

- Data package `package_id`
- THe data package `version_id`
- Your API key and secret
    - `OS_PROJECT_API_KEY`
    - `OS_PROJECT_API_SECRET`

---

## Step 2: Create a folder for your project and install tooling

Install [uv](https://docs.astral.sh/uv/getting-started/installation/), then
create a project and add `uk_address_matcher`:

```bash
mkdir address_project && cd address_project
uv init --bare
uv add uk_address_matcher
```

---

## Step 3: Build the canonical dataset

Our [`ukam-os-builder`](https://github.com/moj-analytical-services/ukam_os_builder) tool
downloads and processes Ordnance Survey data into a Parquet file optimised for
matching.

You can use it as follows.  Make sure you have your `package_id`, `version_id`, and API key/secret to hand from step 1.

```bash
# Config wizard: point the tool to your data package
uvx --from ukam-os-builder ukam-os-setup
```

```bash
# Download and build the flat file
uvx --from ukam-os-builder ukam-os-build
```

By default the output lands in `data/output/`. Unless you set `num_chunks=1`,
the output is a folder of Parquet files representing a single table. DuckDB
reads them as one table via `con.read_parquet('data/output/*.parquet')`.

---

## Step 4: Pre-process for matching (national-scale only)

If you're linking to a small canonical dataset (of say, less than 500,000 rows), then it's simplest to process the data on-the-fly.

If you're linking to a large canonical dataset (for example, national-scale NGD), then we recommend a one-time pre-processing step. It computes reusable datasets (indices and feature tables) once, so subsequent matching runs are fast.

For a local council region, skip this step. Everything runs on the fly.



```python
import duckdb
from uk_address_matcher import prepare_canonical_folder

con = duckdb.connect()
df_canonical = con.read_parquet("data/output/*.parquet")

prepare_canonical_folder(
    df_canonical,
    output_folder="./ukam_prepared_canonical",
    con=con,
    overwrite=True,
)
```

The prepared folder can be used for all subsequent matching runs.

## Filtering before canonical preparation

If you already know that some Ordnance Survey records should never be eligible
matches for your use case, it is usually better to filter them out before
calling `prepare_canonical_folder()`.

This is the right place for organisation-wide policy decisions such as:

- residential-only matching
- excluding parent shells
- excluding clearly unwanted residential subtypes such as garages
- excluding known nuisance classes that your users would not expect to match to

Applying these exclusions before preparation has two advantages:

1. Term frequencies and the inverted index are built on the reduced canonical
    pool, which usually improves discrimination.
2. The prepared folder itself becomes policy-aligned, so downstream users are
    less likely to leave unwanted records in by accident.

By contrast, `canonical_address_filter=` on `AddressMatcher` is most useful when
different users need different subsets from the same prepared folder, or when
you are running exploratory experiments.

!!! note "Do not treat policy exclusions as text cleaning"
        Be careful not to mix up two different concerns:

        - text cleaning, such as normalising abbreviations or whitespace
        - candidate-pool policy, such as deciding that `RG` garages, `PP` parent
            shells, or `CAR PARK SPACE` records should not be eligible matches

        If a record should not participate in matching, prefer filtering the
        canonical rows rather than stripping words from `clean_full_address`.

### AddressBase classification codes

The `classificationcode` column is the main Ordnance Survey field to use for
this. Officially, Ordnance Survey defines the scheme as a hierarchical code with
primary, secondary, tertiary, and quaternary levels.

- Official OS classification documentation:
  [addressclassificationcodevalue](https://docs.os.uk/osngd/code-lists/code-lists-overview/addressclassificationcodevalue)
- Practical AddressBase summary:
  [Ideal Postcodes classification guide](https://docs.ideal-postcodes.co.uk/docs/data/classification-codes/)

??? info "Top-level groups"
    - `C`: Commercial. Usually exclude for residential-only matching.
    - `L`: Land. Usually exclude unless your messy data contains land parcels or parks.
    - `M`: Military. Usually exclude unless you expect military addresses.
    - `O`: Other, Ordnance Survey only. Usually exclude.
    - `P`: Parent shell. Often exclude from candidate pools.
    - `R`: Residential. Usually include for residential matching.
    - `U`: Unclassified. Review carefully before including.
    - `X`: Dual use. Review carefully, may be useful in mixed datasets.
    - `Z`: Object of interest. Usually exclude from postal-style matching.

??? info "Residential secondary groups"
    - `RD`: Dwelling. Usually include.
    - `RG`: Garage. Often exclude for household or council-tax matching.
    - `RH`: House in multiple occupation. Often include, depending on the source data.
    - `RI`: Residential institution. Include only if your messy data covers care homes, halls, prisons, and similar settings.

??? info "Commercial secondary groups"
    - `CA`: Agricultural.
    - `CB`: Ancillary building.
    - `CC`: Community services.
    - `CE`: Education.
    - `CH`: Hotel, motel, boarding, guest house.
    - `CI`: Industrial.
    - `CL`: Leisure.
    - `CM`: Medical.
    - `CN`: Animal centre.
    - `CO`: Office.
    - `CR`: Retail.
    - `CT`: Transport.
    - `CU`: Utility.
    - `CX`: Emergency or rescue service.

??? info "Other commonly relevant non-residential groups"
    - `LD`: Land, development.
    - `LP`: Land, park.
    - `PP`: Parent shell, property shell.
    - `OR`: Royal Mail infrastructure.
    - `UC`: Awaiting classification.
    - `UP`: Pending internal investigation.

Ordnance Survey also defines deeper levels. For example:

- `RD02` means detached house
- `CE01` means college
- `RI01` means care home

For a full, maintained list of tertiary and quaternary codes, use the official
OS page above rather than copying the entire code set into local documentation.

### Recommended filtering approach

Use this rule of thumb:

| Situation | Best place to apply the rule | Why |
|---|---|---|
| A stable organisation-wide exclusion | Before `prepare_canonical_folder()` | The prepared data, term frequencies, and index all reflect the reduced candidate pool |
| Different users need different subsets | `canonical_address_filter=` at match time | You can reuse one prepared folder while still narrowing eligibility |

!!! tip "Best default for most residential OS workflows"
    Start by keeping only residential rows, excluding known garage and parent
    shell classes, and then excluding the smaller ancillary residential groups
    that often contain parking and shared-facility rows.

### Quick filtering recipes

!!! warning "Residential Ordnance Survey data often contains non-household rows"
    Some residential records in Ordnance Survey point to car park spaces,
    garages, bins, communal stores, and similar ancillary objects rather than
    homes. For household-style matching, you will usually want to omit these
    before linkage begins.

The recipes below show a practical way to do that quickly.

=== "Residential only"

    ```python
    df_canonical = df_canonical.filter("substr(classificationcode, 1, 1) = 'R'")
    ```

=== "Residential household subset"

    ```python
    from uk_address_matcher import prepare_canonical_folder

    residential_household_filter = (
        "substr(classificationcode, 1, 1) = 'R' "
        "AND substr(classificationcode, 1, 2) <> 'RG' "
        "AND substr(classificationcode, 1, 2) <> 'PP' "
        "AND substr(classificationcode, 1, 2) <> 'RB' "
        "AND substr(classificationcode, 1, 2) <> 'RC' "
        "AND clean_full_address NOT LIKE 'CAR PARK SPACE%' "
        "AND clean_full_address NOT LIKE 'CAR PARK %' "
        "AND clean_full_address NOT LIKE 'PARKING SPACE%' "
        "AND clean_full_address NOT LIKE 'GARAGE %' "
        "AND clean_full_address NOT LIKE 'GARAGES %'"
    )

    prepare_canonical_folder(
        df_canonical.filter(residential_household_filter),
        output_folder="./ukam_prepared_canonical",
        con=con,
        overwrite=True,
    )
    ```

    This is the clearest starting recipe for a household-style subset. It keeps
    ordinary residential rows while excluding:

    - `RG` garages
    - `PP` parent shells
    - `RB` ancillary/shared residential rows such as bins, stores, and communal spaces
    - `RC` parking and hardstanding-style residential rows
    - parking-style prefixes that still leak through as address text

=== "Prefix exclusions only"

    ```python
    prefix_filter = (
        "clean_full_address NOT LIKE 'CAR PARK SPACE%' "
        "AND clean_full_address NOT LIKE 'CAR PARK %' "
        "AND clean_full_address NOT LIKE 'PARKING SPACE%' "
        "AND clean_full_address NOT LIKE 'GARAGE %' "
        "AND clean_full_address NOT LIKE 'GARAGES %'"
    )

    df_canonical = df_canonical.filter(prefix_filter)
    ```

    Use this only when you need a quick local heuristic. It is less robust than
    excluding the relevant classification groups first.

=== "Parent shells only"

    ```python
    df_parent_shells = df_canonical.filter("substr(classificationcode, 1, 2) = 'PP'")
    ```

If you need different subsets for different users, keep the broader prepared
folder and apply the restriction later with `canonical_address_filter=`.

---

## Step 5: Match

Create a script called `match.py` with the following content.

If you pass `canonical_address_filter=`, the value should be a DuckDB SQL
predicate over the canonical rows. In other words, write the part that would go
after `WHERE`, not a full `SELECT` statement.

=== "Local / regional"

    ```python
    import duckdb
    from uk_address_matcher import AddressMatcher

    con = duckdb.connect()

    df_messy = con.read_parquet("messy_addresses.parquet")
    df_canonical = con.read_parquet("data/output/*.parquet")

    matcher = AddressMatcher(
        canonical_addresses=df_canonical,
        addresses_to_match=df_messy,
        con=con,
    )

    result = matcher.match()

    result.matches().show(max_width=10000)

    ```



=== "Match with common exclusions"

    ```python
    import duckdb
    from uk_address_matcher import AddressMatcher

    con = duckdb.connect()
    df_messy = con.read_parquet("messy_addresses.parquet")

    common_exclusions = (
        # Keep residential rows only.
        "substr(classificationcode, 1, 1) = 'R' "
        # Remove garages and parent shells.
        "AND substr(classificationcode, 1, 2) <> 'RG' "
        "AND substr(classificationcode, 1, 2) <> 'PP' "
        # Remove ancillary/shared residential rows and parking-style residential rows.
        "AND substr(classificationcode, 1, 2) <> 'RB' "
        "AND substr(classificationcode, 1, 2) <> 'RC' "
        # Remove parking and garage-style address text that can still leak through.
        "AND clean_full_address NOT LIKE 'CAR PARK SPACE%' "
        "AND clean_full_address NOT LIKE 'CAR PARK %' "
        "AND clean_full_address NOT LIKE 'PARKING SPACE%' "
        "AND clean_full_address NOT LIKE 'GARAGE %' "
        "AND clean_full_address NOT LIKE 'GARAGES %'"
    )

    matcher = AddressMatcher(
        canonical_addresses="./ukam_prepared_canonical",
        addresses_to_match=df_messy,
        canonical_address_filter=common_exclusions,
        con=con,
    )

    result = matcher.match()
    result.matches().show(max_width=10000)
    ```

    This is useful when you want to trial a household-style subset quickly
    without rebuilding the prepared folder first. If the exclusions become
    standard policy, move them earlier into the Step 4 preparation query.



=== "National-scale (prepared folder)"

    ```python
    import duckdb
    from uk_address_matcher import AddressMatcher

    con = duckdb.connect()
    df_messy = con.read_parquet("messy_addresses.parquet")

    matcher = AddressMatcher(
        canonical_addresses="./ukam_prepared_canonical",
        addresses_to_match=df_messy,
        con=con,
    )

    result = matcher.match()
    result.matches().show(max_width=10000)
    ```

Run with:

```bash
uv run match.py
```

## Video

The following video shows the end-to-end process of matching council tax data from a local council to the full Ordnance Survey dataset for that council.

<video controls preload="metadata" width="100%">
    <source src="../assets/videos/end_to_end_hackney.mp4" type="video/mp4">
    Your browser does not support the video tag.
</video>



