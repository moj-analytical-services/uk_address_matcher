# Performance overview

## Timings

Indicative timings for matching 100,000 messy addresses are as follows.

Note that runtimes depend on whether the canonical data covers a local council region or the whole UK.

| Task | Local council region | Full country |
|------|---------------------|--------------|
| 1. Create data package and API key | 5 minutes | 5 minutes |
| 2. Install Python, uv, and `uk_address_matcher` | 5 minutes | 5 minutes |
| 3. Download and process OS data into a flat file | 5 seconds[^1] | 4 minutes[^2] |
| 4. Pre-process indexes and features | Not necessary | 12 min 53 sec[^3] |
| 5. Match 100,000 records | 18 seconds | 30 seconds |

[^1]: Plus ~15 seconds to download the data.
[^2]: Plus ~18 minutes to download the data.
[^3]: See [Canonical preparation runtime](#canonical-preparation-runtime) for
    the latest benchmark configuration, e2e timing breakdown, and comparison
    with previous implementations.

These timings are measured on a MacBook Pro M4 Max.

Steps 1-3 are one-off; subsequent matching runs only require step 5 (or steps 4-5 for the full UK dataset).

## Benchmarking

There many different address matching solutions and it can be hard to compare performance between them.

Luckily, there are several open datasets of labelled address data that can be used to benchmark accuracy.

In this section, we set out `uk_address_matcher`'s accuracy against these labelled datasets.


### Hackney Council data

The Hackney Council dataset is available [here](https://www.datadaptive.com/addr/.)

The latest run took 29.7 seconds in total against 114,166 labelled records
(28.4 seconds for matching).

<details>
  <summary>Expand to see Hackney benchmarking script</summary>

```python
import duckdb
from uk_address_matcher import AddressMatcher, ExactMatchStage, SplinkStage
import pyarrow as pa
from pathlib import Path

import time


start_time = time.time()

canoncial_prepared_path = "path_to_output_folder_from_ukam-os-builder_tool"

con = duckdb.connect()
hackney_path = "path_to_HACKNEY_CTBANDS_ONSUD_202507.csv"
hackney_data = con.read_csv(hackney_path)


all_uprns_path = str(Path(canoncial_prepared_path) / "ukam_canonical_addresses.parquet")
all_uprns = con.read_parquet(all_uprns_path).select("unique_id as uprn")


sql = """
select propref as unique_id,
concat_ws(' ', addr1, addr2, addr3, addr4) as address_concat,
uprn as ukam_label,
postcode
from hackney_data
where uprn is not null
and uprn in (select uprn from all_uprns)
"""
df_messy = con.sql(sql)


matcher = AddressMatcher(
    canonical_addresses=str(canoncial_prepared_path),
    canonical_address_filter="lowertierlocalauthoritygsscode = 'E09000012' and substr(classificationcode, 1, 1) = 'R'",
    addresses_to_match=df_messy,
    con=con,
    stages=[
        ExactMatchStage(),
        SplinkStage(
            final_distinguishability_threshold=1.0,
        ),
    ],
)

result = matcher.match()

end_time = time.time()
print(f"Execution time: {end_time - start_time} seconds")

chart = result.accuracy_analysis(
    output_type="precision_recall", add_metrics=["f1"], match_weight_round_to_nearest=1
)
accuracy_table = result.accuracy_analysis(
    output_type="table", add_metrics=["f1"], match_weight_round_to_nearest=1
)

df = pa.Table.from_pylist(accuracy_table)
con.sql("select * from df").show(max_width=100000, max_rows=100000)

```



</details>

Note that we:

- Filter out any rows from the messy dataset where the label UPRN does not exist in our canonical dataset. We could not be expected to match these
- Filter the canonical dataset down to the Hackney council region, and to residential properties.

It achieves:

- 99.9% precision with recall of 80.6%
- 99.9% precision with recall of 85.9%
- 99.6% precision with recall of 97.9%

The full precision-recall curve is shown below:

```vegalite
{
    "schema-url": "assets/charts/hackney_precision_recall.json"
}
```

Manual review of the 'false positives' suggests many may in fact be true positives (that the "ground truth" labels contains errors).  So the true precision is likely higher than indicated in this chart.

### Suppressing the postcode from the Hackney data


The following chart shows how much performance is degraded if we suppress the postcode from the messy data, and re-match.

The script is the same as above, except the `postcode` column on the messy data is replaced with `NULL`:

<details>
  <summary>Expand to see Hackney (postcode suppressed) benchmarking script</summary>

```python
import duckdb
from uk_address_matcher import AddressMatcher, ExactMatchStage, SplinkStage
import pyarrow as pa
from pathlib import Path

import time


start_time = time.time()

canoncial_prepared_path = "path_to_output_folder_from_ukam-os-builder_tool"

con = duckdb.connect()
hackney_path = "path_to_HACKNEY_CTBANDS_ONSUD_202507.csv"
hackney_data = con.read_csv(hackney_path)


all_uprns_path = str(Path(canoncial_prepared_path) / "ukam_canonical_addresses.parquet")
all_uprns = con.read_parquet(all_uprns_path).select("unique_id as uprn")


# postcode column intentionally set to NULL to suppress postcode information
sql = """
select propref as unique_id,
concat_ws(' ', addr1, addr2, addr3, addr4) as address_concat,
uprn as ukam_label,
cast(null as varchar) as postcode
from hackney_data
where uprn is not null
and uprn in (select uprn from all_uprns)
"""
df_messy = con.sql(sql)


matcher = AddressMatcher(
    canonical_addresses=str(canoncial_prepared_path),
    canonical_address_filter="lowertierlocalauthoritygsscode = 'E09000012' and substr(classificationcode, 1, 1) = 'R'",
    addresses_to_match=df_messy,
    con=con,
    stages=[
        ExactMatchStage(),
        SplinkStage(
            final_distinguishability_threshold=1.0,
        ),
    ],
)

result = matcher.match()

end_time = time.time()
print(f"Execution time: {end_time - start_time} seconds")

chart = result.accuracy_analysis(
    output_type="precision_recall", add_metrics=["f1"], match_weight_round_to_nearest=1
)
```

</details>

```vegalite
{
    "schema-url": "assets/charts/hackney_precision_recall_postcode_suppressed.json"
}
```

The region of recall between 0% and 25% is now populated because there are no longer any exact matches (which requires a match on postcode); all matches are now Splink matches.

The latest postcode-suppressed run took 19.9 seconds in total (19.2 seconds for
matching).

### Mid Sussex District Council business rates data

This dataset is available [here](https://www.midsussex.gov.uk/housing-council-tax/council-tax-benefits-and-business-rates/business-rates/open-data-business-rates/)

The latest run took 5.8 seconds in total against 3,756 labelled records
(5.6 seconds for matching).

Note that, unlike the Hackney example, here we do **not** use the prepared canonical folder.  Instead we hand `AddressMatcher` a `DuckDBPyRelation` containing only the canonical addresses we care about — commercial properties in Mid Sussex (`E07000228`).

When given a relation rather than a prepared folder, `AddressMatcher` derives the term frequencies and inverted index from that subset alone.  Tokens (street names, building names, town names) are therefore far less ambiguous than in a UK-wide index, which improves both precision and recall:

- Less ambiguous tokens make the Splink model more confident in true matches, so fewer candidates are rejected by the distinguishability threshold (better recall).
- Genuinely unique tokens are weighted more strongly, so spurious matches in unrelated parts of the country are suppressed (better precision).

This technique is appropriate when you know your messy records all live within a small slice of the UK.

<details>
  <summary>Expand to see Mid Sussex benchmarking script</summary>

```python
import time
from pathlib import Path

import duckdb
import pyarrow as pa

from uk_address_matcher import AddressMatcher, SplinkStage


start_time = time.time()

# This is from the output of the ukam-os-builder tool, not the output of `prepare_canonical_folder()`.
canonical_addresses_path = "path_to_canonical_addresses_parquet"
mid_sussex_workbook = "path_to_mid_sussex_business_rates_data.xlsx"

con = duckdb.connect()
con.execute("INSTALL excel")
con.execute("LOAD excel")


# Filter the canonical data down to commercial properties in Mid Sussex only.
sql = f"""
select
    unique_id,
    address_concat,
    postcode
from read_parquet('{canonical_addresses_path}')
where unique_id in (
select uprn from addressbase wherelowertierlocalauthoritygsscode = 'E07000228'
  and substr(classificationcode, 1, 1) = 'C')
"""
canonical_rows = con.sql(sql)


sql = f"""
select *
from read_xlsx(
    '{mid_sussex_workbook}',
    all_varchar = true
)
"""
business_rates_data = con.sql(sql)


sql = """
with cleaned as (
    select
        nullif(nullif(trim("Property Reference"), ''), 'NULL') as property_reference,
        nullif(nullif(trim("UPRN"), ''), 'NULL') as uprn_raw,
        nullif(nullif(trim("Post Code"), ''), 'NULL') as postcode,
        nullif(nullif(trim("Property Name 1"), ''), 'NULL') as property_name_1,
        nullif(nullif(trim("Property Name 2"), ''), 'NULL') as property_name_2,
        nullif(nullif(trim("Address 1"), ''), 'NULL') as address_1,
        nullif(nullif(trim("Address 2"), ''), 'NULL') as address_2,
        nullif(nullif(trim("Address 3"), ''), 'NULL') as address_3,
        nullif(nullif(trim("Address 4"), ''), 'NULL') as address_4
    from business_rates_data
),
uprn_normalised as (
    select
        property_reference,
        try_cast(nullif(ltrim(uprn_raw, '0'), '') as bigint) as uprn_bigint,
        postcode,
        property_name_1,
        property_name_2,
        address_1,
        address_2,
        address_3,
        address_4
    from cleaned
)
select
    property_reference as unique_id,
    concat_ws(
        ' ',
        property_name_1,
        property_name_2,
        address_1,
        address_2
    ) as address_concat,
    uprn_bigint as ukam_label,
    upper(replace(postcode, ' ', '')) as postcode
from uprn_normalised
where property_reference is not null
  and uprn_bigint is not null
  and uprn_bigint in (select unique_id from canonical_rows)
  and (
      property_name_1 is not null
      or property_name_2 is not null
      or address_1 is not null
      or address_2 is not null
      or address_3 is not null
      or address_4 is not null
  )
"""
df_messy = con.sql(sql)


matcher = AddressMatcher(
    canonical_addresses=canonical_rows,
    addresses_to_match=df_messy,
    con=con,
    stages=[
        SplinkStage(
            final_distinguishability_threshold=1,
        ),
    ],
)

result = matcher.match()

end_time = time.time()
print(f"Execution time: {end_time - start_time} seconds")


chart = result.accuracy_analysis(
    output_type="precision_recall",
    add_metrics=["f1"],
    match_weight_round_to_nearest=1,
)
accuracy_table = result.accuracy_analysis(
    output_type="table",
    add_metrics=["f1"],
    match_weight_round_to_nearest=1,
)

df = pa.Table.from_pylist(accuracy_table)
```

Note that we:

- Filter out any rows from the messy dataset where the label UPRN does not exist in our canonical dataset. We could not be expected to match these.
- Filter the canonical dataset down to the Mid Sussex District Council region, and to commercial properties.

</details>

It achieves:

- 94.4% precision with recall of 85.2%
- 95.6% precision with recall of 78.6%
- 97.5% precision with recall of 60.8%

The full precision-recall curve is shown below:

```vegalite
{
	"schema-url": "assets/charts/mid_sussex_precision_recall.json"
}
```

## Canonical preparation runtime

??? info "How the latest 13 min 43 sec timing was measured"
    The latest rerun on 8 September 2026 completed the current production
    `prepare_canonical_folder()` path in **823.349 seconds (13 min 43.3 sec)** for
    71,438,939 national-scale NGD canonical rows. The complete performance script,
    including the three labelled-data benchmarks, took **882.267 seconds
    (14 min 42.3 sec)**.

    The run used a MacBook Pro M4 Max, DuckDB 1.5.0, 14 DuckDB threads, a requested
    16 GB DuckDB memory limit (14.9 GiB effective), 10 work chunks, and eight
    canonical output shards. Road features were enabled. The fresh manifest records
    481,747 roadlike-place rows and 1,931,943,019 bytes across 11 persisted
    artefacts.

    The preparation stages measured in that same production run were:

    | Stage | Seconds | Share of preparation wall time | Cumulative seconds |
    | --- | ---: | ---: | ---: |
    | Preparation setup and input coercion | 0.041s | 0.0% | 0.041s |
    | Foundational cleaning and deterministic ID assignment | 202.078s | 24.5% | 202.119s |
    | Roadlike-place catalogue creation | 168.816s | 20.5% | 370.934s |
    | Term-frequency aggregation | 2.487s | 0.3% | 373.421s |
    | Adjacent distinguishing and term-frequency application | 180.420s | 21.9% | 553.841s |
    | Inverted-index derivation | 84.149s | 10.2% | 637.990s |
    | Road blocking enrichment | 78.425s | 9.5% | 716.416s |
    | Canonical output planning | 0.001s | 0.0% | 716.417s |
    | Term-frequency serialisation | 0.039s | 0.0% | 716.456s |
    | Inverted-index serialisation | 38.926s | 4.7% | 755.382s |
    | Roadlike-place serialisation | 0.094s | 0.0% | 755.476s |
    | Canonical shard serialisation | 66.515s | 8.1% | 821.991s |
    | Manifest metadata and finalisation | 1.342s | 0.2% | 823.333s |

    **Foundational cleaning and deterministic ID assignment dominated the run** at
    202.078 seconds, or 24.5% of preparation wall time. Adjacent distinguishing and
    term-frequency application took 180.420 seconds (21.9%), followed by
    roadlike-place catalogue creation at 168.816 seconds (20.5%).

    The same run recorded a peak process RSS of **15.62 GiB**, peak DuckDB current
    memory of **10.82 GiB**, and peak current DuckDB temporary-file usage of
    **119.84 GiB** across 26 temporary files. The live temporary directory reached
    **119.84 GiB**. DuckDB's current temporary-storage metric was 0 at the sampled
    boundaries; the temporary-file byte counters are the relevant spill measure for
    this run.

    These figures are indicative rather than a hardware-independent guarantee.
    Available memory, temporary-disk speed, source schema, enabled features, and
    output-shard count can materially affect national-scale preparation time.