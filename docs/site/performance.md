# Performance overview

## Timings

Indicative timings for matching 100,000 messy addresses are as follows.

Note that runtimes depend on whether the canonical data covers a local council region or the whole UK.

| Task | Local council region | Full country |
|------|---------------------|--------------|
| 1. Create data package and API key | 5 minutes | 5 minutes |
| 2. Install Python, uv, and `uk_address_matcher` | 5 minutes | 5 minutes |
| 3. Download and process OS data into a flat file | 5 seconds[^1] | 4 minutes[^2] |
| 4. Pre-process indexes and features | Not necessary | 8 min 58 sec[^3] |
| 5. Match 100,000 records | 18 seconds | 26 seconds |

[^1]: Plus ~15 seconds to download the data.
[^2]: Plus ~18 minutes to download the data.
[^3]: See [Canonical preparation runtime](#canonical-preparation-runtime) for
    the benchmark configuration, stage breakdown, and comparison with the
    previous implementation.

These timings are measured on a MacBook Pro M4 Max.

Steps 1–3 are one-off; subsequent matching runs only require step 5 (or steps 4–5 for the full UK dataset).

## Benchmarking

There many different address matching solutions and it can be hard to compare performance between them.

Luckily, there are several open datasets of labelled address data that can be used to benchmark accuracy.

In this section, we set out `uk_address_matcher`'s accuracy against these labelled datasets.


### Hackney Council data

The Hackney Council dataset is available [here](https://www.datadaptive.com/addr/.)

The following script takes 26 seconds to run against 114,544 labelled records.

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

- 99.7% precision with recall of 80%
- 99.6% precision with recall of 86%
- 99.0% precision with recall of 98%

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

### Mid Sussex District Council business rates data

This dataset is available [here](https://www.midsussex.gov.uk/housing-council-tax/council-tax-benefits-and-business-rates/business-rates/open-data-business-rates/)

The following script takes ~8 seconds to run against 3,756 labelled records.

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

- 94.1% precision with recall of 85%
- 95.4% precision with recall of 79%
- 97.5% precision with recall of 61%

The full precision-recall curve is shown below:

```vegalite
{
	"schema-url": "assets/charts/mid_sussex_precision_recall.json"
}
```

## Canonical preparation runtime

??? info "How the 8 min 58 sec timing was measured"
    The current refactored `prepare_canonical_folder()` path completed in
    **537.531 seconds (8 min 57.5 sec)** for 71,438,939 national-scale NGD
    canonical rows. This is the complete one-time preparation job, not just
    address text normalization.

    The run used a MacBook Pro M4 Max, DuckDB 1.5.0, 14 DuckDB threads, a 16 GB
    DuckDB memory limit, 10 work chunks, and eight canonical output shards. Road
    features were enabled. Peak process memory was 17.38 GiB and DuckDB spilled
    approximately 97 GB of temporary data to disk.

    | Stage | Time | Share |
    | --- | ---: | ---: |
    | Shared foundational cleaning | 61.672s | 11.47% |
    | Deterministic ID assignment | 111.690s | 20.78% |
    | Term-frequency aggregation | 2.442s | 0.45% |
    | Adjacent distinguishing | 71.897s | 13.38% |
    | Post-TF feature processing | 80.876s | 15.05% |
    | In-place finalization | 0.268s | 0.05% |
    | Bigram and trigram indexes | 65.606s | 12.21% |
    | Road enrichment | 64.715s | 12.04% |
    | TF/index serialization and canonical exports | 76.720s | 14.27% |
    | Manifest finalization | 1.277s | 0.24% |

    The measured stages account for 537.163 seconds; setup and transitions
    account for the remaining 0.368 seconds. Foundational cleaning itself takes
    61.672 seconds. IDs, adjacent-record features, term-frequency features, and
    all canonical columns are finalized after 328.845 seconds (5 min 28.8 sec).
    Index construction, road enrichment, Parquet serialization, and manifest
    generation bring the complete prepared folder to 537.531 seconds.

    The benchmark includes all work performed by `prepare_canonical_folder()`:
    canonical cleaning, deterministic IDs, term frequencies, adjacent-record
    features, physical indexes, road features, Parquet output, and manifest
    generation. It excludes downloading NGD and transforming the source product
    into the input flat file; those are covered separately by step 3 above.

    Before the refactor, the matched road-enabled path took 1,918.331 seconds
    (31 min 58.3 sec). The current path is 72.0% faster. It produced 1,928,966,850
    bytes of prepared output, 2.20% more than the old road-enabled baseline.

    The full comparison found no term-frequency or inverted-index differences
    and no physical-order violations across the eight canonical shards. There
    were 401 canonical rows whose only difference was the now-deterministic
    ordering of equal-frequency entries in the unusual-token arrays; all other
    persisted values were unchanged.

    These figures are indicative rather than a hardware-independent guarantee.
    Available memory, temporary-disk speed, source schema, enabled features, and
    output-shard count can materially affect national-scale preparation time.