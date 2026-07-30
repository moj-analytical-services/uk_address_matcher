# UK Address Matcher


<div style="text-align: center; margin: 0; padding: 0;">

   <img src="assets/images/uk_address_matcher_web_wide.png" alt="UK Address Matcher logo" width="100%" style=" margin: 0; padding: 0;">

</div>


Fast, simple address matching (geocoding) in Python.

## Why use this library

- **Simple.** Setup in seconds, runs on a laptop. No separate infrastructure of services needed.
- **Fast.** Match 100,000 addresses in ~30 seconds.[^1]
- **Proven accuracy.** We use public, labelled datasets to measure and document accuracy.
- **Support for Ordnance Survey data.**  We provide a automated build pipeline for users wishing to match to Ordnance Survey data.  Matching to any other canonical dataset is also supported.

The end-to-end process of matching 100,000 addresses to Ordnance Survey data, including all software downloads and data processing takes:[^2]

- Less than a minute if you are matching to a small area such as a local council region.
- If matching to the whole UK, there's a one-time preprocessing step that takes around 10 minutes.  Subsequent matching of 100k records takes less than a minute.

## Installation

```bash
pip install uk_address_matcher
```

## What does it do?

Given the following data:

-  a "messy" dataset of addresses that you want to match
-  a "canonical" dataset of known addresses, often an Ordnance Survey dataset such as AddressBase or NGD.

this package will find the best matching canonical address for each messy address.


## Example:

Your data should be in the following format[^3]:

### Messy data

| unique_id | address_concat | postcode |
|----------|----------------|----------|
| m_1 | Flat A Example Court, 10 Demo Road, Townton | AB1 2BC |
| ...more rows |

### Canonical data

| unique_id | address_concat | postcode |
|----------|----------------|----------|
| c_1 | Flat A, 10 Demo Road, Townton | AB1 2BC |
| c_2 | Flat B, 10 Demo Road, Townton | AB1 2BC |
| c_3 | Basement Flat, 10 Demo Road, Townton | AB1 2BC |
| ...more rows |


You can match it as follows:

```python
import duckdb
from uk_address_matcher import AddressMatcher

con = duckdb.connect()
messy = con.read_csv("example_data/messy_example.csv")
canonical = con.read_csv("example_data/canonical_example.csv")

matcher = AddressMatcher(
    canonical_addresses=canonical,
    addresses_to_match=messy,
    con=con,
)
result = matcher.match()
result.matches().show(max_width=10000)
```

Example output:

| unique_id | resolved_canonical_id | original_address_concat | original_address_concat_canonical | match_reason | match_weight | distinguishability |
|----------|------------------------|-------------------------|-----------------------------------|--------------|--------------|--------------------|
| m_1 | c_2 | Flat A Example Court, 10 Demo Road, Townton | Flat A, 10 Demo Road, Townton | splink: probabilistic match | 13.5885 | 11.5033 |


The above is recommended if your canonical dataset is relatively small, say, under 1 million rows. If you're matching to larger canonical dataset, a preprocessing step is recommended. See [choose whether to pre-process your canonical dataset](get_started.md#choose-whether-to-pre-process-your-canonical-dataset) for details.

## Use Cases

Here is a list of some of our known users and their use cases:

=== "Public Sector (UK)"

	- The [Greater London Authority](https://www.london.gov.uk/)'s High Streets Data Service uses the `uk_address_matcher` to precisely geolocate London's businesses and assign each of them a commercial UPRN, in order to fully map the city's retail provision and commercial property use.

## Licence

This project is free and open source and is released under the MIT licence.

## Next steps

- [Overview](index.md)
- [Getting started](get_started.md)
- [Choosing a matching threshold](choosing_a_matching_threshold.md)
- [Optimising accuracy](optimising_accuracy.md)
- [Working with Ordnance Survey data](ordnance_survey.md)
- [Performance and benchmarking](performance.md)
- [API reference](api_reference.md)

[^1]: Timings on a MacBook Pro M4 Max.
[^2]: Does not include the time taken to download Ordnance Survey data since this depends on the speed of your internet connection.
[^3]: The `postcode` column is optional. If you include it, the matcher will use it directly. If you do not, the matcher will attempt to detect and extract postcodes from `address_concat`.  `uk_address_matcher` also supports matching addresses that lack a postcode.
