# Getting started

## Install

`uk_address_matcher` is a Python package available on PyPI. You can install with `pip`:

```bash
pip install uk_address_matcher
```


## Input data requirements

Both your messy addresses and your canonical addresses need at least these
columns:

| Column | Description |
|--------|-------------|
| `address_id` or `unique_id` | Required address entity identifier. May repeat across rows; supply exactly one ID column. |
| `address_concat` | Address text, which can include the postcode |

Optionally you can provide:

| Column | Description |
|--------|-------------|
| `postcode` | If provided, this postcode is used in favour over any postcode provided in `address_concat` |
| `ukam_label` | The unique ID of the true match. If provided, it enables accuracy analysis output |

`address_id` is preferred; `unique_id` remains a supported alias without
deprecation. Both datasets use these input names. `canonical_address_id` is an
output name, not a canonical input requirement. Missing or null postcodes are
extracted from `address_concat` using the existing cleaning rules.

Messy input can be a DuckDB relation, a list of dictionaries, or a list of
`AddressRecord` objects. For example,
`AddressRecord(address_id="M1", address_concat="10 Downing Street SW1A 2AA")`
needs no separate postcode. Additional dictionary and relation fields are
retained for explicit enrichment. Canonical matching input remains a relation
or a prepared-folder path; this does not add canonical dictionary input.


## Choose whether to pre-process your canonical dataset

If you're linking to a small canonical dataset (of say, less than 500,000 rows), then it's simplest to process the data on-the-fly.

If you're linking to a large canonical dataset (for example, national-scale NGD), then we recommend a one-time pre-processing step. It computes reusable datasets (indices and feature tables) once, so subsequent matching runs are fast.

The examples below use the fictional London datasets from `ukam_datasets`, which are included for runnable examples.



=== "Local / regional (processing on-the-fly)"

    ```python exec="true" source="tabbed-left" tabs="Source code|Output"
    import duckdb
    from uk_address_matcher import AddressMatcher
    from uk_address_matcher.datasets import ukam_datasets

    con = duckdb.connect()

    df_messy = ukam_datasets.as_relation("fictional_london_messy", con=con)
    df_canonical = ukam_datasets.as_relation("fictional_london_canonical", con=con)

    matcher = AddressMatcher(
        canonical_addresses=df_canonical,
        addresses_to_match=df_messy,
        con=con,
    )
    with matcher.match() as result:
        print(result.matches().limit(5).to_df().to_markdown(index=False))
    con.close()
    ```

=== "National-scale (preprocessed data)"


    ```python exec="true" source="tabbed-left" tabs="Source code|Output"

    import duckdb
    import os
    import tempfile
    from uk_address_matcher import (
        AddressMatcher,
        prepare_canonical_folder,
    )
    from uk_address_matcher.datasets import ukam_datasets

    con = duckdb.connect()
    df_messy = ukam_datasets.as_relation("fictional_london_messy", con=con)
    df_canonical = ukam_datasets.as_relation("fictional_london_canonical", con=con)

    # One-time preparation
    output_folder = tempfile.mkdtemp()
    prepare_canonical_folder(
        df_canonical,
        output_folder=output_folder,
        con=con,
        cleaning_num_chunks=10,
        overwrite=True,
    )

    # Pass the folder path instead of a relation
    matcher = AddressMatcher(
        canonical_addresses=output_folder,
        addresses_to_match=df_messy,
        con=con,
    )
    print("Prepared folder contents:")
    for f in sorted(os.listdir(output_folder)):
        print(f"  {f}")
    print()
    with matcher.match() as result:
        print(result.matches().limit(5).to_df().to_markdown(index=False))
    con.close()
    ```

    The `output_folder` contains prepared Parquet files and
    `ukam_manifest.json` (package version, row counts, and hashes) for
    reproducibility.

    Subsequent matching exercises that use the same canonical data can reuse this folder, skipping the `prepare_canonical_folder` step.

## Reading results

`matcher.match()` returns a `MatchResult` object:


| Property / method | Returns |
|-------------------|---------|
| `.matches()` | Every input row, with `address_id`, `canonical_address_id`, both UKAM row keys, `match_reason`, and more. |
| `.matches(include_unmatched=False)` | Successful matches only. |
| `.matches(include_unmatched=False, all_columns=True)` | Successful matches with additional diagnostics. |
| `.match_metrics()` | Match-reason breakdown with counts and percentages. |
| `.accuracy_analysis()` | Threshold-based accuracy analysis from labelled data (requires `ukam_label` in messy input). |
| `.join_input_columns(canonical=..., messy=...)` | Match results with explicitly selected input fields and optional aliases. |
| `.close()` | Release tables retained by this result, leaving the connection open. |

`include_unmatched` selects rows; `all_columns` selects columns. Neither changes
the underlying results, metrics or accuracy analysis. Unmatched rows have null
canonical fields. Both column modes use the new entity ID names, without
duplicate legacy ID columns.

`ukam_address_id` identifies a messy row and `canonical_ukam_address_id` identifies
its matched canonical row. These keys distinguish textual variants sharing an
entity ID; they are not stable identifiers across runs.

Use a context manager to release retained tables even if processing fails:

```python
with matcher.match() as result:
    rows = result.matches().fetchall()
con.close()
```

Materialised Python values such as `rows` remain usable afterwards. Lazy DuckDB
relations must be consumed while the result and connection are open. Calling
`close()` repeatedly is safe; close results before closing their connection.

## Enriching matches with input data

Select fields from the retained prepared inputs. Strings keep their names;
dictionaries specify `input_column` and `alias`:

```python
enriched = result.join_input_columns(
    canonical=[
        "clean_full_address",
        {"input_column": "postcode", "alias": "canonical_input_postcode"},
    ],
    messy=[{"input_column": "postcode", "alias": "messy_input_postcode"}],
)
enriched.limit(5).show()
```

Select at least one field. All output-name collisions raise a `ValueError`;
nothing is overwritten or renamed automatically. Values come from prepared
inputs, not raw copies. Unmatched rows retain NULL canonical fields. The lazy
relation requires the result and connection to stay open.
Joins use row keys, so repeated entity IDs do not duplicate or misattribute
metadata. Input fields with reserved result names must be selected under a
distinct alias.

## Exporting a labelling bundle

After matching, `result._export_labelling_bundle_beta()` creates a durable review
folder for later labelling. See [Labelling bundles](labelling.md) for the
initial workflow and a high-level description of the exported artefacts.
This beta workflow still requires unique messy entity IDs; repeated IDs are
supported for matching, not for labelling-bundle export.


??? info "Customising stages"

    The default pipeline is `ExactMatchStage` → `SplinkStage`. Pass your own
    `stages` list to change behaviour:

    ```python
    from uk_address_matcher import (
        AddressMatcher,
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
            PeeledAddressStage(),
            UniqueTrigramStage(),
            SplinkStage(
                final_match_weight_threshold=20.0,
                final_distinguishability_threshold=5.0,
            ),
        ],
    )
    ```

    Use `AddressMatcher.available_stages()` to discover registered stage classes. See [Choosing a matching threshold](choosing_a_matching_threshold.md) and [Optimising accuracy](optimising_accuracy.md) for further accuracy advice. The [API reference](api_reference.md) covers the main API docs.

## Using labelled data

If you know the correct match for each address, add a `ukam_label` column to
your messy data. It propagates through to results, enabling accuracy analysis
with `MatchResult.accuracy_analysis()`.

## Migrating to 2.0

| Previous usage | 2.0 usage |
| --- | --- |
| Input `unique_id` | Still supported; prefer `address_id` for new datasets. Never supply both. |
| Result `unique_id` | `address_id`, including with `all_columns=True`. |
| Result `resolved_canonical_id` | `canonical_address_id`. |
| Row keys only in diagnostics | Both UKAM row keys are included by default. |
| Root `ukam_datasets` import | `from uk_address_matcher.datasets import ukam_datasets`. |
| Root `prepare_data_for_matching` import | Use `AddressMatcher` or `prepare_canonical_folder`; lower-level implementation remains in `uk_address_matcher.cleaning.chunking_strategies`. |
| Preparation `num_of_chunks` | `cleaning_num_chunks`; `output_chunk_count` still controls output files separately. |
| Preparation `_derive_road_catalogue` | No longer a public option; normal preparation derives road artefacts. |

`matches()` still includes unmatched rows by default. Use `include_unmatched=False`
to exclude them. Exported `MatchResult` supports type annotations and context
management; its private construction fields are not public configuration.
`cleaning_num_chunks` also sets the target number of postcode-district batches
for local keys and road parsing. Each batch keeps whole districts and fixed
16-district road fallback groups together, so small inputs can use fewer batches.
The default is ten batches, not ten districts per batch; `output_chunk_count`
continues to control output files independently.
