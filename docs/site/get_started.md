# Geting started

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
| `unique_id` | Stable unique identifier |
| `address_concat` | Address text, which can include the postcode |

Optionally you can provide:

| Column | Description |
|--------|-------------|
| `postcode` | If provided, this postcode is used in favour over any postcode provided in `address_concat` |
| `ukam_label` | The unique ID of the true match. If provided, it enables accuracy analysis output |


## Choose whether to pre-process your canonical dataset

If you're linking to a small canonical dataset (of say, less than 500,000 rows), then it's simplest to process the data on-the-fly.

If you're linking to a large canonical dataset (for example, national-scale NGD), then we recommend a one-time pre-processing step. It computes reusable datasets (indices and feature tables) once, so subsequent matching runs are fast.

The examples below use the fictional London datasets from `ukam_datasets`, which are included for runnable examples.



=== "Local / regional (processing on-the-fly)"

    ```python exec="true" source="tabbed-left" tabs="Source code|Output"
    import duckdb
    from uk_address_matcher import AddressMatcher, ukam_datasets

    con = duckdb.connect()

    df_messy = ukam_datasets.as_relation("fictional_london_messy", con=con)
    df_canonical = ukam_datasets.as_relation("fictional_london_canonical", con=con)

    matcher = AddressMatcher(
        canonical_addresses=df_canonical,
        addresses_to_match=df_messy,
        con=con,
    )
    result = matcher.match()
    print(result.matches().limit(5).to_df().to_markdown(index=False))
    ```

=== "National-scale (preprocessed data)"


    ```python exec="true" source="tabbed-left" tabs="Source code|Output"

    import duckdb
    import os
    import tempfile
    from uk_address_matcher import (
        AddressMatcher,
        prepare_canonical_folder,
        ukam_datasets,
    )

    con = duckdb.connect()
    df_messy = ukam_datasets.as_relation("fictional_london_messy", con=con)
    df_canonical = ukam_datasets.as_relation("fictional_london_canonical", con=con)

    # One-time preparation
    output_folder = tempfile.mkdtemp()
    prepare_canonical_folder(
        df_canonical,
        output_folder=output_folder,
        con=con,
        overwrite=True,
    )

    # Pass the folder path instead of a relation
    matcher = AddressMatcher(
        canonical_addresses=output_folder,
        addresses_to_match=df_messy,
        con=con,
    )
    result = matcher.match()

    print("Prepared folder contents:")
    for f in sorted(os.listdir(output_folder)):
        print(f"  {f}")
    print()
    print(result.matches().limit(5).to_df().to_markdown(index=False))
    ```

    The `output_folder` contains prepared Parquet files and
    `ukam_manifest.json` (package version, row counts, and hashes) for
    reproducibility.

    Subsequent matching exercises that use the same canonical data can reuse this folder, skipping the `prepare_canonical_folder` step.

## Reading results

`matcher.match()` returns a `MatchResult` object:


| Property / method | Returns |
|-------------------|---------|
| `.matches()` | DuckDB relation with `unique_id`, `resolved_canonical_id`, `match_reason`, and more. |
| `.match_metrics()` | Match-reason breakdown with counts and percentages. |
| `.accuracy_analysis()` | Threshold-based accuracy analysis from labelled data (requires `ukam_label` in messy input). |

## Exporting a labelling bundle

After matching, `result._export_labelling_bundle_beta()` creates a durable review
folder for later labelling. See [Labelling bundles](labelling.md) for the
initial workflow and a high-level description of the exported artefacts.


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
