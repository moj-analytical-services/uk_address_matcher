# Labelling

!!! warning "Experimental feature"
    The exporter and labelling app use private beta APIs and may change.

The labelling tool lets a reviewer check address matches without rerunning the
matching pipeline. Review data and canonical lookup data stay on the local
machine.

It is powered by [DuckDB-WASM](https://duckdb.org/docs/current/clients/wasm/overview) in the browser and (when deployed locally) a local Python server that supplies files, persists events, and searches an external canonical path when supplied.

## Preview

<video controls preload="metadata" width="100%" playsinline>
    <source src="../assets/videos/labelling_tool_beta_preview.mov" type="video/quicktime">
    Your browser does not support the video tag.
</video>

## Running locally

The local labeller writes directly to a labelling bundle folder. Run a matching
job end to end and start here with the resulting `MatchResult` object.

1. Export a self-contained bundle.

    ```python
    bundle_path = result._export_labelling_bundle_beta(overwrite=True)
    ```

2. Launch the app with the exported bundle and canonical data.

    ```python
    _launch_labelling_app_beta(
        labelling_bundle_path=bundle_path,
        canonical_address_path=CANONICAL_PATH,
    )
    ```

3. Open the printed localhost URL and review the records.

The app runs review queries in DuckDB-WASM. The local server supplies files,
persists events, and searches an external canonical path when supplied.

## Review workflow

There are two main panels in the labelling app:
- Use **Overview** to filter and open records in a tabulated view. This gives you a high-level view of the review data and lets label directly or drill down into a selected record for review.
- In **Review**, accept the model match, choose a candidate, mark no match or
  uncertain, or search canonical data and select a record.

## Bundle artefacts

Export creates `ukam_labelling_bundle/` by default:

| Artefact | Purpose |
| --- | --- |
| `manifest.json` | Bundle ID, schema, and file names. |
| `review_data.parquet` | Immutable review input: messy records, model outputs, and candidates. |
| `canonical_data.parquet` | Canonical lookup copy used by the browser app. |
| `labelling_updates.json` | Latest reviewer events, updated after each local save or undo. |
| `labelled_review_data.parquet` | Reviewer-labelled output, regenerated after each local save or undo. |

`ukam_label` is the original model/imported label. `ukam_user_label` is the
reviewer decision written to `labelled_review_data.parquet`; neither
`review_data.parquet` nor the original messy dataset is changed.

## Hosted browser app

Open the [hosted labelling tool](https://moj-analytical-services.github.io/uk_address_matcher/labelling-tool/), select the bundle folder, then optionally
select additional canonical Parquet files. The browser uses DuckDB-WASM and
does not upload the selected data. Events are stored in browser IndexedDB for
the session; choose **Download updates** to save `labelling_updates.json`.

Apply downloaded updates to a separate labelled review output:

```bash
uv run ukam-apply-labelling-updates \
    ./ukam_labelling_bundle \
    ./bundle-id-labelling-updates.json \
    ./ukam_labelling_bundle/review_data.parquet \
    --input-label-column ukam_user_label \
    --output ./ukam_labelling_bundle/labelled_review_data.parquet
```

The command validates the bundle ID, referenced records, and latest decisions
before atomically writing the output.

## Canonical data

For local launch, `canonical_address_path` accepts prepared canonical Parquet:
`ukam_canonical_addresses.parquet` or a directory containing
`ukam_canonical_addresses_chunks/`. Raw CSV is not supported. Canonical search
matches unique ID, postcode, or cleaned-address text and returns up to 100
records per page.

??? note "Hackney end-to-end example"

  [`benchmarking/hackney_labelling_examples.py`](https://github.com/moj-analytical-services/uk_address_matcher/blob/main/benchmarking/hackney_labelling_examples.py)
  shows the Hackney dataset setup, residential-address filter, matching run,
  bundle export, and app launch together.

