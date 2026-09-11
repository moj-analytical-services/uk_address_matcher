# Labelling

!!! warning "Experimental feature"
    The exporter and labelling app use private beta APIs and may change.

The labelling tool lets a reviewer check address matches without rerunning the
matching pipeline. Review data and canonical lookup data stay on the local
machine.

It is powered by [DuckDB-WASM](https://duckdb.org/docs/current/clients/wasm/overview) in the browser. Labels can be downloaded manually and saved to a folder chosen by the user.

## Preview

<video controls preload="metadata" width="100%" playsinline>
    <source src="../assets/videos/labelling_tool_beta_preview.mp4" type="video/quicktime">
    Your browser does not support the video tag.
</video>

## Running locally

The local labeller writes directly to a labelling bundle folder. Run a matching
job end to end and start here with the resulting `MatchResult` object.

1. Export a self-contained bundle.

    ```python
    bundle_path = result._export_labelling_bundle_beta(overwrite=True)
    ```

2. Launch the local app with the exported bundle and canonical data.

    ```python
    from uk_address_matcher.labelling import _launch_labelling_app_beta

    _launch_labelling_app_beta(
        labelling_bundle_path=bundle_path,
        canonical_address_path=CANONICAL_PATH,
    )
    ```

3. Open the printed localhost URL and review the records.

The app runs review queries in DuckDB-WASM. The local server supplies files,
persists labelling events to `labelling_updates.json`, materialises labelled
review data, and searches an external canonical path when supplied.

The local workflow is also available from the command line:

```bash
uv run python -m uk_address_matcher.labelling.server \
    --labelling-bundle ukam_labelling_bundle \
    --canonical-address-path ukam_prepared_canonical
```

## Sharing a static HTML page

UKAM does not host this app. You can generate a self-contained static HTML page
and share it with colleagues, who open it directly in their browser and load
their own labelling bundle and, optionally, canonical data.

```bash
uv run ukam-labelling --output ukam_labelling.html
```

Reviewers open the file and select their labelling bundle and, if needed, their
prepared canonical data folder. The page embeds the browser assets but not
team data; it validates both manifests before loading. Reviewers choose
**Download labels** when they are ready and save the resulting CSV to a folder
of their choice.

!!! warning
    The static HTML option is slower because DuckDB-WASM runs in a single
    browser thread. It is suitable for smaller labelling tasks. For a full
    canonical dataset of around 70 million records, use the local server so
    DuckDB can keep the database in memory on the user's machine.

### Controlling export size and chunks

The exporter can create a smaller review set and split its review data across
several Parquet files:

```python
bundle_path = result._export_labelling_bundle_beta(
    overwrite=True,
    total_records_to_export=10_000,
    review_data_chunk_count=4,
)
```

`total_records_to_export` is the exact number of retained messy records to
export. The default `None` exports every retained record. When a limit is set,
records are selected in ascending `unique_id` order, making repeated exports
reproducible rather than randomly sampled.

`review_data_chunk_count` is the number of review-data Parquet files to create;
it does not split the canonical lookup or update files. Records are ordered by
`unique_id` and divided into contiguous chunks whose sizes differ by at most
one record. The default value `1` writes the usual `review_data.parquet`.
With a value greater than `1`, the files are named
`review_data_chunk_001.parquet`, `review_data_chunk_002.parquet`, and so on.
The static HTML page loads every file listed in `manifest.json` as one review
dataset after the reviewer selects the bundle folder.

The requested record count cannot exceed the retained record count, and the
chunk count cannot exceed the number of records being exported. This keeps
empty chunks out of the bundle and prevents a request for a misleadingly large
number of files.

## Review workflow

There are two main panels in the labelling app:
- Use **Overview** to filter and open records in a tabulated view. This gives you a high-level view of the review data and lets label directly or drill down into a selected record for review.
- In **Review**, accept the model match, choose a candidate, mark no match or
  uncertain, or search canonical data and select a record.

## Bundle artefacts

Export creates `ukam_labelling_bundle/` by default:

| Artefact | Purpose |
| --- | --- |
| `manifest.json` | Bundle ID, schema, and file names; checked before loading. |
| `review_data.parquet` or `review_data_chunk_*.parquet` | Immutable review input: messy records, model outputs, and candidates. Multiple chunk files are listed in `manifest.json`. |
| `canonical_data.parquet` | Canonical lookup copy used by the browser app. |

The review data and canonical data are immutable. In the local workflow,
labelling events are persisted to `labelling_updates.json` and the labelled
review data is materialised as `labelled_review_data.parquet`. In the static
HTML workflow, labels are held in the browser session until the reviewer
downloads them manually.

## Downloaded labels

Choose **Download labels** in the app to download a CSV
containing every review record, including records you skip or mark as no match.
The CSV columns are:

| Column | Meaning |
| --- | --- |
| `unique_id` | Review record identifier. |
| `address_name` | Messy address shown to the reviewer. |
| `ukam_label` | Effective current label, including an imported label or a label selected during the session. Blank for skipped, no-match, uncertain, or cleared records. |
| `label_available` | `TRUE` when `ukam_label` is present, otherwise `FALSE`. |

The downloaded CSV is a manual export: the user chooses where to save it, and
the app does not automatically overwrite the original messy dataset.

## Canonical data

The local launcher accepts a prepared canonical path, while pre-rendered HTML
reviewers select the prepared canonical folder containing `ukam_manifest.json`,
the manifest-listed index artefacts, and either
`ukam_canonical_addresses.parquet` or the
`ukam_canonical_addresses_chunks/` directory. Raw CSV is not supported. The
manifest and every referenced file are checked before loading. Canonical search
matches unique ID, postcode, or cleaned-address text and returns up to 100
records per page.

??? note "Hackney end-to-end example"

  [`benchmarking/hackney_labelling_examples.py`](https://github.com/moj-analytical-services/uk_address_matcher/blob/main/benchmarking/hackney_labelling_examples.py)
  shows the Hackney dataset setup, residential-address filter, matching run,
  bundle export, and app launch together.

