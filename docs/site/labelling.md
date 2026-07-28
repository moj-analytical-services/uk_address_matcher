# Labelling

## Exporting a labelling bundle artefact

In order to begin labelling the results of a matching run, you first need to export a labelling bundle. This is a small folder containing a durable review dataset and a manifest describing the bundle. The bundle can be used to review the results of a matching run without needing to rerun the matching pipeline.

In order to export a labelling bundle, you need to have a `MatchResult` object from a matching run. The `MatchResult` object contains the results of the matching process, including the matched and unmatched records, as well as any existing labels.


```python
... rest of the matching pipeline ...

result = matcher.match()
bundle_path = result.export_labelling_bundle()

print(f"Labelling bundle written to: {bundle_path}")
```

This creates a stable folder in the current working directory:

```text
ukam_labelling_bundle/
├── manifest.json
└── review_data.parquet
```

- **`review_data.parquet`**: is the durable review dataset. It contains every input
record, including matched and unmatched records, alongside the final assignment
and available candidate matches. It can be opened later without rerunning the
matching pipeline.
- **`manifest.json`**: describes the bundle. It records the bundle version, creation
time, row counts and the Parquet schema so that a later labelling application
can check that it understands the data.

The export does not change the original messy dataset. Existing `ukam_label`
values are retained in the review data where present.

### Customising the export

You can specify the output directory, control the number of candidate matches included and allow an existing bundle to be replaced:

```python
bundle_path = result.export_labelling_bundle(
    output_directory="./address_review",
    top_n=10,
    overwrite=True,
)
```

> [!WARNING]
> Use `overwrite=True` with care, as it allows an existing bundle in the output directory to be replaced. See the API reference for parameter types, defaults and further details.

---

