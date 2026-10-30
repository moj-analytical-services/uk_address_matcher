# Benchmark Experiment Environment Variables

## Core Experiment Variables

- `UKAM_OS_CANONICAL_PREPARED`
  - Path to the prepared canonical folder used by benchmarking.
  - This folder normally contains `ukam_canonical_addresses.parquet`, `ukam_term_frequencies.parquet`, `ukam_inverted_index.parquet`, and `ukam_manifest.json`.

- `UKAM_EXCLUDE_STRUCTURAL_TF_TOKENS`
  - Structural TF filter enable/disable flag.
  - Typical values: `1`, `0`, `true`, `false`.

- `UKAM_STRUCTURAL_TF_TOKEN_FILTER_MODE`
  - Structural TF filter mode.
  - Supported modes in current code: `none`, `broad`, `unit_only`.

## Dataset Source Variables

These are defined in `benchmarking/config/datasets.py`.

- `UKAM_ABERDEENSHIRE_DATA_PATH`
- `UKAM_GWYNEDD_DATA_PATH`
- `UKAM_HACKNEY_DATA_PATH`
- `UKAM_LAMBETH_DATA_PATH`
- `UKAM_MID_SUSSEX_DATA_PATH`
- `UKAM_RHONDDA_DATA_PATH`

The configured value may point either to:

- a directory or prefix, in which case the default dataset filename is appended
- a specific `.csv`, `.parquet`, or `.xlsx` file

## Resolution Rules

- Benchmark settings are loaded through `benchmarking/settings.py`.
- Dataset paths are resolved through `benchmarking/config/sources.py`.
- Local dataset paths must exist.
- S3 paths must start with `s3://`.

## Practical Guidance

- Treat `UKAM_OS_CANONICAL_PREPARED` as the main experiment switch for canonical variants.
- When debugging a benchmark, report the exact canonical folder and dataset env var values that were in effect.
- If a task only needs a persisted comparison readout, do not change env vars unnecessarily.