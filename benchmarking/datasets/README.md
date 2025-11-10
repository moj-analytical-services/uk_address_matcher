# Benchmark Datasets

This module provides a simple function-based registry for benchmark datasets used in address matching performance testing.

## Available Datasets

- **lambeth_council**: Council tax and electoral register data from Lambeth Borough Council

## Quick Start

Load a dataset with canonical OS data:

```python
from benchmarking.datasets import load_benchmark_data, list_datasets

# See what's available
print(list_datasets())  # ['lambeth_council']

# Load dataset (messy + canonical)
df_messy, df_canonical = load_benchmark_data(con, "lambeth_council")
```

## Private `.config.json` Configuration

Benchmark dataset paths (local & S3) are centrally configured via a private JSON file located at: `benchmarking/.config.json`.

At runtime helpers in `benchmarking.utils.io` load this file and populate any missing environment variables. This means you can declare paths once—no repeated `export` lines in your shell profile.

### Location & Loading

- Path resolved by code: `benchmarking/utils/io.py` → `Path(__file__).resolve().parent.parent / ".config.json"`
- Loaded automatically when you call `get_env_setting(...)` or explicitly via:

```python
from benchmarking.utils.io import apply_env_from_private_config
apply_env_from_private_config()  # populates os.environ if keys absent
```

### Example File

```json
{
    "UKAM_S3_BASE_PREFIX": "s3://my-bucket/prefix",
    "UKAM_LAMBETH_DATA_PATH": "/local/data/lambeth/",
    "UKAM_OS_CANONICAL_PATH": "/local/os/clean_os_without_tf.parquet"
}
```

### Conventions

- Keys become environment variables verbatim (e.g. `UKAM_OS_CANONICAL_PATH`).
- Use a consistent prefix (`UKAM_`) for discoverability.
- Prefer directory paths ending with `/` for dataset folders; single-file paths for canonical parquet.
- S3 objects should include the full URI (e.g. `s3://bucket/key.parquet`). The loader will not infer prefixes.

### Overriding & Precedence

1. Explicit environment variables already set take priority.
2. Missing variables are filled from `.config.json`.
3. If a required key is absent in both, `get_env_setting` raises a `RuntimeError` with guidance.

### Updating for New Datasets

Add new key/value pairs for each dataset you register—typically one local path and (optionally) one S3 path. The registry or loader functions can then reference them via `get_env_setting("UKAM_<DATASET>_DATA_PATH")` without hard‑coding paths.

> [! TIP]
> Commit a template version (e.g. `.config.example.json`) but never commit your real `.config.json` if it contains private bucket names or internal directory structures.

### Why a JSON File?

- Keeps path configuration language‑agnostic.
- Allows easy sharing of a single file between shells and Python scripts.
- Avoids leaking secrets into commit history (file can be git‑ignored).

### Minimal Template

```json
{}
```

Start empty; add keys as needed. Each addition is immediately consumable by existing loaders.

## Advanced Usage

Load components separately for multi-dataset benchmarking:

```python
from benchmarking.datasets import (
    load_dataset,
    load_canonical_data,
    get_dataset_info,
    list_datasets,
)

# Load canonical data once (reuse across multiple datasets)
df_canonical = load_canonical_data(con)

# Benchmark multiple datasets against the same canonical reference
for dataset_name in list_datasets():
    info = get_dataset_info(dataset_name)
    print(f"\nBenchmarking {info.name}...")
    print(info.summary())

    df_messy = load_dataset(dataset_name, con)

    # Run matching pipeline
    matches = run_deterministic_pipeline(con, df_messy, df_canonical)
    # ... analyse results ...
```

## Canonical OS Data

All datasets are matched against a single canonical Ordnance Survey reference. By default, the OS data is expected at:

```
/Users/thomashepworth/data/address_matcher/secret_data/os/clean/clean_os_without_tf.parquet
```

Override this location by passing `os_data_path` to `load_benchmark_data()`:

```python
from pathlib import Path

df_messy, df_canonical = load_benchmark_data(
    con,
    "lambeth_council",
    os_data_path=Path("/custom/path/to/os_data.parquet")
)
```

## Adding a New Dataset

To add a new benchmark dataset:

1. **Create a loader function** in this folder (e.g., `my_dataset.py`):

```python
from benchmarking.datasets.base import DatasetInfo
from uk_address_matcher.cleaning.pipelines import clean_data_with_minimal_steps

MY_DATASET_INFO = DatasetInfo(
    name="My Dataset",
    description="Brief description of what this dataset contains",
    source="Where the data comes from (e.g., 'S3: bucket-name' or 'Local parquet')",
    notes="Any relevant notes about data quality, peculiarities, or usage",
)


def get_my_dataset(con):
    """Load my benchmark dataset.

    Parameters
    ----------
    con:
        Active DuckDB connection.

    Returns
    -------
    duckdb.DuckDBPyRelation
        Cleaned messy data ready for matching.
    """
    # Load raw data
    df_raw = con.read_parquet("path/to/data.parquet")

    # Clean and return
    return clean_data_with_minimal_steps(df_raw, con=con)
```

2. **Register it** in `__init__.py`:

```python
from benchmarking.datasets.my_dataset import MY_DATASET_INFO, get_my_dataset
from benchmarking.datasets.registry import register_dataset

register_dataset("my_dataset", MY_DATASET_INFO, get_my_dataset)
```

3. **Use it**:

```python
df_messy, df_canonical = load_benchmark_data(con, "my_dataset")
```
