from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from benchmarking.datasets.hackney_council import (
    HACKNEY_COUNCIL_INFO,
    get_hackney_council_data,
)
from benchmarking.datasets.lambeth_council import (
    LAMBETH_COUNCIL_INFO,
    get_lambeth_council_data,
)
from benchmarking.datasets.registry import (
    DatasetInfo,
    _DATASET_REGISTRY,
    get_all_dataset_info,
    get_dataset_info,
    list_datasets,
    load_dataset,
    register_dataset,
)
from benchmarking.datasets.sources import (
    CanonicalConfig,
    SourceConfig,
    load_canonical_data,
)
from uk_address_matcher import (
    clean_data_with_minimal_steps,
    clean_data_with_term_frequencies,
)

if TYPE_CHECKING:
    import duckdb

# Register available datasets
register_dataset("lambeth_council", LAMBETH_COUNCIL_INFO, get_lambeth_council_data)
register_dataset("hackney_council", HACKNEY_COUNCIL_INFO, get_hackney_council_data)


def load_benchmark_data(
    con: duckdb.DuckDBPyConnection,
    dataset_name: str,
    os_data_path: Path | None = None,
    include_term_frequencies: bool = False,
    sample_mode: bool = False,
) -> tuple[duckdb.DuckDBPyRelation, duckdb.DuckDBPyRelation]:
    """Load a benchmark dataset with messy and canonical data.

    The canonical OS data is loaded once and can be reused across multiple
    benchmark datasets for efficiency.

    Parameters
    ----------
    con:
        Active DuckDB connection.
    dataset_name:
        Name of the registered dataset to load.
    os_data_path:
        Optional path to canonical OS data. If None, uses default location.
    include_term_frequencies:
        Whether to include term frequency information in the output.
    sample_mode:
        If True, load 100k canonical records and 10k messy records (deterministically).
        If False, load all records.

    Returns
    -------
    tuple[duckdb.DuckDBPyRelation, duckdb.DuckDBPyRelation]
        Messy input data and canonical OS data.
    """
    print(f"Available datasets: {', '.join(list_datasets())}")
    print(f"Loading dataset: {dataset_name}\n")

    # Load raw messy data with optional sampling
    df_messy_raw = load_dataset(dataset_name, con, sample_mode=sample_mode)

    # Load canonical data once with optional sampling
    canonical_config = (
        CanonicalConfig(local_path=os_data_path)
        if os_data_path
        else CanonicalConfig.default()
    )
    df_canonical_raw = load_canonical_data(
        con, canonical_config, sample_mode=sample_mode
    )

    # TODO: This is a hack, we're going to re-process
    df_canonical_raw = df_canonical_raw.select(
        "unique_id, original_address_concat as address_concat, postcode, classification_code"
    )

    # Apply dataset-specific canonical filter if defined
    canonical_filter_sql = _DATASET_REGISTRY[dataset_name].info.canonical_filter_sql
    if canonical_filter_sql is not None and canonical_filter_sql.strip():
        df_canonical_raw_filtered = con.sql(
            f"""
            SELECT *
            FROM df_canonical_raw
            WHERE {canonical_filter_sql.strip()}
            """
        )

    # Apply cleaning logic with reverse index for term frequencies
    if include_term_frequencies:
        df_canonical = clean_data_with_term_frequencies(
            df_canonical_raw_filtered, con, create_reverse_index=True
        )
        df_messy = clean_data_with_term_frequencies(
            df_messy_raw, con, create_reverse_index=False
        )
    else:
        # Apply minimal cleaning without reverse index
        df_messy = clean_data_with_minimal_steps(df_messy_raw, con)
        df_canonical = df_canonical_raw

    # Show dataset info
    info = get_dataset_info(dataset_name)
    record_count = df_messy.count("*").fetchone()[0]
    print(info.summary(record_count))
    print()

    return df_messy, df_canonical


__all__ = [
    "DatasetInfo",
    "CanonicalConfig",
    "SourceConfig",
    "get_dataset_info",
    "get_all_dataset_info",
    "list_datasets",
    "load_benchmark_data",
    "load_canonical_data",
    "load_dataset",
    "register_dataset",
    "LAMBETH_COUNCIL_INFO",
    "get_lambeth_council_data",
    "HACKNEY_COUNCIL_INFO",
    "get_hackney_council_data",
]
