from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from benchmarking.datasets.registry import DatasetInfo
from benchmarking.datasets.lambeth_council import (
    LAMBETH_COUNCIL_INFO,
    get_lambeth_council_data,
)
from benchmarking.datasets.registry import (
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
from uk_address_matcher.cleaning.pipelines import (
    clean_data_using_precomputed_rel_tok_freq,
    clean_data_with_minimal_steps,
)

if TYPE_CHECKING:
    import duckdb

# Register available datasets
register_dataset("lambeth_council", LAMBETH_COUNCIL_INFO, get_lambeth_council_data)


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

    # Load raw messy data
    df_messy = load_dataset(dataset_name, con)

    # Apply deterministic sampling if requested (before cleaning for efficiency)
    if sample_mode:
        df_messy = con.sql(
            "SELECT * FROM df_messy ORDER BY unique_id LIMIT 10000"
        )

    # Apply cleaning logic
    if include_term_frequencies:
        cleaning_function = clean_data_using_precomputed_rel_tok_freq
    else:
        cleaning_function = clean_data_with_minimal_steps

    df_messy = cleaning_function(df_messy, con)

    # Load canonical data once
    canonical_config = (
        CanonicalConfig(local_path=os_data_path)
        if os_data_path
        else CanonicalConfig.default()
    )
    df_canonical = load_canonical_data(con, canonical_config)

    # Apply deterministic sampling if requested
    if sample_mode:
        df_canonical = con.sql(
            "SELECT * FROM df_canonical ORDER BY ukam_address_id LIMIT 1_000_000"
        )

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
]
