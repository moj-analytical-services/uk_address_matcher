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
    _DATASET_REGISTRY,
    DatasetInfo,
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
    clean_data_pre_term_frequencies,
    derive_inverted_index,
    derive_term_frequencies_table,
    prepare_data_for_matching,
)
from uk_address_matcher.prepare_canonical import load_prepared_canonical_data

if TYPE_CHECKING:
    import duckdb

# Register available datasets
register_dataset("lambeth_council", LAMBETH_COUNCIL_INFO, get_lambeth_council_data)
register_dataset("hackney_council", HACKNEY_COUNCIL_INFO, get_hackney_council_data)


def load_benchmark_data(
    con: duckdb.DuckDBPyConnection,
    dataset_name: str,
    canonical_addresses: duckdb.DuckDBPyRelation | None = None,
    canonical_prepared_folder: str | Path | None = None,
    os_data_path: Path | None = None,
    include_term_frequencies: bool = False,
    sample_mode: bool = False,
    filter_canonical_by_messy_postcodes: bool = False,
    clean_canonical_on_the_fly: bool = False,
    derive_term_frequencies_on_the_fly: bool = True,
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
    canonical_addresses:
        Optional raw canonical relation. When provided, this is used instead of
        loading canonical data from a configured path.
    canonical_prepared_folder:
        Optional folder created by prepare_canonical_folder. When provided,
        canonical addresses, term frequencies, and inverted index are loaded
        from disk and no canonical cleaning is performed.
    os_data_path:
        Optional path to canonical OS data. If None, uses default location.
    include_term_frequencies:
        Whether to include term frequency information in the output.
    sample_mode:
        If True, load a small sample of messy records (deterministically).
        Canonical data is always loaded in full and narrowed by any
        dataset-specific filter. If False, load all records.
    filter_canonical_by_messy_postcodes:
        If True, restrict canonical data to postcodes present in the messy
        dataset before any term frequency or tokenisation steps.
    clean_canonical_on_the_fly:
        If True, load raw canonical data and clean it on the fly using
        prepare_data_for_matching. This also derives the inverted index from
        canonical data and uses it when cleaning messy data.
        If False, load pre-cleaned canonical data (default).
    derive_term_frequencies_on_the_fly:
        If True (and clean_canonical_on_the_fly is True), derive term frequencies
        from canonical data. If False, use pre-baked term frequencies.
        Only relevant when clean_canonical_on_the_fly is True.

    Returns
    -------
    tuple[duckdb.DuckDBPyRelation, duckdb.DuckDBPyRelation]
        Messy input data and canonical OS data.
    """
    print(f"Available datasets: {', '.join(list_datasets())}")
    print(f"Loading dataset: {dataset_name}\n")

    # Load raw messy data with optional sampling
    df_messy_raw = load_dataset(dataset_name, con, sample_mode=sample_mode)

    if canonical_addresses is not None and canonical_prepared_folder is not None:
        raise ValueError(
            "Provide either canonical_addresses or canonical_prepared_folder, not both."
        )

    tf_table = None
    inverted_index = None

    if canonical_prepared_folder is not None:
        if filter_canonical_by_messy_postcodes:
            print(
                "Skipping postcode filtering because a prepared canonical folder "
                "was provided."
            )
        if clean_canonical_on_the_fly or derive_term_frequencies_on_the_fly:
            raise ValueError(
                "Canonical cleaning options are not supported when using a prepared "
                "canonical folder."
            )

        prepared = load_prepared_canonical_data(canonical_prepared_folder, con)
        df_canonical = prepared.addresses
        tf_table = prepared.term_frequencies
        inverted_index = prepared.inverted_index
    else:
        if canonical_addresses is not None:
            df_canonical_loaded = canonical_addresses
        else:
            canonical_config = (
                CanonicalConfig(local_path=os_data_path)
                if os_data_path
                else CanonicalConfig.default(use_raw=clean_canonical_on_the_fly)
            )
            df_canonical_loaded = load_canonical_data(con, canonical_config)

        # Apply dataset-specific canonical filter if defined
        canonical_filter_sql = _DATASET_REGISTRY[dataset_name].info.canonical_filter_sql
        if canonical_filter_sql is not None and canonical_filter_sql.strip():
            df_canonical_loaded = con.sql(
                """
                SELECT *
                FROM ({canonical}) AS canon
                WHERE {filter_sql}
                """.format(
                    canonical=df_canonical_loaded.sql_query(),
                    filter_sql=canonical_filter_sql.strip(),
                )
            )

        if filter_canonical_by_messy_postcodes:
            df_canonical_loaded = con.sql(
                """
                SELECT canon.*
                FROM ({canonical}) AS canon
                INNER JOIN (
                    SELECT DISTINCT postcode
                    FROM ({messy}) AS messy
                    WHERE postcode IS NOT NULL
                ) AS messy_postcodes
                    USING (postcode)
                """.format(
                    canonical=df_canonical_loaded.sql_query(),
                    messy=df_messy_raw.sql_query(),
                )
            )

        if (
            "address_concat" not in df_canonical_loaded.columns
            and "original_address_concat" in df_canonical_loaded.columns
        ):
            df_canonical_loaded = con.sql(
                """
                SELECT
                    *,
                    original_address_concat AS address_concat
                FROM ({canonical}) AS canon
                """.format(
                    canonical=df_canonical_loaded.sql_query(),
                )
            )

        # Step 1: Clean or use pre-cleaned canonical data
        if clean_canonical_on_the_fly:
            print("Cleaning canonical data on the fly...")

            # Optionally derive term frequencies from raw canonical data
            if derive_term_frequencies_on_the_fly:
                print("Deriving term frequencies from canonical data...")
                tf_table = derive_term_frequencies_table(df_canonical_loaded, con=con)

            # Clean canonical data (no inverted index so exploding_unique_ids uses unique_id)
            print("Preparing canonical data for matching...")
            df_canonical = prepare_data_for_matching(
                df_canonical_loaded,
                con=con,
                term_frequency_lookup=tf_table,
            )
        else:
            if (
                "tf_numeric_token_1" in df_canonical_loaded.columns
                and "exploding_unique_ids" in df_canonical_loaded.columns
            ):
                df_canonical = df_canonical_loaded
            else:
                print("Preparing canonical data for matching...")
                df_canonical = prepare_data_for_matching(
                    df_canonical_loaded,
                    con=con,
                    term_frequency_lookup=tf_table,
                )

    # Step 2: Derive inverted index from cleaned canonical data when needed
    if inverted_index is None:
        print("Deriving inverted index from canonical data...")
        inverted_index = derive_inverted_index(df_canonical, con=con)

    # Step 3: Clean messy data using the inverted index
    print("Preparing messy data for matching...")
    if include_term_frequencies:
        df_messy = prepare_data_for_matching(
            df_messy_raw,
            con=con,
            term_frequency_lookup=tf_table,
            inverted_index=inverted_index,
        )
    else:
        df_messy = clean_data_pre_term_frequencies(df_messy_raw, con)

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
