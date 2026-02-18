from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Optional

from duckdb import DuckDBPyConnection, DuckDBPyRelation

from uk_address_matcher.cleaning.pipelines import (
    QUEUE_FOR_TF_DERIVATION,
    QUEUE_TRIGRAM_SELF,
    QUEUE_TRIGRAM_WITH_INVERTED_INDEX,
    _clean_data_pre_term_frequencies,
    _clean_data_using_precomputed_rel_tok_freq,
    _create_term_frequency_tables,
    _ensure_postcode_column,
    _register_inverted_index_table,
)
from uk_address_matcher.cleaning.steps.trigram_blocking import (
    _build_inverted_index_from_trigrams,
    _derive_trigrams_from_address_tokens,
)
from uk_address_matcher.sql_pipeline.helpers import _uid
from uk_address_matcher.sql_pipeline.runner import create_sql_pipeline

if TYPE_CHECKING:
    from uk_address_matcher.sql_pipeline.runner import DebugOptions

logger = logging.getLogger("uk_address_matcher")


def _format_elapsed(elapsed_seconds: float) -> str:
    total_seconds = int(round(max(0.0, elapsed_seconds)))
    minutes, seconds = divmod(total_seconds, 60)
    return f"{minutes}m {seconds:02d}s"


def _log_progress(
    total_records: int,
    processed_records: int,
    stage_type: str,
    *,
    chunk_index: int | None = None,
    total_chunks: int | None = None,
    chunk_elapsed_seconds: float | None = None,
) -> None:
    percentage_complete = processed_records / total_records if total_records > 0 else 1.0

    message = (
        f"{stage_type}"
        f"{processed_records:,.0f} records ({percentage_complete:.0%} complete)"
    )
    if chunk_elapsed_seconds is not None:
        chunk_suffix = ""
        if chunk_index is not None and total_chunks is not None:
            chunk_suffix = f"chunk {chunk_index + 1}/{total_chunks} "
        message += f" - {chunk_suffix}took {_format_elapsed(chunk_elapsed_seconds)}"

    logger.info(message)


def _calculate_chunk_size(total_records: int, num_of_chunks: int) -> int:
    if total_records <= 0:
        raise ValueError(
            "Supplied address table has no records. Please provide a non-empty table."
        )

    # Ensure chunk size is reasonable: minimum 10k records per chunk
    max_chunks = max(1, total_records // 10_000)
    num_of_chunks = max(1, min(num_of_chunks, max_chunks))
    chunk_size = (total_records + num_of_chunks - 1) // num_of_chunks
    return max(1, chunk_size)


def clean_data_pre_term_frequencies(
    address_table: DuckDBPyRelation,
    con: DuckDBPyConnection,
    num_of_chunks: int = 10,
    *,
    debug_options: Optional[DebugOptions] = None,
) -> DuckDBPyRelation:
    """Clean address data with foundational steps only (no term frequencies).

    Applies the minimal set of preprocessing transformations: trimming, upper-casing,
    parsing numeric and flat position information, and tokenisation. This is useful
    when you need lightweight cleaning without term frequency analysis.

    Args:
        address_table: Input address relation with standard schema.
        con: DuckDB connection.
        num_of_chunks: Number of chunks to split the data into. Data is processed
            in batches and results are unioned. Set to 1 for no chunking.
        debug_options: Optional debug configuration for pipeline execution.
            Note: Debug options are only applied on the first iteration to avoid
            excessive logging output.

    Returns:
        Cleaned address data without term frequencies, materialised as a relation.
    """
    uid = _uid()
    input_name = f"__ukam_input_addresses_{uid}"
    con.register(input_name, address_table)
    # For chunked processing, don't add ID yet - process chunks first
    total_rows = address_table.count("*").fetchone()[0]

    chunk_size = _calculate_chunk_size(total_rows, num_of_chunks)
    total_chunks = (total_rows + chunk_size - 1) // chunk_size

    con.execute(f"DROP TABLE IF EXISTS __ukam_chunked_addresses_{uid}")

    for chunk_index in range(total_chunks):
        chunk_started_at = time.perf_counter()
        chunk = con.sql(f"""
        SELECT *
            FROM {input_name}
            WHERE (abs(hash(address_concat)) % {total_chunks}) = {chunk_index}
        """)

        # Process the chunk without address ID,
        # applying debug options only on first iteration.
        processed_chunk = _clean_data_pre_term_frequencies(
            chunk,
            con,
            debug_options=debug_options if chunk_index == 0 else None,
        )

        if chunk_index == 0:
            processed_chunk.create(f"__ukam_chunked_addresses_{uid}")
        else:
            processed_chunk.insert_into(f"__ukam_chunked_addresses_{uid}")

        _log_progress(
            total_rows,
            min((chunk_index + 1) * chunk_size, total_rows),
            stage_type="Cleaned and preprocessed: ",
            chunk_index=chunk_index,
            total_chunks=total_chunks,
            chunk_elapsed_seconds=time.perf_counter() - chunk_started_at,
        )

    return con.table(f"__ukam_chunked_addresses_{uid}")


def derive_term_frequencies_table(
    address_table: DuckDBPyRelation,
    con: DuckDBPyConnection,
    num_of_chunks: int = 10,
    *,
    debug_options: Optional["DebugOptions"] = None,
) -> DuckDBPyRelation:
    """Derive a term frequency lookup table from address data.

    This function cleans and tokenises addresses in chunks, then computes
    relative token frequencies from the combined result. The returned table
    can be passed to prepare_data_for_matching to ensure consistent term
    frequencies across multiple datasets.

    Example usage:
        tf_table = derive_term_frequencies_table(df_canonical, con)
        df_messy = prepare_data_for_matching(
            df_messy,
            con,
            term_frequency_lookup=tf_table,
        )
        df_canonical = prepare_data_for_matching(
            df_canonical,
            con,
            term_frequency_lookup=tf_table,
        )

    Args:
        address_table: Input address relation with address_concat column.
        con: DuckDB connection.
        num_of_chunks: Number of chunks to split the data into for cleaning.
            Set to 1 for no chunking.
        debug_options: Optional debug configuration for pipeline execution.

    Returns:
        Term frequency table with 'token' and 'rel_freq' columns.
    """
    uid = _uid()

    # Ensure postcode column exists
    address_table = _ensure_postcode_column(address_table)

    # Register input for chunked access
    input_name = f"__ukam_tf_derive_input_{uid}"
    con.register(input_name, address_table)

    total_rows = address_table.count("*").fetchone()[0]
    chunk_size = _calculate_chunk_size(total_rows, num_of_chunks)
    total_chunks = (total_rows + chunk_size - 1) // chunk_size

    cleaned_table = f"__ukam_tf_derive_cleaned_{uid}"
    con.execute(f"DROP TABLE IF EXISTS {cleaned_table}")

    # Process in chunks using minimal pipeline (clean + tokenise only)
    for chunk_index in range(total_chunks):
        chunk_started_at = time.perf_counter()
        chunk = con.sql(f"""
            SELECT *
            FROM {input_name}
            WHERE (abs(hash(address_concat)) % {total_chunks}) = {chunk_index}
        """)

        pipeline = create_sql_pipeline(
            con,
            input_rel=chunk,
            stage_specs=QUEUE_FOR_TF_DERIVATION,
            pipeline_name="Clean for TF derivation",
            pipeline_description=("Clean and tokenise for term frequency computation"),
        )
        processed_chunk = pipeline.run(debug_options if chunk_index == 0 else None)

        if chunk_index == 0:
            processed_chunk.create(cleaned_table)
        else:
            processed_chunk.insert_into(cleaned_table)

        _log_progress(
            total_rows,
            min((chunk_index + 1) * chunk_size, total_rows),
            stage_type="Cleaned for TF derivation: ",
            chunk_index=chunk_index,
            total_chunks=total_chunks,
            chunk_elapsed_seconds=time.perf_counter() - chunk_started_at,
        )

    # Compute token frequencies from clean_full_address tokens
    tf_sql = f"""
    WITH unnested AS (
        SELECT unnest(string_split(clean_full_address, ' ')) AS token
        FROM {cleaned_table}
    )
    SELECT
        token,
        COUNT(*)::DOUBLE / (SELECT COUNT(*) FROM unnested) AS rel_freq
    FROM unnested
    GROUP BY token
    ORDER BY COUNT(*) DESC
    """

    # Materialise the result (consistent with parquet loading pattern)
    result_table = "__ukam_derived_term_frequencies"
    con.sql(f"DROP TABLE IF EXISTS {result_table}")
    con.sql(tf_sql).create(result_table)

    # Clean up intermediate table
    con.execute(f"DROP TABLE IF EXISTS {cleaned_table}")

    return con.table(result_table)


def derive_inverted_index(
    cleaned_address_table: DuckDBPyRelation,
    con: DuckDBPyConnection,
    max_unique_ids_per_trigram: int = 20,
    *,
    debug_options: Optional["DebugOptions"] = None,
) -> DuckDBPyRelation:
    """Derive a trigram-based inverted index from already-cleaned canonical data.

    This function expects pre-cleaned address data
    (output of prepare_data_for_matching)
    with `clean_full_address` and `unique_id` columns already present.
    It computes trigrams
    (consecutive 3-token sequences) and builds an inverted index mapping each trigram
    to a list of unique_ids. Trigrams appearing in more than `max_unique_ids_per_trigram`
    records are filtered out as they provide poor blocking selectivity.

    Example usage:
        # First clean canonical data (no inverted index needed)
        df_canonical_clean = prepare_data_for_matching(df_canonical, con)

        # Derive inverted index from cleaned canonical data
        inverted_idx = derive_inverted_index(df_canonical_clean, con)

        # Clean messy data using the inverted index
        df_messy_clean = prepare_data_for_matching(
            df_messy, con, inverted_index=inverted_idx
        )

    Args:
        cleaned_address_table: Pre-cleaned address relation with `clean_full_address`
            and `unique_id` columns (output of prepare_data_for_matching).
        con: DuckDB connection.
        max_unique_ids_per_trigram: Maximum number of unique_ids a trigram can
            reference before being filtered out. Default 20.
        debug_options: Optional debug configuration for pipeline execution.

    Returns:
        Inverted index table with 'trigram' (VARCHAR)
        and 'unique_ids' (LIST) columns.
    """
    uid = _uid()

    # Generate trigrams and build inverted index using pipeline stages
    trigram_pipeline = create_sql_pipeline(
        con,
        input_rel=cleaned_address_table,
        stage_specs=[
            _derive_trigrams_from_address_tokens,
            _build_inverted_index_from_trigrams(max_unique_ids_per_trigram),
        ],
        pipeline_name="Build inverted index",
        pipeline_description="Derive trigrams and aggregate into inverted index",
    )
    inverted_index_result = trigram_pipeline.run(debug_options)

    # Materialise the result
    result_table = f"__ukam_derived_inverted_index_{uid}"
    con.sql(f"DROP TABLE IF EXISTS {result_table}")
    inverted_index_result.create(result_table)

    return con.table(result_table)


# Chunking this requires a three phase approach:
# 1. Clean data in chunks without term frequencies
# 2. Register term frequency tables (either provided or pre-baked)
# 3. Use term frequencies to populate term frequency fields in cleaned data and
#   finally apply QUEUE_POST_TF
def prepare_data_for_matching(
    address_table: DuckDBPyRelation,
    con: DuckDBPyConnection,
    num_of_chunks: int = 10,
    term_frequency_lookup: Optional[DuckDBPyRelation] = None,
    inverted_index: Optional[DuckDBPyRelation] = None,
    derive_distinguishing_wrt_adjacent_records: bool = False,
    *,
    debug_options: Optional[DebugOptions] = None,
) -> DuckDBPyRelation:
    """Prepare address data for matching.

    Args:
        address_table: Input address relation with standard schema.
        con: DuckDB connection.
        num_of_chunks: Number of chunks to split the data into. Term frequencies
            are applied from either the provided lookup table or pre-baked frequencies,
            then chunks are processed with those frequencies applied.
        term_frequency_lookup: Optional pre-computed term frequency table with
            'token' and 'rel_freq' columns. Use derive_term_frequencies_table()
            to create this from a reference dataset (typically the canonical addresses).
            If not provided, uses the package's pre-baked term frequencies.
        inverted_index: Optional pre-computed trigram inverted index table with
            'trigram' and 'unique_ids' columns. Use derive_inverted_index()
            to create this from canonical addresses. When provided, the function
            derives trigrams from addresses and looks up matching unique_ids,
            populating the `exploding_unique_ids` column. When not provided,
            `exploding_unique_ids` is set to [unique_id] (single-element array).
        derive_distinguishing_wrt_adjacent_records: Whether to derive distinguishing
            tokens relative to adjacent records.
        debug_options: Optional debug configuration for pipeline execution.
            Note: Debug options are only applied on the first iteration to avoid
            excessive logging output.

    Returns:
        Cleaned address data with computed term frequencies, including numeric
        term frequency columns:
        tf_numeric_token_1, tf_numeric_token_2, tf_numeric_token_3
        and an `exploding_unique_ids` column for blocking.

    Example:
        # Recommended workflow for matching messy data against canonical:
        # 1. Optionally derive term frequencies from canonical
        tf_table = derive_term_frequencies_table(df_canonical, con)

        # 2. Clean canonical data first (no inverted index needed)
        df_canonical_clean = prepare_data_for_matching(
            df_canonical, con, term_frequency_lookup=tf_table
        )

        # 3. Derive inverted index from cleaned canonical
        inverted_idx = derive_inverted_index(df_canonical_clean, con)

        # 4. Clean messy data using the inverted index
        df_messy_clean = prepare_data_for_matching(
            df_messy, con, term_frequency_lookup=tf_table, inverted_index=inverted_idx
        )

        # Using pre-baked term frequencies (default):
        df_prepared = prepare_data_for_matching(df_addresses, con)
    """
    uid = _uid()

    # Clean data in chunks (without term frequencies)
    cleaned_address_table = clean_data_pre_term_frequencies(
        address_table, con, num_of_chunks=num_of_chunks, debug_options=debug_options
    )

    total_rows = cleaned_address_table.count("*").fetchone()[0]
    _create_term_frequency_tables(con, term_frequency_lookup=term_frequency_lookup)

    # Register inverted index table if provided (for use in pipeline stages)
    inv_idx_table_name = _register_inverted_index_table(con, inverted_index)

    # Determine which trigram stages to use as additional pipeline stages
    trigram_stages = (
        list(QUEUE_TRIGRAM_WITH_INVERTED_INDEX)
        if inv_idx_table_name is not None
        else list(QUEUE_TRIGRAM_SELF)
    )

    chunk_size = _calculate_chunk_size(total_rows, num_of_chunks)
    total_chunks = (total_rows + chunk_size - 1) // chunk_size

    # Get the underlying table name for direct access
    cleaned_table_name = cleaned_address_table.alias

    processed_table = f"__ukam_addresses_processed_{uid}"

    # Apply term frequencies and trigram blocking to cleaned chunks
    for chunk_index in range(total_chunks):
        chunk_started_at = time.perf_counter()
        chunk = con.sql(f"""
        SELECT *
            FROM {cleaned_table_name}
            WHERE (abs(hash(original_address_concat)) % {total_chunks}) = {chunk_index}
        """)

        # Process chunk: apply term frequencies + trigram blocking in one pass
        processed_chunk = _clean_data_using_precomputed_rel_tok_freq(
            chunk,
            con=con,
            pre_cleaned_addresses=True,
            derive_distinguishing_wrt_adjacent_records=(
                derive_distinguishing_wrt_adjacent_records
            ),
            additional_stages=trigram_stages,
            debug_options=debug_options if chunk_index == 0 else None,
        )

        if chunk_index == 0:
            con.execute(f"DROP TABLE IF EXISTS {processed_table}")
            processed_chunk.create(processed_table)
        else:
            processed_chunk.insert_into(processed_table)

        # Delete processed rows from the intermediate cleaned table to free memory
        con.execute(f"""
            DELETE FROM {cleaned_table_name}
            WHERE (abs(hash(original_address_concat)) % {total_chunks}) = {chunk_index}
        """)

        _log_progress(
            total_rows,
            min((chunk_index + 1) * chunk_size, total_rows),
            stage_type="Applied term frequencies: ",
            chunk_index=chunk_index,
            total_chunks=total_chunks,
            chunk_elapsed_seconds=time.perf_counter() - chunk_started_at,
        )

    # Verify the intermediate table is now empty (all chunks processed)
    remaining_rows = con.sql(f"SELECT COUNT(*) FROM {cleaned_table_name}").fetchone()[0]
    if remaining_rows != 0:
        raise ValueError(
            "Expected intermediate table "
            f"{cleaned_table_name} to be empty after processing, "
            f"but found {remaining_rows} rows remaining."
        )

    # Drop the now-empty intermediate table
    con.execute(f"DROP TABLE IF EXISTS {cleaned_table_name}")

    # Clean up inverted index table if it was registered
    if inv_idx_table_name is not None:
        con.execute(f"DROP TABLE IF EXISTS {inv_idx_table_name}")

    return con.table(processed_table)


__all__ = [
    "clean_data_pre_term_frequencies",
    "derive_inverted_index",
    "derive_term_frequencies_table",
    "prepare_data_for_matching",
]
