from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Optional

from duckdb import DuckDBPyConnection, DuckDBPyRelation

from uk_address_matcher.cleaning.pipelines import (
    _clean_data_using_precomputed_rel_tok_freq,
    _clean_data_with_minimal_steps,
    _create_term_frequency_tables,
)
from uk_address_matcher.linking_model.exact_matching.resolve_with_trigrams import (
    _ngram_expression,
)
from uk_address_matcher.sql_pipeline.helpers import _uid

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
    percentage_complete = (
        processed_records / total_records if total_records > 0 else 1.0
    )

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


def _should_use_data_specific_term_frequencies(
    total_records: int,
    use_data_specific_term_frequencies: bool | None,
) -> bool:
    if use_data_specific_term_frequencies is True:
        return True
    elif use_data_specific_term_frequencies is False:
        return False
    else:
        # Auto-select TF strategy based on record count if not explicitly specified
        # Use data-specific TFs for large datasets (>= 500k records)
        return total_records >= 500_000


def _create_trigram_reverse_index(
    cleaned_address_table: DuckDBPyRelation,
    con: DuckDBPyConnection,
    *,
    max_unique_ids_per_trigram: int = 20,
) -> None:
    """Create a reverse index mapping trigrams to unique_ids for blocking.

    Generates all trigrams (3-token sequences) from each address's clean_full_address
    field, then builds an inverted index mapping each trigram to the list of unique_ids
    that contain it. Only trigrams appearing in between 1 and max_unique_ids_per_trigram
    addresses are retained, to balance selectivity and coverage.

    The resulting table is persisted as __ukam_ngram_reverse_index with columns:
        - trigram: LIST(VARCHAR) - the 3-token sequence
        - unique_ids_to_explode: LIST(VARCHAR) - list of unique_ids containing this trigram

    Args:
        cleaned_address_table: Pre-cleaned address relation with clean_full_address
            and unique_id columns.
        con: DuckDB connection.
        max_unique_ids_per_trigram: Maximum number of unique_ids per trigram to retain.
            Trigrams appearing in more addresses are excluded as they provide
            insufficient selectivity for blocking. Defaults to 20.
    """
    table_name = cleaned_address_table.alias
    ngram_expr = _ngram_expression("string_split(clean_full_address, ' ')", 3)

    # Generate trigrams and explode into individual rows
    con.execute(f"""
        CREATE OR REPLACE TEMPORARY TABLE __ukam_trigram_index AS
        SELECT
            unnest({ngram_expr}) AS trigram,
            unique_id
        FROM {table_name}
    """)

    # Build reverse index: trigram -> list of unique_ids
    # Filter to keep only trigrams with 1 to max_unique_ids_per_trigram unique_ids
    con.execute(f"""
        CREATE OR REPLACE TABLE __ukam_ngram_reverse_index AS
        SELECT
            trigram,
            LIST(DISTINCT unique_id) AS unique_ids_to_explode
        FROM __ukam_trigram_index
        GROUP BY trigram
        HAVING length(unique_ids_to_explode) BETWEEN 1 AND {max_unique_ids_per_trigram}
    """)

    # Clean up temporary table
    con.execute("DROP TABLE IF EXISTS __ukam_trigram_index")


def _add_exploding_unique_ids_from_reverse_index(
    cleaned_address_table: DuckDBPyRelation,
    con: DuckDBPyConnection,
) -> DuckDBPyRelation:
    """Add exploding_unique_ids column by looking up trigrams in the reverse index.

    For each address, generates all trigrams from clean_full_address, joins them
    against the pre-built reverse index (__ukam_ngram_reverse_index), and aggregates
    the matched unique_ids into a single list. If no matching trigrams are found,
    the column contains an empty list.

    Args:
        cleaned_address_table: Pre-cleaned address relation with clean_full_address
            and unique_id columns.
        con: DuckDB connection with __ukam_ngram_reverse_index table available.

    Returns:
        Address relation with an additional exploding_unique_ids column containing
        the list of candidate unique_ids from the reverse index.
    """
    table_name = cleaned_address_table.alias
    ngram_expr = _ngram_expression("string_split(clean_full_address, ' ')", 3)

    # Create a temporary table of address trigrams
    con.execute(f"""
        CREATE OR REPLACE TEMPORARY TABLE __ukam_address_trigrams AS
        SELECT
            ukam_address_id,
            unnest({ngram_expr}) AS trigram
        FROM {table_name}
    """)

    # Join address trigrams to reverse index and aggregate unique_ids
    con.execute("""
        CREATE OR REPLACE TEMPORARY TABLE __ukam_matched_unique_ids AS
        SELECT
            addr_tri.ukam_address_id,
            LIST(DISTINCT ri_uid ORDER BY ri_uid) AS exploding_unique_ids
        FROM __ukam_address_trigrams AS addr_tri
        INNER JOIN __ukam_ngram_reverse_index AS ri
            ON addr_tri.trigram = ri.trigram,
        UNNEST(ri.unique_ids_to_explode) AS u(ri_uid)
        GROUP BY addr_tri.ukam_address_id
    """)

    # Join back to original table to add the exploding_unique_ids column
    # Materialise immediately as a table to avoid lazy evaluation issues
    # when the temporary tables are dropped
    uid_result = _uid()
    con.execute(f"""
        CREATE OR REPLACE TABLE __ukam_with_exploding_result_{uid_result} AS
        SELECT
            addr.*,
            COALESCE(matched.exploding_unique_ids, []) AS exploding_unique_ids
        FROM {table_name} AS addr
        LEFT JOIN __ukam_matched_unique_ids AS matched
            ON addr.ukam_address_id = matched.ukam_address_id
    """)
    result = con.table(f"__ukam_with_exploding_result_{uid_result}")

    # Clean up temporary tables
    con.execute("DROP TABLE IF EXISTS __ukam_address_trigrams")
    con.execute("DROP TABLE IF EXISTS __ukam_matched_unique_ids")

    return result


def _add_self_unique_id_as_exploding(
    cleaned_address_table: DuckDBPyRelation,
    con: DuckDBPyConnection,
) -> DuckDBPyRelation:
    """Add exploding_unique_ids column containing just the record's own unique_id.

    Used for canonical/reference data where each record should only match itself
    during the Splink blocking phase.

    Args:
        cleaned_address_table: Pre-cleaned address relation with unique_id column.
        con: DuckDB connection.

    Returns:
        Address relation with exploding_unique_ids column as [unique_id].
    """
    table_name = cleaned_address_table.alias
    return con.sql(f"""
        SELECT
            *,
            [unique_id] AS exploding_unique_ids
        FROM {table_name}
    """)


def clean_data_with_minimal_steps(
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

        # Process the chunk without address ID, applying debug options only on first iteration
        processed_chunk = _clean_data_with_minimal_steps(
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


# Chunking this requires a three phase approach:
# 1. Clean data in chunks without term frequencies
# 2. At the end of each chunk, accumulate token counts to compute global term frequencies
# 3. Use computed term frequencies to populate term frequency fields in cleaned data and
#   finally apply QUEUE_POST_TF
def clean_data_with_term_frequencies(
    address_table: DuckDBPyRelation,
    con: DuckDBPyConnection,
    num_of_chunks: int = 10,
    use_data_specific_term_frequencies: bool | None = None,
    derive_distinguishing_wrt_adjacent_records: bool = False,
    create_reverse_index: bool | None = None,
    *,
    debug_options: Optional[DebugOptions] = None,
) -> DuckDBPyRelation:
    """Clean address data using term frequencies computed from the input data.

    Computes relative token frequencies directly from the input address table
    and applies them during cleaning. This approach ensures term frequencies
    reflect the specific input dataset, making it ideal for single-run analyses
    or when you have a representative sample.

    The pipeline applies all stages from QUEUE_PRE_TF + term frequency stage + QUEUE_POST_TF
    (see pipelines.py for full stage list). Post-TF stages include:
    - Moving common end tokens to a dedicated field
    - Identifying first unusual tokens
    - Separating distinguishing unusual tokens

    When chunking is enabled, term frequencies are computed once across the full dataset,
    then each chunk is processed independently and results are unioned.

    Args:
        address_table: Input address relation with standard schema.
        con: DuckDB connection.
        num_of_chunks: Number of chunks to split the data into. Term frequencies
            are computed upfront from the full dataset, then chunks are processed with
            precomputed frequencies applied.
        use_data_specific_term_frequencies:
            - True: Always compute TFs from input data
            - False: Always use package's precomputed TFs
            - None (default): Auto-select based on record count
                (< 1M → precomputed; ≥ 1M → data-specific)
        derive_distinguishing_wrt_adjacent_records: Whether to derive distinguishing
            tokens relative to adjacent records.
        create_reverse_index:
            - True: Create the trigram reverse index from this data (for canonical/
              reference data). Persists the index as __ukam_ngram_reverse_index and
              adds exploding_unique_ids as [unique_id] (i.e. just itself).
            - False: Look up trigrams against an existing reverse index (for fuzzy/
              input data). Adds exploding_unique_ids containing candidate unique_ids
              from matching trigrams in the index.
            - None (default): Do not use reverse index functionality; no
              exploding_unique_ids column is added.
        debug_options: Optional debug configuration for pipeline execution.
            Note: Debug options are only applied on the first iteration to avoid
            excessive logging output.

    Returns:
        Cleaned address data with computed term frequencies, including numeric
        term frequency columns (tf_numeric_token_1, tf_numeric_token_2, tf_numeric_token_3).
    """
    uid = _uid()

    # Clean data in chunks (without term frequencies)
    cleaned_address_table = clean_data_with_minimal_steps(
        address_table, con, num_of_chunks=num_of_chunks, debug_options=debug_options
    )

    # Handle reverse index creation or lookup for blocking
    if create_reverse_index is True:
        # For canonical/reference data: build the trigram reverse index
        _create_trigram_reverse_index(cleaned_address_table, con)
        # Add exploding_unique_ids as [unique_id] for canonical data
        cleaned_address_table = _add_self_unique_id_as_exploding(
            cleaned_address_table, con
        )
        # Re-register the updated table
        uid_canonical = _uid()
        con.execute(f"DROP TABLE IF EXISTS __ukam_with_exploding_{uid_canonical}")
        cleaned_address_table.create(f"__ukam_with_exploding_{uid_canonical}")
        cleaned_address_table = con.table(f"__ukam_with_exploding_{uid_canonical}")
    elif create_reverse_index is False:
        # For fuzzy/input data: look up against existing reverse index
        # The index must have been created by a prior call with create_reverse_index=True
        cleaned_address_table = _add_exploding_unique_ids_from_reverse_index(
            cleaned_address_table, con
        )
        # Re-register the updated table
        uid_lookup = _uid()
        con.execute(f"DROP TABLE IF EXISTS __ukam_with_exploding_{uid_lookup}")
        cleaned_address_table.create(f"__ukam_with_exploding_{uid_lookup}")
        cleaned_address_table = con.table(f"__ukam_with_exploding_{uid_lookup}")
    # If create_reverse_index is None, skip reverse index handling entirely

    total_rows = cleaned_address_table.count("*").fetchone()[0]
    use_data_specific_tfs = _should_use_data_specific_term_frequencies(
        total_rows, use_data_specific_term_frequencies
    )
    _create_term_frequency_tables(
        cleaned_address_table,
        con,
        use_data_specific_term_frequencies=use_data_specific_tfs,
    )

    chunk_size = _calculate_chunk_size(total_rows, num_of_chunks)
    total_chunks = (total_rows + chunk_size - 1) // chunk_size

    # Get the underlying table name for direct access
    cleaned_table_name = cleaned_address_table.alias

    # Apply term frequencies to cleaned chunks
    for chunk_index in range(total_chunks):
        chunk_started_at = time.perf_counter()
        chunk = con.sql(f"""
        SELECT *
            FROM {cleaned_table_name}
            WHERE (abs(hash(original_address_concat)) % {total_chunks}) = {chunk_index}
        """)

        # Numeric TF columns should only be attached when using precomputed TFs
        # If we are chunking, we want to precompute rel token freqs and then use them
        processed_chunk = _clean_data_using_precomputed_rel_tok_freq(
            chunk,
            con=con,
            pre_cleaned_addresses=True,
            derive_distinguishing_wrt_adjacent_records=derive_distinguishing_wrt_adjacent_records,
            debug_options=debug_options if chunk_index == 0 else None,
        )

        if chunk_index == 0:
            con.execute(f"DROP TABLE IF EXISTS __ukam_addresses_processed_{uid}")
            processed_chunk.create(f"__ukam_addresses_processed_{uid}")
        else:
            processed_chunk.insert_into(f"__ukam_addresses_processed_{uid}")

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
            f"Expected intermediate table {cleaned_table_name} to be empty after processing, "
            f"but found {remaining_rows} rows remaining."
        )

    # Drop the now-empty intermediate table
    con.execute(f"DROP TABLE IF EXISTS {cleaned_table_name}")

    return con.table(f"__ukam_addresses_processed_{uid}")


__all__ = [
    "clean_data_with_minimal_steps",
    "clean_data_with_term_frequencies",
]
