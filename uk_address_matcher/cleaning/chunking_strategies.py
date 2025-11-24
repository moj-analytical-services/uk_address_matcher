from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Optional

from duckdb import DuckDBPyConnection, DuckDBPyRelation

from uk_address_matcher.cleaning.pipelines import (
    _clean_data_using_precomputed_rel_tok_freq,
    _clean_data_with_minimal_steps,
    _create_term_frequency_tables,
)
from uk_address_matcher.sql_pipeline.helpers import _uid

if TYPE_CHECKING:
    from uk_address_matcher.sql_pipeline.runner import DebugOptions

logger = logging.getLogger("uk_address_matcher")


def _log_progress(total_records: int, processed_records: int, stage_type: str) -> None:
    percentage_complete = (
        processed_records / total_records if total_records > 0 else 1.0
    )
    logger.info(
        f"{stage_type}"
        f"{processed_records:,.0f} records ({percentage_complete:.0%} complete)"
    )


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
    address_table.to_table(f"__ukam_input_addresses_{uid}")
    # For chunked processing, don't add ID yet - process chunks first
    total_rows = address_table.count("*").fetchone()[0]

    chunk_size = _calculate_chunk_size(total_rows, num_of_chunks)

    con.execute(f"DROP TABLE IF EXISTS __ukam_chunked_addresses_{uid}")

    for chunk_index, offset in enumerate(range(0, total_rows, chunk_size)):
        # NB: using address_table.limit(n=chunk_size, offset=offset).execute()
        # causes the lazy eval to return the same rows each time
        chunk = con.sql(f"""
        SELECT *
            FROM __ukam_input_addresses_{uid}
            LIMIT {chunk_size} OFFSET {offset}
        """)

        # Process the chunk without address ID, applying debug options only on first iteration
        processed_chunk = _clean_data_with_minimal_steps(
            chunk, con, debug_options=debug_options if chunk_index == 0 else None
        )

        _log_progress(
            total_rows,
            min(offset + chunk_size, total_rows),
            stage_type="Cleaned and preprocessed: ",
        )

        if chunk_index == 0:
            processed_chunk.create(f"__ukam_chunked_addresses_{uid}")
        else:
            processed_chunk.insert_into(f"__ukam_chunked_addresses_{uid}")

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
    cleaned_address_table.to_table(f"__ukam_cleaned_addresses_{uid}")

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

    # Apply term frequencies to cleaned chunks
    for chunk_index, offset in enumerate(range(0, total_rows, chunk_size)):
        chunk = con.sql(f"""
        SELECT *
            FROM __ukam_cleaned_addresses_{uid}
            LIMIT {chunk_size} OFFSET {offset}
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

        _log_progress(
            total_rows,
            min(offset + chunk_size, total_rows),
            stage_type="Applied term frequencies: ",
        )

        if offset == 0:
            con.execute(f"DROP TABLE IF EXISTS __ukam_addresses_processed_{uid}")
            processed_chunk.create(f"__ukam_addresses_processed_{uid}")
        else:
            processed_chunk.insert_into(f"__ukam_addresses_processed_{uid}")

    return con.table(f"__ukam_addresses_processed_{uid}")


__all__ = [
    "clean_data_with_minimal_steps",
    "clean_data_with_term_frequencies",
]
