from __future__ import annotations

import logging
from typing import Callable, Optional

from duckdb import DuckDBPyConnection, DuckDBPyRelation

from uk_address_matcher.cleaning.pipelines import (
    _clean_data_using_precomputed_rel_tok_freq,
    _clean_data_with_minimal_steps,
    _get_address_token_frequencies_from_address_table,
    _get_numeric_term_frequencies_from_address_table,
    clean_data_on_the_fly,
)
from uk_address_matcher.sql_pipeline.helpers import _uid
from uk_address_matcher.sql_pipeline.runner import DebugOptions, DuckDBPipeline

PipelineFactory = Callable[[DuckDBPyRelation], DuckDBPipeline]

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
    # Ensure chunk size is reasonable: minimum 10k records per chunk
    num_of_chunks = min(num_of_chunks, max(1, total_records // 10_000))
    chunk_size = (total_records + num_of_chunks - 1) // num_of_chunks
    return chunk_size


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
def clean_data_using_precomputed_rel_tok_freq(
    address_table: DuckDBPyRelation,
    con: DuckDBPyConnection,
    num_of_chunks: int = 10,
    derive_distinguishing_wrt_adjacent_records: bool = False,
    *,
    debug_options: Optional[DebugOptions] = None,
):
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
    address_table.to_table(f"__ukam_input_addresses_{uid}")

    # Clean data in chunks (without term frequencies)
    cleaned_address_table = clean_data_with_minimal_steps(
        address_table, con, num_of_chunks=num_of_chunks
    )
    cleaned_address_table.to_table(f"__ukam_cleaned_addresses_{uid}")

    # Compute term frequencies from the cleaned data
    address_token_frequencies_rel = _get_address_token_frequencies_from_address_table(
        cleaned_address_table, con, pre_cleaned_addresses=True
    )
    numeric_term_frequencies_rel = _get_numeric_term_frequencies_from_address_table(
        cleaned_address_table, con, pre_cleaned_addresses=True
    )
    numeric_term_frequencies_rel.create(f"__ukam_numeric_term_frequencies_{uid}")
    total_rows = cleaned_address_table.count("*").fetchone()[0]

    chunk_size = _calculate_chunk_size(total_rows, num_of_chunks)

    # Apply term frequencies to cleaned chunks
    for chunk_index, offset in enumerate(range(0, total_rows, chunk_size)):
        # Chunk from the CLEANED data, not the original
        chunk = con.sql(f"""
        SELECT *
            FROM __ukam_cleaned_addresses_{uid}
            LIMIT {chunk_size} OFFSET {offset}
        """)

        # If we are chunking, we want to precompute rel token freqs and then use them
        processed_chunk = _clean_data_using_precomputed_rel_tok_freq(
            chunk,
            con=con,
            rel_tok_freq_table=address_token_frequencies_rel,
            pre_cleaned_addresses=True,
            derive_distinguishing_wrt_adjacent_records=derive_distinguishing_wrt_adjacent_records,
            debug_options=debug_options if chunk_index == 0 else None,
        )
        processed_chunk.create_view("__ukam_cleaned_chunk")

        # TODO(ThomasHepworth): really, this should be another stage...
        # Optional staging for our pipeline runners?
        chunk_with_tf = con.sql(
            f"""
            SELECT
                df.*,
                tf1.tf_numeric_token AS tf_numeric_token_1,
                tf2.tf_numeric_token AS tf_numeric_token_2,
                tf3.tf_numeric_token AS tf_numeric_token_3
            FROM __ukam_cleaned_chunk AS df
            LEFT JOIN __ukam_numeric_term_frequencies_{uid} AS tf1
                ON df.numeric_token_1 = tf1.numeric_token
            LEFT JOIN __ukam_numeric_term_frequencies_{uid} AS tf2
                ON df.numeric_token_2 = tf2.numeric_token
            LEFT JOIN __ukam_numeric_term_frequencies_{uid} AS tf3
                ON df.numeric_token_3 = tf3.numeric_token
            """
        )
        _log_progress(
            total_rows,
            min(offset + chunk_size, total_rows),
            stage_type="Applied term frequencies: ",
        )

        if offset == 0:
            con.execute(f"DROP TABLE IF EXISTS __ukam_addresses_processed_{uid}")
            chunk_with_tf.create(f"__ukam_addresses_processed_{uid}")
        else:
            chunk_with_tf.insert_into(f"__ukam_addresses_processed_{uid}")

    return con.table(f"__ukam_addresses_processed_{uid}")


__all__ = [
    "clean_data_with_minimal_steps",
    "clean_data_on_the_fly",
    "clean_data_using_precomputed_rel_tok_freq",
]
