from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Optional

from duckdb import DuckDBPyConnection, DuckDBPyRelation

from uk_address_matcher.cleaning.pipelines import (
    QUEUE_FOR_TF_DERIVATION,
    QUEUE_INVERTED_INDEX_LOOKUP,
    QUEUE_INVERTED_INDEX_SELF,
    QUEUE_ROADLIKE_PLACE_PREPARATION,
    _clean_data_pre_term_frequencies,
    _clean_data_using_precomputed_rel_tok_freq,
    _create_term_frequency_tables,
    _ensure_postcode_column,
    _register_inverted_index_table,
)
from uk_address_matcher.cleaning.steps.inverted_index import (
    DEFAULT_INDEXING_STRATEGIES,
    DEFAULT_INVERTED_INDEX_LOOKUP_STRATEGIES,
    MESSY_INVERTED_INDEX_LOOKUP_STRATEGIES,
    InvertedIndexLookupStrategy,
    PhysicalIndexStrategy,
    _build_inverted_index_from_keys,
    _build_inverted_index_from_scalar_keys,
    _derive_keys_for_strategy,
    _lookup_keys_in_inverted_index,
)
from uk_address_matcher.cleaning.steps.roadlike_places import (
    derive_top_1_road_keys,
    roadlike_place_catalog_sql,
    roadlike_place_prepared_candidate_sql,
)
from uk_address_matcher.cleaning.steps.token_parsing import (
    _separate_distinguishing_start_tokens_from_with_respect_to_adjacent_records,
)
from uk_address_matcher.logging.chunking import (
    log_chunk_progress,
    log_stage_complete,
    log_stage_start,
)
from uk_address_matcher.logging.progress import (
    ShowProgress,
    _ProgressBar,
    resolve_progress_mode,
)
from uk_address_matcher.sql_pipeline.helpers import (
    _drop_table_and_registered_aliases,
    _uid,
)
from uk_address_matcher.sql_pipeline.runner import create_sql_pipeline

if TYPE_CHECKING:
    from uk_address_matcher.sql_pipeline.runner import DebugOptions

logger = logging.getLogger("uk_address_matcher")

ROAD_SCORING_CHUNK_ROWS = 10_000_000


def _materialise_relation(
    con: DuckDBPyConnection,
    relation: DuckDBPyRelation,
    table_name: str,
) -> DuckDBPyRelation:
    # Ensure any prior table/view/registered alias with this name is removed.
    _drop_table_and_registered_aliases(con, table_name)

    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM ({relation.sql_query()})")
    return con.table(table_name)


def _materialise_relation_with_ukam_address_id(
    con: DuckDBPyConnection,
    relation: DuckDBPyRelation,
    table_name: str,
) -> DuckDBPyRelation:
    """Sort cleaned rows and assign matching public and private row IDs."""
    source_columns = tuple(
        column
        for column in relation.columns
        if column not in {"ukam_address_id", "__ukam_row_id"}
    )
    sort_columns = tuple(
        column
        for column in ("postcode", "unique_id", "clean_full_address", "filename")
        if column in source_columns
    )
    required_sort_columns = {"postcode", "unique_id", "clean_full_address"}
    if not required_sort_columns.issubset(sort_columns):
        missing = sorted(required_sort_columns.difference(sort_columns))
        raise ValueError(f"Cleaned relation is missing ordering columns: {missing}")

    tie_breaker_columns = tuple(
        column for column in source_columns if column not in sort_columns
    )
    order_columns = (*sort_columns, *tie_breaker_columns)
    qualified_order = ", ".join(f'_ukam_src."{column}"' for column in order_columns)
    source_projection = ", ".join(f'_ukam_src."{column}"' for column in source_columns)

    _drop_table_and_registered_aliases(con, table_name)
    con.execute(f"""
        CREATE TABLE {table_name} AS
        WITH numbered AS (
            SELECT
                CAST(ROW_NUMBER() OVER (ORDER BY {qualified_order}) AS INTEGER)
                    AS ukam_address_id,
                {source_projection}
            FROM ({relation.sql_query()}) AS _ukam_src
        )
        SELECT
            numbered.*,
            CAST(numbered.ukam_address_id AS BIGINT) AS __ukam_row_id
        FROM numbered
        ORDER BY numbered.ukam_address_id
    """)
    return con.table(table_name)


def _drop_tables_with_prefix(con: DuckDBPyConnection, prefix: str) -> None:
    table_names = [name for (name,) in con.execute("SHOW TABLES").fetchall()]
    for table_name in table_names:
        if table_name.startswith(prefix):
            _drop_table_and_registered_aliases(con, table_name)


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


def _add_canonical_road_blocking_keys(
    canonical_addresses: DuckDBPyRelation,
    con: DuckDBPyConnection,
    *,
    num_of_chunks: int = 1,
    require_catalogue_support: bool = True,
) -> DuckDBPyRelation:
    """Add compact, global road-key cardinalities used by selective blockers."""
    preference_order = []
    if "filename" in canonical_addresses.columns:
        preference_order.append("""
            CASE filename
                WHEN 'add_gb_builtaddress.parquet' THEN 1
                WHEN 'add_gb_royalmailaddress.parquet' THEN 2
                ELSE 3
            END
        """)
    preference_order.extend(
        column
        for column in ("clean_full_address", "postcode", "ukam_address_id")
        if column in canonical_addresses.columns
    )
    uid = _uid()
    preferred_table = f"__ukam_canonical_road_preferred_{uid}"
    road_keys_table = f"__ukam_canonical_road_keys_{uid}"
    chunk_keys_table = f"__ukam_canonical_road_chunk_{uid}"
    enriched_table = f"__ukam_canonical_with_road_{uid}"
    preferred_columns = [
        "unique_id",
        "clean_full_address",
        "postcode",
        "numeric_tokens",
    ]
    if "rightmost_numeric_position" in canonical_addresses.columns:
        preferred_columns.append("rightmost_numeric_position")
    preserve_insertion_order = bool(
        con.execute("SELECT current_setting('preserve_insertion_order')").fetchone()[0]
    )
    con.execute("SET preserve_insertion_order = false")
    try:
        _drop_table_and_registered_aliases(con, preferred_table)
        con.execute(f"""
            CREATE TEMPORARY TABLE {preferred_table} AS
            SELECT {", ".join(preferred_columns)}
            FROM ({canonical_addresses.sql_query()}) AS source
            QUALIFY row_number() OVER (
                PARTITION BY unique_id
                ORDER BY {", ".join(preference_order)}
            ) = 1
        """)
        preferred_addresses = con.table(preferred_table)
        preferred_row_count = int(preferred_addresses.count("*").fetchone()[0])
    except Exception:
        con.execute(
            f"SET preserve_insertion_order = {str(preserve_insertion_order).lower()}"
        )
        raise
    required_chunks = max(
        1,
        (preferred_row_count + ROAD_SCORING_CHUNK_ROWS - 1) // ROAD_SCORING_CHUNK_ROWS,
    )
    road_chunk_count = min(max(1, num_of_chunks), required_chunks)
    _drop_table_and_registered_aliases(con, road_keys_table)
    try:
        for chunk_index in range(road_chunk_count):
            if road_chunk_count == 1:
                chunk = preferred_addresses
            else:
                chunk = con.sql(f"""
                    SELECT *
                    FROM {preferred_table}
                    WHERE hash(CAST(unique_id AS VARCHAR)) % {road_chunk_count}
                        = {chunk_index}
                """)
            chunk_keys = derive_top_1_road_keys(
                con,
                chunk,
                output_table=chunk_keys_table,
                require_catalogue_support=require_catalogue_support,
            )
            if chunk_index == 0:
                con.execute(f"""
                    CREATE TEMPORARY TABLE {road_keys_table} AS
                    SELECT * FROM ({chunk_keys.sql_query()})
                """)
            else:
                con.execute(f"""
                    INSERT INTO {road_keys_table}
                    SELECT * FROM ({chunk_keys.sql_query()})
                """)
            _drop_table_and_registered_aliases(con, chunk_keys_table)
    except Exception:
        _drop_table_and_registered_aliases(con, road_keys_table)
        raise
    finally:
        _drop_table_and_registered_aliases(con, chunk_keys_table)
        _drop_table_and_registered_aliases(con, preferred_table)
        con.execute(
            f"SET preserve_insertion_order = {str(preserve_insertion_order).lower()}"
        )
    source_columns = ", ".join(
        f'canonical."{column}"' for column in canonical_addresses.columns
    )
    enriched = con.sql(f"""
        WITH road_n1_frequency AS (
            SELECT
                road_features.road_1_norm,
                canonical.numeric_token_1,
                count(*) AS address_count,
                sum(count(*)) OVER (
                    PARTITION BY road_features.road_1_norm
                ) AS road_address_count
            FROM ({canonical_addresses.sql_query()}) AS canonical
            JOIN {road_keys_table} AS road_features USING (unique_id)
            WHERE road_features.road_1_norm IS NOT NULL
            GROUP BY road_features.road_1_norm, canonical.numeric_token_1
        ), road_frequency AS (
            SELECT
                road_1_norm,
                max(road_address_count) AS address_count
            FROM road_n1_frequency
            GROUP BY road_1_norm
        )
        SELECT
            {source_columns},
            road_features.road_1_norm,
            coalesce(road_frequency.address_count, 0) <= 1000
                AS road_frequency_lte_1000,
            coalesce(road_n1_frequency.address_count, 0) <= 32
                AS road_n1_block_size_lte_32
        FROM ({canonical_addresses.sql_query()}) AS canonical
        LEFT JOIN {road_keys_table} AS road_features USING (unique_id)
        LEFT JOIN road_frequency USING (road_1_norm)
        LEFT JOIN road_n1_frequency USING (road_1_norm, numeric_token_1)
    """)
    return _materialise_relation(con, enriched, enriched_table)


def derive_roadlike_places(
    canonical_address_table: DuckDBPyRelation,
    con: DuckDBPyConnection,
    output_path: Path | str,
    *,
    postcode_districts_per_batch: int | None = None,
    debug_options: Optional[DebugOptions] = None,
    show_progress: ShowProgress = "auto",
) -> DuckDBPyRelation:
    """Build a global roadlike-place catalogue from canonical addresses.

    The input must already be canonical-cleaned, including ``clean_full_address``
    and ``numeric_tokens``. Road-specific prepared fields are materialized once;
    candidate extraction is a single pass by default. Postcode-district batching
    is available only as a memory-constrained fallback.
    """
    if postcode_districts_per_batch is not None and postcode_districts_per_batch < 1:
        raise ValueError("postcode_districts_per_batch must be at least 1")

    required_columns = {"unique_id", "clean_full_address", "postcode", "numeric_tokens"}
    missing_columns = sorted(required_columns.difference(canonical_address_table.columns))
    if missing_columns:
        raise ValueError(
            "Canonical address table must be cleaned before deriving roadlike places; "
            f"missing columns: {missing_columns}"
        )

    progress_mode = resolve_progress_mode(show_progress)
    uid = _uid()
    prepared_table = f"__ukam_roadlike_prepared_{uid}"
    candidates_table = f"__ukam_roadlike_candidates_{uid}"
    catalogue_table = f"__ukam_roadlike_places_{uid}"
    total_rows = canonical_address_table.count("*").fetchone()[0]
    if total_rows == 0:
        raise ValueError(
            "Supplied address table has no records. Please provide a non-empty table."
        )

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    stage_label = "Building roadlike-place catalogue"
    started_at = time.perf_counter()
    log_stage_start(
        stage_label,
        total_rows,
        1,
        progress_mode=progress_mode,
    )
    preparation = create_sql_pipeline(
        con,
        input_rel=canonical_address_table,
        stage_specs=QUEUE_ROADLIKE_PLACE_PREPARATION,
        pipeline_name="Prepare roadlike-place input",
        pipeline_description=(
            "Derive suffix-peeled tokens and rightmost numeric anchors once"
        ),
    ).run(debug_options)
    con.execute(
        f"CREATE TEMPORARY TABLE {prepared_table} AS "
        f"SELECT * FROM ({preparation.sql_query()}) AS prepared"
    )
    con.execute(f"ANALYZE {prepared_table}")

    district_expression = "coalesce(nullif(postcode_district, ''), '__UNKNOWN__')"
    if postcode_districts_per_batch is None:
        candidate_sources = [prepared_table]
    else:
        districts = [
            district
            for (district,) in con.sql(
                f"SELECT DISTINCT {district_expression} FROM {prepared_table} ORDER BY 1"
            ).fetchall()
        ]
        candidate_sources = []
        for start_index in range(0, len(districts), postcode_districts_per_batch):
            district_values = ", ".join(
                "'" + district.replace("'", "''") + "'"
                for district in districts[
                    start_index : start_index + postcode_districts_per_batch
                ]
            )
            candidate_sources.append(
                f"(SELECT * FROM {prepared_table} "
                f"WHERE {district_expression} IN ({district_values}))"
            )

    total_batches = len(candidate_sources)
    progress = _ProgressBar(
        label=stage_label,
        total=total_rows,
        total_units=total_batches,
        enabled=progress_mode == "auto",
    )
    con.execute(f"DROP TABLE IF EXISTS {candidates_table}")
    processed_rows = 0

    try:
        for batch_index, candidate_source in enumerate(candidate_sources, start=1):
            batch_started_at = time.perf_counter()
            batch_input = con.sql(f"SELECT * FROM {candidate_source}")
            batch_rows = batch_input.count("*").fetchone()[0]
            candidates = con.sql(roadlike_place_prepared_candidate_sql(candidate_source))
            if batch_index == 1:
                con.execute(
                    f"CREATE TEMPORARY TABLE {candidates_table} AS "
                    f"SELECT * FROM ({candidates.sql_query()}) AS candidates"
                )
            else:
                candidates.insert_into(candidates_table)

            processed_rows += batch_rows
            progress.update(processed_rows, completed_units=batch_index)
            log_chunk_progress(
                total_rows,
                processed_rows,
                stage_label=stage_label,
                progress_mode=progress_mode,
                progress=progress,
                chunk_index=batch_index - 1,
                total_chunks=total_batches,
                chunk_elapsed_seconds=time.perf_counter() - batch_started_at,
            )
    finally:
        progress.close()
        _drop_table_and_registered_aliases(con, prepared_table)

    con.execute(f"ANALYZE {candidates_table}")
    con.execute(f"DROP TABLE IF EXISTS {catalogue_table}")
    con.execute(
        f"CREATE TABLE {catalogue_table} AS "
        f"{roadlike_place_catalog_sql(candidates_table)}"
    )
    escaped_output_path = str(output_path).replace("'", "''")
    con.execute(
        f"COPY {catalogue_table} TO '{escaped_output_path}' "
        "(FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    _drop_table_and_registered_aliases(con, candidates_table)
    log_stage_complete(
        stage_label,
        total_rows,
        time.perf_counter() - started_at,
        progress_mode=progress_mode,
    )
    return con.table(catalogue_table)


def clean_data_pre_term_frequencies(
    address_table: DuckDBPyRelation,
    con: DuckDBPyConnection,
    num_of_chunks: int = 10,
    *,
    debug_options: Optional[DebugOptions] = None,
    show_progress: ShowProgress = "auto",
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
    progress_mode = resolve_progress_mode(show_progress)
    uid = _uid()
    input_name = f"__ukam_input_addresses_{uid}"
    con.register(input_name, address_table)
    total_rows = address_table.count("*").fetchone()[0]

    chunk_size = _calculate_chunk_size(total_rows, num_of_chunks)
    total_chunks = (total_rows + chunk_size - 1) // chunk_size
    processed_records = 0
    stage_label = "Cleaning and preprocessing"
    progress = _ProgressBar(
        label=stage_label,
        total=total_rows,
        total_units=total_chunks,
        enabled=progress_mode == "auto",
    )

    con.execute(f"DROP TABLE IF EXISTS __ukam_chunked_addresses_{uid}")

    stage_started_at = time.perf_counter()
    log_stage_start(
        stage_label,
        total_rows,
        total_chunks,
        progress_mode=progress_mode,
    )

    try:
        for chunk_index in range(total_chunks):
            chunk_started_at = time.perf_counter()
            chunk_query = con.sql(f"""
            SELECT *
                FROM {input_name}
                WHERE (abs(hash(address_concat)) % {total_chunks}) = {chunk_index}
            """)
            chunk_table = f"__ukam_chunk_input_{uid}_{chunk_index}"
            chunk = _materialise_relation(
                con,
                chunk_query,
                chunk_table,
            )
            chunk_row_count = chunk.count("*").fetchone()[0]

            # Assign IDs only after all cleaned chunks have been combined.
            # Apply debug options only on the first iteration.
            processed_chunk = _clean_data_pre_term_frequencies(
                chunk,
                con,
                debug_options=debug_options if chunk_index == 0 else None,
            )

            if chunk_index == 0:
                processed_chunk.create(f"__ukam_chunked_addresses_{uid}")
            else:
                processed_chunk.insert_into(f"__ukam_chunked_addresses_{uid}")

            processed_records += chunk_row_count
            progress.update(
                processed_records,
                completed_units=chunk_index + 1,
            )

            log_chunk_progress(
                total_rows,
                processed_records,
                stage_label=stage_label,
                progress_mode=progress_mode,
                progress=progress,
                chunk_index=chunk_index,
                total_chunks=total_chunks,
                chunk_elapsed_seconds=time.perf_counter() - chunk_started_at,
            )

            _drop_table_and_registered_aliases(con, chunk_table)
    finally:
        progress.close()

    log_stage_complete(
        stage_label,
        total_rows,
        time.perf_counter() - stage_started_at,
        progress_mode=progress_mode,
    )

    _drop_table_and_registered_aliases(con, input_name)
    _drop_tables_with_prefix(con, f"__ukam_chunk_input_{uid}_")

    chunked_table = f"__ukam_chunked_addresses_{uid}"
    cleaned_table = f"__ukam_cleaned_{uid}"
    logger.debug("Assigning deterministic UKAM address IDs")
    _materialise_relation_with_ukam_address_id(
        con,
        con.table(chunked_table),
        cleaned_table,
    )
    logger.debug("Deterministic UKAM address IDs assigned")
    _drop_table_and_registered_aliases(con, chunked_table)
    return con.table(cleaned_table)


def derive_term_frequencies_table(
    address_table: DuckDBPyRelation,
    con: DuckDBPyConnection,
    num_of_chunks: int = 10,
    *,
    debug_options: Optional["DebugOptions"] = None,
    show_progress: ShowProgress = "auto",
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
        show_progress: ``"auto"`` renders live updates in a supported
            interactive terminal and otherwise logs stage boundaries.
            ``"stages"`` logs only stage boundaries; ``"off"`` suppresses
            progress output.

    Returns:
        Term frequency table with 'token' and 'rel_freq' columns.
    """
    progress_mode = resolve_progress_mode(show_progress)
    uid = _uid()

    # Ensure postcode column exists
    address_table = _ensure_postcode_column(address_table)

    # Register input for chunked access
    input_name = f"__ukam_tf_derive_input_{uid}"
    con.register(input_name, address_table)

    total_rows = address_table.count("*").fetchone()[0]
    chunk_size = _calculate_chunk_size(total_rows, num_of_chunks)
    total_chunks = (total_rows + chunk_size - 1) // chunk_size
    processed_records = 0
    stage_label = "Cleaning for TF derivation"
    progress = _ProgressBar(
        label=stage_label,
        total=total_rows,
        total_units=total_chunks,
        enabled=progress_mode == "auto",
    )

    cleaned_table = f"__ukam_tf_derive_cleaned_{uid}"
    con.execute(f"DROP TABLE IF EXISTS {cleaned_table}")

    # Process in chunks using minimal pipeline (clean + tokenise only)
    stage_started_at = time.perf_counter()
    log_stage_start(
        stage_label,
        total_rows,
        total_chunks,
        progress_mode=progress_mode,
    )

    try:
        for chunk_index in range(total_chunks):
            chunk_started_at = time.perf_counter()
            chunk_query = con.sql(f"""
                SELECT *
                FROM {input_name}
                WHERE (abs(hash(address_concat)) % {total_chunks}) = {chunk_index}
            """)
            chunk_table = f"__ukam_tf_chunk_input_{uid}_{chunk_index}"
            chunk = _materialise_relation(
                con,
                chunk_query,
                chunk_table,
            )
            chunk_row_count = chunk.count("*").fetchone()[0]

            pipeline = create_sql_pipeline(
                con,
                input_rel=chunk,
                stage_specs=QUEUE_FOR_TF_DERIVATION,
                pipeline_name="Clean for TF derivation",
                pipeline_description=(
                    "Clean and tokenise for term frequency computation"
                ),
            )
            processed_chunk = pipeline.run(debug_options if chunk_index == 0 else None)

            if chunk_index == 0:
                processed_chunk.create(cleaned_table)
            else:
                processed_chunk.insert_into(cleaned_table)

            processed_records += chunk_row_count
            progress.update(
                processed_records,
                completed_units=chunk_index + 1,
            )

            log_chunk_progress(
                total_rows,
                processed_records,
                stage_label=stage_label,
                progress_mode=progress_mode,
                progress=progress,
                chunk_index=chunk_index,
                total_chunks=total_chunks,
                chunk_elapsed_seconds=time.perf_counter() - chunk_started_at,
            )

            _drop_table_and_registered_aliases(con, chunk_table)
    finally:
        progress.close()

    log_stage_complete(
        stage_label,
        total_rows,
        time.perf_counter() - stage_started_at,
        progress_mode=progress_mode,
    )

    result = _derive_term_frequencies_from_precleaned(cleaned_table, con)

    # Clean up intermediate table
    con.execute(f"DROP TABLE IF EXISTS {cleaned_table}")
    _drop_table_and_registered_aliases(con, input_name)

    return result


def _derive_term_frequencies_from_precleaned(
    cleaned_address_table: DuckDBPyRelation | str,
    con: DuckDBPyConnection,
) -> DuckDBPyRelation:
    """Aggregate corpus token frequencies from canonical pre-clean output."""
    source_sql = (
        cleaned_address_table
        if isinstance(cleaned_address_table, str)
        else f"({cleaned_address_table.sql_query()})"
    )
    tf_sql = f"""
    WITH unnested AS (
        SELECT unnest(string_split(clean_full_address, ' ')) AS token
        FROM {source_sql}
    )
    SELECT
        token,
        COUNT(*)::DOUBLE / (SELECT COUNT(*) FROM unnested) AS rel_freq
    FROM unnested
    GROUP BY token
    ORDER BY COUNT(*) DESC
    """

    result_table = "__ukam_derived_term_frequencies"
    con.sql(f"DROP TABLE IF EXISTS {result_table}")
    con.sql(tf_sql).create(result_table)
    return con.table(result_table)


def derive_inverted_index(
    cleaned_address_table: DuckDBPyRelation,
    con: DuckDBPyConnection,
    num_of_chunks: int = 1,
    strategies: list[PhysicalIndexStrategy] | None = None,
    *,
    debug_options: Optional["DebugOptions"] = None,
    show_progress: ShowProgress = "auto",
) -> DuckDBPyRelation:
    """Derive an inverted index from already-cleaned canonical data.

    This function expects pre-cleaned address data
    (output of prepare_data_for_matching)
    with ``clean_full_address`` and ``unique_id`` columns already present.
    For each indexing strategy it generates keys and builds an inverted
    index mapping each key to a list of unique_ids.  Keys appearing in more
    than ``max_unique_ids_per_key`` records are filtered out as they provide
    poor blocking selectivity. A strategy can specify a stricter limit and
    suppress raw keys already retained by an earlier strategy.

    When ``num_of_chunks`` > 1, the inverted index is built in chunks
    partitioned by **key hash** (not by address). This ensures every
    occurrence of a given key is processed within the same chunk so the
    global frequency filter is applied correctly. Chunk results are
    vertically concatenated.

    Example usage::

        df_canonical_clean = prepare_data_for_matching(df_canonical, con)
        inverted_idx = derive_inverted_index(df_canonical_clean, con)
        df_messy_clean = prepare_data_for_matching(
            df_messy, con, inverted_index=inverted_idx
        )

    Args:
        cleaned_address_table: Pre-cleaned address relation with
            ``clean_full_address`` and ``unique_id`` columns.
        con: DuckDB connection.
        num_of_chunks: Number of chunks to split the work into.  Set to 1
            (the default) for no chunking.
        strategies: List of :class:`PhysicalIndexStrategy` instances. Defaults
            to :data:`DEFAULT_INDEXING_STRATEGIES` (trigram + bigram).
        debug_options: Optional debug configuration for pipeline execution.
        show_progress: ``"auto"`` renders live updates in a supported
            interactive terminal and otherwise logs stage boundaries.
            ``"stages"`` logs only stage boundaries; ``"off"`` suppresses
            progress output.

    Returns:
        Inverted index table with ``key`` (VARCHAR), ``unique_ids`` (LIST),
        and ``index_strategy`` (VARCHAR) columns.
    """
    progress_mode = resolve_progress_mode(show_progress)

    if strategies is None:
        strategies = DEFAULT_INDEXING_STRATEGIES

    uid = _uid()
    num_of_chunks = max(1, num_of_chunks)

    result_table = f"__ukam_derived_inverted_index_{uid}"
    con.execute(f"DROP TABLE IF EXISTS {result_table}")
    first_insert = True

    total_rows = cleaned_address_table.count("*").fetchone()[0]

    for strategy in strategies:
        stage_label = f"Building inverted index ({strategy.name})"
        if num_of_chunks == 1:
            stage_started_at = time.perf_counter()
            log_stage_start(
                stage_label,
                total_rows,
                1,
                progress_mode=progress_mode,
            )
            # Single-pass for this strategy
            pipeline = create_sql_pipeline(
                con,
                input_rel=cleaned_address_table,
                stage_specs=[
                    _derive_keys_for_strategy(strategy),
                    _build_inverted_index_from_keys(strategy),
                ],
                pipeline_name=f"Build inverted index ({strategy.name})",
                pipeline_description=(
                    f"Derive {strategy.name} keys and aggregate into inverted index"
                ),
            )
            chunk_result = pipeline.run(debug_options if first_insert else None)

            if first_insert:
                chunk_result.create(result_table)
                first_insert = False
            else:
                chunk_result.insert_into(result_table)
            log_chunk_progress(
                total_rows,
                total_rows,
                stage_label=stage_label,
                progress_mode=progress_mode,
                chunk_index=0,
                total_chunks=1,
                chunk_elapsed_seconds=time.perf_counter() - stage_started_at,
            )
            log_stage_complete(
                stage_label,
                total_rows,
                time.perf_counter() - stage_started_at,
                progress_mode=progress_mode,
            )
        else:
            # Chunked path for this strategy
            stage_started_at = time.perf_counter()
            strategy_keys_table = f"__ukam_index_keys_{uid}_{strategy.name}"
            progress = _ProgressBar(
                label=stage_label,
                total=total_rows,
                total_units=num_of_chunks,
                enabled=progress_mode == "auto",
            )
            log_stage_start(
                stage_label,
                total_rows,
                num_of_chunks,
                progress_mode=progress_mode,
            )
            try:
                logger.debug("%s: staging keys once", stage_label)
                key_pipeline = create_sql_pipeline(
                    con,
                    input_rel=cleaned_address_table,
                    stage_specs=[_derive_keys_for_strategy(strategy)],
                    pipeline_name=f"Stage inverted index keys ({strategy.name})",
                    pipeline_description=(
                        f"Derive {strategy.name} keys once before bucket aggregation"
                    ),
                )
                strategy_keys = key_pipeline.run(debug_options if first_insert else None)
                scalar_keys = con.sql(f"""
                    WITH scalar_keys AS (
                        SELECT unique_id, unnest(__index_keys) AS key
                        FROM ({strategy_keys.sql_query()})
                    )
                    SELECT
                        unique_id,
                        key,
                        abs(hash(key)) % {num_of_chunks} AS key_bucket
                    FROM scalar_keys
                    ORDER BY key_bucket
                """)
                _materialise_relation(
                    con,
                    scalar_keys,
                    strategy_keys_table,
                )
                logger.debug("%s: keys staged", stage_label)

                for chunk_index in range(num_of_chunks):
                    chunk_started_at = time.perf_counter()
                    chunk_keys = con.sql(f"""
                        SELECT unique_id, key
                        FROM {strategy_keys_table}
                        WHERE key_bucket = {chunk_index}
                    """)

                    pipeline = create_sql_pipeline(
                        con,
                        input_rel=chunk_keys,
                        stage_specs=[_build_inverted_index_from_scalar_keys(strategy)],
                        pipeline_name=f"Build inverted index ({strategy.name})",
                        pipeline_description=(
                            f"Aggregate staged {strategy.name} keys into inverted index "
                            f"(chunk {chunk_index + 1}/{num_of_chunks})"
                        ),
                    )
                    chunk_result = pipeline.run(debug_options if first_insert else None)

                    if first_insert:
                        chunk_result.create(result_table)
                        first_insert = False
                    else:
                        chunk_result.insert_into(result_table)

                    processed_records = min(
                        (chunk_index + 1)
                        * ((total_rows + num_of_chunks - 1) // num_of_chunks),
                        total_rows,
                    )
                    progress.update(
                        processed_records,
                        completed_units=chunk_index + 1,
                    )
                    log_chunk_progress(
                        total_rows,
                        processed_records,
                        stage_label=stage_label,
                        progress_mode=progress_mode,
                        progress=progress,
                        chunk_index=chunk_index,
                        total_chunks=num_of_chunks,
                        chunk_elapsed_seconds=(time.perf_counter() - chunk_started_at),
                    )
            finally:
                progress.close()
                _drop_table_and_registered_aliases(con, strategy_keys_table)

            log_stage_complete(
                stage_label,
                total_rows,
                time.perf_counter() - stage_started_at,
                progress_mode=progress_mode,
            )

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
    _inverted_index_strategies: list[InvertedIndexLookupStrategy] | None = None,
    inverted_index_n: Optional[int] = None,
    derive_distinguishing_wrt_adjacent_records: bool = False,
    *,
    dataset_role: Literal["messy", "canonical"] | None = None,
    _precleaned_addresses: bool = False,
    debug_options: Optional[DebugOptions] = None,
    show_progress: ShowProgress = "auto",
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
        inverted_index: Optional pre-computed inverted index table with
            'key', 'unique_ids', and 'index_strategy' columns.
            Use derive_inverted_index()
            to create this from canonical addresses. When provided, the function
            derives index keys from addresses and looks up matching unique_ids,
            populating the `exploding_unique_ids` column. When not provided,
            `exploding_unique_ids` is set to [unique_id] (single-element array).
        derive_distinguishing_wrt_adjacent_records: Whether to derive distinguishing
            tokens relative to adjacent records.
        dataset_role: Optional role hint used to make output table names more
            descriptive in DuckDB catalogs. Use ``"canonical"`` or ``"messy"``.
        debug_options: Optional debug configuration for pipeline execution.
            Note: Debug options are only applied on the first iteration to avoid
            excessive logging output.
        show_progress: ``"auto"`` renders live updates in a supported
            interactive terminal and otherwise logs stage boundaries.
            ``"stages"`` logs only stage boundaries; ``"off"`` suppresses
            progress output.

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
    progress_mode = resolve_progress_mode(show_progress)
    uid = _uid()
    distinguishing_table_name = None

    if _precleaned_addresses:
        cleaned_address_table = address_table
    else:
        cleaned_address_table = clean_data_pre_term_frequencies(
            address_table,
            con,
            num_of_chunks=num_of_chunks,
            debug_options=debug_options,
            show_progress=progress_mode,
        )
    cleaned_table_name = cleaned_address_table.alias

    if derive_distinguishing_wrt_adjacent_records:
        logger.debug("Deriving adjacent-record distinguishing tokens")
        distinguishing_pipeline = create_sql_pipeline(
            con,
            input_rel=cleaned_address_table,
            stage_specs=[
                _separate_distinguishing_start_tokens_from_with_respect_to_adjacent_records(
                    include_input_columns=False
                )
            ],
            pipeline_name="Derive locally distinguishing canonical tokens",
            pipeline_description=(
                "Compare each canonical address with nearby suffix-similar records"
            ),
        )
        distinguishing_tokens = distinguishing_pipeline.run(debug_options)
        distinguishing_table_name = f"__ukam_distinguishing_tokens_{uid}"
        _materialise_relation(
            con,
            distinguishing_tokens,
            distinguishing_table_name,
        )
        logger.debug("Adjacent-record distinguishing tokens derived")

    total_rows = cleaned_address_table.count("*").fetchone()[0]
    _create_term_frequency_tables(con, term_frequency_lookup=term_frequency_lookup)

    inv_idx_table_name = _register_inverted_index_table(
        con, inverted_index, inverted_index_n
    )

    lookup_strategies = _inverted_index_strategies
    if lookup_strategies is None:
        lookup_strategies = (
            MESSY_INVERTED_INDEX_LOOKUP_STRATEGIES
            if dataset_role == "messy"
            else DEFAULT_INVERTED_INDEX_LOOKUP_STRATEGIES
        )

    inverted_index_stages = (
        (
            [_lookup_keys_in_inverted_index(lookup_strategies)]
            if lookup_strategies != DEFAULT_INVERTED_INDEX_LOOKUP_STRATEGIES
            else list(QUEUE_INVERTED_INDEX_LOOKUP)
        )
        if inv_idx_table_name is not None
        else list(QUEUE_INVERTED_INDEX_SELF)
    )

    chunk_size = _calculate_chunk_size(total_rows, num_of_chunks)
    total_chunks = (total_rows + chunk_size - 1) // chunk_size
    stage_label = "Applying term frequencies"
    progress = _ProgressBar(
        label=stage_label,
        total=total_rows,
        total_units=total_chunks,
        enabled=progress_mode == "auto",
    )

    if distinguishing_table_name is None:
        distinguishing_select_sql = ""
        distinguishing_join_sql = ""
    else:
        distinguishing_select_sql = """,
                distinguishing.distinguishing_adj_start_tokens,
                distinguishing.common_adj_start_tokens"""
        distinguishing_join_sql = f"""
            LEFT JOIN {distinguishing_table_name} AS distinguishing
              ON cleaned.__ukam_row_id = distinguishing.__ukam_row_id
        """

    if dataset_role == "canonical":
        processed_table = f"__ukam__processed_canonical_{uid}"
    elif dataset_role == "messy":
        processed_table = f"__ukam__processed_messy_{uid}"
    elif dataset_role is None:
        processed_table = f"__ukam__processed_{uid}"
    else:
        raise ValueError("dataset_role must be one of: 'messy', 'canonical', or None.")

    # Apply term frequencies and trigram blocking to cleaned chunks
    stage_started_at = time.perf_counter()
    log_stage_start(
        stage_label,
        total_rows,
        total_chunks,
        progress_mode=progress_mode,
    )
    try:
        for chunk_index in range(total_chunks):
            chunk_started_at = time.perf_counter()
            first_id = chunk_index * chunk_size + 1
            last_id = min((chunk_index + 1) * chunk_size, total_rows)
            chunk_query = con.sql(f"""
            SELECT cleaned.*{distinguishing_select_sql}
                FROM {cleaned_table_name} AS cleaned
                {distinguishing_join_sql}
                WHERE cleaned.__ukam_row_id BETWEEN {first_id} AND {last_id}
            """)

            # Process chunk: apply term frequencies + inverted index blocking in one pass
            processed_chunk = _clean_data_using_precomputed_rel_tok_freq(
                chunk_query,
                con=con,
                pre_cleaned_addresses=True,
                additional_stages=inverted_index_stages,
                debug_options=debug_options if chunk_index == 0 else None,
            )

            if chunk_index == 0:
                con.execute(f"DROP TABLE IF EXISTS {processed_table}")
                processed_chunk.create(processed_table)
            else:
                processed_chunk.insert_into(processed_table)

            processed_records = min((chunk_index + 1) * chunk_size, total_rows)
            progress.update(
                processed_records,
                completed_units=chunk_index + 1,
            )
            log_chunk_progress(
                total_rows,
                processed_records,
                stage_label=stage_label,
                progress_mode=progress_mode,
                progress=progress,
                chunk_index=chunk_index,
                total_chunks=total_chunks,
                chunk_elapsed_seconds=time.perf_counter() - chunk_started_at,
            )
    finally:
        progress.close()

    log_stage_complete(
        stage_label,
        total_rows,
        time.perf_counter() - stage_started_at,
        progress_mode=progress_mode,
    )

    logger.debug("Finalizing prepared address table")
    con.execute(f"DROP TABLE IF EXISTS {cleaned_table_name}")
    if distinguishing_table_name is not None:
        con.execute(f"DROP TABLE IF EXISTS {distinguishing_table_name}")

    # Clean up inverted index table if it was registered
    if inv_idx_table_name == "__ukam_inverted_index":
        _drop_table_and_registered_aliases(con, inv_idx_table_name)

    con.execute(f"ALTER TABLE {processed_table} DROP COLUMN __ukam_row_id")
    logger.debug("Prepared address table finalized")

    return con.table(processed_table)


__all__ = [
    "prepare_data_for_matching",
]
