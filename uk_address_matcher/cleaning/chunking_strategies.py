from __future__ import annotations

import logging
from collections.abc import Collection
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Literal, Optional

from duckdb import DuckDBPyConnection, DuckDBPyRelation

from uk_address_matcher.cleaning.pipelines import (
    QUEUE_FOR_TF_DERIVATION,
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
    roadlike_place_prepared_candidate_sources_sql,
    roadlike_place_prepared_candidate_sql,
)
from uk_address_matcher.cleaning.steps.token_parsing import (
    _derive_distinguishing_token_components,
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
_CANONICAL_ADJACENT_BATCH_ROWS = 4_000_000

DISTINGUISHING_FEATURE_COLUMNS = (
    "distinguishing_adj_start_tokens",
    "common_adj_start_tokens",
    "distinguishing_structural_tokens",
    "distinguishing_lexical_tokens",
    "distinguishing_token_parts",
)


def _materialise_relation(
    con: DuckDBPyConnection,
    relation: DuckDBPyRelation,
    table_name: str,
) -> DuckDBPyRelation:
    # Ensure any prior table/view/registered alias with this name is removed.
    _drop_table_and_registered_aliases(con, table_name)

    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM ({relation.sql_query()})")
    return con.table(table_name)


def _drop_tables_with_prefix(con: DuckDBPyConnection, prefix: str) -> None:
    table_names = [name for (name,) in con.execute("SHOW TABLES").fetchall()]
    for table_name in table_names:
        if table_name.startswith(prefix):
            _drop_table_and_registered_aliases(con, table_name)


def _drop_cleaned_chunk_relation(
    con: DuckDBPyConnection,
    relation_name: str,
) -> None:
    _drop_table_and_registered_aliases(con, relation_name)
    if relation_name.startswith("__ukam_chunked_addresses_"):
        uid = relation_name.removeprefix("__ukam_chunked_addresses_")
        _drop_tables_with_prefix(con, f"__ukam_cleaned_chunk_{uid}_")


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


def _normalise_postcode_districts(
    postcode_districts: Collection[str] | None,
) -> tuple[str, ...] | None:
    if postcode_districts is None:
        return None
    if any(
        not isinstance(district, str) or not district.strip()
        for district in postcode_districts
    ):
        raise ValueError("postcode_districts must contain non-empty strings")
    normalised = tuple(
        sorted({district.strip().upper() for district in postcode_districts})
    )
    if not normalised:
        raise ValueError("postcode_districts must contain at least one district")
    return normalised


def _add_canonical_road_blocking_keys(
    canonical_addresses: DuckDBPyRelation,
    con: DuckDBPyConnection,
    *,
    num_of_chunks: int = 1,
    roadlike_places: DuckDBPyRelation | None = None,
    require_catalogue_support: bool = True,
    _stored_unique_address_ids: bool = False,
) -> DuckDBPyRelation:
    """Add road keys; the ID fast path requires stored, unique, non-null IDs."""
    if "road_1_norm" in canonical_addresses.columns:
        return canonical_addresses
    if roadlike_places is None:
        # TODO(ThomasHepworth): remove in 2.0; keep legacy callers neutral.
        return canonical_addresses.select("*, NULL::VARCHAR AS road_1_norm")

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
    preferred_value_fields = ", ".join(
        f'"{column}" := source."{column}"'
        for column in preferred_columns
        if column != "unique_id"
    )
    preferred_order_fields = []
    if "filename" in canonical_addresses.columns:
        preferred_order_fields.append("""
            file_rank := CASE source.filename
                WHEN 'add_gb_builtaddress.parquet' THEN 1
                WHEN 'add_gb_royalmailaddress.parquet' THEN 2
                ELSE 3
            END
        """)
    preferred_order_fields.extend(
        f'"{column}" := source."{column}"'
        for column in ("clean_full_address", "postcode", "ukam_address_id")
        if column in canonical_addresses.columns
    )
    preferred_order = ", ".join(preferred_order_fields)
    preferred_value = f"struct_pack({preferred_value_fields})"
    preferred_source = "preferred"
    preferred_join = ""
    if _stored_unique_address_ids:
        # Keep text/lists out of aggregate state; recover the winning row by ID.
        preferred_value = "source.ukam_address_id"
        preferred_source = "chosen"
        preferred_join = f"""
            INNER JOIN ({canonical_addresses.sql_query()}) AS chosen
                ON chosen.ukam_address_id = grouped.preferred
        """
    preserve_insertion_order = bool(
        con.execute("SELECT current_setting('preserve_insertion_order')").fetchone()[0]
    )
    con.execute("SET preserve_insertion_order = false")
    try:
        _drop_table_and_registered_aliases(con, preferred_table)
        con.execute(f"""
            CREATE TEMPORARY TABLE {preferred_table} AS
            WITH grouped AS (
                SELECT
                    source.unique_id,
                    min_by(
                        {preferred_value},
                        struct_pack({preferred_order})
                    ) AS preferred
                FROM ({canonical_addresses.sql_query()}) AS source
                GROUP BY source.unique_id
            )
            SELECT
                grouped.unique_id,
                {
            ", ".join(
                f'{preferred_source}."{column}" AS "{column}"'
                for column in preferred_columns
                if column != "unique_id"
            )
        }
            FROM grouped
            {preferred_join}
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
    road_chunk_key = "CAST(unique_id AS VARCHAR)"
    if "postcode_district" in roadlike_places.columns:
        road_chunk_key = r"""regexp_extract(
            upper(coalesce(postcode, '')),
            '^\s*([A-Z]{1,2}[0-9]{1,2}[A-Z]?)\s+\d', 1
        )"""
    _drop_table_and_registered_aliases(con, road_keys_table)
    try:
        for chunk_index in range(road_chunk_count):
            if road_chunk_count == 1:
                chunk = preferred_addresses
            else:
                chunk = con.sql(f"""
                    SELECT *
                    FROM {preferred_table}
                    WHERE hash({road_chunk_key}) % {road_chunk_count}
                        = {chunk_index}
                """)
            chunk_keys = derive_top_1_road_keys(
                con,
                chunk,
                output_table=chunk_keys_table,
                roadlike_places=roadlike_places,
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
        SELECT
            {source_columns},
            road_features.road_1_norm
        FROM ({canonical_addresses.sql_query()}) AS canonical
        LEFT JOIN {road_keys_table} AS road_features USING (unique_id)
    """)
    try:
        # Retain the narrow road keys instead of copying every canonical column.
        con.execute(f"CREATE TEMPORARY VIEW {enriched_table} AS {enriched.sql_query()}")
        return con.table(enriched_table)
    except BaseException:
        _drop_table_and_registered_aliases(con, enriched_table)
        _drop_table_and_registered_aliases(con, road_keys_table)
        raise
    finally:
        con.execute(
            f"SET preserve_insertion_order = {str(preserve_insertion_order).lower()}"
        )


def derive_roadlike_places(
    canonical_address_table: DuckDBPyRelation,
    con: DuckDBPyConnection,
    output_path: Path | str | None = None,
    *,
    postcode_districts: Collection[str] | None = None,
    postcode_districts_per_batch: int | None = 16,
    debug_options: Optional[DebugOptions] = None,
    show_progress: ShowProgress = "auto",
) -> DuckDBPyRelation:
    """Build a roadlike-place catalogue from canonical addresses.

    The input must already be canonical-cleaned, including ``clean_full_address``
    and ``numeric_tokens``. Road preparation and candidate extraction run in
    batches of postcode districts; catalogue evidence is local to each district.
    ``postcode_districts`` restricts the source before batching. When
    ``classificationcode`` is available, only top-level ``R`` rows contribute.
    """
    selected_postcode_districts = _normalise_postcode_districts(postcode_districts)
    if postcode_districts_per_batch is not None and postcode_districts_per_batch < 1:
        raise ValueError("postcode_districts_per_batch must be at least 1")

    required_columns = {"unique_id", "clean_full_address", "postcode", "numeric_tokens"}
    missing_columns = sorted(required_columns.difference(canonical_address_table.columns))
    if missing_columns:
        raise ValueError(
            "Canonical address table must be cleaned before deriving roadlike places; "
            f"missing columns: {missing_columns}"
        )

    road_catalogue_source = canonical_address_table
    if selected_postcode_districts is not None:
        postcode_district_expression = (
            "regexp_extract("
            "upper(coalesce(postcode, '')), "
            "'^\\s*([A-Z]{1,2}[0-9]{1,2}[A-Z]?)\\s+\\d', 1)"
        )
        district_values = ", ".join(
            "'" + district.replace("'", "''") + "'"
            for district in selected_postcode_districts
        )
        road_catalogue_source = con.sql(f"""
            SELECT *
            FROM ({canonical_address_table.sql_query()}) AS canonical
            WHERE {postcode_district_expression} IN ({district_values})
        """)
        scoped_row_count = road_catalogue_source.count("*").fetchone()[0]
        if scoped_row_count == 0:
            raise ValueError(
                "No canonical addresses matched the supplied postcode districts"
            )
        logger.info(
            "Restricting road catalogue to %s postcode districts and %s rows",
            len(selected_postcode_districts),
            scoped_row_count,
        )
    road_catalogue_row_count: int | None = None
    if "classificationcode" not in canonical_address_table.columns:
        logger.warning(
            "Residential road catalogue filter requested, but "
            "classificationcode is unavailable; processing all canonical rows"
        )
    else:
        filtered_source = con.sql(f"""
            SELECT *
            FROM ({road_catalogue_source.sql_query()}) AS canonical
            WHERE substr(CAST(classificationcode AS VARCHAR), 1, 1) = 'R'
        """)
        filtered_row_count = filtered_source.count("*").fetchone()[0]
        if filtered_row_count:
            road_catalogue_source = filtered_source
            road_catalogue_row_count = filtered_row_count
            logger.info(
                "Building residential road catalogue from %s canonical rows",
                filtered_row_count,
            )
        else:
            logger.warning(
                "No residential canonical rows matched classificationcode; "
                "processing all canonical rows"
            )

    progress_mode = resolve_progress_mode(show_progress)
    uid = _uid()
    district_table = f"__ukam_roadlike_districts_{uid}"
    candidate_sources_table = f"__ukam_roadlike_candidate_sources_{uid}"
    catalogue_table = f"__ukam_roadlike_places_{uid}"
    total_rows = (
        road_catalogue_row_count
        if road_catalogue_row_count is not None
        else road_catalogue_source.count("*").fetchone()[0]
    )
    if total_rows == 0:
        raise ValueError(
            "Supplied address table has no records. Please provide a non-empty table."
        )

    if output_path is not None:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
    stage_label = "Building roadlike-place catalogue"
    district_expression = (
        "coalesce(nullif(regexp_extract(upper(coalesce(postcode, '')), "
        "'^\\s*([A-Z]{1,2}[0-9]{1,2}[A-Z]?)\\s+\\d', 1), ''), '__UNKNOWN__')"
    )
    batch_size = postcode_districts_per_batch or 16
    con.execute(f"""
        CREATE TEMPORARY TABLE {district_table} AS
        SELECT district,
            (row_number() OVER (ORDER BY district) - 1) // {batch_size} AS batch_id
        FROM (
            SELECT DISTINCT {district_expression} AS district
            FROM ({road_catalogue_source.sql_query()}) AS source
        ) AS districts
    """)
    total_batches = con.sql(f"SELECT max(batch_id) + 1 FROM {district_table}").fetchone()[
        0
    ]
    log_stage_start(stage_label, total_rows, total_batches, progress_mode=progress_mode)
    progress = _ProgressBar(
        label=stage_label,
        total=total_rows,
        total_units=total_batches,
        enabled=progress_mode == "auto",
    )
    processed_rows = 0

    try:
        with TemporaryDirectory(prefix="ukam-road-batches-") as batch_directory:
            batch_path = batch_directory.replace("'", "''")
            precomputed_column = (
                ", canonical.rightmost_numeric_position"
                if "rightmost_numeric_position" in road_catalogue_source.columns
                else ""
            )
            if total_batches > 1:
                con.execute(f"""
                    COPY (
                        SELECT canonical.unique_id, canonical.clean_full_address,
                            canonical.postcode, canonical.numeric_tokens
                            {precomputed_column}, batches.batch_id
                        FROM ({road_catalogue_source.sql_query()}) AS canonical
                        JOIN {district_table} AS batches
                            ON {district_expression} = batches.district
                    ) TO '{batch_path}' (FORMAT PARQUET, PARTITION_BY (batch_id))
                """)
            for batch_index in range(total_batches):
                if total_batches == 1:
                    batch_input = con.sql(f"""
                        SELECT canonical.unique_id, canonical.clean_full_address,
                            canonical.postcode, canonical.numeric_tokens
                            {precomputed_column}
                        FROM ({road_catalogue_source.sql_query()}) AS canonical
                    """)
                else:
                    batch_files = (
                        Path(batch_directory) / f"batch_id={batch_index}" / "*.parquet"
                    )
                    batch_file_pattern = str(batch_files).replace("'", "''")
                    batch_input = con.sql(
                        f"SELECT * FROM read_parquet('{batch_file_pattern}', "
                        "hive_partitioning=false)"
                    )
                batch_rows = batch_input.count("*").fetchone()[0]
                preparation = create_sql_pipeline(
                    con,
                    input_rel=batch_input,
                    stage_specs=QUEUE_ROADLIKE_PLACE_PREPARATION,
                    pipeline_name="Prepare roadlike-place input",
                    pipeline_description=(
                        "Derive suffix-peeled tokens and numeric anchors"
                    ),
                ).run(debug_options if batch_index == 0 else None)
                candidate_sources_sql = roadlike_place_prepared_candidate_sources_sql(
                    f"({preparation.sql_query()})"
                )
                candidate_sources_sql = (
                    "SELECT address_id, full_postcode, postcode_district, "
                    "rightmost_numeric_value, 0::INTEGER AS numeric_anchor, "
                    "road_tail_tokens AS address_tokens, allow_truncated_windows "
                    f"FROM ({candidate_sources_sql}) AS candidate_sources"
                )
                con.execute(
                    f"CREATE TEMPORARY TABLE {candidate_sources_table} AS "
                    f"{candidate_sources_sql}"
                )
                candidate_relation = roadlike_place_prepared_candidate_sql(
                    candidate_sources_table,
                    candidate_source_relation=candidate_sources_table,
                )
                catalogue_sql = roadlike_place_catalog_sql(
                    f"({candidate_relation})", by_postcode_district=True
                )
                if batch_index == 0:
                    con.execute(f"CREATE TABLE {catalogue_table} AS {catalogue_sql}")
                else:
                    con.execute(f"INSERT INTO {catalogue_table} {catalogue_sql}")
                _drop_table_and_registered_aliases(con, candidate_sources_table)

                processed_rows += batch_rows
                progress.update(processed_rows, completed_units=batch_index + 1)
                log_chunk_progress(
                    total_rows,
                    processed_rows,
                    stage_label=stage_label,
                    progress_mode=progress_mode,
                    progress=progress,
                    chunk_index=batch_index,
                    total_chunks=total_batches,
                )
        if output_path is not None:
            escaped_output_path = str(output_path).replace("'", "''")
            con.execute(
                f"COPY {catalogue_table} TO '{escaped_output_path}' "
                "(FORMAT PARQUET, COMPRESSION ZSTD)"
            )
    finally:
        progress.close()
        _drop_table_and_registered_aliases(con, candidate_sources_table)
        _drop_table_and_registered_aliases(con, district_table)

    log_stage_complete(
        stage_label,
        total_rows,
        progress_mode=progress_mode,
    )
    return con.table(catalogue_table)


def clean_data_pre_term_frequencies(
    address_table: DuckDBPyRelation,
    con: DuckDBPyConnection,
    num_of_chunks: int = 10,
    *,
    _drop_columns: Collection[str] = (),
    _owned_chunks: dict[str, int] | None = None,
    debug_options: DebugOptions | None = None,
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
    chunked_input_name = f"__ukam_chunked_input_{uid}"
    chunk_index_column = f"__ukam_chunk_index_{uid}"
    cleaned_chunk_prefix = f"__ukam_cleaned_chunk_{uid}_"

    log_stage_start(
        stage_label,
        total_rows,
        total_chunks,
        progress_mode=progress_mode,
    )

    try:
        con.execute(f"""
            CREATE TABLE {chunked_input_name} AS
            SELECT
                *,
                CAST(abs(hash(address_concat)) % {total_chunks} AS INTEGER)
                    AS {chunk_index_column}
            FROM {input_name}
        """)

        chunk_row_counts = dict(
            con.execute(f"""
            SELECT {chunk_index_column}, COUNT(*) AS row_count
            FROM {chunked_input_name}
            GROUP BY {chunk_index_column}
            ORDER BY {chunk_index_column}
        """).fetchall()
        )
        chunk_offsets = []
        current_offset = 0
        for chunk_index in range(total_chunks):
            chunk_offsets.append(current_offset)
            current_offset += chunk_row_counts.get(chunk_index, 0)

        source_columns = tuple(
            column for column in address_table.columns if column != "ukam_address_id"
        )
        source_projection = ", ".join(f'"{column}"' for column in source_columns)

        for chunk_index in range(total_chunks):
            chunk_query = con.sql(f"""
            SELECT
                CAST({chunk_offsets[chunk_index]} + ROW_NUMBER() OVER () AS INTEGER)
                    AS ukam_address_id,
                {source_projection}
            FROM {chunked_input_name}
            WHERE {chunk_index_column} = {chunk_index}
            """)
            chunk_table = f"__ukam_chunk_input_{uid}_{chunk_index}"
            chunk = _materialise_relation(
                con,
                chunk_query,
                chunk_table,
            )
            chunk_row_count = chunk.count("*").fetchone()[0]

            # Apply debug options only on the first iteration.
            processed_chunk = _clean_data_pre_term_frequencies(
                chunk,
                con,
                debug_options=debug_options if chunk_index == 0 else None,
            )

            if _drop_columns:
                processed_chunk = processed_chunk.project(
                    ", ".join(
                        f'"{column}"'
                        for column in processed_chunk.columns
                        if column not in _drop_columns
                    )
                )
            chunk_name = f"{cleaned_chunk_prefix}{chunk_index}"
            processed_chunk.create(chunk_name)
            if _owned_chunks is not None:
                # Explicitly transfer owned tables and inclusive ID bounds to
                # finishing; arbitrary caller-owned relations are never enrolled.
                _owned_chunks[chunk_name] = chunk_offsets[chunk_index] + chunk_row_count

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
            )

            _drop_table_and_registered_aliases(con, chunk_table)
    except BaseException:
        _drop_tables_with_prefix(con, cleaned_chunk_prefix)
        raise
    finally:
        progress.close()
        _drop_tables_with_prefix(con, f"__ukam_chunk_input_{uid}_")
        _drop_table_and_registered_aliases(con, chunked_input_name)
        _drop_table_and_registered_aliases(con, input_name)

    chunked_table = f"__ukam_chunked_addresses_{uid}"
    try:
        log_stage_complete(
            stage_label,
            total_rows,
            progress_mode=progress_mode,
        )

        chunk_tables_sql = " UNION ALL ".join(
            f"SELECT * FROM {cleaned_chunk_prefix}{chunk_index}"
            for chunk_index in range(total_chunks)
        )
        con.execute(f"CREATE VIEW {chunked_table} AS {chunk_tables_sql}")
        return con.table(chunked_table)
    except BaseException:
        _drop_cleaned_chunk_relation(con, chunked_table)
        raise


def derive_term_frequencies_table(
    address_table: DuckDBPyRelation,
    con: DuckDBPyConnection,
    num_of_chunks: int = 10,
    *,
    debug_options: DebugOptions | None = None,
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
    log_stage_start(
        stage_label,
        total_rows,
        total_chunks,
        progress_mode=progress_mode,
    )

    try:
        for chunk_index in range(total_chunks):
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
            )

            _drop_table_and_registered_aliases(con, chunk_table)
    finally:
        progress.close()

    log_stage_complete(
        stage_label,
        total_rows,
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
    if isinstance(cleaned_address_table, str):
        source_sql = cleaned_address_table
        source_columns = con.table(cleaned_address_table).columns
    else:
        source_sql = f"({cleaned_address_table.sql_query()})"
        source_columns = cleaned_address_table.columns
    token_expression = (
        "clean_full_address_tokens"
        if "clean_full_address_tokens" in source_columns
        else "string_split(clean_full_address, ' ')"
    )
    tf_sql = f"""
    WITH unnested AS (
        SELECT unnest({token_expression}) AS token
        FROM {source_sql}
    )
    SELECT
        token,
        COUNT(*)::DOUBLE / SUM(COUNT(*)) OVER () AS rel_freq
    FROM unnested
    GROUP BY token
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
    debug_options: DebugOptions | None = None,
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
    token_column = (
        "clean_full_address_tokens"
        if "clean_full_address_tokens" in cleaned_address_table.columns
        else "clean_full_address"
    )

    for strategy in strategies:
        stage_label = f"Building inverted index ({strategy.name})"
        if num_of_chunks == 1:
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
                    _derive_keys_for_strategy(strategy, token_column=token_column),
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
            )
            log_stage_complete(
                stage_label,
                total_rows,
                progress_mode=progress_mode,
            )
        else:
            # Chunked path for this strategy
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
            key_directory = TemporaryDirectory(prefix="ukam-index-buckets-")
            try:
                logger.debug("%s: staging keys once", stage_label)
                key_pipeline = create_sql_pipeline(
                    con,
                    input_rel=cleaned_address_table,
                    stage_specs=[
                        _derive_keys_for_strategy(
                            strategy,
                            token_column=token_column,
                        )
                    ],
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
                """)
                # Keep native storage for identifier types whose Parquet
                # round trip is not established here (for example HUGEINT).
                if str(scalar_keys.types[0]) not in {"BIGINT", "VARCHAR"}:
                    _materialise_relation(
                        con, scalar_keys.order("key_bucket"), strategy_keys_table
                    )
                else:
                    # Physical buckets avoid a global sort while keeping
                    # each unchanged bucket query local to its own files.
                    key_path = Path(key_directory.name) / "keys"
                    escaped_path = str(key_path).replace("'", "''")
                    con.execute(f"""
                        COPY ({scalar_keys.sql_query()}) TO '{escaped_path}'
                        (FORMAT PARQUET, PARTITION_BY (key_bucket), COMPRESSION SNAPPY)
                    """)
                    if any(key_path.rglob("*.parquet")):
                        _drop_table_and_registered_aliases(con, strategy_keys_table)
                        con.execute(f"""
                            CREATE TEMPORARY VIEW {strategy_keys_table} AS
                            SELECT unique_id, key, key_bucket::UBIGINT AS key_bucket
                            FROM read_parquet('{escaped_path}/**/*.parquet',
                                hive_partitioning=true)
                        """)
                    else:
                        # Partitioned COPY produces no files when all keys
                        # are empty; retain the native typed empty relation.
                        _materialise_relation(
                            con, scalar_keys.limit(0), strategy_keys_table
                        )
                logger.debug("%s: keys staged", stage_label)

                for chunk_index in range(num_of_chunks):
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
                    )
            finally:
                try:
                    progress.close()
                    _drop_table_and_registered_aliases(con, strategy_keys_table)
                finally:
                    # Bucket results have been inserted before their files
                    # disappear; the temporary view never outlives its input.
                    key_directory.cleanup()

            log_stage_complete(
                stage_label,
                total_rows,
                progress_mode=progress_mode,
            )

    return con.table(result_table)


def _canonical_distinguishing_features(
    con: DuckDBPyConnection,
    source: DuckDBPyRelation,
    debug_options: DebugOptions | None,
) -> DuckDBPyRelation:
    return (
        create_sql_pipeline(
            con,
            input_rel=source,
            stage_specs=[
                _separate_distinguishing_start_tokens_from_with_respect_to_adjacent_records(
                    include_input_columns=False, use_precomputed_tokens=True
                ),
                _derive_distinguishing_token_components,
            ],
            pipeline_name="Derive locally distinguishing canonical tokens",
            pipeline_description="Compare nearby suffix-similar records",
        )
        .run(debug_options)
        .project(", ".join(("ukam_address_id", *DISTINGUISHING_FEATURE_COLUMNS)))
    )


def _adjacent_range_case(first: int, last: int) -> str:
    if first == last:
        return str(first)
    middle = (first + last) // 2
    splitter = f"list_extract(splitters, {middle + 1})"
    return (
        f"CASE WHEN range_key IS NOT NULL "
        f"AND ({splitter} IS NULL OR range_key < {splitter}) "
        f"THEN ({_adjacent_range_case(first, middle)}) "
        f"ELSE ({_adjacent_range_case(middle + 1, last)}) END"
    )


def _materialise_canonical_distinguishing_features(
    con: DuckDBPyConnection,
    source: DuckDBPyRelation,
    table_name: str,
    debug_options: DebugOptions | None = None,
) -> DuckDBPyRelation:
    """Limit the adjacent window's working set using contiguous address ranges.

    Up to three rows on either side of each boundary may get different features:
    neighbours outside that range are deliberately omitted. Equal addresses stay
    together, so the batch size is a target rather than a hard memory bound.
    """
    rows = source.count("*").fetchone()[0]
    batch_count = (rows + _CANONICAL_ADJACENT_BATCH_ROWS - 1) // (
        _CANONICAL_ADJACENT_BATCH_ROWS
    )
    collation, order, null_order = con.execute("""
        SELECT current_setting('default_collation'),
            current_setting('default_order'), current_setting('default_null_order')
    """).fetchone()
    # Other types (notably HUGEINT) may change values when written to Parquet.
    unique_id_type = str(source.types[source.columns.index("unique_id")])
    if (
        batch_count <= 1
        or unique_id_type not in {"VARCHAR", "BIGINT", "INTEGER"}
        or collation not in ("", "binary")
        or order not in ("ASC", "ASCENDING")
        or null_order not in ("NULLS_LAST", "NULLS_LAST_ON_ASC_FIRST_ON_DESC")
    ):
        # Consolidate global UNION input; Parquet batches already have this boundary.
        adjacent_input = con.sql(f"""
            WITH adjacent_input AS MATERIALIZED (
                SELECT ukam_address_id, unique_id, clean_full_address,
                    clean_full_address_tokens
                FROM ({source.sql_query()})
            )
            SELECT * FROM adjacent_input
        """)
        return _materialise_relation(
            con,
            _canonical_distinguishing_features(con, adjacent_input, debug_options),
            table_name,
        )

    uid = _uid()
    bounds_table = f"__ukam_adjacent_bounds_{uid}"
    input_table = f"__ukam_adjacent_batch_{uid}"
    columns = "ukam_address_id, unique_id, clean_full_address, clean_full_address_tokens"
    source_sql = source.project(columns).sql_query()
    # Hash-sample a bounded number of addresses; sort only that small sample.
    modulus = max(1, rows // 65_536)
    fractions = ", ".join(str(i / batch_count) for i in range(1, batch_count))
    try:
        con.execute(f"""
            CREATE TEMPORARY TABLE {bounds_table} AS
            SELECT quantile_disc(range_key, [{fractions}]) AS splitters
            FROM (
                SELECT reverse(clean_full_address) AS range_key
                FROM ({source_sql}) AS source
                WHERE hash(ukam_address_id) % {modulus} = 0
                LIMIT 131072
            ) AS sample
        """)
        with TemporaryDirectory(prefix="ukam-adjacent-batches-") as directory:
            batch_path = directory.replace("'", "''")
            con.execute(f"""
                COPY (
                    SELECT {columns},
                        {_adjacent_range_case(0, batch_count - 1)} AS batch_id
                    FROM (
                        SELECT *, reverse(clean_full_address) AS range_key
                        FROM ({source_sql}) AS source
                    ) AS keyed CROSS JOIN {bounds_table}
                ) TO '{batch_path}'
                (FORMAT PARQUET, PARTITION_BY (batch_id), COMPRESSION UNCOMPRESSED)
            """)
            # Missing directories are empty ranges (e.g. repeated split points).
            for index, batch in enumerate(sorted(Path(directory).glob("batch_id=*"))):
                batch_files = str(batch / "*.parquet").replace("'", "''")
                con.execute(f"""
                    CREATE TEMPORARY VIEW {input_table} AS
                    SELECT {columns} FROM read_parquet(
                        '{batch_files}', hive_partitioning=false
                    )
                """)
                try:
                    features = _canonical_distinguishing_features(
                        con, con.table(input_table), debug_options if index == 0 else None
                    )
                    if index == 0:
                        _materialise_relation(con, features, table_name)
                    else:
                        con.execute(f"INSERT INTO {table_name} {features.sql_query()}")
                finally:
                    _drop_table_and_registered_aliases(con, input_table)
        return con.table(table_name)
    except BaseException:
        _drop_table_and_registered_aliases(con, table_name)
        raise
    finally:
        _drop_table_and_registered_aliases(con, bounds_table)


# Chunking this requires a three phase approach:
# 1. Clean data in chunks without term frequencies
# 2. Register term frequency tables (either provided or pre-baked)
# 3. Use term frequencies to populate term frequency fields in cleaned data and
#   finally apply QUEUE_POST_TF
def prepare_data_for_matching(
    address_table: DuckDBPyRelation,
    con: DuckDBPyConnection,
    num_of_chunks: int = 10,
    term_frequency_lookup: DuckDBPyRelation | None = None,
    inverted_index: DuckDBPyRelation | None = None,
    _inverted_index_strategies: list[InvertedIndexLookupStrategy] | None = None,
    inverted_index_n: int | None = None,
    derive_distinguishing_wrt_adjacent_records: bool = False,
    *,
    dataset_role: Literal["messy", "canonical"] | None = None,
    _precleaned_addresses: bool = False,
    _drop_columns: Collection[str] = (),
    _owned_chunks: dict[str, int] | None = None,
    debug_options: DebugOptions | None = None,
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
    cleaned_table_name = None
    inv_idx_table_name = None
    processed_table = None
    progress = None

    def cleanup_on_failure() -> None:
        if cleaned_table_name is not None and (
            not _precleaned_addresses
            or cleaned_table_name.startswith("__ukam_chunked_addresses_")
        ):
            _drop_cleaned_chunk_relation(con, cleaned_table_name)
        if distinguishing_table_name is not None:
            _drop_table_and_registered_aliases(con, distinguishing_table_name)
        if inv_idx_table_name == "__ukam_inverted_index":
            _drop_table_and_registered_aliases(con, inv_idx_table_name)
        if processed_table is not None:
            _drop_table_and_registered_aliases(con, processed_table)

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
    token_column = (
        "clean_full_address_tokens"
        if "clean_full_address_tokens" in cleaned_address_table.columns
        else "clean_full_address"
    )

    if derive_distinguishing_wrt_adjacent_records:
        try:
            logger.debug("Deriving adjacent-record distinguishing tokens")
            distinguishing_table_name = f"__ukam_distinguishing_tokens_{uid}"
            _materialise_canonical_distinguishing_features(
                con, cleaned_address_table, distinguishing_table_name, debug_options
            )
            logger.debug("Adjacent-record distinguishing tokens derived")
        except BaseException:
            cleanup_on_failure()
            raise

    try:
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
            [
                _lookup_keys_in_inverted_index(
                    lookup_strategies,
                    token_column=token_column,
                )
            ]
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
            distinguishing_select_sql = (
                ",\n                "
                + ",\n                ".join(
                    f"distinguishing.{column}"
                    for column in DISTINGUISHING_FEATURE_COLUMNS
                )
            )
            distinguishing_join_sql = f"""
                LEFT JOIN {distinguishing_table_name} AS distinguishing
                                ON cleaned.ukam_address_id =
                                    distinguishing.ukam_address_id
            """

        if dataset_role == "canonical":
            processed_table = f"__ukam__processed_canonical_{uid}"
        elif dataset_role == "messy":
            processed_table = f"__ukam__processed_messy_{uid}"
        elif dataset_role is None:
            processed_table = f"__ukam__processed_{uid}"
        else:
            raise ValueError(
                "dataset_role must be one of: 'messy', 'canonical', or None."
            )
    except BaseException:
        if progress is not None:
            progress.close()
        cleanup_on_failure()
        raise

    # Apply term frequencies and trigram blocking to cleaned chunks
    try:
        log_stage_start(
            stage_label,
            total_rows,
            total_chunks,
            progress_mode=progress_mode,
        )
        for chunk_index in range(total_chunks):
            first_id = chunk_index * chunk_size + 1
            last_id = min((chunk_index + 1) * chunk_size, total_rows)
            chunk_query = con.sql(f"""
            SELECT cleaned.*{distinguishing_select_sql}
                FROM {cleaned_table_name} AS cleaned
                {distinguishing_join_sql}
                WHERE cleaned.ukam_address_id BETWEEN {first_id} AND {last_id}
            """)

            # Process chunk: apply term frequencies + inverted index blocking in one pass
            processed_chunk = _clean_data_using_precomputed_rel_tok_freq(
                chunk_query,
                con=con,
                pre_cleaned_addresses=True,
                additional_stages=inverted_index_stages,
                debug_options=debug_options if chunk_index == 0 else None,
                narrow_post_tf=True,
            )
            if _drop_columns:
                processed_chunk = processed_chunk.project(
                    ", ".join(
                        f'"{column}"'
                        for column in processed_chunk.columns
                        if column not in _drop_columns
                    )
                )

            if chunk_index == 0:
                con.execute(f"DROP TABLE IF EXISTS {processed_table}")
                processed_chunk.create(processed_table)
            else:
                processed_chunk.insert_into(processed_table)

            # Global TF and adjacent features are already materialised. Once
            # these IDs are written, only the remaining source chunks are live.
            completed = [
                name
                for name, maximum in (_owned_chunks or {}).items()
                if maximum <= last_id
            ]
            if completed:
                remaining = [name for name in _owned_chunks if name not in completed]
                if remaining:
                    union_sql = " UNION ALL ".join(
                        f"SELECT * FROM {name}" for name in remaining
                    )
                    con.execute(
                        f"CREATE OR REPLACE VIEW {cleaned_table_name} AS {union_sql}"
                    )
                else:
                    _drop_table_and_registered_aliases(con, cleaned_table_name)
                for name in completed:
                    _drop_table_and_registered_aliases(con, name)
                    del _owned_chunks[name]

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
            )
        log_stage_complete(
            stage_label,
            total_rows,
            progress_mode=progress_mode,
        )
    except BaseException:
        cleanup_on_failure()
        raise
    finally:
        progress.close()

    try:
        logger.debug("Finalizing prepared address table")
        _drop_cleaned_chunk_relation(con, cleaned_table_name)
        if distinguishing_table_name is not None:
            con.execute(f"DROP TABLE IF EXISTS {distinguishing_table_name}")

        # Clean up inverted index table if it was registered
        if inv_idx_table_name == "__ukam_inverted_index":
            _drop_table_and_registered_aliases(con, inv_idx_table_name)
    except BaseException:
        cleanup_on_failure()
        raise

    logger.debug("Prepared address table finalized")

    return con.table(processed_table)


__all__ = [
    "prepare_data_for_matching",
]
