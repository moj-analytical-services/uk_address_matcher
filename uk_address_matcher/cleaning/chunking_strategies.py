from __future__ import annotations

import logging
from collections.abc import Collection, Iterator
from contextlib import closing
from pathlib import Path
from shutil import rmtree
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Literal, Optional

from duckdb import DuckDBPyConnection, DuckDBPyRelation

from uk_address_matcher.cleaning.local_keys import prepare_canonical_local_keys
from uk_address_matcher.cleaning.materialisation import (
    _CanonicalIntermediates,
    _input_has_collation,
    _parquet_preserves_type,
)
from uk_address_matcher.cleaning.pipelines import (
    QUEUE_FOR_TF_DERIVATION,
    QUEUE_INVERTED_INDEX_SELF,
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
    _prepare_roadlike_place_input,
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
_CANONICAL_LOCAL_KEY_BATCH_ROWS = 1_000_000
_ROAD_FALLBACK_SCOPE_DISTRICTS = 16
_CANONICAL_POSTCODE_DISTRICTS_PER_BATCH = 128

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
    *,
    _storage: _CanonicalIntermediates | None = None,
    append: bool = False,
    temporary: bool = False,
    partition_by: str | None = None,
    order_by: str | None = None,
) -> DuckDBPyRelation:
    if not append:
        _drop_materialised_relation(con, table_name, _storage)
    if _storage is not None:
        return _storage.write(
            relation,
            table_name,
            append=append,
            partition_by=partition_by,
            order_by=order_by,
        )
    if append:
        relation.insert_into(table_name)
    else:
        temporary_sql = "TEMPORARY " if temporary else ""
        con.execute(f"CREATE {temporary_sql}TABLE {table_name} AS {relation.sql_query()}")
    return con.table(table_name)


def _drop_materialised_relation(
    con: DuckDBPyConnection,
    table_name: str,
    _storage: _CanonicalIntermediates | None = None,
) -> None:
    if _storage is not None:
        _storage.drop(table_name)
    else:
        _drop_table_and_registered_aliases(con, table_name)


def _drop_tables_with_prefix(
    con: DuckDBPyConnection,
    prefix: str,
    _storage: _CanonicalIntermediates | None = None,
) -> None:
    table_names = [name for (name,) in con.execute("SHOW TABLES").fetchall()]
    for table_name in table_names:
        if table_name.startswith(prefix):
            _drop_materialised_relation(con, table_name, _storage)


def _drop_cleaned_chunk_relation(
    con: DuckDBPyConnection,
    relation_name: str,
    _storage: _CanonicalIntermediates | None = None,
) -> None:
    _drop_materialised_relation(con, relation_name, _storage)
    if relation_name.startswith("__ukam_chunked_addresses_"):
        uid = relation_name.removeprefix("__ukam_chunked_addresses_")
        _drop_tables_with_prefix(con, f"__ukam_cleaned_chunk_{uid}_", _storage)


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
    _storage: _CanonicalIntermediates | None = None,
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
        preferred = con.sql(f"""
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
        preferred_addresses = _materialise_relation(
            con,
            preferred,
            preferred_table,
            _storage=_storage,
            temporary=True,
        )
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
    _drop_materialised_relation(con, road_keys_table, _storage)
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
            _materialise_relation(
                con,
                chunk_keys,
                road_keys_table,
                _storage=_storage,
                append=chunk_index > 0,
                temporary=True,
            )
            _drop_table_and_registered_aliases(con, chunk_keys_table)
    except Exception:
        _drop_materialised_relation(con, road_keys_table, _storage)
        raise
    finally:
        _drop_table_and_registered_aliases(con, chunk_keys_table)
        _drop_materialised_relation(con, preferred_table, _storage)
        con.execute(
            f"SET preserve_insertion_order = {str(preserve_insertion_order).lower()}"
        )
    enriched = con.sql(f"""
        SELECT canonical.*, road_features.road_1_norm
        FROM ({canonical_addresses.sql_query()}) AS canonical
        LEFT JOIN {road_keys_table} AS road_features USING (unique_id)
    """)
    try:
        # Retain the narrow road keys instead of copying every canonical column.
        con.execute(f"CREATE TEMPORARY VIEW {enriched_table} AS {enriched.sql_query()}")
        if _storage is not None:
            _storage.retain_view(enriched_table)
        return con.table(enriched_table)
    except BaseException:
        _drop_table_and_registered_aliases(con, enriched_table)
        _drop_materialised_relation(con, road_keys_table, _storage)
        raise
    finally:
        con.execute(
            f"SET preserve_insertion_order = {str(preserve_insertion_order).lower()}"
        )


def _postcode_batches(
    source: DuckDBPyRelation,
    con: DuckDBPyConnection,
    *,
    num_of_chunks: int | None = None,
    postcode_districts_per_batch: int | None = None,
    target_rows_per_batch: int = 125_000,
    property_scope_column: str | None = None,
) -> Iterator[tuple[DuckDBPyRelation, int]]:
    """Materialise whole districts while retaining fixed road fallback groups."""
    if num_of_chunks is not None and num_of_chunks < 1:
        raise ValueError("num_of_chunks must be at least 1")
    if postcode_districts_per_batch is not None and postcode_districts_per_batch < 1:
        raise ValueError("postcode_districts_per_batch must be at least 1")
    if target_rows_per_batch < 1:
        raise ValueError("target_rows_per_batch must be at least 1")
    districts_per_batch = postcode_districts_per_batch
    if property_scope_column is not None and districts_per_batch is not None:
        if districts_per_batch >= _ROAD_FALLBACK_SCOPE_DISTRICTS:
            districts_per_batch -= districts_per_batch % _ROAD_FALLBACK_SCOPE_DISTRICTS
    uid = _uid()
    input_table = f"__ukam_postcode_input_{uid}"
    districts_table = f"__ukam_postcode_districts_{uid}"
    batch_table = f"__ukam_postcode_batch_input_{uid}"
    partition_column = f"__ukam_postcode_batch_{uid}"
    district_expression = (
        "coalesce(nullif(regexp_extract(upper(coalesce(postcode, '')), "
        "'^\\s*([A-Z]{1,2}[0-9]{1,2}[A-Z]?)\\s+\\d', 1), ''), '__UNKNOWN__')"
    )
    con.register(input_table, source)
    try:
        district_counts = con.sql(f"""
            SELECT {district_expression} AS district, count(*) AS alias_count
            FROM {input_table} GROUP BY district ORDER BY district
        """).fetchall()
        scope_size = _ROAD_FALLBACK_SCOPE_DISTRICTS if property_scope_column else 1
        scope_count = (len(district_counts) + scope_size - 1) // scope_size
        requested_batches = min(num_of_chunks, scope_count) if num_of_chunks else 0
        districts = []
        batch_ids = []
        property_scopes = []
        batch_id = 0
        batch_rows = 0
        for district_index, (district, alias_count) in enumerate(district_counts):
            if num_of_chunks is not None:
                batch_id = (
                    (district_index // scope_size) * requested_batches // scope_count
                )
            elif districts_per_batch is not None:
                batch_id = district_index // districts_per_batch
            elif (
                batch_rows
                and batch_rows + alias_count > target_rows_per_batch
                and (
                    property_scope_column is None
                    or district_index % _ROAD_FALLBACK_SCOPE_DISTRICTS == 0
                )
            ):
                batch_id += 1
                batch_rows = 0
            districts.append(district)
            batch_ids.append(batch_id)
            property_scopes.append(district_index // _ROAD_FALLBACK_SCOPE_DISTRICTS)
            batch_rows += alias_count
        total_batches = batch_ids[-1] + 1 if batch_ids else 0
        if not total_batches:
            return
        con.execute(
            f"""
            CREATE TEMP TABLE {districts_table} AS
            SELECT unnest($districts::VARCHAR[]) AS district,
                unnest($batch_ids::INTEGER[]) AS batch_id,
                unnest($property_scopes::INTEGER[]) AS property_scope
        """,
            {
                "districts": districts,
                "batch_ids": batch_ids,
                "property_scopes": property_scopes,
            },
        )
        scope_projection = (
            f", districts.property_scope AS {property_scope_column}"
            if property_scope_column is not None
            else ""
        )
        batch_sql = f"""
            SELECT source.* {scope_projection}, districts.batch_id AS {partition_column}
            FROM {input_table} source JOIN {districts_table} districts
                ON {district_expression} = districts.district
        """
        if total_batches == 1:
            con.execute(f"""
                CREATE TEMP TABLE {batch_table} AS
                SELECT * EXCLUDE ({partition_column}) FROM ({batch_sql}) batches
            """)
            yield con.table(batch_table), total_batches
            return
        parquet_safe = all(_parquet_preserves_type(kind) for kind in source.types)
        parquet_safe = parquet_safe and not _input_has_collation(con, source)
        with TemporaryDirectory(prefix="ukam-postcode-batches-") as directory:
            if parquet_safe:
                escaped_directory = directory.replace("'", "''")
                con.execute(f"""
                    COPY ({batch_sql}) TO '{escaped_directory}'
                    (FORMAT PARQUET, PARTITION_BY ({partition_column}),
                        COMPRESSION SNAPPY)
                """)
            for batch_index in range(total_batches):
                if parquet_safe:
                    batch_directory = (
                        Path(directory) / f"{partition_column}={batch_index}"
                    )
                    batch = con.read_parquet(
                        str(batch_directory / "*.parquet"), hive_partitioning=False
                    ).set_alias(f"{batch_table}_{batch_index}")
                else:
                    _drop_table_and_registered_aliases(con, batch_table)
                    batch_query = f"""
                        SELECT * EXCLUDE ({partition_column}) FROM ({batch_sql}) batches
                        WHERE {partition_column} = {batch_index}
                    """
                    con.execute(f"CREATE TEMP TABLE {batch_table} AS {batch_query}")
                    batch = con.table(batch_table)
                yield batch, total_batches
                if parquet_safe:
                    rmtree(batch_directory)
    finally:
        _drop_table_and_registered_aliases(con, batch_table)
        _drop_table_and_registered_aliases(con, districts_table)
        _drop_table_and_registered_aliases(con, input_table)


def derive_roadlike_places(
    canonical_address_table: DuckDBPyRelation,
    con: DuckDBPyConnection,
    output_path: Path | str | None = None,
    *,
    postcode_districts: Collection[str] | None = None,
    num_of_chunks: int | None = None,
    postcode_districts_per_batch: int | None = _CANONICAL_POSTCODE_DISTRICTS_PER_BATCH,
    debug_options: Optional[DebugOptions] = None,
    show_progress: ShowProgress = "auto",
    _storage: _CanonicalIntermediates | None = None,
) -> DuckDBPyRelation:
    """Build a roadlike-place catalogue from canonical addresses.

    The input must already be canonical-cleaned, including ``clean_full_address``
    and ``numeric_tokens``. Road preparation and candidate extraction run in
    batches of postcode districts; catalogue evidence is local to each district.
    ``postcode_districts`` restricts the source before batching.
    Physical batches default to 128 districts; road fallback groups retain their
    existing 16-district boundaries. Larger requested batches are rounded down
    to a whole number of fallback groups. `num_of_chunks`, when supplied, takes
    precedence and divides whole fallback groups across at most that many batches.
    """
    catalogue, _ = _derive_postcode_features(
        canonical_address_table,
        con,
        output_path,
        include_local_keys=False,
        postcode_districts=postcode_districts,
        num_of_chunks=num_of_chunks,
        postcode_districts_per_batch=postcode_districts_per_batch,
        debug_options=debug_options,
        show_progress=show_progress,
        _storage=_storage,
    )
    return catalogue


def _derive_postcode_features(
    canonical_address_table: DuckDBPyRelation,
    con: DuckDBPyConnection,
    output_path: Path | str | None = None,
    *,
    include_local_keys: bool,
    include_road_catalogue: bool = True,
    postcode_districts: Collection[str] | None = None,
    num_of_chunks: int | None = None,
    postcode_districts_per_batch: int | None = _CANONICAL_POSTCODE_DISTRICTS_PER_BATCH,
    debug_options: Optional[DebugOptions] = None,
    show_progress: ShowProgress = "auto",
    _storage: _CanonicalIntermediates | None = None,
) -> tuple[DuckDBPyRelation | None, DuckDBPyRelation | None]:
    selected_postcode_districts = _normalise_postcode_districts(postcode_districts)
    if postcode_districts_per_batch is not None and postcode_districts_per_batch < 1:
        raise ValueError("postcode_districts_per_batch must be at least 1")

    required_columns = {"unique_id", "clean_full_address", "postcode"}
    if include_local_keys:
        required_columns.add("ukam_address_id")
    if include_road_catalogue:
        required_columns.add("numeric_tokens")
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
    total_rows = road_catalogue_source.count("*").fetchone()[0]
    if total_rows == 0:
        if selected_postcode_districts is not None:
            raise ValueError(
                "No canonical addresses matched the supplied postcode districts"
            )
        raise ValueError(
            "Supplied address table has no records. Please provide a non-empty table."
        )
    if selected_postcode_districts is not None:
        logger.info(
            "Restricting road catalogue to %s postcode districts and %s rows",
            len(selected_postcode_districts),
            total_rows,
        )

    progress_mode = resolve_progress_mode(show_progress)
    uid = _uid()
    candidate_sources_table = f"__ukam_roadlike_candidate_sources_{uid}"
    catalogue_table = f"__ukam_roadlike_places_{uid}"
    local_key_table = f"__ukam_local_key_features_{uid}"
    property_scope_column = f"__ukam_road_scope_{uid}" if include_road_catalogue else None

    if output_path is not None:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
    stage_label = "Deriving canonical postcode features"
    batch_columns = ["unique_id", "postcode", "clean_full_address"]
    if include_local_keys:
        batch_columns.insert(0, "ukam_address_id")
    if include_road_catalogue:
        batch_columns.append("numeric_tokens")
    narrow_source = con.sql(f"""
        SELECT {", ".join(batch_columns)}
        FROM ({road_catalogue_source.sql_query()}) AS canonical
    """)
    progress = None
    processed_rows = 0

    try:
        with closing(
            _postcode_batches(
                narrow_source,
                con,
                num_of_chunks=num_of_chunks,
                postcode_districts_per_batch=postcode_districts_per_batch,
                property_scope_column=property_scope_column,
            )
        ) as batches:
            for batch_index, (batch_input, total_batches) in enumerate(batches):
                if progress is None:
                    log_stage_start(
                        stage_label,
                        total_rows,
                        total_batches,
                        progress_mode=progress_mode,
                    )
                    progress = _ProgressBar(
                        label=stage_label,
                        total=total_rows,
                        total_units=total_batches,
                        enabled=progress_mode == "auto",
                    )
                batch_rows = batch_input.count("*").fetchone()[0]
                if include_local_keys:
                    features = _canonical_local_key_features(con, batch_input)
                    _materialise_relation(
                        con,
                        features,
                        local_key_table,
                        _storage=_storage,
                        append=batch_index > 0,
                    )
                if include_road_catalogue:
                    preparation = create_sql_pipeline(
                        con,
                        input_rel=batch_input,
                        stage_specs=[
                            _prepare_roadlike_place_input(
                                property_scope_column=property_scope_column
                            )
                        ],
                        pipeline_name="Prepare roadlike-place input",
                    ).run(debug_options if batch_index == 0 else None)
                    candidate_sources_sql = roadlike_place_prepared_candidate_sources_sql(
                        f"({preparation.sql_query()})",
                        property_scope_column=property_scope_column,
                    )
                    _materialise_relation(
                        con,
                        con.sql(f"""
                            SELECT address_id, {property_scope_column},
                                full_postcode, postcode_district,
                                rightmost_numeric_value, 0::INTEGER AS numeric_anchor,
                                road_tail_tokens AS address_tokens,
                                allow_truncated_windows
                            FROM ({candidate_sources_sql}) candidate_sources
                        """),
                        candidate_sources_table,
                        _storage=_storage,
                        temporary=True,
                    )
                    candidate_relation = roadlike_place_prepared_candidate_sql(
                        candidate_sources_table,
                        candidate_source_relation=candidate_sources_table,
                        property_scope_column=property_scope_column,
                    )
                    _materialise_relation(
                        con,
                        con.sql(
                            roadlike_place_catalog_sql(
                                f"({candidate_relation})", by_postcode_district=True
                            )
                        ),
                        catalogue_table,
                        _storage=_storage,
                        append=batch_index > 0,
                    )
                    _drop_materialised_relation(con, candidate_sources_table, _storage)

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
        if output_path is not None and include_road_catalogue:
            escaped_output_path = str(output_path).replace("'", "''")
            con.execute(
                f"COPY {catalogue_table} TO '{escaped_output_path}' "
                "(FORMAT PARQUET, COMPRESSION ZSTD)"
            )
    except BaseException:
        _drop_materialised_relation(con, local_key_table, _storage)
        _drop_materialised_relation(con, catalogue_table, _storage)
        raise
    finally:
        if progress is not None:
            progress.close()
        _drop_materialised_relation(con, candidate_sources_table, _storage)

    log_stage_complete(
        stage_label,
        total_rows,
        progress_mode=progress_mode,
    )
    return (
        con.table(catalogue_table) if include_road_catalogue else None,
        con.table(local_key_table) if include_local_keys else None,
    )


def clean_data_pre_term_frequencies(
    address_table: DuckDBPyRelation,
    con: DuckDBPyConnection,
    num_of_chunks: int = 10,
    *,
    _drop_columns: Collection[str] = (),
    _owned_chunks: dict[str, int] | None = None,
    debug_options: DebugOptions | None = None,
    show_progress: ShowProgress = "auto",
    _storage: _CanonicalIntermediates | None = None,
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
        raw_input = con.sql(f"""
            SELECT
                *,
                CAST(abs(hash(address_concat)) % {total_chunks} AS INTEGER)
                    AS {chunk_index_column}
            FROM {input_name}
        """)
        _materialise_relation(
            con,
            raw_input,
            chunked_input_name,
            _storage=_storage,
            partition_by=chunk_index_column,
        )

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
                _storage=_storage,
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
            _materialise_relation(con, processed_chunk, chunk_name, _storage=_storage)
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

            _drop_materialised_relation(con, chunk_table, _storage)
    except BaseException:
        _drop_tables_with_prefix(con, cleaned_chunk_prefix, _storage)
        raise
    finally:
        progress.close()
        _drop_tables_with_prefix(con, f"__ukam_chunk_input_{uid}_", _storage)
        _drop_materialised_relation(con, chunked_input_name, _storage)
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
        if _storage is not None:
            _storage.retain_view(chunked_table)
        return con.table(chunked_table)
    except BaseException:
        _drop_cleaned_chunk_relation(con, chunked_table, _storage)
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


def _index_keys_have_collation(con: DuckDBPyConnection, keys: DuckDBPyRelation) -> bool:
    if con.execute("SELECT current_setting('default_collation')").fetchone()[0] not in (
        "",
        "binary",
    ):
        return True
    name = f"__ukam_index_collation_{_uid()}"
    try:
        # CTAS exposes bound column collations that logical VARCHAR types omit.
        con.execute(
            f"CREATE TEMPORARY TABLE {name} AS "
            f"SELECT unique_id, key FROM ({keys.sql_query()}) LIMIT 0"
        )
        ddl = con.execute(
            "SELECT sql FROM duckdb_tables() WHERE table_name = ?", [name]
        ).fetchone()[0]
        return " COLLATE " in ddl.upper()
    finally:
        _drop_table_and_registered_aliases(con, name)


def derive_inverted_index(
    cleaned_address_table: DuckDBPyRelation,
    con: DuckDBPyConnection,
    num_of_chunks: int = 1,
    strategies: list[PhysicalIndexStrategy] | None = None,
    *,
    debug_options: DebugOptions | None = None,
    show_progress: ShowProgress = "auto",
    _storage: _CanonicalIntermediates | None = None,
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

            _materialise_relation(
                con,
                chunk_result,
                result_table,
                _storage=_storage,
                append=not first_insert,
            )
            first_insert = False
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
            key_path = None
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
                # Keep native storage for collations and identifier types
                # whose Parquet round trip is not safe (for example HUGEINT).
                if str(scalar_keys.types[0]) not in {
                    "BIGINT",
                    "VARCHAR",
                } or _index_keys_have_collation(con, scalar_keys):
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
                logger.debug("%s: keys staged", stage_label)

                for chunk_index in range(num_of_chunks):
                    if key_path is not None:
                        # Select only this bucket's files; never infer columns
                        # from ancestor directories such as unique_id=999.
                        files = sorted(
                            (key_path / f"key_bucket={chunk_index}").glob("*.parquet")
                        )
                        chunk_keys = (
                            con.read_parquet(
                                [str(p) for p in files], hive_partitioning=False
                            )
                            if files
                            else scalar_keys.select("unique_id, key").limit(0)
                        )
                    else:
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

                    _materialise_relation(
                        con,
                        chunk_result,
                        result_table,
                        _storage=_storage,
                        append=not first_insert,
                    )
                    first_insert = False

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
                    # disappear; no lazy reader outlives its input.
                    key_directory.cleanup()

            log_stage_complete(
                stage_label,
                total_rows,
                progress_mode=progress_mode,
            )

    return con.table(result_table)


def _canonical_local_key_features(
    con: DuckDBPyConnection,
    source: DuckDBPyRelation,
) -> DuckDBPyRelation:
    inputs = source.project("ukam_address_id, unique_id, postcode, clean_full_address")
    features = prepare_canonical_local_keys(con, inputs)
    return features.project("ukam_address_id, local_key_index")


def _attach_canonical_features(
    source: DuckDBPyRelation,
    con: DuckDBPyConnection,
    table_name: str,
    *,
    road_features: DuckDBPyRelation | None = None,
    _storage: _CanonicalIntermediates | None = None,
) -> DuckDBPyRelation:
    clustered_table = f"__ukam_local_key_clustered_{_uid()}"
    enriched_table = f"__ukam_canonical_with_local_keys_{_uid()}"
    original_table = table_name
    try:
        features = con.table(table_name)
        if road_features is not None:
            features = con.sql(f"""
                SELECT local.ukam_address_id, road.road_1_norm,
                    local.* EXCLUDE (ukam_address_id)
                FROM {table_name} local
                LEFT JOIN ({road_features.sql_query()}) road USING (ukam_address_id)
            """)
        if (
            road_features is not None
            or features.count("*").fetchone()[0] > _CANONICAL_LOCAL_KEY_BATCH_ROWS
        ):
            _materialise_relation(
                con,
                features,
                clustered_table,
                _storage=_storage,
                order_by="ukam_address_id",
            )
            _drop_materialised_relation(con, table_name, _storage)
            table_name = clustered_table
        feature_columns = con.table(table_name).columns
        source_columns = ", ".join(
            f'"{column}"'
            for column in source.columns
            if column == "ukam_address_id"
            or column not in (*feature_columns, "local_key_tokens")
        )
        canonical = con.sql(f"""
            SELECT {source_columns} FROM ({source.sql_query()}) canonical
        """)
        first_id, last_id = con.sql(f"""
            SELECT min(ukam_address_id), max(ukam_address_id)
            FROM ({canonical.sql_query()}) canonical
        """).fetchone()
        if first_id is None:
            first_id = last_id = 0
        for index, start in enumerate(
            range(first_id, last_id + 1, _CANONICAL_LOCAL_KEY_BATCH_ROWS)
        ):
            end = start + _CANONICAL_LOCAL_KEY_BATCH_ROWS - 1
            chunk = con.sql(f"""
                SELECT canonical.*, features.* EXCLUDE (ukam_address_id)
                FROM ({canonical.sql_query()}) AS canonical
                LEFT JOIN (
                    SELECT * FROM {table_name}
                    WHERE ukam_address_id BETWEEN {start} AND {end}
                ) AS features USING (ukam_address_id)
                WHERE canonical.ukam_address_id BETWEEN {start} AND {end}
            """)
            _materialise_relation(
                con,
                chunk,
                enriched_table,
                _storage=_storage,
                append=index > 0,
                order_by="ukam_address_id",
            )
        return con.table(enriched_table)
    except BaseException:
        _drop_materialised_relation(con, enriched_table, _storage)
        raise
    finally:
        _drop_materialised_relation(con, original_table, _storage)
        _drop_materialised_relation(con, clustered_table, _storage)


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
    _storage: _CanonicalIntermediates | None = None,
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
    columns = "ukam_address_id, unique_id, clean_full_address, clean_full_address_tokens"
    if (
        batch_count <= 1
        or unique_id_type not in {"VARCHAR", "BIGINT", "INTEGER"}
        or collation not in ("", "binary")
        or order not in ("ASC", "ASCENDING")
        or null_order not in ("NULLS_LAST", "NULLS_LAST_ON_ASC_FIRST_ON_DESC")
        or _input_has_collation(con, source.project(columns))
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
            _storage=_storage,
            order_by="ukam_address_id",
        )

    uid = _uid()
    bounds_table = f"__ukam_adjacent_bounds_{uid}"
    input_table = f"__ukam_adjacent_batch_{uid}"
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
                    _materialise_relation(
                        con,
                        features,
                        table_name,
                        _storage=_storage,
                        append=index > 0,
                        order_by="ukam_address_id",
                    )
                finally:
                    _drop_table_and_registered_aliases(con, input_table)
        return con.table(table_name)
    except BaseException:
        _drop_materialised_relation(con, table_name, _storage)
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
    _storage: _CanonicalIntermediates | None = None,
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
            _drop_cleaned_chunk_relation(con, cleaned_table_name, _storage)
        if distinguishing_table_name is not None:
            _drop_materialised_relation(con, distinguishing_table_name, _storage)
        if inv_idx_table_name == "__ukam_inverted_index":
            _drop_table_and_registered_aliases(con, inv_idx_table_name)
        if processed_table is not None:
            _drop_materialised_relation(con, processed_table, _storage)

    if _precleaned_addresses:
        cleaned_address_table = address_table
    else:
        cleaned_address_table = clean_data_pre_term_frequencies(
            address_table,
            con,
            num_of_chunks=num_of_chunks,
            debug_options=debug_options,
            show_progress=progress_mode,
            _storage=_storage,
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
                con,
                cleaned_address_table,
                distinguishing_table_name,
                debug_options,
                _storage=_storage,
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

            _materialise_relation(
                con,
                processed_chunk,
                processed_table,
                _storage=_storage,
                append=chunk_index > 0,
            )

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
                    _drop_materialised_relation(con, name, _storage)
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
        _drop_cleaned_chunk_relation(con, cleaned_table_name, _storage)
        if distinguishing_table_name is not None:
            _drop_materialised_relation(con, distinguishing_table_name, _storage)

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
