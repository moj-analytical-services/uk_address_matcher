from __future__ import annotations

import hashlib
import json
import logging
import shutil
import time
import warnings
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Literal
from uuid import uuid4

from uk_address_matcher._typing import PrepareCanonicalInput
from uk_address_matcher.helpers.canonical_inputs import (
    normalise_and_validate_raw_canonical,
)
from uk_address_matcher.helpers.path_parsing import (
    is_path_like_input,
    is_remote_folder_reference,
    is_sequence_of_path_like_inputs,
    join_remote_path,
    read_duckdb_relation_from_path,
    relative_remote_path,
)
from uk_address_matcher.sql_pipeline.helpers import _register_input_relation_once

if TYPE_CHECKING:
    import duckdb

logger = logging.getLogger("uk_address_matcher")

PREPARED_ADDRESSES_FILENAME = "ukam_canonical_addresses.parquet"
PREPARED_ADDRESSES_CHUNK_DIRNAME = "ukam_canonical_addresses_chunks"
PREPARED_TERM_FREQUENCIES_FILENAME = "ukam_term_frequencies.parquet"
PREPARED_INVERTED_INDEX_FILENAME = "ukam_inverted_index.parquet"
PREPARED_DUCKDB_FILENAME = "ukam_prepared_canonical.duckdb"
PREPARED_REALTIME_DUCKDB_FILENAME = PREPARED_DUCKDB_FILENAME
MANIFEST_FILENAME = "ukam_manifest.json"
CHUNK_FILE_INDEX_DIGITS = 5
MAX_CHUNK_COUNT = (10**CHUNK_FILE_INDEX_DIGITS) - 1

REALTIME_CANONICAL_TABLE = "canonical_addresses"
REALTIME_TERM_FREQUENCIES_TABLE = "term_frequencies"
REALTIME_INVERTED_INDEX_TABLE = "inverted_index"
REALTIME_INVERTED_INDEX_HASHED_TABLE = "inverted_index_hashed"

REQUIRED_FILES = [
    PREPARED_TERM_FREQUENCIES_FILENAME,
    PREPARED_INVERTED_INDEX_FILENAME,
]

# All files managed by the preparation step (parquets + manifest + temps)
_MANAGED_FILES = [
    PREPARED_ADDRESSES_FILENAME,
    PREPARED_ADDRESSES_CHUNK_DIRNAME,
    PREPARED_DUCKDB_FILENAME,
    f"{PREPARED_DUCKDB_FILENAME}.wal",
    PREPARED_REALTIME_DUCKDB_FILENAME,
    f"{PREPARED_REALTIME_DUCKDB_FILENAME}.wal",
    *REQUIRED_FILES,
    MANIFEST_FILENAME,
    f"{MANIFEST_FILENAME}.tmp",
]

# Parquet write tuning for the prepared artefacts.
# ZSTD with a moderate-high level is paid once at preparation time and does not
# affect match-time runtime (zstd decompression speed is independent of the
# compression level), while meaningfully reducing on-disk and in-memory size.
PARQUET_COMPRESSION = "ZSTD"
PARQUET_COMPRESSION_LEVEL = 9

# Sorting canonical rows by these columns dramatically improves compression: rows
# sharing a postcode share large substrings across the address/token columns,
# which ZSTD and dictionary/run-length encoding exploit. Row order is irrelevant
# to matching, so this is a free win.
CANONICAL_SORT_COLUMNS = ("postcode", "clean_full_address")
INVERTED_INDEX_SORT_COLUMNS = ("index_strategy", "key")

# Columns that are trivially recomputable at load time and therefore not persisted,
# to reduce file size. They are restored in ``load_prepared_canonical_data``.
# For canonical data ``exploding_unique_ids`` is always ``[unique_id]``.
RECOMPUTABLE_DROP_COLUMNS = ("exploding_unique_ids",)


@dataclass(frozen=True)
class _PreparedCanonical:
    """Container for the three artefacts loaded from a prepared folder.

    Attributes:
        addresses: Cleaned and tokenised canonical addresses.
        term_frequencies: Term frequency lookup table.
        inverted_index: Inverted index for candidate retrieval.
    """

    addresses: duckdb.DuckDBPyRelation
    term_frequencies: duckdb.DuckDBPyRelation
    inverted_index: duckdb.DuckDBPyRelation


@dataclass(frozen=True)
class _PreparedFolderLayout:
    """Resolved prepared-data layout information for a folder."""

    folder: Path
    canonical_paths: list[Path]


def _escape_sql_string(value: str) -> str:
    """Escape a Python string for embedding in a DuckDB SQL string literal."""
    return value.replace("'", "''")


def _write_parquet_artefact(
    con: duckdb.DuckDBPyConnection,
    relation: duckdb.DuckDBPyRelation,
    path: str | Path,
    *,
    sort_columns: tuple[str, ...] = (),
    drop_columns: tuple[str, ...] = (),
) -> None:
    """Write a relation to a Parquet file using ZSTD compression.

    Optionally sorts rows by ``sort_columns`` (to maximise compression locality)
    and excludes ``drop_columns`` that are recomputed at load time. Works for
    both local paths and remote object-store URIs.
    """
    columns = relation.columns
    existing_drops = [c for c in drop_columns if c in columns]
    drop_clause = f" EXCLUDE ({', '.join(existing_drops)})" if existing_drops else ""
    sort_cols = [c for c in sort_columns if c in columns]
    order_clause = (" ORDER BY " + ", ".join(sort_cols)) if sort_cols else ""
    escaped_path = _escape_sql_string(str(path))
    con.execute(
        f"COPY (SELECT *{drop_clause} "
        f"FROM ({relation.sql_query()}) AS _ukam_src{order_clause}) "
        f"TO '{escaped_path}' "
        f"(FORMAT PARQUET, COMPRESSION {PARQUET_COMPRESSION}, "
        f"COMPRESSION_LEVEL {PARQUET_COMPRESSION_LEVEL})"
    )


def _describe_prepare_input(data: PrepareCanonicalInput) -> str:
    """Return a concise description of the canonical input source."""
    if is_path_like_input(data):
        return str(data)
    if is_sequence_of_path_like_inputs(data):
        return f"{len(data)} path(s): {', '.join(str(item) for item in data)}"
    return "DuckDB relation"


def _coerce_prepare_input_to_relation(
    data: PrepareCanonicalInput,
    *,
    con: duckdb.DuckDBPyConnection,
) -> duckdb.DuckDBPyRelation:
    """Coerce supported canonical input types into a DuckDB relation."""
    import duckdb as _duckdb

    if isinstance(data, _duckdb.DuckDBPyRelation):
        rel = _register_input_relation_once(data, con=con, role="prepare_canonical")
        return normalise_and_validate_raw_canonical(rel)

    if is_path_like_input(data) or is_sequence_of_path_like_inputs(data):
        rel = read_duckdb_relation_from_path(data, con=con)
        return normalise_and_validate_raw_canonical(rel)

    raise TypeError(
        "prepare_canonical_folder expected a DuckDB relation, a CSV/Parquet path, "
        "or a non-empty list of CSV/Parquet paths."
    )


def _remote_output_exists(
    folder_uri: str,
    *,
    con: duckdb.DuckDBPyConnection,
) -> bool:
    """Best-effort check for existing remote prepared artefacts."""
    candidate_paths = [
        join_remote_path(folder_uri, PREPARED_TERM_FREQUENCIES_FILENAME),
        join_remote_path(folder_uri, PREPARED_INVERTED_INDEX_FILENAME),
        join_remote_path(folder_uri, PREPARED_ADDRESSES_FILENAME),
        join_remote_path(folder_uri, f"{PREPARED_ADDRESSES_CHUNK_DIRNAME}/*.parquet"),
    ]

    for candidate_path in candidate_paths:
        try:
            con.read_parquet(candidate_path).limit(1).fetchone()
            return True
        except Exception as exc:
            _rollback_if_needed(con)
            if _is_permission_error(exc):
                raise PermissionError(
                    f"Cannot inspect remote output folder '{folder_uri}'. "
                    "Check object-store credentials and permissions. "
                    f"Underlying error: {exc}"
                ) from exc

    return False


def _is_permission_error(exc: Exception) -> bool:
    """Best-effort check for credential/permission access failures."""
    message = str(exc).lower()
    markers = [
        "access denied",
        "accessdenied",
        "forbidden",
        "http 403",
        "credential",
        "expiredtoken",
        "signaturedoesnotmatch",
        "permission",
        "unauthor",
        "unauthorizedssotoken",
        "sso session",
    ]
    return any(marker in message for marker in markers)


def _rollback_if_needed(con: duckdb.DuckDBPyConnection) -> None:
    """Best-effort rollback to clear aborted transaction state."""
    try:
        con.execute("ROLLBACK")
    except Exception:
        # Ignore when there is no active transaction.
        pass


def _rehydrate_canonical_addresses(
    addresses: duckdb.DuckDBPyRelation,
) -> duckdb.DuckDBPyRelation:
    """Restore recomputable columns not persisted in the canonical parquet.

    ``exploding_unique_ids`` is always ``[unique_id]`` for canonical data, so it
    is omitted at write time and reconstructed here to keep the in-memory schema
    identical to a freshly-prepared relation.
    """
    columns = addresses.columns
    if "exploding_unique_ids" not in columns and "unique_id" in columns:
        addresses = addresses.select("*, list_value(unique_id) AS exploding_unique_ids")
    return addresses


def _load_prepared_canonical_data_remote(
    folder_uri: str,
    con: duckdb.DuckDBPyConnection,
    canonical_address_filter: str | None,
) -> _PreparedCanonical:
    """Load prepared canonical artefacts from a remote folder URI via DuckDB."""
    # A previous failed read on the same connection can leave DuckDB in an aborted
    # transaction state. Clear it before attempting fallback reads.
    _rollback_if_needed(con)

    tf_uri = join_remote_path(folder_uri, PREPARED_TERM_FREQUENCIES_FILENAME)
    idx_uri = join_remote_path(folder_uri, PREPARED_INVERTED_INDEX_FILENAME)
    single_canonical_uri = join_remote_path(folder_uri, PREPARED_ADDRESSES_FILENAME)
    chunk_glob_uri = join_remote_path(
        folder_uri,
        f"{PREPARED_ADDRESSES_CHUNK_DIRNAME}/*.parquet",
    )
    single_dataset_glob_uri = join_remote_path(
        folder_uri,
        f"{PREPARED_ADDRESSES_FILENAME}/*.parquet",
    )

    try:
        term_frequencies = con.read_parquet(tf_uri)
        term_frequencies.limit(1).fetchone()
        inverted_index = con.read_parquet(idx_uri)
        inverted_index.limit(1).fetchone()
    except Exception as exc:
        _rollback_if_needed(con)
        if _is_permission_error(exc):
            raise PermissionError(
                f"Cannot access prepared canonical data at '{folder_uri}'. "
                "Check object-store credentials and permissions. "
                f"Underlying error: {exc}"
            ) from exc
        raise FileNotFoundError(
            f"Prepared canonical remote folder '{folder_uri}' is missing required "
            "files or is inaccessible. Expected: "
            f"{PREPARED_TERM_FREQUENCIES_FILENAME}, "
            f"{PREPARED_INVERTED_INDEX_FILENAME}. "
            f"Underlying error: {exc}"
        ) from exc

    addresses: duckdb.DuckDBPyRelation
    address_candidates = [
        chunk_glob_uri,
        single_canonical_uri,
        single_dataset_glob_uri,
    ]
    read_errors: list[Exception] = []
    for candidate_uri in address_candidates:
        try:
            addresses = con.read_parquet(candidate_uri)
            addresses.limit(1).fetchone()
            break
        except Exception as exc:
            read_errors.append(exc)
            _rollback_if_needed(con)
    else:
        permission_exc = next((e for e in read_errors if _is_permission_error(e)), None)
        if permission_exc is not None:
            raise PermissionError(
                f"Cannot access canonical addresses in '{folder_uri}'. "
                "Check object-store credentials and permissions. "
                f"Underlying error: {permission_exc}"
            ) from permission_exc

        last_error = read_errors[-1] if read_errors else RuntimeError("Unknown error")
        raise FileNotFoundError(
            f"Prepared canonical remote folder '{folder_uri}' is missing "
            "canonical addresses artefacts. Expected one of: "
            f"{PREPARED_ADDRESSES_FILENAME}, "
            f"{PREPARED_ADDRESSES_CHUNK_DIRNAME}/*.parquet, or "
            f"{PREPARED_ADDRESSES_FILENAME}/*.parquet. "
            f"Underlying error: {last_error}"
        ) from last_error

    addresses = _rehydrate_canonical_addresses(addresses)

    if canonical_address_filter is not None:
        addresses = addresses.filter(canonical_address_filter)

    logger.debug("Loaded prepared canonical data from remote '%s'", folder_uri)

    return _PreparedCanonical(
        addresses=addresses,
        term_frequencies=term_frequencies,
        inverted_index=inverted_index,
    )


def _sha256_file(path: Path) -> str:
    """Return the hex SHA-256 digest of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _clear_stale_artefacts(folder: Path) -> None:
    """Remove all known artefacts and temp files from a previous run."""
    for name in _MANAGED_FILES:
        p = folder / name
        if p.exists():
            if p.is_dir():
                shutil.rmtree(p)
            else:
                p.unlink()


def _format_elapsed(elapsed_seconds: float) -> str:
    total_seconds = int(round(max(0.0, elapsed_seconds)))
    minutes, seconds = divmod(total_seconds, 60)
    return f"{minutes}m {seconds:02d}s"


def _chunk_file_name(chunk_index: int, total_chunks: int) -> str:
    return (
        "canonical_addresses_chunk_"
        f"{chunk_index + 1:0{CHUNK_FILE_INDEX_DIGITS}d}_"
        f"of_{total_chunks:0{CHUNK_FILE_INDEX_DIGITS}d}.parquet"
    )


def _validate_chunk_count(value: int, *, name: str) -> None:
    if value < 1:
        raise ValueError(f"{name} must be at least 1")
    if value > MAX_CHUNK_COUNT:
        raise ValueError(
            f"{name} must be at most {MAX_CHUNK_COUNT} "
            f"(supports {CHUNK_FILE_INDEX_DIGITS}-digit chunk numbering)."
        )


def _resolve_chunk_hash_key(columns: list[str]) -> str:
    """Pick a stable hash-partition key present in cleaned canonical output."""
    preferred_keys = [
        "address_concat",
        "clean_full_address",
        "address_to_parse",
        "unique_id",
        "address_id",
    ]
    for key in preferred_keys:
        if key in columns:
            return key

    raise ValueError(
        "Unable to chunk canonical output: no suitable hash key column found. "
        "Expected one of address_concat, clean_full_address, address_to_parse, "
        "unique_id, or address_id."
    )


def _resolve_canonical_parquet_paths(folder: Path) -> list[Path]:
    """Resolve canonical addresses parquet paths for single-file or chunked layouts."""
    chunk_dir = folder / PREPARED_ADDRESSES_CHUNK_DIRNAME
    if chunk_dir.is_dir():
        chunk_paths = sorted(chunk_dir.glob("*.parquet"))
        if chunk_paths:
            return chunk_paths

    single_path = folder / PREPARED_ADDRESSES_FILENAME
    if single_path.exists():
        return [single_path]

    return []


def _build_manifest(
    *,
    created_with_duckdb_version: str,
    files_meta: dict[str, dict[str, object]],
    row_counts: dict[str, int],
    realtime: bool = False,
    storage_format: str = "parquet",
) -> dict[str, object]:
    """Build the manifest payload shared by local and remote writers."""
    from uk_address_matcher import __version__

    return {
        "ukam_version": __version__,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "created_with_duckdb_version": created_with_duckdb_version,
        "storage_format": storage_format,
        "realtime": realtime,
        "row_counts": row_counts,
        "files": files_meta,
    }


def prepare_canonical_folder(
    data: PrepareCanonicalInput,
    output_folder: str | Path,
    *,
    con: duckdb.DuckDBPyConnection,
    num_of_chunks: int = 10,
    output_chunk_count: int = 1,
    output_format: Literal["parquet", "duckdb"] = "parquet",
    overwrite: bool = False,
    show_progress: bool = True,
) -> None:
    """Prepare canonical data and persist to a folder for later use.

    Performs address cleaning and tokenisation, term frequency computation,
    and inverted index generation. Writes two Parquet files, canonical
    addresses (single-file or chunked), and a manifest
    to `output_folder`:

        - `ukam_canonical_addresses.parquet` — cleaned and tokenised addresses
            or
          `ukam_canonical_addresses_chunks/`
          `canonical_addresses_chunk_XXXXX_of_YYYYY.parquet`
    - `ukam_term_frequencies.parquet` — term frequency lookup table
    - `ukam_inverted_index.parquet` — inverted index for candidate retrieval
    - `ukam_manifest.json` — provenance metadata (version, row counts, hashes)

    Args:
        data: Raw canonical address data as a DuckDB relation, a CSV/Parquet
            path, or a non-empty list of CSV/Parquet paths.
        output_folder: Folder to write prepared artefacts to.
        con: DuckDB connection.
        num_of_chunks: Number of chunks to split the data into for cleaning
            and term frequency derivation. Set to 1 for no chunking.
        output_chunk_count: Number of output chunks to write for canonical
            addresses. Set to 1 to write `ukam_canonical_addresses.parquet`.
            Set above 1 to write hash-partitioned chunks under
            `ukam_canonical_addresses_chunks/`.
        output_format: Storage format for the prepared artefacts. Defaults to
            `parquet` for backwards compatibility. Use `duckdb` to write a
            single portable DuckDB database file.
        overwrite: Whether to overwrite existing files in the folder. When
            `True`, all known artefacts are removed before writing to ensure
            the folder ends up in a consistent state.
        show_progress: Whether to show live interactive progress bars for
            chunked cleaning stages.

    Raises:
        FileExistsError: If the output folder already contains prepared files
            and `overwrite` is `False`.
    """
    from uk_address_matcher.cleaning.chunking_strategies import (
        derive_inverted_index,
        derive_term_frequencies_table,
        prepare_data_for_matching,
    )

    output_is_remote = is_remote_folder_reference(output_folder)
    if output_format not in {"parquet", "duckdb"}:
        raise ValueError("output_format must be either 'parquet' or 'duckdb'.")
    if output_is_remote and output_format == "duckdb":
        raise ValueError("output_format='duckdb' only supports local output folders.")
    if output_format == "duckdb" and output_chunk_count != 1:
        raise ValueError("output_chunk_count must be 1 when output_format='duckdb'.")

    output_folder_uri = str(output_folder) if output_is_remote else None
    output_folder_path = None if output_is_remote else Path(output_folder)
    data = _coerce_prepare_input_to_relation(data, con=con)

    logger.info("Preparing canonical data from '%s'", _describe_prepare_input(data))
    logger.info("Writing prepared canonical artefacts to '%s'", output_folder)

    _validate_chunk_count(num_of_chunks, name="num_of_chunks")
    _validate_chunk_count(output_chunk_count, name="output_chunk_count")

    if output_is_remote:
        assert output_folder_uri is not None
        if not overwrite and _remote_output_exists(output_folder_uri, con=con):
            raise FileExistsError(
                f"Output folder '{output_folder_uri}' already contains prepared "
                "files. Set overwrite=True to replace the managed artefacts."
            )
    else:
        assert output_folder_path is not None
        if output_folder_path.exists() and not overwrite:
            existing = [f for f in REQUIRED_FILES if (output_folder_path / f).exists()]
            if (output_folder_path / PREPARED_DUCKDB_FILENAME).exists():
                existing.append(PREPARED_DUCKDB_FILENAME)
            if _resolve_canonical_parquet_paths(output_folder_path):
                existing.append("canonical_addresses")
            if existing:
                raise FileExistsError(
                    f"Output folder '{output_folder_path}' already contains prepared "
                    f"files: {existing}. Set overwrite=True to replace them."
                )

        output_folder_path.mkdir(parents=True, exist_ok=True)

        if overwrite:
            _clear_stale_artefacts(output_folder_path)

    # Derive artefacts / cleaned canonical data for export
    logger.debug("Deriving term frequencies from canonical data")
    tf_table = derive_term_frequencies_table(
        data,
        con=con,
        num_of_chunks=num_of_chunks,
        show_progress=show_progress,
    )

    logger.debug("Cleaning canonical addresses")
    df_clean = prepare_data_for_matching(
        data,
        con=con,
        num_of_chunks=num_of_chunks,
        term_frequency_lookup=tf_table,
        show_progress=show_progress,
    )

    logger.debug("Building inverted index")
    inverted_index = derive_inverted_index(
        df_clean,
        con=con,
        num_of_chunks=num_of_chunks,
        show_progress=show_progress,
    )

    canonical_paths: list[str | Path] = []
    artefact_paths: list[str | Path]
    artefact_columns: dict[str, list[str]]
    # Write parquet files
    tf_path = (
        join_remote_path(output_folder_uri, PREPARED_TERM_FREQUENCIES_FILENAME)
        if output_is_remote
        else output_folder_path / PREPARED_TERM_FREQUENCIES_FILENAME
    )
    idx_path = (
        join_remote_path(output_folder_uri, PREPARED_INVERTED_INDEX_FILENAME)
        if output_is_remote
        else output_folder_path / PREPARED_INVERTED_INDEX_FILENAME
    )

    _write_parquet_artefact(con, tf_table, tf_path)
    _write_parquet_artefact(
        con,
        inverted_index,
        idx_path,
        sort_columns=INVERTED_INDEX_SORT_COLUMNS,
    )

    canonical_paths: list[str | Path]
    chunk_output_location: str | Path | None = None
    if output_format == "duckdb":
        assert output_folder_path is not None
        db_path = output_folder_path / PREPARED_DUCKDB_FILENAME
        _write_prepared_duckdb_database(
            con=con,
            db_path=db_path,
            addresses=df_clean,
            term_frequencies=tf_table,
            inverted_index=inverted_index,
        )
        artefact_paths = [db_path]
        artefact_columns = {PREPARED_DUCKDB_FILENAME: []}
    else:
        tf_path = (
            join_remote_path(output_folder_uri, PREPARED_TERM_FREQUENCIES_FILENAME)
            if output_is_remote
            else output_folder_path / PREPARED_TERM_FREQUENCIES_FILENAME
        )
        idx_path = (
            join_remote_path(output_folder_uri, PREPARED_INVERTED_INDEX_FILENAME)
            if output_is_remote
            else output_folder_path / PREPARED_INVERTED_INDEX_FILENAME
        )

        _write_parquet_artefact(con, tf_table, tf_path)
        _write_parquet_artefact(con, inverted_index, idx_path)

        if output_chunk_count == 1:
            addr_path = (
                join_remote_path(output_folder_uri, PREPARED_ADDRESSES_FILENAME)
                if output_is_remote
                else output_folder_path / PREPARED_ADDRESSES_FILENAME
            )
            _write_parquet_artefact(
                con,
                df_clean,
                addr_path,
                sort_columns=CANONICAL_SORT_COLUMNS,
                drop_columns=RECOMPUTABLE_DROP_COLUMNS,
            )
            canonical_paths = [addr_path]
        else:
            chunk_dir = (
                join_remote_path(output_folder_uri, PREPARED_ADDRESSES_CHUNK_DIRNAME)
                if output_is_remote
                else output_folder_path / PREPARED_ADDRESSES_CHUNK_DIRNAME
            )
            chunk_output_location = chunk_dir
            if not output_is_remote:
                Path(chunk_dir).mkdir(parents=True, exist_ok=True)

            uid = uuid4().hex
            input_table = f"__ukam_prepare_canonical_clean_{uid}"
            hash_key = _resolve_chunk_hash_key(df_clean.columns)
            con.execute(f"DROP VIEW IF EXISTS {input_table}")
            con.execute(f"DROP TABLE IF EXISTS {input_table}")
            df_clean.create(input_table)

            try:
                for chunk_index in range(output_chunk_count):
                    started_at = time.perf_counter()
                    chunk_query = con.sql(f"""
                        SELECT *
                        FROM {input_table}
                        WHERE
                            (abs(hash({hash_key})) % {output_chunk_count})
                            = {chunk_index}
                    """)
                    chunk_path = (
                        join_remote_path(
                            str(chunk_dir),
                            _chunk_file_name(chunk_index, output_chunk_count),
                        )
                        if output_is_remote
                        else Path(chunk_dir)
                        / _chunk_file_name(chunk_index, output_chunk_count)
                    )
                    _write_parquet_artefact(
                        con,
                        chunk_query,
                        chunk_path,
                        sort_columns=CANONICAL_SORT_COLUMNS,
                        drop_columns=RECOMPUTABLE_DROP_COLUMNS,
                    )
                    chunk_count = chunk_query.count("*").fetchone()[0]
                    canonical_paths.append(chunk_path)
                    logger.debug(
                        "Wrote canonical output chunk %d/%d to '%s' (%d rows) - took %s",
                        chunk_index + 1,
                        output_chunk_count,
                        chunk_path,
                        chunk_count,
                        _format_elapsed(time.perf_counter() - started_at),
                    )
            finally:
                con.execute(f"DROP TABLE IF EXISTS {input_table}")

        artefact_paths = [*canonical_paths, tf_path, idx_path]
        artefact_columns = {
            PREPARED_TERM_FREQUENCIES_FILENAME: tf_table.columns,
            PREPARED_INVERTED_INDEX_FILENAME: inverted_index.columns,
        }
        for canonical_path in canonical_paths:
            relative_name = (
                relative_remote_path(output_folder_uri, str(canonical_path))
                if output_is_remote
                else str(Path(canonical_path).relative_to(output_folder_path))
            )
            artefact_columns[relative_name] = [
                c for c in df_clean.columns if c not in RECOMPUTABLE_DROP_COLUMNS
            ]

    # Compute row counts once (avoids repeated full scans)
    addr_count = df_clean.count("*").fetchone()[0]
    tf_count = tf_table.count("*").fetchone()[0]
    idx_count = inverted_index.count("*").fetchone()[0]

    logger.debug(
        "Wrote artefacts to '%s': %d addresses, %d term frequencies, %d index rows",
        output_folder,
        addr_count,
        tf_count,
        idx_count,
    )

    if output_chunk_count > 1:
        logger.debug(
            "Wrote %d canonical output chunks to '%s'",
            output_chunk_count,
            chunk_output_location,
        )

    manifest_row_counts = {
        "canonical_addresses": addr_count,
        "term_frequencies": tf_count,
        "inverted_index": idx_count,
        "canonical_output_chunks": output_chunk_count,
    }

    if output_is_remote:
        _write_manifest_remote(
            output_folder_uri,
            con=con,
            artefact_paths=[str(path) for path in artefact_paths],
            artefact_columns=artefact_columns,
            row_counts=manifest_row_counts,
        )
    else:
        _write_manifest_local(
            output_folder_path,
            con=con,
            artefact_paths=[Path(path) for path in artefact_paths],
            artefact_columns=artefact_columns,
            row_counts=manifest_row_counts,
            storage_format=output_format,
        )

    logger.info("Prepared canonical artefacts written to '%s'", output_folder)


def prepare_canonical_folder_for_realtime(
    data: PrepareCanonicalInput,
    output_folder: str | Path,
    *,
    con: duckdb.DuckDBPyConnection,
    num_of_chunks: int = 10,
    output_chunk_count: int = 1,
    overwrite: bool = False,
    show_progress: bool = True,
) -> None:
    """Prepare canonical data with additional realtime blocker artefacts.

    This keeps the batch preparation path slim. Realtime users opt into the
    extra DuckDB tables that back the inverted-index blocker used by
    ``RealTimeAddressMatcher``.
    """
    if is_remote_folder_reference(output_folder):
        raise ValueError(
            "prepare_canonical_folder_for_realtime() only supports local output "
            "folders because the realtime blocker is stored in a DuckDB "
            "database file."
        )

    prepare_canonical_folder(
        data,
        output_folder,
        con=con,
        num_of_chunks=num_of_chunks,
        output_chunk_count=output_chunk_count,
        output_format="duckdb",
        overwrite=overwrite,
        show_progress=show_progress,
    )

    output_folder_path = Path(output_folder)
    _write_realtime_blocker_table(output_folder_path)
    _mark_manifest_realtime(output_folder_path)

    logger.info("Realtime canonical artefacts written to '%s'", output_folder)


def _write_prepared_duckdb_database(
    *,
    con: duckdb.DuckDBPyConnection,
    db_path: Path,
    addresses: duckdb.DuckDBPyRelation,
    term_frequencies: duckdb.DuckDBPyRelation,
    inverted_index: duckdb.DuckDBPyRelation,
) -> None:
    if db_path.exists():
        db_path.unlink()

    alias = f"ukam_prepared_{uuid4().hex}"
    escaped_path = _escape_sql_string(str(db_path))
    con.execute(f"ATTACH '{escaped_path}' AS \"{alias}\"")
    try:
        drop_clause = (
            " EXCLUDE (exploding_unique_ids)"
            if "exploding_unique_ids" in addresses.columns
            else ""
        )
        con.execute(f"""
            CREATE TABLE "{alias}".{REALTIME_CANONICAL_TABLE} AS
            SELECT *{drop_clause}
            FROM ({addresses.sql_query()}) AS canonical
            ORDER BY postcode, clean_full_address
        """)
        con.execute(f"""
            CREATE TABLE "{alias}".{REALTIME_TERM_FREQUENCIES_TABLE} AS
            SELECT * FROM ({term_frequencies.sql_query()})
        """)
        con.execute(f"""
            CREATE TABLE "{alias}".{REALTIME_INVERTED_INDEX_TABLE} AS
            SELECT * FROM ({inverted_index.sql_query()})
        """)
        con.execute(f'CHECKPOINT "{alias}"')
    finally:
        con.execute(f'DETACH "{alias}"')


def _write_realtime_blocker_table(folder: Path) -> None:
    import duckdb as _duckdb

    db_path = folder / PREPARED_REALTIME_DUCKDB_FILENAME
    if not db_path.exists():
        raise FileNotFoundError(
            f"Prepared canonical DuckDB file '{PREPARED_DUCKDB_FILENAME}' is "
            f"missing from '{folder}'. Re-run prepare_canonical_folder_for_realtime()."
        )

    art_con = _duckdb.connect(str(db_path))
    try:
        art_con.execute(f"DROP TABLE IF EXISTS {REALTIME_INVERTED_INDEX_HASHED_TABLE}")
        art_con.execute(
            f"CREATE TABLE {REALTIME_INVERTED_INDEX_HASHED_TABLE} AS "
            f"SELECT hash(key) AS key_hash, unnest(unique_ids) AS unique_id "
            f"FROM {REALTIME_INVERTED_INDEX_TABLE}"
        )
    finally:
        art_con.close()


def _mark_manifest_realtime(folder: Path) -> None:
    import duckdb as _duckdb

    manifest_path = folder / MANIFEST_FILENAME
    if not manifest_path.exists():
        raise FileNotFoundError(
            f"Prepared canonical folder '{folder}' is missing {MANIFEST_FILENAME}."
        )

    manifest = json.loads(manifest_path.read_text())
    db_path = folder / PREPARED_REALTIME_DUCKDB_FILENAME
    if not db_path.exists():
        raise FileNotFoundError(
            f"Realtime prepared database not found at '{db_path}'."
        )

    db_con = _duckdb.connect(str(db_path), read_only=True)
    try:
        realtime_row_counts = {
            "realtime_canonical_addresses": db_con.execute(
                f"SELECT COUNT(*) FROM {REALTIME_CANONICAL_TABLE}"
            ).fetchone()[0],
            "realtime_term_frequencies": db_con.execute(
                f"SELECT COUNT(*) FROM {REALTIME_TERM_FREQUENCIES_TABLE}"
            ).fetchone()[0],
            "realtime_inverted_index": db_con.execute(
                f"SELECT COUNT(*) FROM {REALTIME_INVERTED_INDEX_TABLE}"
            ).fetchone()[0],
            "realtime_inverted_index_hashed": db_con.execute(
                f"SELECT COUNT(*) FROM {REALTIME_INVERTED_INDEX_HASHED_TABLE}"
            ).fetchone()[0],
        }
    finally:
        db_con.close()

    manifest["realtime"] = True
    manifest["row_counts"] = {
        **manifest.get("row_counts", {}),
        **realtime_row_counts,
    }
    files_meta = manifest.setdefault("files", {})
    stat = db_path.stat()
    files_meta[PREPARED_REALTIME_DUCKDB_FILENAME] = {
        "size_bytes": stat.st_size,
        "sha256": _sha256_file(db_path),
        "columns": [],
    }

    tmp = folder / f"{MANIFEST_FILENAME}.tmp"
    tmp.write_text(json.dumps(manifest, indent=2))
    tmp.replace(manifest_path)


def _write_manifest_local(
    folder: Path,
    *,
    con: duckdb.DuckDBPyConnection,
    artefact_paths: list[Path],
    artefact_columns: dict[str, list[str]],
    row_counts: dict[str, int],
    storage_format: str = "parquet",
) -> None:
    """Write a JSON manifest recording provenance information.

    The manifest is written atomically via a temp file to guard against
    truncation if the process is interrupted.
    """
    import duckdb as _duckdb

    files_meta = {}
    for p in artefact_paths:
        name = str(p.relative_to(folder))
        stat = p.stat()
        files_meta[name] = {
            "size_bytes": stat.st_size,
            "sha256": _sha256_file(p),
            "columns": artefact_columns.get(name, []),
        }

    manifest = _build_manifest(
        created_with_duckdb_version=_duckdb.__version__,
        files_meta=files_meta,
        row_counts=row_counts,
        realtime=False,
        storage_format=storage_format,
    )

    # Atomic write: write to a temp file then replace
    tmp = folder / f"{MANIFEST_FILENAME}.tmp"
    tmp.write_text(json.dumps(manifest, indent=2))
    tmp.replace(folder / MANIFEST_FILENAME)

    logger.debug("Manifest written to '%s'", folder / MANIFEST_FILENAME)


def _write_manifest_remote(
    folder_uri: str,
    *,
    con: duckdb.DuckDBPyConnection,
    artefact_paths: list[str],
    artefact_columns: dict[str, list[str]],
    row_counts: dict[str, int],
) -> None:
    """Write a JSON manifest to a remote folder via DuckDB COPY."""
    import duckdb as _duckdb

    files_meta = {}
    for path in artefact_paths:
        name = relative_remote_path(folder_uri, path)
        files_meta[name] = {
            "size_bytes": None,
            "sha256": None,
            "columns": artefact_columns.get(name, []),
        }

    manifest = _build_manifest(
        created_with_duckdb_version=_duckdb.__version__,
        files_meta=files_meta,
        row_counts=row_counts,
        realtime=False,
    )

    manifest_table = f"__ukam_manifest_{uuid4().hex}"
    manifest_uri = join_remote_path(folder_uri, MANIFEST_FILENAME)
    escaped_manifest_uri = _escape_sql_string(manifest_uri)
    con.execute(
        (
            f"CREATE TEMP TABLE {manifest_table} AS "
            "SELECT ? AS ukam_version, ? AS created_at, "
            "? AS created_with_duckdb_version, ? AS realtime, "
            "?::JSON AS row_counts, ?::JSON AS files"
        ),
        [
            manifest["ukam_version"],
            manifest["created_at"],
            manifest["created_with_duckdb_version"],
            manifest["realtime"],
            json.dumps(manifest["row_counts"]),
            json.dumps(manifest["files"]),
        ],
    )
    try:
        con.execute(
            f"COPY {manifest_table} TO '{escaped_manifest_uri}' "
            "(FORMAT JSON, ARRAY false)"
        )
    finally:
        con.execute(f"DROP TABLE IF EXISTS {manifest_table}")

    logger.debug("Manifest written to '%s'", manifest_uri)


def _check_manifest(folder: Path) -> None:
    """Check manifest for version mismatches and file integrity.

    Warns if:
    - The manifest file is missing entirely.
    - The recorded `ukam_version` differs from the running package.
    - Any artefact file size does not match the manifest.
    """
    manifest_path = folder / MANIFEST_FILENAME
    if not manifest_path.exists():
        warnings.warn(
            f"No manifest file found in '{folder}'. The prepared data may "
            f"be incomplete or was created by an older version of "
            f"uk_address_matcher. Consider re-running "
            f"prepare_canonical_folder() to regenerate.",
            stacklevel=3,
        )
        return

    from uk_address_matcher import __version__

    try:
        manifest = json.loads(manifest_path.read_text())
    except (json.JSONDecodeError, OSError):
        warnings.warn(
            f"Could not read manifest in '{folder}'. "
            f"Consider re-running prepare_canonical_folder().",
            stacklevel=3,
        )
        return

    prepared_version = manifest.get("ukam_version")
    if prepared_version and prepared_version != __version__:
        warnings.warn(
            f"Prepared canonical data was created with uk_address_matcher "
            f"v{prepared_version}, but you are running v{__version__}. "
            f"Consider re-running prepare_canonical_folder() "
            f"if you encounter unexpected results.",
            stacklevel=3,
        )

    # Check file sizes match recorded values
    files_meta = manifest.get("files", {})
    for name, meta in files_meta.items():
        p = folder / name
        if p.exists() and "size_bytes" in meta:
            actual_size = p.stat().st_size
            if actual_size != meta["size_bytes"]:
                warnings.warn(
                    f"Artefact '{name}' has size {actual_size} bytes but the "
                    f"manifest records {meta['size_bytes']} bytes. The file "
                    f"may have been modified. Consider re-running "
                    f"prepare_canonical_folder().",
                    stacklevel=3,
                )


def _manifest_storage_format(folder: Path) -> str:
    manifest_path = folder / MANIFEST_FILENAME
    if not manifest_path.exists():
        return "parquet"
    try:
        manifest = json.loads(manifest_path.read_text())
    except (json.JSONDecodeError, OSError):
        return "parquet"
    return str(manifest.get("storage_format", "parquet"))


def _validate_prepared_duckdb_folder(folder: str | Path) -> Path:
    import duckdb as _duckdb

    folder = Path(folder)
    if not folder.is_dir():
        raise FileNotFoundError(
            f"Canonical data folder not found: '{folder}'. Ensure the path points "
            "to a folder created by prepare_canonical_folder()."
        )

    db_path = folder / PREPARED_DUCKDB_FILENAME
    if not db_path.exists():
        raise FileNotFoundError(
            f"Prepared canonical DuckDB file '{PREPARED_DUCKDB_FILENAME}' is "
            f"missing from '{folder}'. Re-run prepare_canonical_folder()."
        )

    validation_con = _duckdb.connect(str(db_path), read_only=True)
    try:
        tables = {
            row[0]
            for row in validation_con.execute(
                "SELECT table_name FROM duckdb_tables()"
            ).fetchall()
        }
    finally:
        validation_con.close()

    required_tables = {
        REALTIME_CANONICAL_TABLE,
        REALTIME_TERM_FREQUENCIES_TABLE,
        REALTIME_INVERTED_INDEX_TABLE,
    }
    missing_tables = sorted(required_tables - tables)
    if missing_tables:
        raise FileNotFoundError(
            f"Prepared canonical DuckDB file '{db_path}' is missing tables: "
            f"{missing_tables}. Re-run prepare_canonical_folder()."
        )
    return db_path


def _validate_prepared_folder(
    folder: str | Path,
    con: duckdb.DuckDBPyConnection,
) -> _PreparedFolderLayout:
    """Validate that a folder contains readable prepared Parquet files."""
    folder = Path(folder)

    if not folder.is_dir():
        raise FileNotFoundError(
            f"Canonical data folder not found: '{folder}'. "
            "Ensure the path points to a folder created by "
            "prepare_canonical_folder()."
        )

    missing = [f for f in REQUIRED_FILES if not (folder / f).exists()]
    if missing:
        raise FileNotFoundError(
            f"Prepared canonical folder '{folder}' is missing required files: "
            f"{missing}. Expected: ukam_canonical_addresses.parquet, "
            "ukam_term_frequencies.parquet, ukam_inverted_index.parquet. "
            "Re-run prepare_canonical_folder() to regenerate."
        )

    canonical_paths = _resolve_canonical_parquet_paths(folder)
    if not canonical_paths:
        raise FileNotFoundError(
            f"Prepared canonical folder '{folder}' is missing canonical addresses "
            "artefacts. Expected either ukam_canonical_addresses.parquet or "
            "parquet chunks in ukam_canonical_addresses_chunks/. Re-run "
            "prepare_canonical_folder() to regenerate."
        )

    # Verify each file is a readable Parquet file
    required_paths = [folder / f for f in REQUIRED_FILES]
    for path in [*canonical_paths, *required_paths]:
        relative_name = str(path.relative_to(folder))
        try:
            con.read_parquet(str(path)).limit(1).fetchone()
        except Exception as exc:
            raise FileNotFoundError(
                f"Artefact '{relative_name}' in '{folder}' exists but is not a valid "
                f"Parquet file: {exc}. Re-run prepare_canonical_folder() "
                f"to regenerate."
            ) from exc

    return _PreparedFolderLayout(folder=folder, canonical_paths=canonical_paths)


def validate_realtime_prepared_folder(
    folder: str | Path,
    con: duckdb.DuckDBPyConnection,
) -> Path:
    """Validate that a prepared folder contains realtime blocker artefacts."""
    import duckdb as _duckdb

    if is_remote_folder_reference(folder):
        raise ValueError(
            "RealTimeAddressMatcher requires a local folder created by "
            "prepare_canonical_folder_for_realtime()."
        )

    folder = Path(folder)
    if _manifest_storage_format(folder) == "duckdb":
        _validate_prepared_duckdb_folder(folder)
        layout_folder = folder
    else:
        layout_folder = _validate_prepared_folder(folder, con=con).folder
    manifest_path = layout_folder / MANIFEST_FILENAME
    if not manifest_path.exists():
        raise FileNotFoundError(
            f"Realtime canonical folder '{layout_folder}' is missing "
            f"{MANIFEST_FILENAME}. Re-run prepare_canonical_folder_for_realtime()."
        )

    try:
        manifest = json.loads(manifest_path.read_text())
    except (json.JSONDecodeError, OSError) as exc:
        raise ValueError(
            f"Could not read realtime manifest in '{layout_folder}'. "
            "Re-run prepare_canonical_folder_for_realtime()."
        ) from exc

    if manifest.get("realtime") is not True:
        raise ValueError(
            f"Prepared canonical folder '{layout_folder}' was not prepared for "
            "realtime matching. Re-run prepare_canonical_folder_for_realtime()."
        )

    db_path = layout_folder / PREPARED_REALTIME_DUCKDB_FILENAME
    if not db_path.exists():
        raise FileNotFoundError(
            f"Realtime prepared database '{PREPARED_REALTIME_DUCKDB_FILENAME}' is "
            f"missing from '{layout_folder}'. Re-run "
            "prepare_canonical_folder_for_realtime()."
        )

    validation_con = _duckdb.connect(str(db_path), read_only=True)
    try:
        tables = {
            row[0]
            for row in validation_con.execute(
                "SELECT table_name FROM duckdb_tables()"
            ).fetchall()
        }
        required_tables = {
            REALTIME_CANONICAL_TABLE,
            REALTIME_TERM_FREQUENCIES_TABLE,
            REALTIME_INVERTED_INDEX_TABLE,
            REALTIME_INVERTED_INDEX_HASHED_TABLE,
        }
        missing_tables = sorted(required_tables - tables)
        if missing_tables:
            raise FileNotFoundError(
                f"Realtime prepared database '{db_path}' is missing tables: "
                f"{missing_tables}. Re-run prepare_canonical_folder_for_realtime()."
            )
    finally:
        validation_con.close()

    return db_path


def load_prepared_canonical_data(
    folder: str | Path,
    con: duckdb.DuckDBPyConnection,
    canonical_address_filter: str | None = None,
) -> _PreparedCanonical:
    """Load prepared canonical artefacts from a folder.

    Args:
        folder: Path to the prepared canonical data folder.
        con: DuckDB connection.
        canonical_address_filter: Optional DuckDB SQL filter expression
            applied to the loaded canonical addresses relation.

    Returns:
        A `_PreparedCanonical` containing `addresses`, `term_frequencies`,
        and `inverted_index` relations.
    """
    if is_remote_folder_reference(folder):
        return _load_prepared_canonical_data_remote(
            folder,
            con=con,
            canonical_address_filter=canonical_address_filter,
        )

    storage_format = _manifest_storage_format(Path(folder))
    if storage_format == "duckdb":
        return _load_prepared_canonical_data_duckdb(
            folder,
            con=con,
            canonical_address_filter=canonical_address_filter,
        )
    if storage_format != "parquet":
        raise ValueError(
            f"Unsupported prepared canonical storage_format '{storage_format}'."
        )

    layout = _validate_prepared_folder(folder, con=con)
    _check_manifest(layout.folder)

    addresses = con.read_parquet([str(path) for path in layout.canonical_paths])
    term_frequencies = con.read_parquet(
        str(layout.folder / PREPARED_TERM_FREQUENCIES_FILENAME)
    )
    inverted_index = con.read_parquet(
        str(layout.folder / PREPARED_INVERTED_INDEX_FILENAME)
    )

    addresses = _rehydrate_canonical_addresses(addresses)

    if canonical_address_filter is not None:
        addresses = addresses.filter(canonical_address_filter)

    logger.debug("Loaded prepared canonical data from '%s'", layout.folder)

    return _PreparedCanonical(
        addresses=addresses,
        term_frequencies=term_frequencies,
        inverted_index=inverted_index,
    )


def _load_prepared_canonical_data_duckdb(
    folder: str | Path,
    *,
    con: duckdb.DuckDBPyConnection,
    canonical_address_filter: str | None = None,
) -> _PreparedCanonical:
    folder = Path(folder)
    db_path = _validate_prepared_duckdb_folder(folder)
    _check_manifest(folder)

    alias = f"ukam_prepared_{uuid4().hex}"
    escaped_path = _escape_sql_string(str(db_path))
    con.execute(f"ATTACH '{escaped_path}' AS \"{alias}\" (READ_ONLY)")

    addresses = _rehydrate_canonical_addresses(
        con.sql(f'SELECT * FROM "{alias}".{REALTIME_CANONICAL_TABLE}')
    )
    if canonical_address_filter is not None:
        addresses = addresses.filter(canonical_address_filter)

    logger.debug("Loaded prepared canonical DuckDB data from '%s'", db_path)
    return _PreparedCanonical(
        addresses=addresses,
        term_frequencies=con.sql(
            f'SELECT * FROM "{alias}".{REALTIME_TERM_FREQUENCIES_TABLE}'
        ),
        inverted_index=con.sql(
            f'SELECT * FROM "{alias}".{REALTIME_INVERTED_INDEX_TABLE}'
        ),
    )
