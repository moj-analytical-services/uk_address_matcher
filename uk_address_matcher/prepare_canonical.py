from __future__ import annotations

import hashlib
import json
import logging
import re
import shutil
import time
import warnings
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

from uk_address_matcher.sql_pipeline.helpers import _register_input_relation_once

if TYPE_CHECKING:
    import duckdb

logger = logging.getLogger("uk_address_matcher")

PREPARED_ADDRESSES_FILENAME = "ukam_canonical_addresses.parquet"
PREPARED_ADDRESSES_CHUNK_DIRNAME = "ukam_canonical_addresses_chunks"
PREPARED_TERM_FREQUENCIES_FILENAME = "ukam_term_frequencies.parquet"
PREPARED_INVERTED_INDEX_FILENAME = "ukam_inverted_index.parquet"
MANIFEST_FILENAME = "ukam_manifest.json"
CHUNK_FILE_INDEX_DIGITS = 5
MAX_CHUNK_COUNT = (10**CHUNK_FILE_INDEX_DIGITS) - 1

REQUIRED_FILES = [
    PREPARED_TERM_FREQUENCIES_FILENAME,
    PREPARED_INVERTED_INDEX_FILENAME,
]

# All files managed by the preparation step (parquets + manifest + temps)
_MANAGED_FILES = [
    PREPARED_ADDRESSES_FILENAME,
    PREPARED_ADDRESSES_CHUNK_DIRNAME,
    *REQUIRED_FILES,
    MANIFEST_FILENAME,
    f"{MANIFEST_FILENAME}.tmp",
]


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


_URI_SCHEME_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9+.-]*://")


def _is_remote_folder_reference(folder: str | Path) -> bool:
    """Return True if the provided folder points to a remote URI."""
    return isinstance(folder, str) and bool(_URI_SCHEME_RE.match(folder))


def _join_remote_path(base: str, name: str) -> str:
    """Join a remote base URI and relative artefact name."""
    return f"{base.rstrip('/')}/{name}"


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


def _load_prepared_canonical_data_remote(
    folder_uri: str,
    con: duckdb.DuckDBPyConnection,
    canonical_address_filter: str | None,
) -> _PreparedCanonical:
    """Load prepared canonical artefacts from a remote folder URI via DuckDB."""
    # A previous failed read on the same connection can leave DuckDB in an aborted
    # transaction state. Clear it before attempting fallback reads.
    _rollback_if_needed(con)

    tf_uri = _join_remote_path(folder_uri, PREPARED_TERM_FREQUENCIES_FILENAME)
    idx_uri = _join_remote_path(folder_uri, PREPARED_INVERTED_INDEX_FILENAME)
    single_canonical_uri = _join_remote_path(folder_uri, PREPARED_ADDRESSES_FILENAME)
    chunk_glob_uri = _join_remote_path(
        folder_uri,
        f"{PREPARED_ADDRESSES_CHUNK_DIRNAME}/*.parquet",
    )
    single_dataset_glob_uri = _join_remote_path(
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


def prepare_canonical_folder(
    data: duckdb.DuckDBPyRelation,
    output_folder: str | Path,
    *,
    con: duckdb.DuckDBPyConnection,
    num_of_chunks: int = 10,
    output_chunk_count: int = 1,
    overwrite: bool = False,
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
        data: Raw canonical address data as a DuckDB relation.
        output_folder: Folder to write prepared artefacts to.
        con: DuckDB connection.
        num_of_chunks: Number of chunks to split the data into for cleaning
            and term frequency derivation. Set to 1 for no chunking.
        output_chunk_count: Number of output chunks to write for canonical
            addresses. Set to 1 to write `ukam_canonical_addresses.parquet`.
            Set above 1 to write hash-partitioned chunks under
            `ukam_canonical_addresses_chunks/`.
        overwrite: Whether to overwrite existing files in the folder. When
            `True`, all known artefacts are removed before writing to ensure
            the folder ends up in a consistent state.

    Raises:
        FileExistsError: If the output folder already contains prepared files
            and `overwrite` is `False`.
    """
    from uk_address_matcher.cleaning.chunking_strategies import (
        derive_inverted_index,
        derive_term_frequencies_table,
        prepare_data_for_matching,
    )

    output_folder = Path(output_folder)
    data = _register_input_relation_once(data, con=con, role="prepare_canonical")

    _validate_chunk_count(num_of_chunks, name="num_of_chunks")
    _validate_chunk_count(output_chunk_count, name="output_chunk_count")

    if output_folder.exists() and not overwrite:
        existing = [f for f in REQUIRED_FILES if (output_folder / f).exists()]
        if _resolve_canonical_parquet_paths(output_folder):
            existing.append("canonical_addresses")
        if existing:
            raise FileExistsError(
                f"Output folder '{output_folder}' already contains prepared files: "
                f"{existing}. Set overwrite=True to replace them."
            )

    output_folder.mkdir(parents=True, exist_ok=True)

    if overwrite:
        _clear_stale_artefacts(output_folder)

    # Derive artefacts / cleaned canonical data for export
    logger.debug("Deriving term frequencies from canonical data")
    tf_table = derive_term_frequencies_table(data, con=con, num_of_chunks=num_of_chunks)

    logger.debug("Cleaning canonical addresses")
    df_clean = prepare_data_for_matching(
        data,
        con=con,
        num_of_chunks=num_of_chunks,
        term_frequency_lookup=tf_table,
    )

    logger.debug("Building inverted index")
    inverted_index = derive_inverted_index(df_clean, con=con, num_of_chunks=num_of_chunks)

    # Write parquet files
    tf_path = output_folder / PREPARED_TERM_FREQUENCIES_FILENAME
    idx_path = output_folder / PREPARED_INVERTED_INDEX_FILENAME

    tf_table.write_parquet(str(tf_path))
    inverted_index.write_parquet(str(idx_path))

    canonical_paths: list[Path]
    if output_chunk_count == 1:
        addr_path = output_folder / PREPARED_ADDRESSES_FILENAME
        df_clean.write_parquet(str(addr_path))
        canonical_paths = [addr_path]
    else:
        chunk_dir = output_folder / PREPARED_ADDRESSES_CHUNK_DIRNAME
        chunk_dir.mkdir(parents=True, exist_ok=True)

        uid = uuid4().hex
        input_table = f"__ukam_prepare_canonical_clean_{uid}"
        hash_key = _resolve_chunk_hash_key(df_clean.columns)
        con.execute(f"DROP VIEW IF EXISTS {input_table}")
        con.execute(f"DROP TABLE IF EXISTS {input_table}")
        df_clean.create(input_table)

        canonical_paths = []
        for chunk_index in range(output_chunk_count):
            started_at = time.perf_counter()
            chunk_query = con.sql(f"""
                SELECT *
                FROM {input_table}
                WHERE (abs(hash({hash_key})) % {output_chunk_count}) = {chunk_index}
            """)
            chunk_path = chunk_dir / _chunk_file_name(chunk_index, output_chunk_count)
            chunk_query.write_parquet(str(chunk_path))
            chunk_count = chunk_query.count("*").fetchone()[0]
            canonical_paths.append(chunk_path)
            logger.info(
                "Wrote canonical output chunk %d/%d to '%s' (%d rows) - took %s",
                chunk_index + 1,
                output_chunk_count,
                chunk_path,
                chunk_count,
                _format_elapsed(time.perf_counter() - started_at),
            )

        con.execute(f"DROP TABLE IF EXISTS {input_table}")

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
        logger.info(
            "Wrote %d canonical output chunks to '%s'",
            output_chunk_count,
            output_folder / PREPARED_ADDRESSES_CHUNK_DIRNAME,
        )

    artefact_columns: dict[str, list[str]] = {
        PREPARED_TERM_FREQUENCIES_FILENAME: tf_table.columns,
        PREPARED_INVERTED_INDEX_FILENAME: inverted_index.columns,
    }
    for canonical_path in canonical_paths:
        relative_name = str(canonical_path.relative_to(output_folder))
        artefact_columns[relative_name] = df_clean.columns

    _write_manifest(
        output_folder,
        con=con,
        artefact_paths=[*canonical_paths, tf_path, idx_path],
        artefact_columns=artefact_columns,
        row_counts={
            "canonical_addresses": addr_count,
            "term_frequencies": tf_count,
            "inverted_index": idx_count,
            "canonical_output_chunks": output_chunk_count,
        },
    )


def _write_manifest(
    folder: Path,
    *,
    con: duckdb.DuckDBPyConnection,
    artefact_paths: list[Path],
    artefact_columns: dict[str, list[str]],
    row_counts: dict[str, int],
) -> None:
    """Write a JSON manifest recording provenance information.

    The manifest is written atomically via a temp file to guard against
    truncation if the process is interrupted.
    """
    import duckdb as _duckdb

    from uk_address_matcher import __version__

    files_meta = {}
    for p in artefact_paths:
        name = str(p.relative_to(folder))
        stat = p.stat()
        files_meta[name] = {
            "size_bytes": stat.st_size,
            "sha256": _sha256_file(p),
            "columns": artefact_columns.get(name, []),
        }

    manifest = {
        "ukam_version": __version__,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "created_with_duckdb_version": _duckdb.__version__,
        "row_counts": row_counts,
        "files": files_meta,
    }

    # Atomic write: write to a temp file then replace
    tmp = folder / f"{MANIFEST_FILENAME}.tmp"
    tmp.write_text(json.dumps(manifest, indent=2))
    tmp.replace(folder / MANIFEST_FILENAME)

    logger.debug("Manifest written to '%s'", folder / MANIFEST_FILENAME)


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
    if _is_remote_folder_reference(folder):
        return _load_prepared_canonical_data_remote(
            folder,
            con=con,
            canonical_address_filter=canonical_address_filter,
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

    if canonical_address_filter is not None:
        addresses = addresses.filter(canonical_address_filter)

    logger.debug("Loaded prepared canonical data from '%s'", layout.folder)

    return _PreparedCanonical(
        addresses=addresses,
        term_frequencies=term_frequencies,
        inverted_index=inverted_index,
    )
