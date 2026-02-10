from __future__ import annotations

import hashlib
import json
import logging
import warnings
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb

logger = logging.getLogger("uk_address_matcher")

PREPARED_ADDRESSES_FILENAME = "ukam_canonical_addresses.parquet"
PREPARED_TERM_FREQUENCIES_FILENAME = "ukam_term_frequencies.parquet"
PREPARED_INVERTED_INDEX_FILENAME = "ukam_inverted_index.parquet"
MANIFEST_FILENAME = "ukam_manifest.json"

REQUIRED_FILES = [
    PREPARED_ADDRESSES_FILENAME,
    PREPARED_TERM_FREQUENCIES_FILENAME,
    PREPARED_INVERTED_INDEX_FILENAME,
]

# All files managed by the preparation step (parquets + manifest + temps)
_MANAGED_FILES = REQUIRED_FILES + [MANIFEST_FILENAME, f"{MANIFEST_FILENAME}.tmp"]


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
            p.unlink()


def prepare_canonical_folder(
    data: duckdb.DuckDBPyRelation,
    output_folder: str | Path,
    *,
    con: duckdb.DuckDBPyConnection,
    num_of_chunks: int = 10,
    overwrite: bool = False,
) -> None:
    """Prepare canonical data and persist to a folder for later use.

    Performs address cleaning and tokenisation, term frequency computation,
    and inverted index generation. Writes three Parquet files and a manifest
    to `output_folder`:

    - `ukam_canonical_addresses.parquet` — cleaned and tokenised addresses
    - `ukam_term_frequencies.parquet` — term frequency lookup table
    - `ukam_inverted_index.parquet` — inverted index for candidate retrieval
    - `ukam_manifest.json` — provenance metadata (version, row counts, hashes)

    Args:
        data: Raw canonical address data as a DuckDB relation.
        output_folder: Folder to write prepared artefacts to.
        con: DuckDB connection.
        num_of_chunks: Number of chunks to split the data into for cleaning
            and term frequency derivation. Set to 1 for no chunking.
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

    if output_folder.exists() and not overwrite:
        existing = [f for f in REQUIRED_FILES if (output_folder / f).exists()]
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
    inverted_index = derive_inverted_index(df_clean, con=con)

    # Write parquet files
    addr_path = output_folder / PREPARED_ADDRESSES_FILENAME
    tf_path = output_folder / PREPARED_TERM_FREQUENCIES_FILENAME
    idx_path = output_folder / PREPARED_INVERTED_INDEX_FILENAME

    df_clean.write_parquet(str(addr_path))
    tf_table.write_parquet(str(tf_path))
    inverted_index.write_parquet(str(idx_path))

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

    _write_manifest(
        output_folder,
        con=con,
        artefact_columns={
            PREPARED_ADDRESSES_FILENAME: df_clean.columns,
            PREPARED_TERM_FREQUENCIES_FILENAME: tf_table.columns,
            PREPARED_INVERTED_INDEX_FILENAME: inverted_index.columns,
        },
        row_counts={
            "canonical_addresses": addr_count,
            "term_frequencies": tf_count,
            "inverted_index": idx_count,
        },
    )


def _write_manifest(
    folder: Path,
    *,
    con: duckdb.DuckDBPyConnection,
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
    for name in REQUIRED_FILES:
        p = folder / name
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
) -> Path:
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

    # Verify each file is a readable Parquet file
    for name in REQUIRED_FILES:
        path = folder / name
        try:
            con.read_parquet(str(path)).limit(1).fetchone()
        except Exception as exc:
            raise FileNotFoundError(
                f"Artefact '{name}' in '{folder}' exists but is not a valid "
                f"Parquet file: {exc}. Re-run prepare_canonical_folder() "
                f"to regenerate."
            ) from exc

    return folder


def load_prepared_canonical_data(
    folder: str | Path,
    con: duckdb.DuckDBPyConnection,
) -> _PreparedCanonical:
    """Load prepared canonical artefacts from a folder.

    Args:
        folder: Path to the prepared canonical data folder.
        con: DuckDB connection.

    Returns:
        A `_PreparedCanonical` containing `addresses`, `term_frequencies`,
        and `inverted_index` relations.
    """
    folder = _validate_prepared_folder(folder, con=con)
    _check_manifest(folder)

    addresses = con.read_parquet(str(folder / PREPARED_ADDRESSES_FILENAME))
    term_frequencies = con.read_parquet(
        str(folder / PREPARED_TERM_FREQUENCIES_FILENAME)
    )
    inverted_index = con.read_parquet(str(folder / PREPARED_INVERTED_INDEX_FILENAME))

    logger.debug("Loaded prepared canonical data from '%s'", folder)

    return _PreparedCanonical(
        addresses=addresses,
        term_frequencies=term_frequencies,
        inverted_index=inverted_index,
    )
