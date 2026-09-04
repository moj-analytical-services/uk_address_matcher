from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

from uk_address_matcher.prepare_canonical import (
    PREPARED_ADDRESSES_CHUNK_DIRNAME,
    PREPARED_ADDRESSES_FILENAME,
)

CANONICAL_PAGE_SIZE = 100
CLEANED_ADDRESS_COLUMNS = ("clean_full_address", "cleaned_full_address")
DISPLAY_ADDRESS_COLUMNS = (
    "original_address_concat",
    "address_concat",
    "clean_full_address",
    "cleaned_full_address",
)


@dataclass(frozen=True)
class CanonicalSource:
    supplied_path: Path
    parquet_paths: tuple[Path, ...]
    available_columns: frozenset[str]
    unique_id_column: str
    postcode_column: str
    cleaned_address_column: str
    display_address_column: str

    @property
    def display_name(self) -> str:
        return self.supplied_path.name


@dataclass(frozen=True)
class CanonicalSearchPage:
    page: int
    page_size: int
    has_previous: bool
    has_next: bool
    unique_id_query: str
    postcode: str | None
    address_query: str
    rows: list[dict[str, Any]]


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _quote_sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _resolve_canonical_parquet_paths(supplied_path: Path) -> list[Path]:
    if supplied_path.is_file():
        if supplied_path.suffix.lower() != ".parquet":
            raise ValueError("A direct canonical-data file must be a Parquet file.")
        return [supplied_path]
    if not supplied_path.is_dir():
        return []
    chunk_paths = sorted(
        (supplied_path / PREPARED_ADDRESSES_CHUNK_DIRNAME).glob("*.parquet")
    )
    if chunk_paths:
        return chunk_paths
    single_path = supplied_path / PREPARED_ADDRESSES_FILENAME
    return [single_path] if single_path.is_file() else []


def load_canonical_source(
    canonical_data_path: str | Path | None,
) -> CanonicalSource | None:
    if canonical_data_path is None:
        return None
    supplied_path = Path(canonical_data_path).expanduser().resolve()
    if not supplied_path.exists():
        raise FileNotFoundError(f"Canonical data path does not exist: {supplied_path}")
    parquet_paths = _resolve_canonical_parquet_paths(supplied_path)
    if not parquet_paths:
        raise FileNotFoundError(
            "No prepared canonical address Parquet files were found at: "
            f"{supplied_path}. Expected either {PREPARED_ADDRESSES_FILENAME} or "
            f"{PREPARED_ADDRESSES_CHUNK_DIRNAME}/*.parquet."
        )
    connection = duckdb.connect()
    try:
        relation = connection.read_parquet([str(path) for path in parquet_paths])
        available_columns = frozenset(relation.columns)
        relation.limit(1).fetchone()
    except Exception as error:
        raise ValueError(
            f"Canonical data could not be read as a Parquet dataset: {error}"
        ) from error
    finally:
        connection.close()
    missing_columns = {"unique_id", "postcode"} - available_columns
    if missing_columns:
        raise ValueError(
            "Canonical data is missing required columns: "
            + ", ".join(sorted(missing_columns))
        )
    cleaned_address_column = next(
        (column for column in CLEANED_ADDRESS_COLUMNS if column in available_columns),
        None,
    )
    if cleaned_address_column is None:
        raise ValueError(
            "Canonical data must contain either 'clean_full_address' or "
            "'cleaned_full_address'."
        )
    display_address_column = next(
        (column for column in DISPLAY_ADDRESS_COLUMNS if column in available_columns),
        cleaned_address_column,
    )
    return CanonicalSource(
        supplied_path=supplied_path,
        parquet_paths=tuple(parquet_paths),
        available_columns=available_columns,
        unique_id_column="unique_id",
        postcode_column="postcode",
        cleaned_address_column=cleaned_address_column,
        display_address_column=display_address_column,
    )


def canonical_scan_sql(source: CanonicalSource) -> str:
    quoted_paths = ", ".join(
        _quote_sql_string(str(path)) for path in source.parquet_paths
    )
    return f"read_parquet([{quoted_paths}])"


def normalise_postcode_search(value: str | None) -> str | None:
    if value is None:
        return None
    compact = "".join(str(value).upper().split())
    if not compact:
        return None
    if len(compact) > 16:
        raise ValueError("Postcode search is too long.")
    return compact if len(compact) <= 3 else f"{compact[:-3]} {compact[-3:]}"


def _canonical_columns(source: CanonicalSource) -> tuple[str, str, str, str]:
    return tuple(
        _quote_identifier(column)
        for column in (
            source.unique_id_column,
            source.postcode_column,
            source.cleaned_address_column,
            source.display_address_column,
        )
    )


def _row_dicts(cursor: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    columns = [description[0] for description in cursor.description]
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def search_canonical_data(
    source: CanonicalSource,
    *,
    unique_id_query: str | None = None,
    postcode: str | None = None,
    address_query: str | None = None,
    page: int = 1,
) -> CanonicalSearchPage:
    if not isinstance(page, int):
        raise TypeError("Canonical search page must be an integer.")
    if page < 1:
        raise ValueError("Canonical search page must be at least 1.")
    normalised_postcode = normalise_postcode_search(postcode)
    cleaned_unique_id_query = (
        "" if unique_id_query is None else str(unique_id_query).strip()
    )
    if len(cleaned_unique_id_query) > 100:
        raise ValueError("Unique ID search must contain no more than 100 characters.")
    cleaned_query = "" if address_query is None else str(address_query).strip()
    if len(cleaned_query) > 100:
        raise ValueError("Address search must contain no more than 100 characters.")
    if normalised_postcode is None and not cleaned_query and not cleaned_unique_id_query:
        raise ValueError(
            "Enter a unique ID, postcode, or address value before searching."
        )
    unique_id, postcode_column, cleaned_address, display_address = _canonical_columns(
        source
    )
    conditions = [f"{unique_id} IS NOT NULL"]
    parameters: list[Any] = []
    if cleaned_unique_id_query:
        conditions.append(f"contains(upper(CAST({unique_id} AS VARCHAR)), upper(?))")
        parameters.append(cleaned_unique_id_query)
    if normalised_postcode is not None:
        conditions.append(f"{postcode_column} = ?")
        parameters.append(normalised_postcode)
    if cleaned_query:
        conditions.append(f"contains(upper({cleaned_address}), upper(?))")
        parameters.append(cleaned_query)
    offset = (page - 1) * CANONICAL_PAGE_SIZE
    query = f"""
        SELECT
            CAST({unique_id} AS VARCHAR) AS canonical_id,
            CAST({display_address} AS VARCHAR) AS canonical_address,
            CAST({cleaned_address} AS VARCHAR) AS cleaned_address,
            CAST({postcode_column} AS VARCHAR) AS canonical_postcode
        FROM {canonical_scan_sql(source)}
        WHERE {" AND ".join(conditions)}
        ORDER BY canonical_postcode, cleaned_address, canonical_address, canonical_id
        LIMIT ? OFFSET ?
    """
    connection = duckdb.connect()
    try:
        cursor = connection.execute(query, [*parameters, CANONICAL_PAGE_SIZE + 1, offset])
        rows = _row_dicts(cursor)
    finally:
        connection.close()
    return CanonicalSearchPage(
        page=page,
        page_size=CANONICAL_PAGE_SIZE,
        has_previous=page > 1,
        has_next=len(rows) > CANONICAL_PAGE_SIZE,
        unique_id_query=cleaned_unique_id_query,
        postcode=normalised_postcode,
        address_query=cleaned_query,
        rows=rows[:CANONICAL_PAGE_SIZE],
    )


def find_canonical_record(
    source: CanonicalSource, canonical_id: str
) -> dict[str, Any] | None:
    cleaned_id = str(canonical_id).strip()
    if not cleaned_id:
        return None
    unique_id, postcode_column, cleaned_address, display_address = _canonical_columns(
        source
    )
    query = f"""
        SELECT
            CAST({unique_id} AS VARCHAR) AS canonical_id,
            CAST({display_address} AS VARCHAR) AS canonical_address,
            CAST({cleaned_address} AS VARCHAR) AS cleaned_address,
            CAST({postcode_column} AS VARCHAR) AS canonical_postcode
        FROM {canonical_scan_sql(source)}
        WHERE CAST({unique_id} AS VARCHAR) = ?
        ORDER BY canonical_postcode, cleaned_address, canonical_address, canonical_id
        LIMIT 1
    """
    connection = duckdb.connect()
    try:
        cursor = connection.execute(query, [cleaned_id])
        rows = _row_dicts(cursor)
    finally:
        connection.close()
    return rows[0] if rows else None
