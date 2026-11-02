from __future__ import annotations

from collections.abc import Collection

import duckdb


def _input_id_column(columns: Collection[str], *, dataset: str) -> str:
    if "address_id" in columns and "unique_id" in columns:
        raise ValueError(
            f"Input {dataset} contained both `address_id` and `unique_id`. "
            "Ensure only one is present, not both."
        )
    id_column = "address_id" if "address_id" in columns else "unique_id"
    missing = [field for field in (id_column, "address_concat") if field not in columns]
    if missing:
        raise ValueError(
            f"Input {dataset} is missing required columns: {missing}. "
            "Provide address_concat and either address_id or unique_id."
        )
    return id_column


def _normalise_address_dict(data: dict) -> dict:
    id_column = _input_id_column(data, dataset="messy")
    for field in (id_column, "address_concat"):
        if data[field] is None:
            raise ValueError(f"Input messy field '{field}' must not be null.")
    normalised = {key: value for key, value in data.items() if key != id_column}
    normalised["address_id"] = data[id_column]
    normalised.setdefault("postcode", None)
    return normalised


def _normalise_and_validate_address_input(
    rel: duckdb.DuckDBPyRelation,
    *,
    dataset: str,
) -> duckdb.DuckDBPyRelation:
    if not isinstance(rel, duckdb.DuckDBPyRelation):
        raise TypeError(f"Input {dataset} must be a DuckDB relation.")
    id_column = _input_id_column(rel.columns, dataset=dataset)
    null_ids, null_addresses = rel.aggregate(
        f"count(*) FILTER (WHERE {id_column} IS NULL), "
        "count(*) FILTER (WHERE address_concat IS NULL)"
    ).fetchone()
    for field, null_count in ((id_column, null_ids), ("address_concat", null_addresses)):
        if null_count:
            raise ValueError(f"Input {dataset} field '{field}' must not be null.")
    if id_column == "address_id":
        rel = rel.project("*, address_id AS unique_id")
    return rel


def normalise_and_validate_raw_canonical(
    rel: duckdb.DuckDBPyRelation,
) -> duckdb.DuckDBPyRelation:
    """Normalise and validate required columns for raw canonical input."""
    if "address_concat" not in rel.columns and "original_address_concat" in rel.columns:
        rel = rel.project("*, original_address_concat AS address_concat")

    if (
        not {"address_id", "unique_id"}.intersection(rel.columns)
        and "input_id" in rel.columns
    ):
        rel = rel.project("*, input_id AS unique_id")

    return _normalise_and_validate_address_input(rel, dataset="canonical")
