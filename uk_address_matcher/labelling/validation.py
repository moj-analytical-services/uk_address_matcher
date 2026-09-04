from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb

from uk_address_matcher.labelling.schema import (
    DEFAULT_TOP_N_CANDIDATES,
    MAX_TOP_N_CANDIDATES,
    REQUIRED_TOP_LEVEL_COLUMNS,
    quote_identifier,
)

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection, DuckDBPyRelation


def validate_export_arguments(
    *,
    output_directory: str | Path,
    top_n_candidates: int,
) -> Path:
    """Validate the public bundle export arguments."""
    if isinstance(output_directory, Path):
        output_path = output_directory
    elif isinstance(output_directory, str):
        output_path = Path(output_directory)
    else:
        raise TypeError("output_directory must be a str or pathlib.Path.")

    if isinstance(top_n_candidates, bool) or not isinstance(top_n_candidates, int):
        raise TypeError("top_n_candidates must be an integer between 1 and 10.")
    if not 1 <= top_n_candidates <= MAX_TOP_N_CANDIDATES:
        raise ValueError(
            f"top_n_candidates must be between 1 and {MAX_TOP_N_CANDIDATES}."
        )
    return output_path.resolve()


def validate_source_relations(
    *,
    con: DuckDBPyConnection,
    messy_relation: DuckDBPyRelation | None,
    canonical_relation: DuckDBPyRelation | None,
    canonical_label_column: str,
    messy_columns: tuple[str, ...],
    canonical_columns: tuple[str, ...],
) -> tuple[str, str]:
    """Validate source schemas and return canonical identifier DuckDB types."""
    if messy_relation is None or canonical_relation is None:
        raise ValueError(
            "Labelling bundles require the MatchResult's retained messy and canonical "
            "relations. Export before closing the matching connection."
        )

    _validate_columns(messy_relation.columns, ("unique_id", *messy_columns), "messy")
    _validate_columns(
        canonical_relation.columns,
        ("unique_id", canonical_label_column, *canonical_columns),
        "canonical",
    )
    _validate_unique_messy_ids(con, messy_relation)

    label_type = _column_type(con, canonical_relation, canonical_label_column)
    canonical_id_type = _column_type(con, canonical_relation, "unique_id")
    return canonical_id_type, label_type


def _validate_columns(
    available_columns: Sequence[str],
    requested_columns: Sequence[str],
    relation_name: str,
) -> None:
    missing_columns = [
        column for column in requested_columns if column not in available_columns
    ]
    if missing_columns:
        available = ", ".join(available_columns)
        missing = ", ".join(missing_columns)
        raise ValueError(
            f"Requested {relation_name} column(s) not found: {missing}. "
            f"Available {relation_name} columns: {available}."
        )


def _validate_unique_messy_ids(
    con: DuckDBPyConnection,
    messy_relation: DuckDBPyRelation,
) -> None:
    relation_sql = messy_relation.sql_query()
    null_count, duplicate_count = con.execute(
        f"""
        SELECT
            COUNT(*) FILTER (WHERE unique_id IS NULL),
            COUNT(*) - COUNT(DISTINCT unique_id)
        FROM ({relation_sql}) AS messy
        """
    ).fetchone()
    if null_count:
        raise ValueError(
            "Messy unique_id contains null values; bundle export requires IDs."
        )
    if duplicate_count:
        raise ValueError("Messy unique_id must be unique for labelling bundle export.")


def _validate_label_compatibility(
    con: DuckDBPyConnection,
    messy_relation: DuckDBPyRelation,
    canonical_label_type: str,
) -> None:
    relation_sql = messy_relation.sql_query()
    incompatible_count = con.execute(
        f"""
        SELECT COUNT(*)
        FROM ({relation_sql}) AS messy
        WHERE ukam_label IS NOT NULL
            AND TRY_CAST(ukam_label AS {canonical_label_type}) IS NULL
        """
    ).fetchone()[0]
    if incompatible_count:
        raise ValueError(
            "Existing ukam_label values cannot be represented using the canonical "
            "unique_id type."
        )


def _column_type(
    con: DuckDBPyConnection,
    relation: DuckDBPyRelation,
    column_name: str,
) -> str:
    return str(
        con.execute(
            f"DESCRIBE SELECT {quote_identifier(column_name)} "
            f"FROM ({relation.sql_query()}) AS source_relation"
        ).fetchone()[1]
    )


def validate_output_directory(output_directory: Path, *, overwrite: bool) -> None:
    """Reject unsafe output targets before any data is written."""
    if output_directory.exists() and not output_directory.is_dir():
        raise FileExistsError(
            f"Output path exists and is not a directory: {output_directory}"
        )
    if not output_directory.exists():
        return
    if not overwrite:
        raise FileExistsError(
            f"Output directory already exists: {output_directory}. "
            "Pass overwrite=True to replace a recognised labelling bundle."
        )
    if not any(output_directory.iterdir()):
        return
    manifest_path = output_directory / "manifest.json"
    data_path = output_directory / "review_data.parquet"
    if not manifest_path.is_file() or not data_path.is_file():
        raise FileExistsError(
            "Refusing to replace a populated directory that is not a recognised "
            f"labelling bundle: {output_directory}"
        )
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise FileExistsError(
            f"Refusing to replace an unreadable labelling bundle: {output_directory}"
        ) from error
    if (
        not manifest.get("uk_address_matcher_version")
        or manifest.get("data_file") != "review_data.parquet"
    ):
        raise FileExistsError(
            "Refusing to replace a directory whose manifest is not a recognised "
            "labelling bundle."
        )


def validate_splink_relations(match_result: object) -> None:
    """Ensure an executed Splink stage retained the relations needed for export."""
    stage = getattr(match_result, "_splink_stage", None)
    if stage is None or stage.linker is None:
        return
    table_names = (
        getattr(stage, "predictions_table", None),
        getattr(stage, "improved_predictions_table", None),
    )
    if any(
        not isinstance(table_name, str) or not table_name for table_name in table_names
    ):
        raise ValueError(
            "Splink ran but retained candidate relations are unavailable. "
            "Export the bundle before closing or cleaning the matching connection."
        )
    con = getattr(match_result, "con")
    for table_name in table_names:
        try:
            con.execute(f"SELECT 1 FROM {quote_identifier(table_name)} LIMIT 1")
        except duckdb.Error as error:
            raise ValueError(
                "Splink ran but required retained candidate relations are no longer "
                "accessible. Export before closing the matching connection."
            ) from error


def validate_written_parquet(
    parquet_path: Path,
    *,
    expected_row_count: int,
) -> dict[str, object]:
    """Open the completed Parquet in a fresh connection and validate its contract."""
    parquet_sql = _sql_path(parquet_path)
    with duckdb.connect() as con:
        schema_rows = con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{parquet_sql}')"
        ).fetchall()
        schema = {str(name): str(type_name) for name, type_name, *_ in schema_rows}
        missing = [
            column for column in REQUIRED_TOP_LEVEL_COLUMNS if column not in schema
        ]
        if missing:
            raise ValueError(
                "Written Parquet is missing required bundle columns: "
                + ", ".join(missing)
            )
        row_count, unique_id_count, null_id_count = con.execute(
            f"""
            SELECT COUNT(*), COUNT(DISTINCT unique_id),
                COUNT(*) FILTER (WHERE unique_id IS NULL)
            FROM read_parquet('{parquet_sql}')
            """
        ).fetchone()
        if row_count != expected_row_count:
            raise ValueError(
                "Written Parquet row count differs from the retained messy input: "
                f"expected {expected_row_count}, got {row_count}."
            )
        if null_id_count or unique_id_count != row_count:
            raise ValueError(
                "Written Parquet must contain one non-null row per messy unique_id."
            )
        counts = con.execute(
            f"""
            SELECT
                COUNT(*) FILTER (WHERE is_matched),
                COUNT(*) FILTER (WHERE NOT is_matched),
                COUNT(*) FILTER (WHERE candidate_count > 0),
                COUNT(*) FILTER (WHERE has_existing_label)
            FROM read_parquet('{parquet_sql}')
            """
        ).fetchone()
    return {
        "schema": schema,
        "row_count": int(row_count),
        "matched_row_count": int(counts[0]),
        "unmatched_row_count": int(counts[1]),
        "rows_with_candidates": int(counts[2]),
        "rows_with_existing_labels": int(counts[3]),
    }


def _sql_path(path: Path) -> str:
    return str(path).replace("'", "''")


def default_top_n_candidates() -> int:
    """Expose the default through one source of truth for internal callers."""
    return DEFAULT_TOP_N_CANDIDATES
