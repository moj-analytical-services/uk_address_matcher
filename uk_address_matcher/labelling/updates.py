from __future__ import annotations

import argparse
import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb

SUPPORTED_DATA_SUFFIXES = {".csv", ".parquet"}
UPDATES_SCHEMA_VERSION = 1
ALLOWED_DECISIONS = {
    "accept_model",
    "select_candidate",
    "select_canonical",
    "use_existing",
    "no_match",
    "uncertain",
    "clear",
}
REQUIRED_REVIEW_COLUMNS = {
    "bundle_id",
    "unique_id",
    "ukam_label",
    "resolved_label_id",
    "top_candidates",
}


def _data_source_sql(data_file: Path) -> str:
    if data_file.suffix.lower() == ".parquet":
        return "read_parquet(?)"
    if data_file.suffix.lower() == ".csv":
        return "read_csv_auto(?)"
    raise ValueError(f"Unsupported data format: {data_file}")


def _quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _quote_sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _load_bundle(bundle_path: str | Path) -> tuple[Path, str]:
    root = Path(bundle_path).expanduser().resolve()
    if not root.is_dir():
        raise FileNotFoundError(f"Labelling bundle does not exist: {root}")
    manifest_path = root / "manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(f"Bundle manifest not found: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    bundle_id = str(manifest.get("bundle_id", "")).strip()
    if not bundle_id:
        raise ValueError("Bundle manifest is missing bundle_id")
    data_file = (root / manifest.get("data_file", "review_data.parquet")).resolve()
    if not data_file.is_file() or data_file.suffix.lower() not in SUPPORTED_DATA_SUFFIXES:
        raise ValueError("Bundle review data must be a CSV or Parquet file")
    connection = duckdb.connect()
    try:
        cursor = connection.execute(
            f"SELECT * FROM {_data_source_sql(data_file)} LIMIT 0",
            [str(data_file)],
        )
        columns = {description[0] for description in cursor.description}
    finally:
        connection.close()
    missing = REQUIRED_REVIEW_COLUMNS - columns
    if missing:
        raise ValueError(
            "The labelling bundle is missing required columns: "
            + ", ".join(sorted(missing))
        )
    return data_file, bundle_id


def _load_updates(updates_path: str | Path, bundle_id: str) -> list[dict[str, Any]]:
    path = Path(updates_path).expanduser().resolve()
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Updates JSON must contain an object")
    if payload.get("schema_version") != UPDATES_SCHEMA_VERSION:
        raise ValueError("Unsupported updates JSON schema version")
    if str(payload.get("bundle_id", "")) != bundle_id:
        raise ValueError("Updates JSON bundle_id does not match the labelling bundle")
    events = payload.get("events")
    if not isinstance(events, list):
        raise ValueError("Updates JSON events must be a list")
    result: list[dict[str, Any]] = []
    event_ids: set[str] = set()
    for event in events:
        normalised_event = _normalise_event(event, bundle_id)
        event_id = normalised_event["event_id"]
        if event_id in event_ids:
            raise ValueError(f"Duplicate update event_id: {event_id}")
        event_ids.add(event_id)
        result.append(normalised_event)
    return result


def _normalise_event(event: Any, bundle_id: str) -> dict[str, Any]:
    if not isinstance(event, dict):
        raise ValueError("Each update event must be an object")
    event_bundle_id = str(event.get("bundle_id", "")).strip()
    if event_bundle_id != bundle_id:
        raise ValueError("Each update event bundle_id must match the labelling bundle")
    event_id = str(event.get("event_id", "")).strip()
    unique_id = str(event.get("unique_id", "")).strip()
    decision = str(event.get("decision", "")).strip()
    if not event_id or not unique_id:
        raise ValueError("Each update event requires event_id and unique_id")
    if decision not in ALLOWED_DECISIONS:
        raise ValueError(f"Unsupported decision: {decision}")
    created_at = str(event.get("created_at_utc", "")).strip()
    try:
        datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError("Each update event requires a valid created_at_utc") from error
    label = event.get("ukam_label")
    if label is not None:
        label = str(label)
    clean_full_address = event.get("clean_full_address")
    if clean_full_address is not None:
        clean_full_address = str(clean_full_address)
    postcode = event.get("postcode")
    if postcode is not None:
        postcode = str(postcode)
    rank = event.get("selected_candidate_rank")
    if rank is not None and (isinstance(rank, bool) or not isinstance(rank, int)):
        raise ValueError("selected_candidate_rank must be an integer")
    if decision in {"no_match", "uncertain", "clear"} and any(
        value is not None for value in (label, clean_full_address, postcode, rank)
    ):
        raise ValueError(f"{decision} events cannot contain label details or rank")
    return {
        "event_id": event_id,
        "bundle_id": event_bundle_id,
        "unique_id": unique_id,
        "decision": decision,
        "ukam_label": label,
        "clean_full_address": clean_full_address,
        "postcode": postcode,
        "selected_candidate_rank": rank,
        "created_at_utc": created_at,
    }


def _review_rows(data_file: Path, unique_ids: set[str]) -> dict[str, dict[str, Any]]:
    connection = duckdb.connect()
    try:
        cursor = connection.execute(
            f"SELECT CAST(unique_id AS VARCHAR), CAST(resolved_label_id AS VARCHAR), "
            f"CAST(ukam_label AS VARCHAR), CAST(resolved_canonical_address AS VARCHAR), "
            f"CAST(resolved_canonical_postcode AS VARCHAR), top_candidates FROM "
            f"{_data_source_sql(data_file)}",
            [str(data_file)],
        )
        rows = {
            str(row[0]): {
                "resolved_label_id": row[1],
                "imported_label": row[2],
                "resolved_canonical_address": row[3],
                "resolved_canonical_postcode": row[4],
                "top_candidates": row[5],
            }
            for row in cursor.fetchall()
        }
    finally:
        connection.close()
    unknown = unique_ids - rows.keys()
    if unknown:
        raise ValueError("Updates contain unknown messy unique_id: " + sorted(unknown)[0])
    return rows


def _normalise_candidates(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return []
    return (
        [item for item in value if isinstance(item, dict)]
        if isinstance(value, list)
        else []
    )


def _validated_labels(
    events: list[dict[str, Any]], rows: dict[str, dict[str, Any]]
) -> dict[str, dict[str, str | None]]:
    latest: dict[str, dict[str, Any]] = {}
    for event in events:
        current = latest.get(event["unique_id"])
        if current is None or (event["created_at_utc"], event["event_id"]) > (
            current["created_at_utc"],
            current["event_id"],
        ):
            latest[event["unique_id"]] = event
    labels: dict[str, dict[str, str | None]] = {}
    for unique_id, event in latest.items():
        row = rows[unique_id]
        decision = event["decision"]
        label = event["ukam_label"]
        candidates = _normalise_candidates(row["top_candidates"])
        candidate_ranks = {
            str(candidate["label_id"]): candidate.get("rank")
            for candidate in candidates
            if candidate.get("label_id") is not None
        }
        if decision == "accept_model" and label != row["resolved_label_id"]:
            raise ValueError(
                "The submitted label does not match the model-selected label"
            )
        if decision == "select_candidate":
            if label not in candidate_ranks:
                raise ValueError(
                    "The submitted label is not one of the exported candidates"
                )
            rank = event["selected_candidate_rank"]
            if rank is not None and rank != candidate_ranks[label]:
                raise ValueError(
                    "The submitted candidate rank does not match the candidate"
                )
        if decision == "use_existing" and label != row["imported_label"]:
            raise ValueError("The submitted label does not match the imported label")
        if decision == "select_canonical" and not label:
            raise ValueError("A canonical label is required")
        if decision in {"no_match", "uncertain", "clear"}:
            labels[unique_id] = {
                "ukam_label": None,
                "clean_full_address": None,
                "postcode": None,
            }
            continue
        candidate = next(
            (
                candidate
                for candidate in candidates
                if str(candidate.get("label_id")) == label
            ),
            {},
        )
        labels[unique_id] = {
            "ukam_label": label,
            "clean_full_address": event["clean_full_address"]
            or (
                row["resolved_canonical_address"]
                if label == row["resolved_label_id"]
                else candidate.get("canonical_address")
            ),
            "postcode": event["postcode"]
            or (
                row["resolved_canonical_postcode"]
                if label == row["resolved_label_id"]
                else candidate.get("canonical_postcode")
            ),
        }
    return labels


def _infer_unique_id_column(
    bundle_file: Path, input_file: Path, columns: set[str]
) -> str:
    source_sql = _data_source_sql(input_file)
    bundle_sql = _data_source_sql(bundle_file)
    connection = duckdb.connect()
    try:
        bundle_count = connection.execute(
            f"SELECT COUNT(*) FROM {bundle_sql}", [str(bundle_file)]
        ).fetchone()[0]
        matches: list[str] = []
        for column in columns:
            quoted = _quote_identifier(column)
            row_count, distinct_count = connection.execute(
                f"""
                WITH bundle_ids AS (
                    SELECT CAST(unique_id AS VARCHAR) AS unique_id FROM {bundle_sql}
                ), source_ids AS (
                    SELECT CAST({quoted} AS VARCHAR) AS unique_id FROM {source_sql}
                )
                SELECT COUNT(*), COUNT(DISTINCT unique_id)
                FROM source_ids
                WHERE unique_id IN (SELECT unique_id FROM bundle_ids)
                """,
                [str(bundle_file), str(input_file)],
            ).fetchone()
            if row_count == bundle_count and distinct_count == bundle_count:
                matches.append(column)
    finally:
        connection.close()
    if len(matches) != 1:
        if not matches:
            raise ValueError(
                "Input dataset has no column containing every bundle unique_id"
            )
        raise ValueError(
            "Input dataset has multiple columns containing every bundle unique_id"
        )
    return matches[0]


def apply_labelling_updates(
    labelling_bundle_path: str | Path,
    updates_json_path: str | Path,
    input_dataset_path: str | Path,
    *,
    input_dataset_label_column: str = "ukam_label",
    output_path: str | Path | None = None,
    include_label_details: bool = False,
) -> tuple[Path, int]:
    bundle_file, bundle_id = _load_bundle(labelling_bundle_path)
    events = _load_updates(updates_json_path, bundle_id)
    input_file = Path(input_dataset_path).expanduser().resolve()
    if (
        not input_file.is_file()
        or input_file.suffix.lower() not in SUPPORTED_DATA_SUFFIXES
    ):
        raise ValueError("Input dataset must be a CSV or Parquet file")
    connection = duckdb.connect()
    try:
        cursor = connection.execute(
            f"SELECT * FROM {_data_source_sql(input_file)} LIMIT 0", [str(input_file)]
        )
        input_columns = {description[0] for description in cursor.description}
    finally:
        connection.close()
    unique_id_column = _infer_unique_id_column(bundle_file, input_file, input_columns)
    rows = _review_rows(bundle_file, {event["unique_id"] for event in events})
    label_details = _validated_labels(events, rows)
    labels = {
        unique_id: details["ukam_label"] for unique_id, details in label_details.items()
    }
    target = Path(output_path).expanduser().resolve() if output_path else input_file
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary_file = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
    if target.suffix.lower() not in SUPPORTED_DATA_SUFFIXES:
        raise ValueError("Output dataset must be a CSV or Parquet file")
    source_sql = _data_source_sql(input_file)
    quoted_id = _quote_identifier(unique_id_column)
    quoted_label = _quote_identifier(input_dataset_label_column)
    clauses = " ".join(f"WHEN CAST({quoted_id} AS VARCHAR) = ? THEN ?" for _ in labels)
    parameters: list[Any] = []
    for unique_id, label in labels.items():
        parameters.extend([unique_id, label])
    connection = duckdb.connect()
    try:
        if include_label_details:
            details_columns = {
                "ukam_user_label_clean_full_address": "clean_full_address",
                "ukam_user_label_postcode": "postcode",
            }
            existing_label = (
                quoted_label if input_dataset_label_column in input_columns else "NULL"
            )
            label_expression = (
                f"CASE {clauses} ELSE {existing_label} END" if labels else existing_label
            )
            label_output = f"CAST({label_expression} AS VARCHAR) AS {quoted_label}"
            replacements = (
                [label_output] if input_dataset_label_column in input_columns else []
            )
            additions: list[str] = []
            if input_dataset_label_column not in input_columns:
                additions.append(label_output)
            for column, detail_key in details_columns.items():
                quoted_column = _quote_identifier(column)
                detail_clauses = " ".join(
                    f"WHEN CAST({quoted_id} AS VARCHAR) = ? THEN ?" for _ in labels
                )
                existing_detail = quoted_column if column in input_columns else "NULL"
                detail_expression = (
                    f"CASE {detail_clauses} ELSE {existing_detail} END"
                    if labels
                    else existing_detail
                )
                expression = f"CAST({detail_expression} AS VARCHAR) AS {quoted_column}"
                if column in input_columns:
                    replacements.append(expression)
                else:
                    additions.append(expression)
                for unique_id, details in label_details.items():
                    parameters.extend([unique_id, details[detail_key]])
            projection = "*"
            if replacements:
                projection += f" REPLACE ({', '.join(replacements)})"
            if additions:
                projection += f", {', '.join(additions)}"
            output_query = f"SELECT {projection} FROM {source_sql}"
        elif input_dataset_label_column in input_columns:
            if not labels:
                output_query = f"SELECT * FROM {source_sql}"
            else:
                label_expression = f"CASE {clauses} ELSE {quoted_label} END"
                output_query = (
                    f"SELECT * REPLACE ({label_expression} AS {quoted_label}) "
                    f"FROM {source_sql}"
                )
        elif not labels:
            output_query = f"SELECT *, NULL::VARCHAR AS {quoted_label} FROM {source_sql}"
        else:
            label_expression = f"CASE {clauses} ELSE NULL END"
            output_query = (
                f"SELECT *, CAST({label_expression} AS VARCHAR) AS {quoted_label} "
                f"FROM {source_sql}"
            )
        output_format = target.suffix.removeprefix(".").upper()
        header = ", HEADER" if output_format == "CSV" else ""
        connection.execute(
            f"COPY ({output_query}) TO {_quote_sql_string(str(temporary_file))} "
            f"(FORMAT {output_format}{header})",
            [*parameters, str(input_file)],
        )
    finally:
        connection.close()
    temporary_file.replace(target)
    return target, len(labels)


def main() -> int:
    parser = argparse.ArgumentParser(description="Apply browser labelling updates")
    parser.add_argument("labelling_bundle")
    parser.add_argument("updates_json")
    parser.add_argument("input_dataset")
    parser.add_argument("--input-label-column", default="ukam_label")
    parser.add_argument("--output")
    arguments = parser.parse_args()
    output_path, updated_count = apply_labelling_updates(
        arguments.labelling_bundle,
        arguments.updates_json,
        arguments.input_dataset,
        input_dataset_label_column=arguments.input_label_column,
        output_path=arguments.output,
    )
    print(f"Applied {updated_count} labelling updates to: {output_path}")  # noqa: T201
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
