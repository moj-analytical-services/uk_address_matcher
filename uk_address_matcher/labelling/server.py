from __future__ import annotations

import argparse
import json
import secrets
import threading
import time
import uuid
import webbrowser
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib.resources import files
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

import duckdb

from uk_address_matcher.labelling.canonical import (
    CANONICAL_PAGE_SIZE,
    CanonicalSource,
    find_canonical_record,
    load_canonical_source,
    search_canonical_data,
)

DEFAULT_PAGE_SIZE = 20
ALLOWED_PAGE_SIZES = {10, 20, 50, 100}
ALLOWED_MATCH_STAGES = {"exact", "peeled", "splink", "unique_trigram", "unmatched"}
RECORD_SORT_COLUMNS = {
    "unique_id": "unique_id",
    "reranked_score": "match_weight",
    "splink_score": "splink_match_weight",
    "distinguishability": "distinguishability",
}
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
    "uk_address_matcher_version",
    "created_at_utc",
    "unique_id",
    "messy_address",
    "messy_cleaned_address",
    "messy_postcode",
    "ukam_label",
    "has_existing_label",
    "resolved_canonical_id",
    "resolved_label_id",
    "resolved_canonical_address",
    "resolved_canonical_postcode",
    "match_reason",
    "match_stage",
    "is_matched",
    "match_weight",
    "distinguishability",
    "candidate_count",
    "top_candidates",
}
SUPPORTED_DATA_SUFFIXES = {".csv", ".parquet"}


@dataclass(frozen=True)
class Bundle:
    root: Path
    manifest: dict[str, Any]
    data_file: Path
    state_file: Path
    review_columns: frozenset[str]


@dataclass(frozen=True)
class InputDataset:
    data_file: Path
    unique_id_column: str
    label_column: str
    has_label_column: bool


@dataclass(frozen=True)
class RecordFilters:
    unique_id_query: str
    address_query: str
    stages: tuple[str, ...]
    score_min: float | None
    score_max: float | None
    distinguishability_min: float | None
    distinguishability_max: float | None
    show_labelled: bool
    mismatches_only: bool
    sort_by: str
    sort_order: str


class SessionState:
    def __init__(self, idle_timeout_seconds: float) -> None:
        self.token = secrets.token_urlsafe(32)
        self.idle_timeout_seconds = idle_timeout_seconds
        self._last_activity = time.monotonic()
        self._lock = threading.Lock()

    def touch(self) -> None:
        with self._lock:
            self._last_activity = time.monotonic()

    def remaining_seconds(self) -> float:
        with self._lock:
            elapsed = time.monotonic() - self._last_activity
        return max(0.0, self.idle_timeout_seconds - elapsed)

    def is_expired(self) -> bool:
        return self.remaining_seconds() <= 0


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serialisable")


def _load_bundle(bundle_path: str | Path) -> Bundle:
    root = Path(bundle_path).expanduser().resolve()
    if not root.exists():
        raise FileNotFoundError(f"Labelling bundle does not exist: {root}")
    if not root.is_dir():
        raise NotADirectoryError(f"Labelling bundle must be a directory: {root}")
    manifest_path = root / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"Bundle manifest not found: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    data_file = (root / manifest.get("data_file", "review_data.parquet")).resolve()
    if not data_file.exists():
        raise FileNotFoundError(f"Review data file not found: {data_file}")
    if data_file.suffix.lower() not in SUPPORTED_DATA_SUFFIXES:
        supported = ", ".join(sorted(SUPPORTED_DATA_SUFFIXES))
        raise ValueError(
            f"Review data must be a CSV or Parquet file ({supported}): {data_file}"
        )
    connection = duckdb.connect()
    try:
        cursor = connection.execute(
            f"SELECT * FROM {_data_source_sql(data_file)} LIMIT 0",
            [str(data_file)],
        )
        review_columns = frozenset(column[0] for column in cursor.description)
        missing = REQUIRED_REVIEW_COLUMNS - review_columns
    finally:
        connection.close()
    if missing:
        raise ValueError(
            "The labelling bundle is missing required columns: "
            + ", ".join(sorted(missing))
        )
    return Bundle(
        root,
        manifest,
        data_file,
        root / "review_state.duckdb",
        review_columns,
    )


def _data_source_sql(data_file: Path) -> str:
    if data_file.suffix.lower() == ".parquet":
        return "read_parquet(?)"
    if data_file.suffix.lower() == ".csv":
        return "read_csv_auto(?)"
    raise ValueError(f"Unsupported review data format: {data_file}")


def _quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _load_input_dataset(
    bundle: Bundle,
    input_dataset_path: str | Path,
    *,
    label_column: str = "ukam_label",
) -> InputDataset:
    data_file = Path(input_dataset_path).expanduser().resolve()
    if not data_file.exists():
        raise FileNotFoundError(f"Input dataset not found: {data_file}")
    if not data_file.is_file():
        raise ValueError(f"Input dataset must be a file: {data_file}")
    if data_file.suffix.lower() not in SUPPORTED_DATA_SUFFIXES:
        supported = ", ".join(sorted(SUPPORTED_DATA_SUFFIXES))
        raise ValueError(
            f"Input dataset must be a CSV or Parquet file ({supported}): {data_file}"
        )
    connection = duckdb.connect()
    try:
        cursor = connection.execute(
            f"SELECT * FROM {_data_source_sql(data_file)} LIMIT 0",
            [str(data_file)],
        )
        columns = {column[0] for column in cursor.description}
    finally:
        connection.close()
    unique_id_column = _infer_input_unique_id_column(bundle, data_file, columns)
    if label_column not in columns:
        label_column = "ukam_label"
    return InputDataset(
        data_file,
        unique_id_column,
        label_column,
        label_column in columns,
    )


def _infer_input_unique_id_column(
    bundle: Bundle,
    data_file: Path,
    columns: set[str],
) -> str:
    source_sql = _data_source_sql(data_file)
    bundle_sql = _data_source_sql(bundle.data_file)
    connection = duckdb.connect()
    try:
        bundle_count = connection.execute(
            f"SELECT COUNT(*) FROM {bundle_sql}", [str(bundle.data_file)]
        ).fetchone()[0]
        matches = []
        for column in columns:
            quoted_column = _quote_identifier(column)
            row_count, distinct_count = connection.execute(
                f"""
                WITH bundle_ids AS (
                    SELECT CAST(unique_id AS VARCHAR) AS unique_id FROM {bundle_sql}
                ), source_ids AS (
                    SELECT CAST({quoted_column} AS VARCHAR) AS unique_id FROM {source_sql}
                )
                SELECT COUNT(*), COUNT(DISTINCT unique_id)
                FROM source_ids
                WHERE unique_id IN (SELECT unique_id FROM bundle_ids)
                """,
                [str(bundle.data_file), str(data_file)],
            ).fetchone()
            if row_count == bundle_count and distinct_count == bundle_count:
                matches.append(column)
    finally:
        connection.close()
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise ValueError(
            "Input dataset has no column containing every unique_id in the "
            "labelling bundle"
        )
    raise ValueError(
        "Input dataset has multiple columns containing every unique_id in the "
        "labelling bundle: " + ", ".join(sorted(matches))
    )


def _ensure_state_database(bundle: Bundle) -> None:
    connection = duckdb.connect(str(bundle.state_file))
    try:
        connection.execute("""
            CREATE TABLE IF NOT EXISTS label_events (
                event_id VARCHAR NOT NULL, unique_id VARCHAR NOT NULL,
                decision VARCHAR NOT NULL, ukam_label VARCHAR,
                selected_candidate_rank BIGINT, created_at_utc TIMESTAMPTZ NOT NULL
            )
        """)
    finally:
        connection.close()


def _base_review_cte(bundle: Bundle) -> str:
    messy_cleaned_address = (
        "r.messy_cleaned_address"
        if "messy_cleaned_address" in bundle.review_columns
        else "r.messy_address"
    )
    resolved_canonical_id = (
        "CAST(r.resolved_canonical_id AS VARCHAR)"
        if "resolved_canonical_id" in bundle.review_columns
        else "NULL::VARCHAR"
    )
    return f"""
        WITH latest_labels AS (
            SELECT event_id, unique_id, decision, ukam_label, selected_candidate_rank
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY unique_id ORDER BY created_at_utc DESC, event_id DESC
                ) AS event_rank FROM label_events
            ) WHERE event_rank = 1
        ), base AS (
            SELECT CAST(r.unique_id AS VARCHAR) AS unique_id, r.messy_address,
                {messy_cleaned_address} AS messy_cleaned_address,
                r.messy_postcode, CAST(r.ukam_label AS VARCHAR) AS imported_label,
                COALESCE(r.has_existing_label, FALSE) AS has_existing_label,
                {resolved_canonical_id} AS resolved_canonical_id,
                CAST(r.resolved_label_id AS VARCHAR) AS resolved_label_id,
                r.resolved_canonical_address, r.resolved_canonical_postcode,
                r.match_reason, r.match_stage, r.is_matched, r.match_weight,
                r.distinguishability,
                TRY_CAST(json_extract_string(
                    CAST(r.top_candidates AS JSON), '$[0].splink_match_weight'
                ) AS DOUBLE) AS splink_match_weight,
                r.candidate_count, r.top_candidates,
                l.decision AS saved_decision,
                l.selected_candidate_rank,
                CASE WHEN l.decision = 'clear' THEN FALSE
                     WHEN l.decision IS NOT NULL THEN TRUE
                     ELSE COALESCE(r.has_existing_label, FALSE)
                 END AS is_labelled,
                 CASE WHEN l.decision = 'clear' THEN NULL
                     WHEN l.decision IS NOT NULL THEN l.decision
                     WHEN COALESCE(r.has_existing_label, FALSE) THEN 'imported'
                 END AS current_decision,
                CASE WHEN l.decision IN ('clear', 'no_match', 'uncertain') THEN NULL
                     WHEN l.ukam_label IS NOT NULL THEN l.ukam_label
                     WHEN COALESCE(r.has_existing_label, FALSE)
                         THEN CAST(r.ukam_label AS VARCHAR)
                END AS current_label
            FROM {_data_source_sql(bundle.data_file)} AS r LEFT JOIN latest_labels AS l
                ON CAST(r.unique_id AS VARCHAR) = l.unique_id
        )
    """


def _stage_counts(bundle: Bundle) -> dict[str, int]:
    connection = duckdb.connect(str(bundle.state_file))
    try:
        rows = connection.execute(
            f"SELECT match_stage, COUNT(*) FROM {_data_source_sql(bundle.data_file)} "
            "GROUP BY match_stage",
            [str(bundle.data_file)],
        ).fetchall()
    finally:
        connection.close()
    return {
        str(stage): int(count) for stage, count in rows if stage in ALLOWED_MATCH_STAGES
    }


def _bootstrap_payload(
    bundle: Bundle,
    session: SessionState,
    canonical_source: CanonicalSource | None = None,
) -> dict[str, Any]:
    connection = duckdb.connect(str(bundle.state_file))
    try:
        row = connection.execute(
            f"""{_base_review_cte(bundle)}
            SELECT COUNT(*), COUNT(*) FILTER (WHERE is_labelled),
                MIN(match_weight), MAX(match_weight), MIN(distinguishability),
                MAX(distinguishability)
            FROM base""",
            [str(bundle.data_file)],
        ).fetchone()
    finally:
        connection.close()
    return {
        "bundle_name": bundle.root.name,
        "bundle_id": bundle.manifest.get("bundle_id"),
        "idle_timeout_seconds": int(session.idle_timeout_seconds),
        "total_records": int(row[0]),
        "labelled_records": int(row[1]),
        "stage_counts": _stage_counts(bundle),
        "score_bounds": {"minimum": row[2], "maximum": row[3]},
        "distinguishability_bounds": {"minimum": row[4], "maximum": row[5]},
        "canonical_search": {
            "available": canonical_source is not None,
            "source_name": (
                None if canonical_source is None else canonical_source.display_name
            ),
            "page_size": CANONICAL_PAGE_SIZE,
            "warning": (
                "No canonical path provided. If you wish to view canonical data in "
                "this app, relaunch the application with canonical_data_path."
                if canonical_source is None
                else None
            ),
        },
    }


def _optional_float(query: dict[str, list[str]], name: str) -> float | None:
    value = query.get(name, [""])[0]
    if not value:
        return None
    try:
        return float(value)
    except ValueError as error:
        raise ValueError(f"Query parameter {name!r} must be numeric") from error


def _optional_text(query: dict[str, list[str]], name: str) -> str:
    value = query.get(name, [""])[0].strip()
    if len(value) > 100:
        raise ValueError(
            f"Query parameter {name!r} must contain no more than 100 characters"
        )
    return value


def _parse_record_filters(query: dict[str, list[str]]) -> RecordFilters:
    stages = tuple(query.get("stage", []))
    if set(stages) - ALLOWED_MATCH_STAGES:
        raise ValueError("Unsupported match stage")
    sort_by = query.get("sort_by", ["unique_id"])[0]
    if sort_by not in RECORD_SORT_COLUMNS:
        raise ValueError("Unsupported record sort")
    sort_order = query.get("sort_order", ["asc"])[0].lower()
    if sort_order not in {"asc", "desc"}:
        raise ValueError("Record sort order must be asc or desc")
    return RecordFilters(
        unique_id_query=_optional_text(query, "unique_id_query"),
        address_query=_optional_text(query, "address_query"),
        stages=stages,
        score_min=_optional_float(query, "score_min"),
        score_max=_optional_float(query, "score_max"),
        distinguishability_min=_optional_float(query, "distinguishability_min"),
        distinguishability_max=_optional_float(query, "distinguishability_max"),
        show_labelled=query.get("show_labelled", ["true"])[0].lower()
        in {"1", "true", "yes", "on"},
        mismatches_only=query.get("mismatches_only", ["false"])[0].lower()
        in {"1", "true", "yes", "on"},
        sort_by=sort_by,
        sort_order=sort_order,
    )


def _record_filter_sql(filters: RecordFilters) -> tuple[str, list[Any]]:
    conditions: list[str] = []
    parameters: list[Any] = []
    if filters.unique_id_query:
        conditions.append("contains(upper(unique_id), upper(?))")
        parameters.append(filters.unique_id_query)
    if filters.address_query:
        conditions.append(
            "(contains(upper(COALESCE(CAST(messy_address AS VARCHAR), '')), upper(?)) "
            "OR contains(upper(COALESCE(CAST(messy_cleaned_address AS VARCHAR), '')), "
            "upper(?)) "
            "OR contains(upper(COALESCE(CAST(messy_postcode AS VARCHAR), '')), upper(?)))"
        )
        parameters.extend([filters.address_query] * 3)
    if filters.stages:
        conditions.append(
            "match_stage IN (" + ", ".join("?" for _ in filters.stages) + ")"
        )
        parameters.extend(filters.stages)
    for column, value, operator in (
        ("match_weight", filters.score_min, ">="),
        ("match_weight", filters.score_max, "<="),
        ("distinguishability", filters.distinguishability_min, ">="),
        ("distinguishability", filters.distinguishability_max, "<="),
    ):
        if value is not None:
            conditions.append(f"(match_stage != 'splink' OR {column} {operator} ?)")
            parameters.append(value)
    if not filters.show_labelled and not filters.mismatches_only:
        conditions.append("is_labelled = FALSE")
    if filters.mismatches_only:
        conditions.append(
            "match_stage <> 'unmatched' AND has_existing_label AND is_matched AND "
            "imported_label IS NOT NULL AND resolved_label_id IS NOT NULL AND "
            "resolved_label_id IS DISTINCT FROM imported_label"
        )
    return ("WHERE " + " AND ".join(conditions) if conditions else "", parameters)


def _record_order_sql(filters: RecordFilters) -> str:
    column = RECORD_SORT_COLUMNS[filters.sort_by]
    direction = filters.sort_order.upper()
    return f"{column} {direction} NULLS LAST, unique_id ASC"


def _records_payload(bundle: Bundle, query: dict[str, list[str]]) -> dict[str, Any]:
    try:
        page = max(1, int(query.get("page", ["1"])[0]))
        page_size = int(query.get("page_size", [str(DEFAULT_PAGE_SIZE)])[0])
    except ValueError as error:
        raise ValueError("page and page_size must be integers") from error
    if page_size not in ALLOWED_PAGE_SIZES:
        raise ValueError(f"page_size must be one of {sorted(ALLOWED_PAGE_SIZES)}")
    filters = _parse_record_filters(query)
    where_clause, parameters = _record_filter_sql(filters)
    connection = duckdb.connect(str(bundle.state_file))
    try:
        total = connection.execute(
            f"{_base_review_cte(bundle)} SELECT COUNT(*) FROM base {where_clause}",
            [str(bundle.data_file), *parameters],
        ).fetchone()[0]
        maximum_page = max(1, (int(total) + page_size - 1) // page_size)
        page = min(page, maximum_page)
        cursor = connection.execute(
            f"""{_base_review_cte(bundle)}
            SELECT unique_id, messy_address, messy_cleaned_address, messy_postcode,
                imported_label,
                has_existing_label, resolved_label_id,
                resolved_canonical_address, resolved_canonical_postcode,
                match_reason, match_stage, is_matched, match_weight, distinguishability,
                splink_match_weight, candidate_count, top_candidates, current_decision,
                current_label, selected_candidate_rank, is_labelled
            FROM base {where_clause} ORDER BY {_record_order_sql(filters)}
            LIMIT ? OFFSET ?""",
            [str(bundle.data_file), *parameters, page_size, (page - 1) * page_size],
        )
        names = [column[0] for column in cursor.description]
        rows = [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]
        for row in rows:
            row["top_candidates"] = _normalise_candidates(row["top_candidates"])
    finally:
        connection.close()
    return {
        "page": page,
        "page_size": page_size,
        "maximum_page": maximum_page,
        "total_filtered": int(total),
        "rows": rows,
    }


def _review_record_payload(bundle: Bundle, query: dict[str, list[str]]) -> dict[str, Any]:
    unique_id = query.get("unique_id", [""])[0]
    if not unique_id:
        raise ValueError("unique_id is required")
    filters = _parse_record_filters(query)
    where_clause, parameters = _record_filter_sql(filters)
    cleaned_address = (
        "messy_cleaned_address"
        if "messy_cleaned_address" in bundle.review_columns
        else "NULL::VARCHAR"
    )
    connection = duckdb.connect(str(bundle.state_file))
    try:
        cursor = connection.execute(
            f"""{_base_review_cte(bundle)}, filtered AS (
                SELECT *, ROW_NUMBER() OVER (
                        ORDER BY {_record_order_sql(filters)}
                    ) AS review_position,
                    COUNT(*) OVER () AS review_total,
                    LAG(unique_id) OVER (
                        ORDER BY {_record_order_sql(filters)}
                    ) AS previous_unique_id,
                    LEAD(unique_id) OVER (
                        ORDER BY {_record_order_sql(filters)}
                    ) AS next_unique_id
                FROM base {where_clause}
            )
            SELECT unique_id, messy_address, {cleaned_address} AS messy_cleaned_address,
                messy_postcode, imported_label, current_decision, current_label,
                is_labelled, resolved_canonical_id, resolved_label_id,
                resolved_canonical_address, resolved_canonical_postcode, match_reason,
                match_stage, is_matched, match_weight, distinguishability,
                candidate_count, top_candidates AS candidates, review_position,
                review_total, previous_unique_id, next_unique_id
            FROM filtered WHERE unique_id = ? LIMIT 1""",
            [str(bundle.data_file), *parameters, str(unique_id)],
        )
        row = cursor.fetchone()
        if row is None:
            raise ValueError(
                "The requested record does not exist in the current filtered review set"
            )
        result = dict(zip([column[0] for column in cursor.description], row, strict=True))
    finally:
        connection.close()
    candidates = _normalise_candidates(result.pop("candidates"))
    navigation = {
        "position": result.pop("review_position"),
        "total": result.pop("review_total"),
        "previous_unique_id": result.pop("previous_unique_id"),
        "next_unique_id": result.pop("next_unique_id"),
    }
    result["candidates"] = candidates
    return {"record": result, "navigation": navigation}


def _record_for_validation(bundle: Bundle, unique_id: str) -> dict[str, Any]:
    connection = duckdb.connect(str(bundle.state_file))
    try:
        cursor = connection.execute(
            f"""SELECT CAST(resolved_label_id AS VARCHAR) AS resolved_label_id,
            CAST(ukam_label AS VARCHAR) AS imported_label,
            top_candidates FROM {_data_source_sql(bundle.data_file)}
            WHERE CAST(unique_id AS VARCHAR) = ? LIMIT 1""",
            [str(bundle.data_file), unique_id],
        )
        row = cursor.fetchone()
        if row is None:
            raise ValueError(f"Unknown messy unique_id: {unique_id}")
        return dict(zip([column[0] for column in cursor.description], row, strict=True))
    finally:
        connection.close()


def _normalise_candidates(candidates: Any) -> list[dict[str, Any]]:
    if isinstance(candidates, str):
        try:
            candidates = json.loads(candidates)
        except json.JSONDecodeError:
            return []
    if not isinstance(candidates, list):
        return []
    return [candidate for candidate in candidates if isinstance(candidate, dict)]


def _validate_label_payload(
    bundle: Bundle,
    payload: dict[str, Any],
    canonical_source: CanonicalSource | None = None,
) -> tuple[str, str, str | None, int | None]:
    unique_id = str(payload.get("unique_id", "")).strip()
    decision = str(payload.get("decision", "")).strip()
    label = None if payload.get("ukam_label") is None else str(payload["ukam_label"])
    rank = (
        None
        if payload.get("selected_candidate_rank") is None
        else int(payload["selected_candidate_rank"])
    )
    if not unique_id:
        raise ValueError("unique_id is required")
    if decision not in ALLOWED_DECISIONS:
        raise ValueError(f"Unsupported decision: {decision}")
    record = _record_for_validation(bundle, unique_id)
    candidates = _normalise_candidates(record["top_candidates"])
    candidate_ranks = {
        str(item["label_id"]): item.get("rank")
        for item in candidates
        if item.get("label_id") is not None
    }
    if decision == "accept_model" and (
        record["resolved_label_id"] is None or label != record["resolved_label_id"]
    ):
        raise ValueError("The submitted label does not match the model-selected label")
    if decision == "select_candidate":
        if label not in candidate_ranks:
            raise ValueError("The submitted label is not one of the exported candidates")
        rank = candidate_ranks[label] if rank is None else rank
    if decision == "select_canonical":
        if canonical_source is None:
            raise ValueError(
                "A canonical-data path is required to select a canonical-search result"
            )
        if label is None:
            raise ValueError("A canonical label is required")
        if find_canonical_record(canonical_source, label) is None:
            raise ValueError(
                "The selected canonical ID does not exist in the configured "
                "canonical data"
            )
        rank = None
    if decision == "use_existing" and (
        record["imported_label"] is None or label != record["imported_label"]
    ):
        raise ValueError("The submitted label does not match the imported label")
    if decision in {"no_match", "uncertain", "clear"}:
        label, rank = None, None
    return unique_id, decision, label, rank


def _replace_input_label(
    input_dataset: InputDataset,
    *,
    unique_id: str,
    label: str | None,
) -> None:
    source_sql = _data_source_sql(input_dataset.data_file)
    unique_id_column = _quote_identifier(input_dataset.unique_id_column)
    label_column = _quote_identifier(input_dataset.label_column)
    temporary_file = input_dataset.data_file.with_name(
        f".{input_dataset.data_file.stem}-{uuid.uuid4().hex}"
        f"{input_dataset.data_file.suffix}"
    )
    temporary_sql = str(temporary_file).replace("'", "''")
    connection = duckdb.connect()
    try:
        matching_rows = connection.execute(
            f"SELECT COUNT(*) FROM {source_sql} "
            f"WHERE CAST({unique_id_column} AS VARCHAR) = ?",
            [str(input_dataset.data_file), unique_id],
        ).fetchone()[0]
        if matching_rows != 1:
            raise ValueError(
                "Input dataset must contain exactly one row for "
                f"unique_id {unique_id!r}; found {matching_rows}"
            )
        output_format = input_dataset.data_file.suffix.removeprefix(".").upper()
        header_option = ", HEADER" if output_format == "CSV" else ""
        if input_dataset.has_label_column:
            output_query = f"""
                SELECT * REPLACE (
                    CASE
                        WHEN CAST({unique_id_column} AS VARCHAR) = ? THEN ?
                        ELSE {label_column}
                    END AS {label_column}
                )
                FROM {source_sql}
            """
        else:
            output_query = f"""
                SELECT *, CAST(
                    CASE WHEN CAST({unique_id_column} AS VARCHAR) = ? THEN ? END
                    AS VARCHAR
                ) AS {label_column}
                FROM {source_sql}
            """
        connection.execute(
            f"COPY ({output_query}) TO '{temporary_sql}' "
            f"(FORMAT {output_format}{header_option})",
            [unique_id, label, str(input_dataset.data_file)],
        )
    finally:
        connection.close()
    try:
        temporary_file.replace(input_dataset.data_file)
    finally:
        temporary_file.unlink(missing_ok=True)


def _save_label(
    bundle: Bundle,
    payload: dict[str, Any],
    input_dataset: InputDataset | None = None,
    canonical_source: CanonicalSource | None = None,
) -> dict[str, Any]:
    unique_id, decision, label, rank = _validate_label_payload(
        bundle, payload, canonical_source
    )
    if input_dataset is not None:
        _replace_input_label(
            input_dataset,
            unique_id=unique_id,
            label=label,
        )
    event_id, created_at = str(uuid.uuid4()), datetime.now(timezone.utc)
    connection = duckdb.connect(str(bundle.state_file))
    try:
        connection.execute(
            "INSERT INTO label_events VALUES (?, ?, ?, ?, ?, ?)",
            [event_id, unique_id, decision, label, rank, created_at],
        )
    finally:
        connection.close()
    return {
        "event_id": event_id,
        "unique_id": unique_id,
        "decision": decision,
        "ukam_label": label,
        "selected_candidate_rank": rank,
        "created_at_utc": created_at,
    }


def _undo_last_label(
    bundle: Bundle,
    input_dataset: InputDataset | None = None,
) -> dict[str, Any]:
    connection = duckdb.connect(str(bundle.state_file))
    try:
        event = connection.execute(
            """
            SELECT event_id, unique_id
            FROM label_events
            ORDER BY created_at_utc DESC, event_id DESC
            LIMIT 1
            """
        ).fetchone()
        if event is None:
            raise ValueError("There are no label actions to undo")
        event_id, unique_id = str(event[0]), str(event[1])
        connection.execute("DELETE FROM label_events WHERE event_id = ?", [event_id])
        cursor = connection.execute(
            f"""{_base_review_cte(bundle)}
            SELECT current_label FROM base WHERE unique_id = ?""",
            [str(bundle.data_file), unique_id],
        )
        row = cursor.fetchone()
        if row is None:
            raise ValueError(f"Unknown messy unique_id: {unique_id}")
        restored_label = row[0]
    finally:
        connection.close()
    if input_dataset is not None:
        _replace_input_label(
            input_dataset,
            unique_id=unique_id,
            label=restored_label,
        )
    return {
        "undone_event_id": event_id,
        "unique_id": unique_id,
        "ukam_label": restored_label,
    }


def _handler_factory(
    bundle: Bundle,
    input_dataset: InputDataset,
    session: SessionState,
    canonical_source: CanonicalSource | None = None,
) -> type[BaseHTTPRequestHandler]:
    static_root = files("uk_address_matcher.labelling.app")

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format_string: str, *args: Any) -> None:
            return

        def _send(
            self,
            status: HTTPStatus,
            payload: Any = None,
            content_type: str = "application/json; charset=utf-8",
        ) -> None:
            body = (
                b""
                if payload is None
                else payload
                if isinstance(payload, bytes)
                else json.dumps(payload, default=_json_default).encode("utf-8")
            )
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            if body:
                self.wfile.write(body)

        def _authorised(self) -> bool:
            return secrets.compare_digest(
                self.headers.get("X-UKAM-Session-Token", ""), session.token
            )

        def _serve_static(self, name: str) -> None:
            types = {
                ".html": "text/html; charset=utf-8",
                ".css": "text/css; charset=utf-8",
                ".js": "text/javascript; charset=utf-8",
                ".png": "image/png",
            }
            try:
                self._send(
                    HTTPStatus.OK,
                    static_root.joinpath(name).read_bytes(),
                    types.get(Path(name).suffix, "application/octet-stream"),
                )
            except FileNotFoundError:
                self._send(HTTPStatus.NOT_FOUND, {"error": "Static asset not found"})

        def do_GET(self) -> None:
            parsed, path = urlparse(self.path), unquote(urlparse(self.path).path)
            query = parse_qs(parsed.query, keep_blank_values=True)
            if path == "/":
                if not secrets.compare_digest(query.get("token", [""])[0], session.token):
                    self._send(
                        HTTPStatus.FORBIDDEN,
                        {"error": "Invalid or missing session token"},
                    )
                    return
                session.touch()
                self._serve_static("index.html")
                return
            if path in {"/app.css", "/app.js", "/icon.png"}:
                self._serve_static(path[1:])
                return
            if not self._authorised():
                self._send(
                    HTTPStatus.FORBIDDEN, {"error": "Invalid or missing session token"}
                )
                return
            try:
                if path == "/api/bootstrap":
                    session.touch()
                    self._send(
                        HTTPStatus.OK,
                        _bootstrap_payload(bundle, session, canonical_source),
                    )
                    return
                if path == "/api/records":
                    session.touch()
                    self._send(HTTPStatus.OK, _records_payload(bundle, query))
                    return
                if path == "/api/review-record":
                    session.touch()
                    self._send(HTTPStatus.OK, _review_record_payload(bundle, query))
                    return
                if path == "/api/canonical-search":
                    session.touch()
                    if canonical_source is None:
                        self._send(
                            HTTPStatus.CONFLICT,
                            {
                                "error": "Canonical search is unavailable because no "
                                "canonical_data_path was supplied."
                            },
                        )
                        return
                    try:
                        page = int(query.get("page", ["1"])[0])
                    except ValueError as error:
                        raise ValueError("Canonical page must be an integer.") from error
                    result = search_canonical_data(
                        canonical_source,
                        unique_id_query=query.get("unique_id_query", [None])[0],
                        postcode=query.get("postcode", [None])[0],
                        address_query=query.get("address_query", [None])[0],
                        page=page,
                    )
                    self._send(
                        HTTPStatus.OK,
                        {
                            "page": result.page,
                            "page_size": result.page_size,
                            "has_previous": result.has_previous,
                            "has_next": result.has_next,
                            "unique_id_query": result.unique_id_query,
                            "postcode": result.postcode,
                            "address_query": result.address_query,
                            "rows": result.rows,
                        },
                    )
                    return
                self._send(HTTPStatus.NOT_FOUND, {"error": "API route not found"})
            except ValueError as error:
                self._send(HTTPStatus.BAD_REQUEST, {"error": str(error)})
            except Exception as error:
                self._send(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    {"error": f"Unexpected server error: {error}"},
                )

        def do_POST(self) -> None:
            path = unquote(urlparse(self.path).path)
            if not self._authorised():
                self._send(
                    HTTPStatus.FORBIDDEN, {"error": "Invalid or missing session token"}
                )
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if length > 100_000:
                    raise ValueError("Request body is too large")
                payload = json.loads(self.rfile.read(length) or b"{}")
                if path == "/api/activity":
                    session.touch()
                    self._send(HTTPStatus.NO_CONTENT)
                    return
                if path == "/api/labels":
                    session.touch()
                    self._send(
                        HTTPStatus.CREATED,
                        _save_label(bundle, payload, input_dataset, canonical_source),
                    )
                    return
                if path == "/api/undo":
                    session.touch()
                    self._send(HTTPStatus.OK, _undo_last_label(bundle, input_dataset))
                    return
                self._send(HTTPStatus.NOT_FOUND, {"error": "API route not found"})
            except (ValueError, json.JSONDecodeError) as error:
                self._send(HTTPStatus.BAD_REQUEST, {"error": str(error)})
            except Exception as error:
                self._send(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    {"error": f"Unexpected server error: {error}"},
                )

    return Handler


def launch_labelling_app(
    labelling_bundle_path: str | Path = Path("ukam_labelling_bundle"),
    *,
    input_dataset_path: str | Path,
    input_dataset_label_column: str = "ukam_label",
    canonical_address_path: str | Path | None = None,
    port: int = 0,
    open_browser: bool = True,
) -> None:
    if not isinstance(port, int):
        raise TypeError("port must be an integer")
    if not 0 <= port <= 65535:
        raise ValueError("port must be between 0 and 65535")
    bundle = _load_bundle(labelling_bundle_path)
    input_dataset = _load_input_dataset(
        bundle,
        input_dataset_path,
        label_column=input_dataset_label_column,
    )
    _ensure_state_database(bundle)
    canonical_source = load_canonical_source(canonical_address_path)
    session = SessionState(600)
    server = ThreadingHTTPServer(
        ("127.0.0.1", port),
        _handler_factory(bundle, input_dataset, session, canonical_source),
    )
    url = f"http://127.0.0.1:{server.server_address[1]}/?token={session.token}"
    print(f"UKAM labelling tool: {url}", flush=True)  # noqa: T201
    print(  # noqa: T201
        "This session will remain live until 10 minutes after your last interaction.",
        flush=True,
    )
    stop = threading.Event()

    def watchdog() -> None:
        while not stop.wait(0.1):
            if session.is_expired():
                server.shutdown()
                return

    thread = threading.Thread(target=watchdog, daemon=True)
    thread.start()
    if open_browser:
        webbrowser.open(url)
    try:
        server.serve_forever(poll_interval=0.1)
    finally:
        stop.set()
        server.server_close()
        thread.join(timeout=2)


def main() -> None:
    parser = argparse.ArgumentParser(description="Launch the local UKAM labelling tool")
    parser.add_argument("--labelling-bundle", default="ukam_labelling_bundle")
    parser.add_argument("--input-dataset", required=True)
    parser.add_argument("--input-label-column", default="ukam_label")
    parser.add_argument("--canonical-address-path", default=None)
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--no-browser", action="store_true")
    arguments = parser.parse_args()
    launch_labelling_app(
        arguments.labelling_bundle,
        input_dataset_path=arguments.input_dataset,
        input_dataset_label_column=arguments.input_label_column,
        canonical_address_path=arguments.canonical_address_path,
        port=arguments.port,
        open_browser=not arguments.no_browser,
    )
