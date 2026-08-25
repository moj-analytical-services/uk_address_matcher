from __future__ import annotations

import argparse
import errno
import json
import mimetypes
import re
import webbrowser
from dataclasses import dataclass
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib.resources import files
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

import duckdb

from .updates import apply_labelling_updates

SUPPORTED_DATA_SUFFIXES = {".csv", ".parquet"}
CANONICAL_PAGE_SIZE = 100
_FULL_POSTCODE_PATTERN = re.compile(r"^(?:GIR 0AA|[A-Z][A-HJ-Y]?\d[A-Z\d]? \d[A-Z]{2})$")


def _write_body(output: Any, body: bytes) -> None:
    try:
        output.write(body)
    except OSError as error:
        if error.errno not in {errno.ECONNRESET, errno.EINVAL, errno.EPIPE}:
            raise


def _quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _is_prepared_canonical(canonical_paths: tuple[Path, ...]) -> bool:
    return bool(canonical_paths) and all(
        path.name == "ukam_canonical_addresses.parquet"
        or path.parent.name == "ukam_canonical_addresses_chunks"
        for path in canonical_paths
    )


def _canonical_search_payload(
    canonical_paths: tuple[Path, ...],
    query: dict[str, list[str]],
) -> dict[str, Any]:
    prepared = _is_prepared_canonical(canonical_paths)
    if not canonical_paths:
        raise ValueError("Canonical search is not available")
    unique_id_query = query.get("unique_id_query", [""])[0].strip()
    postcode = "".join(query.get("postcode", [""])[0].upper().split())
    address = " ".join(query.get("address_query", [""])[0].split())
    if any(len(value) > 100 for value in (unique_id_query, address)):
        raise ValueError(
            "Canonical search values must contain no more than 100 characters"
        )
    if not unique_id_query and not postcode and not address:
        raise ValueError(
            "Enter a unique ID, postcode, or address value before searching."
        )
    try:
        page = max(1, int(query.get("page", ["1"])[0]))
    except ValueError as error:
        raise ValueError("Canonical page must be an integer") from error
    connection = duckdb.connect(":memory:")
    try:
        source = "read_parquet(?)"
        source_parameters = [[str(path) for path in canonical_paths]]
        columns = {
            row[0].lower(): row[0]
            for row in connection.execute(
                f"DESCRIBE SELECT * FROM {source}",
                source_parameters,
            ).fetchall()
        }
        required = {"unique_id", "postcode"}
        cleaned = next(
            (
                columns[name]
                for name in ("clean_full_address", "cleaned_full_address")
                if name in columns
            ),
            None,
        )
        if required - columns.keys() or cleaned is None:
            raise ValueError(
                "Canonical data is missing required unique_id, postcode, or cleaned address columns"
            )
        unique_id = columns["unique_id"]
        postcode_column = columns["postcode"]
        display = next(
            (
                columns[name]
                for name in ("original_address_concat", "address_concat")
                if name in columns
            ),
            cleaned,
        )
        additional = [
            columns[name]
            for name in ("classificationcode", "floorlevel")
            if name in columns
        ]
        conditions = [f"{_quote_identifier(unique_id)} IS NOT NULL"]
        parameters: list[Any] = []
        if unique_id_query:
            conditions.append(
                f"contains(upper(CAST({_quote_identifier(unique_id)} AS VARCHAR)), upper(?))"
            )
            parameters.append(unique_id_query)
        if postcode:
            postcode_identifier = _quote_identifier(postcode_column)
            formatted_postcode = (
                postcode if len(postcode) <= 3 else f"{postcode[:-3]} {postcode[-3:]}"
            )
            if prepared and _FULL_POSTCODE_PATTERN.fullmatch(formatted_postcode):
                conditions.append(f"{postcode_identifier} = ?")
                parameters.append(formatted_postcode)
            elif prepared:
                conditions.append(f"contains(replace({postcode_identifier}, ' ', ''), ?)")
                parameters.append(postcode)
            else:
                conditions.append(
                    f"contains(upper(replace(CAST({postcode_identifier} AS VARCHAR), ' ', '')), ?)"
                )
                parameters.append(postcode)
        for token in address.split():
            if prepared:
                conditions.append(f"contains({_quote_identifier(cleaned)}, ?)")
                parameters.append(token.upper())
            else:
                conditions.append(
                    f"contains(upper(CAST({_quote_identifier(cleaned)} AS VARCHAR)), upper(?))"
                )
                parameters.append(token)
        additional_sql = "".join(
            f", CAST({_quote_identifier(column)} AS VARCHAR) AS {_quote_identifier(column)}"
            for column in additional
        )
        cursor = connection.execute(
            f"""
            SELECT CAST({_quote_identifier(unique_id)} AS VARCHAR) AS canonical_id,
                CAST({_quote_identifier(display)} AS VARCHAR) AS canonical_address,
                CAST({_quote_identifier(cleaned)} AS VARCHAR) AS cleaned_address,
                CAST({_quote_identifier(postcode_column)} AS VARCHAR) AS canonical_postcode
                {additional_sql}
            FROM {source}
            WHERE {" AND ".join(conditions)}
            ORDER BY canonical_postcode, cleaned_address, canonical_address, canonical_id
            LIMIT ? OFFSET ?
            """,
            [
                *source_parameters,
                *parameters,
                CANONICAL_PAGE_SIZE + 1,
                (page - 1) * CANONICAL_PAGE_SIZE,
            ],
        )
        names = [column[0] for column in cursor.description]
        rows = [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]
    finally:
        connection.close()
    return {
        "page": page,
        "page_size": CANONICAL_PAGE_SIZE,
        "has_previous": page > 1,
        "has_next": len(rows) > CANONICAL_PAGE_SIZE,
        "unique_id_query": unique_id_query,
        "postcode": postcode,
        "address_query": address,
        "additional_canonical_columns": additional,
        "rows": rows[:CANONICAL_PAGE_SIZE],
    }


@dataclass(frozen=True)
class LocalLabellingFiles:
    bundle_root: Path | None
    manifest_path: Path | None
    review_path: Path | None
    canonical_paths: tuple[Path, ...]
    input_path: Path | None
    input_label_column: str


def _data_file_from_manifest(bundle_root: Path) -> tuple[Path, dict[str, Any]]:
    manifest_path = bundle_root / "manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(f"Bundle manifest not found: {manifest_path}")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise ValueError(f"Bundle manifest is not valid JSON: {manifest_path}") from error
    if not isinstance(manifest, dict) or not manifest.get("bundle_id"):
        raise ValueError("Bundle manifest must contain a bundle_id")
    data_file = (bundle_root / manifest.get("data_file", "review_data.parquet")).resolve()
    if not data_file.is_relative_to(bundle_root) or not data_file.is_file():
        raise FileNotFoundError(f"Review data file not found: {data_file}")
    if data_file.suffix.lower() not in SUPPORTED_DATA_SUFFIXES:
        raise ValueError(f"Review data must be CSV or Parquet: {data_file}")
    return data_file, manifest


def _canonical_files(path: str | Path | None) -> tuple[Path, ...]:
    if path is None:
        return ()
    canonical_path = Path(path).expanduser().resolve()
    if canonical_path.is_file():
        paths = (canonical_path,)
    elif canonical_path.is_dir():
        chunk_dir = canonical_path / "ukam_canonical_addresses_chunks"
        if chunk_dir.is_dir():
            paths = tuple(sorted(chunk_dir.glob("*.parquet")))
        else:
            single_path = canonical_path / "ukam_canonical_addresses.parquet"
            paths = (
                (single_path,)
                if single_path.is_file()
                else tuple(sorted(canonical_path.glob("*.parquet")))
            )
    else:
        raise FileNotFoundError(f"Canonical address data not found: {canonical_path}")
    if not paths:
        raise ValueError(f"No canonical Parquet files found in: {canonical_path}")
    if any(path.suffix.lower() != ".parquet" for path in paths):
        raise ValueError("Canonical address data must be supplied as Parquet files")
    return paths


def _local_files(
    labelling_bundle_path: str | Path | None,
    input_dataset_path: str | Path | None,
    input_dataset_label_column: str,
    canonical_address_path: str | Path | None,
) -> LocalLabellingFiles:
    bundle_root = None
    manifest_path = None
    review_path = None
    if labelling_bundle_path is not None:
        bundle_root = Path(labelling_bundle_path).expanduser().resolve()
        if not bundle_root.is_dir():
            raise NotADirectoryError(
                f"Labelling bundle must be a directory: {bundle_root}"
            )
        review_path, _ = _data_file_from_manifest(bundle_root)
        manifest_path = bundle_root / "manifest.json"
    input_path = None
    if input_dataset_path is not None:
        input_path = Path(input_dataset_path).expanduser().resolve()
        if not input_path.is_file():
            raise FileNotFoundError(f"Input dataset not found: {input_path}")
        if input_path.suffix.lower() not in SUPPORTED_DATA_SUFFIXES:
            raise ValueError(f"Input dataset must be CSV or Parquet: {input_path}")
    return LocalLabellingFiles(
        bundle_root,
        manifest_path,
        review_path,
        _canonical_files(canonical_address_path),
        input_path,
        input_dataset_label_column,
    )


def _static_root() -> Path:
    package_root = Path(str(files("uk_address_matcher.labelling.app")))
    static_root = package_root / "static"
    if (static_root / "index.html").is_file():
        return static_root
    raise FileNotFoundError(
        "The labelling app assets are not built. Run `npm ci && npm run build` first."
    )


def _updates_payload(bundle_id: str, events: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "bundle_id": bundle_id,
        "exported_at_utc": datetime.now(timezone.utc).isoformat(),
        "events": events,
    }


def _read_events(fileset: LocalLabellingFiles) -> list[dict[str, Any]]:
    if fileset.bundle_root is None:
        return []
    updates_path = fileset.bundle_root / "labelling_updates.json"
    if not updates_path.is_file():
        return []
    payload = json.loads(updates_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or not isinstance(payload.get("events"), list):
        raise ValueError(f"Invalid labelling updates file: {updates_path}")
    return [event for event in payload["events"] if isinstance(event, dict)]


def _write_events(fileset: LocalLabellingFiles, events: list[dict[str, Any]]) -> None:
    if fileset.bundle_root is None:
        return
    manifest = json.loads((fileset.bundle_root / "manifest.json").read_text())
    updates_path = fileset.bundle_root / "labelling_updates.json"
    temporary_path = updates_path.with_suffix(".tmp")
    temporary_path.write_text(
        json.dumps(_updates_payload(manifest["bundle_id"], events), indent=2) + "\n",
        encoding="utf-8",
    )
    temporary_path.replace(updates_path)


def _apply_input_dataset(fileset: LocalLabellingFiles) -> None:
    if fileset.bundle_root is None or fileset.input_path is None:
        return
    apply_labelling_updates(
        fileset.bundle_root,
        fileset.bundle_root / "labelling_updates.json",
        fileset.input_path,
        input_dataset_label_column=fileset.input_label_column,
    )


def _handler_factory(fileset: LocalLabellingFiles, static_root: Path):
    local_files: dict[str, Path] = {}
    if fileset.manifest_path is not None:
        local_files["manifest.json"] = fileset.manifest_path
        local_files["review_data" + fileset.review_path.suffix] = fileset.review_path
        for index, path in enumerate(fileset.canonical_paths):
            local_files[f"canonical/{index}{path.suffix}"] = path

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format_string: str, *args: Any) -> None:
            return

        def _send(self, status: HTTPStatus, payload: Any) -> None:
            body = json.dumps(payload, default=str).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            _write_body(self.wfile, body)

        def _send_file(self, path: Path, content_type: str | None = None) -> None:
            body = path.read_bytes()
            self.send_response(HTTPStatus.OK)
            self.send_header(
                "Content-Type",
                content_type
                or mimetypes.guess_type(path.name)[0]
                or "application/octet-stream",
            )
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            _write_body(self.wfile, body)

        def _config(self) -> dict[str, Any]:
            if fileset.manifest_path is None:
                return {"bundle": None}
            return {
                "bundle": {
                    "manifest_url": "/api/local-file/manifest.json",
                    "review_url": "/api/local-file/review_data"
                    + fileset.review_path.suffix,
                    "manifest_name": fileset.manifest_path.name,
                    "review_name": fileset.review_path.name,
                },
                "canonical_urls": [
                    {
                        "url": f"/api/local-file/canonical/{index}{path.suffix}",
                        "name": path.name,
                    }
                    for index, path in enumerate(fileset.canonical_paths)
                ],
                "canonical_search_url": (
                    "/api/canonical-search" if fileset.canonical_paths else None
                ),
                "events_url": "/api/events",
            }

        def _save_event(self, event: dict[str, Any]) -> None:
            events = _read_events(fileset)
            events = [
                item for item in events if item.get("event_id") != event.get("event_id")
            ]
            events.append(event)
            _write_events(fileset, events)
            try:
                _apply_input_dataset(fileset)
            except Exception:
                _write_events(fileset, [item for item in events if item is not event])
                raise

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path == "/api/local-config":
                self._send(HTTPStatus.OK, self._config())
                return
            if parsed.path == "/api/events":
                self._send(HTTPStatus.OK, {"events": _read_events(fileset)})
                return
            if parsed.path == "/api/canonical-search":
                try:
                    self._send(
                        HTTPStatus.OK,
                        _canonical_search_payload(
                            fileset.canonical_paths,
                            parse_qs(parsed.query, keep_blank_values=True),
                        ),
                    )
                except ValueError as error:
                    self._send(HTTPStatus.BAD_REQUEST, {"error": str(error)})
                return
            if parsed.path.startswith("/api/local-file/"):
                key = unquote(parsed.path.removeprefix("/api/local-file/"))
                path = local_files.get(key)
                if path is None:
                    self.send_error(HTTPStatus.NOT_FOUND)
                else:
                    self._send_file(path)
                return
            relative = unquote(parsed.path.removeprefix("/")) or "index.html"
            path = (static_root / relative).resolve()
            if not path.is_relative_to(static_root) or not path.is_file():
                self.send_error(HTTPStatus.NOT_FOUND)
                return
            self._send_file(path)

        def do_POST(self) -> None:
            if urlparse(self.path).path != "/api/events":
                self.send_error(HTTPStatus.NOT_FOUND)
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                event = json.loads(self.rfile.read(length))
                if not isinstance(event, dict) or not event.get("event_id"):
                    raise ValueError("Invalid labelling event")
                self._save_event(event)
            except Exception as error:
                self._send(HTTPStatus.BAD_REQUEST, {"error": str(error)})
                return
            self._send(HTTPStatus.OK, {"saved": True})

        def do_DELETE(self) -> None:
            if urlparse(self.path).path != "/api/events":
                self.send_error(HTTPStatus.NOT_FOUND)
                return
            event_id = parse_qs(urlparse(self.path).query).get("event_id", [""])[0]
            events = _read_events(fileset)
            remaining = [event for event in events if event.get("event_id") != event_id]
            _write_events(fileset, remaining)
            try:
                _apply_input_dataset(fileset)
            except Exception as error:
                _write_events(fileset, events)
                self._send(HTTPStatus.BAD_REQUEST, {"error": str(error)})
                return
            self._send(HTTPStatus.OK, {"deleted": event_id})

    return Handler


def _launch_labelling_app_beta(
    labelling_bundle_path: str | Path | None = None,
    *,
    input_dataset_path: str | Path | None = None,
    input_dataset_label_column: str = "ukam_label",
    canonical_address_path: str | Path | None = None,
    port: int = 0,
    open_browser: bool = True,
) -> None:
    if not 0 <= port <= 65535:
        raise ValueError("port must be between 0 and 65535")
    fileset = _local_files(
        labelling_bundle_path,
        input_dataset_path,
        input_dataset_label_column,
        canonical_address_path,
    )
    server = ThreadingHTTPServer(
        ("127.0.0.1", port), _handler_factory(fileset, _static_root())
    )
    url = f"http://127.0.0.1:{server.server_address[1]}/"
    print(f"UKAM labelling tool: {url}", flush=True)  # noqa: T201
    if labelling_bundle_path is None:
        print("Select a labelling bundle from the browser page.", flush=True)  # noqa: T201
    if open_browser:
        webbrowser.open(url)
    try:
        server.serve_forever(poll_interval=0.1)
    finally:
        server.server_close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Launch the local UKAM labelling tool")
    parser.add_argument("--labelling-bundle", type=Path)
    parser.add_argument("--input-dataset", type=Path)
    parser.add_argument("--input-label-column", default="ukam_label")
    parser.add_argument("--canonical-address-path", type=Path)
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--no-browser", action="store_true")
    arguments = parser.parse_args()
    _launch_labelling_app_beta(
        arguments.labelling_bundle,
        input_dataset_path=arguments.input_dataset,
        input_dataset_label_column=arguments.input_label_column,
        canonical_address_path=arguments.canonical_address_path,
        port=arguments.port,
        open_browser=not arguments.no_browser,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
