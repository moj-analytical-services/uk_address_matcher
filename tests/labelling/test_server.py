from __future__ import annotations

import errno
import json
from http.client import HTTPConnection
from pathlib import Path
from threading import Thread

import duckdb

from tests.labelling.test_updates import create_test_bundle
from uk_address_matcher.labelling.server import (
    _canonical_files,
    _handler_factory,
    _local_files,
    _write_body,
)


def request(server, method: str, path: str, body: dict[str, object] | None = None):
    connection = HTTPConnection(*server.server_address)
    payload = None if body is None else json.dumps(body).encode()
    connection.request(
        method,
        path,
        body=payload,
        headers={"Content-Type": "application/json"} if payload else {},
    )
    response = connection.getresponse()
    result = response.status, json.loads(response.read())
    connection.close()
    return result


def test_response_body_ignores_expected_client_disconnect() -> None:
    class DisconnectingWriter:
        def write(self, body: bytes) -> None:
            raise OSError(errno.EINVAL, "client disconnected")

    _write_body(DisconnectingWriter(), b"response")


def test_local_file_server_streams_complete_file(tmp_path: Path) -> None:
    static = tmp_path / "static"
    static.mkdir()
    body = b"a" * (128 * 1024 + 1)
    (static / "data.bin").write_bytes(body)
    from http.server import ThreadingHTTPServer

    server = ThreadingHTTPServer(
        ("127.0.0.1", 0),
        _handler_factory(_local_files(None, None), static),
    )
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        connection = HTTPConnection(*server.server_address)
        connection.request("GET", "/data.bin")
        response = connection.getresponse()
        assert response.status == 200
        assert response.getheader("Content-Length") == str(len(body))
        assert response.read() == body
        connection.close()
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_canonical_directory_excludes_prepared_support_files(tmp_path: Path) -> None:
    canonical_root = tmp_path / "canonical"
    canonical_root.mkdir()
    (canonical_root / "ukam_canonical_addresses.parquet").touch()
    (canonical_root / "ukam_inverted_index.parquet").touch()
    (canonical_root / "ukam_term_frequencies.parquet").touch()

    assert _canonical_files(canonical_root) == (
        canonical_root / "ukam_canonical_addresses.parquet",
    )


def test_local_canonical_search_projects_required_columns(tmp_path: Path) -> None:
    canonical_file = tmp_path / "canonical.parquet"
    connection = duckdb.connect()
    try:
        connection.execute(
            """
            COPY (
                SELECT 1::BIGINT AS unique_id,
                    '1 TEST ROAD' AS original_address_concat,
                    '1 TEST ROAD' AS clean_full_address,
                    'E1 1AA' AS postcode,
                    'RD06' AS classificationcode,
                    '2' AS floorlevel
            ) TO ? (FORMAT PARQUET)
            """,
            [str(canonical_file)],
        )
    finally:
        connection.close()
    static = tmp_path / "static"
    static.mkdir()
    (static / "index.html").write_text("<!doctype html>", encoding="utf-8")
    from http.server import ThreadingHTTPServer

    fileset = _local_files(None, canonical_file)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _handler_factory(fileset, static))
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        status, payload = request(server, "GET", "/api/canonical-search?postcode=E1+1AA")
        assert status == 200
        assert payload["rows"] == [
            {
                "canonical_id": "1",
                "canonical_unique_id": "1",
                "canonical_address": "1 TEST ROAD",
                "cleaned_address": "1 TEST ROAD",
                "canonical_postcode": "E1 1AA",
                "classificationcode": "RD06",
                "floorlevel": "2",
            }
        ]
        assert payload["additional_canonical_columns"] == [
            "classificationcode",
            "floorlevel",
        ]
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_local_canonical_parquet_enables_native_search(tmp_path: Path) -> None:
    bundle = create_test_bundle(tmp_path / "bundle")
    canonical_root = tmp_path / "canonical"
    canonical_root.mkdir()
    canonical_path = canonical_root / "ukam_canonical_addresses.parquet"
    connection = duckdb.connect()
    try:
        connection.execute(
            """
            COPY (
                SELECT 1::BIGINT AS unique_id,
                '1 TEST ROAD' AS original_address_concat,
                '1 TEST ROAD' AS clean_full_address,
                'E1 1AA' AS postcode,
                'RD06' AS classificationcode,
                '2' AS floorlevel
            ) TO ? (FORMAT PARQUET)
            """,
            [str(canonical_path)],
        )
    finally:
        connection.close()
    static = tmp_path / "static"
    static.mkdir()
    (static / "index.html").write_text("<!doctype html>", encoding="utf-8")
    from http.server import ThreadingHTTPServer

    fileset = _local_files(bundle, canonical_root)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _handler_factory(fileset, static))
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        status, config = request(server, "GET", "/api/local-config")
        assert status == 200
        assert config["canonical_urls"] == [
            {
                "url": "/api/local-file/canonical/0.parquet",
                "name": "ukam_canonical_addresses.parquet",
            }
        ]
        assert config["canonical_search_url"] == "/api/canonical-search"
        status, payload = request(server, "GET", "/api/canonical-search?postcode=E1+1AA")
        assert status == 200
        assert payload["rows"][0]["canonical_id"] == "1"
        status, payload = request(
            server, "GET", "/api/canonical-search?unique_id_query=1"
        )
        assert status == 200
        assert payload["rows"][0]["canonical_id"] == "1"
        status, payload = request(
            server, "GET", "/api/canonical-search?address_query=test+road"
        )
        assert status == 200
        assert payload["rows"][0]["canonical_id"] == "1"
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_local_launcher_prefers_bundle_canonical_lookup(tmp_path: Path) -> None:
    bundle = create_test_bundle(tmp_path / "bundle")
    canonical_lookup = bundle / "canonical_data.parquet"
    connection = duckdb.connect()
    try:
        connection.execute(
            """
            COPY (
                SELECT '1' AS ukam_address_id,
                    'canonical-1' AS unique_id,
                    '1 TEST ROAD' AS original_address_concat,
                    '1 TEST ROAD' AS clean_full_address,
                    'E1 1AA' AS postcode
            ) TO ? (FORMAT PARQUET)
            """,
            [str(canonical_lookup)],
        )
    finally:
        connection.close()
    manifest_path = bundle / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest.update(
        {
            "canonical_label_column": "ukam_address_id",
            "canonical_data_file": canonical_lookup.name,
        }
    )
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    static = tmp_path / "static"
    static.mkdir()
    (static / "index.html").write_text("<!doctype html>", encoding="utf-8")
    from http.server import ThreadingHTTPServer

    server = ThreadingHTTPServer(
        ("127.0.0.1", 0),
        _handler_factory(_local_files(bundle, None), static),
    )
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        status, config = request(server, "GET", "/api/local-config")
        assert status == 200
        assert config["canonical_urls"] == [
            {
                "url": "/api/local-file/canonical_data.parquet",
                "name": "canonical_data.parquet",
            }
        ]
        assert config["canonical_search_url"] is None
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_local_launcher_persists_events_without_applying_input_dataset(
    tmp_path: Path,
) -> None:
    from http.server import ThreadingHTTPServer

    bundle = create_test_bundle(tmp_path / "bundle")
    input_file = tmp_path / "input.csv"
    input_file.write_text(
        "unique_id,review_label\nmessy-1,old\nmessy-2,keep\n", encoding="utf-8"
    )
    static = tmp_path / "static"
    static.mkdir()
    (static / "index.html").write_text("<!doctype html>", encoding="utf-8")
    fileset = _local_files(bundle, None)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _handler_factory(fileset, static))
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        status, config = request(server, "GET", "/api/local-config")
        assert status == 200
        assert config["bundle"]["manifest_url"] == "/api/local-file/manifest.json"
        assert config["canonical_urls"] == []
        assert config["labelled_review_path"] == str(
            bundle / "labelled_review_data.parquet"
        )

        event = {
            "event_id": "event-1",
            "bundle_id": "bundle-1",
            "unique_id": "messy-1",
            "decision": "accept_model",
            "ukam_label": "label-1",
            "clean_full_address": "1 TEST ROAD LONDON",
            "postcode": "E1 1AA",
            "selected_candidate_rank": None,
            "created_at_utc": "2026-08-19T12:00:00Z",
        }
        status, payload = request(server, "POST", "/api/events", event)
        assert status == 200, payload
        assert payload == {"saved": True}
        assert json.loads((bundle / "labelling_updates.json").read_text())["events"] == [
            event
        ]
        connection = duckdb.connect()
        try:
            assert connection.execute(
                "SELECT ukam_label, ukam_user_label, "
                "ukam_user_label_clean_full_address, ukam_user_label_postcode "
                "FROM read_parquet(?) "
                "WHERE unique_id = 'messy-1'",
                [str(bundle / "labelled_review_data.parquet")],
            ).fetchone() == (
                None,
                "label-1",
                "1 TEST ROAD LONDON",
                "E1 1AA",
            )
        finally:
            connection.close()
        invalid_event = {**event, "event_id": "event-2", "bundle_id": "wrong"}
        status, payload = request(server, "POST", "/api/events", invalid_event)
        assert status == 400
        assert "bundle_id" in payload["error"]
        assert json.loads((bundle / "labelling_updates.json").read_text())["events"] == [
            event
        ]
        connection = duckdb.connect()
        try:
            assert connection.execute(
                "SELECT review_label FROM read_csv_auto(?)", [str(input_file)]
            ).fetchone() == ("old",)
        finally:
            connection.close()

        status, payload = request(server, "GET", "/api/events")
        assert status == 200
        assert payload["events"] == [event]
        status, payload = request(server, "DELETE", "/api/events?event_id=event-1")
        assert status == 200
        connection = duckdb.connect()
        try:
            assert connection.execute(
                "SELECT ukam_user_label FROM read_parquet(?) WHERE unique_id = 'messy-1'",
                [str(bundle / "labelled_review_data.parquet")],
            ).fetchone() == (None,)
        finally:
            connection.close()
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_local_launcher_without_bundle_keeps_file_picker_available(
    tmp_path: Path,
) -> None:
    from http.server import ThreadingHTTPServer

    static = tmp_path / "static"
    static.mkdir()
    (static / "index.html").write_text("<!doctype html>", encoding="utf-8")
    fileset = _local_files(None, None)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _handler_factory(fileset, static))
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        status, config = request(server, "GET", "/api/local-config")
        assert status == 200
        assert config == {"bundle": None}
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
