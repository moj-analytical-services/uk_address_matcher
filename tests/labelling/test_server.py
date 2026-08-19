from __future__ import annotations

import csv
import json
import threading
import time
from collections.abc import Iterator
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import duckdb
import pytest

import uk_address_matcher.labelling.server as server_module
from tests.labelling.test_state import create_test_bundle
from uk_address_matcher.labelling.canonical import load_canonical_source
from uk_address_matcher.labelling.server import (
    InputDataset,
    SessionState,
    _bootstrap_payload,
    _ensure_state_database,
    _handler_factory,
    _launch_labelling_app_beta,
    _load_bundle,
    _load_input_dataset,
    _records_payload,
    _ReviewPrefetchCache,
    _save_label,
)


def create_api_test_bundle(root: Path, *, include_unmatched: bool = False) -> Path:
    root.mkdir()
    data_file = root / "review_data.parquet"
    first_imported_label = "'label-imported'" if include_unmatched else "NULL::VARCHAR"
    first_has_existing_label = "TRUE" if include_unmatched else "FALSE"
    unmatched_record = (
        """
                UNION ALL
                SELECT
                    'bundle-1', '1.2.3', CURRENT_TIMESTAMP, 'messy-unmatched',
                    'UNMATCHED TEST ROAD', 'UNMATCHED TEST ROAD', 'E1 1AC',
                    'label-unmatched', TRUE, NULL::VARCHAR, NULL::VARCHAR,
                    NULL::VARCHAR, NULL::VARCHAR, 'No candidate match', 'unmatched',
                    FALSE, NULL::DOUBLE, NULL::DOUBLE, 0, []
        """
        if include_unmatched
        else ""
    )
    connection = duckdb.connect()
    try:
        connection.execute(
            (
                """COPY (
                SELECT
                    'bundle-1' AS bundle_id,
                    '1.2.3' AS uk_address_matcher_version,
                    CURRENT_TIMESTAMP AS created_at_utc,
                    'messy-1' AS unique_id,
                    '1 TEST ROAD' AS messy_address,
                    '1 TEST ROAD' AS messy_cleaned_address,
                    'E1 1AA' AS messy_postcode,
                    __FIRST_IMPORTED_LABEL__ AS ukam_label,
                    __FIRST_HAS_EXISTING_LABEL__ AS has_existing_label,
                    'canonical-1' AS resolved_canonical_id,
                    'label-1' AS resolved_label_id,
                    '1 TEST ROAD LONDON' AS resolved_canonical_address,
                    'E1 1AA' AS resolved_canonical_postcode,
                    'splink: probabilistic match' AS match_reason,
                    'splink' AS match_stage,
                    TRUE AS is_matched,
                    12.5 AS match_weight,
                    2.1 AS distinguishability,
                    2 AS candidate_count,
                    [
                        {'rank': 1::BIGINT, 'label_id': 'label-1'::VARCHAR,
                         'splink_match_weight': 10.0::DOUBLE},
                        {'rank': 2::BIGINT, 'label_id': 'label-2'::VARCHAR,
                         'splink_match_weight': 8.0::DOUBLE}
                    ] AS top_candidates
                UNION ALL
                SELECT
                    'bundle-1', '1.2.3', CURRENT_TIMESTAMP, 'messy-2',
                    '2 TEST ROAD', '2 TEST ROAD', 'E1 1AB', NULL::VARCHAR,
                    FALSE, 'canonical-2', 'label-3', '2 TEST ROAD LONDON',
                    'E1 1AB', 'exact: full match', 'exact', TRUE,
                    NULL::DOUBLE, NULL::DOUBLE, 1,
                                        [{'rank': 1::BIGINT,
                                            'label_id': 'label-3'::VARCHAR,
                                            'splink_match_weight': 8.0::DOUBLE}]
"""
                + unmatched_record
                + """
                ) TO ? (FORMAT PARQUET)"""
            )
            .replace("__FIRST_IMPORTED_LABEL__", first_imported_label)
            .replace("__FIRST_HAS_EXISTING_LABEL__", first_has_existing_label),
            [str(data_file)],
        )
    finally:
        connection.close()
    (root / "manifest.json").write_text(
        json.dumps({"bundle_id": "bundle-1", "data_file": "review_data.parquet"}),
        encoding="utf-8",
    )
    return root


@pytest.fixture
def running_app(tmp_path: Path) -> Iterator[tuple[str, SessionState, Path]]:
    bundle = _load_bundle(create_api_test_bundle(tmp_path / "bundle"))
    _ensure_state_database(bundle)
    input_file = tmp_path / "input.csv"
    input_file.write_text(
        "unique_id,review_label\nmessy-1,\nmessy-2,\n",
        encoding="utf-8",
    )
    input_dataset = _load_input_dataset(bundle, input_file, label_column="review_label")
    session = SessionState(idle_timeout_seconds=600)
    server = ThreadingHTTPServer(
        ("127.0.0.1", 0),
        _handler_factory(bundle, input_dataset, session),
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address[:2]
    try:
        yield f"http://{host}:{port}", session, input_file
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


def request(
    base_url: str,
    path: str,
    *,
    token: str | None = None,
    method: str = "GET",
    payload: dict[str, object] | None = None,
) -> tuple[int, dict[str, object] | str]:
    headers = {"Content-Type": "application/json"}
    if token is not None:
        headers["X-UKAM-Session-Token"] = token
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    request_object = Request(
        f"{base_url}{path}", data=data, headers=headers, method=method
    )
    try:
        with urlopen(request_object) as response:
            body = response.read().decode("utf-8")
            return response.status, json.loads(body) if body.startswith("{") else body
    except HTTPError as error:
        body = error.read().decode("utf-8")
        return error.code, json.loads(body) if body else ""


def test_server_requires_token_and_serves_application_shell(
    running_app: tuple[str, SessionState, Path],
) -> None:
    base_url, session, _ = running_app

    status, payload = request(base_url, "/")
    assert status == 403
    assert payload == {"error": "Invalid or missing session token"}

    status, payload = request(base_url, f"/?token={session.token}")
    assert status == 200
    assert isinstance(payload, str)
    assert 'id="score-range-min"' in payload
    assert 'id="review-current-label-value"' in payload
    assert 'id="review-canonical-additional-fields"' in payload
    assert 'id="canonical-results-header-row"' in payload
    assert 'id="review-cards"' in payload
    assert 'id="review-sticky-context"' in payload
    assert 'id="review-sticky-messy-address"' in payload
    assert 'id="review-sticky-canonical-address"' in payload
    assert payload.index('id="review-sticky-context"') < payload.index(
        'id="review-cards"'
    )
    assert 'id="review-next"' in payload
    assert 'id="review-complete"' in payload
    assert 'id="review-content"' in payload
    assert 'id="review-search-canonical"' not in payload

    status, payload = request(base_url, "/app.css")
    assert status == 200
    assert ".review-sticky-context {\n  position: sticky;\n  top: 117px;" in payload
    assert "align-self: start" in payload
    assert ".review-sticky-context.is-active" not in payload
    assert "grid-template-columns: minmax(0, 1fr) minmax(0, 1fr)" in payload
    assert "#review-canonical-card .review-canonical-additional-field" in payload
    assert "grid-template-rows: auto auto" in payload

    status, payload = request(base_url, "/app.js")
    assert status == 200
    assert "IntersectionObserver" not in payload
    assert "setReviewStickyActive" not in payload
    assert "Predicted value -" in payload
    assert "function resetReviewScroll()" in payload
    assert 'window.scrollTo({ top: 0, behavior: "auto" })' in payload
    assert "reviewSearch" not in payload
    assert "review-sticky-sentinel" not in payload

    status, payload = request(base_url, "/api/bootstrap", token=session.token)
    assert status == 200
    assert payload["total_records"] == 2
    assert payload["stage_counts"] == {"exact": 1, "splink": 1}
    assert payload["idle_timeout_seconds"] == 600


def test_records_and_review_share_score_and_stage_filters(
    running_app: tuple[str, SessionState, Path],
) -> None:
    base_url, session, _ = running_app
    filter_query = "stage=splink&score_min=10&score_max=20&show_labelled=false"

    status, payload = request(
        base_url,
        f"/api/records?{filter_query}",
        token=session.token,
    )
    assert status == 200
    assert payload["total_filtered"] == 1
    assert [record["unique_id"] for record in payload["rows"]] == ["messy-1"]

    status, payload = request(
        base_url,
        f"/api/review-record?unique_id=messy-1&{filter_query}",
        token=session.token,
    )
    assert status == 200
    assert payload["record"]["messy_cleaned_address"] == "1 TEST ROAD"
    assert [candidate["label_id"] for candidate in payload["record"]["candidates"]] == [
        "label-1",
        "label-2",
    ]
    assert payload["navigation"] == {
        "position": 1,
        "total": 1,
        "previous_unique_id": None,
        "next_unique_id": None,
    }
    status, payload = request(
        base_url,
        "/api/review-record?unique_id=messy-2&stage=splink",
        token=session.token,
    )
    assert status == 400
    assert payload == {
        "error": "The requested record does not exist in the current filtered review set"
    }


def test_review_prefetch_cache_stores_the_next_review_payload(tmp_path: Path) -> None:
    bundle = _load_bundle(create_api_test_bundle(tmp_path / "bundle"))
    _ensure_state_database(bundle)
    cache = _ReviewPrefetchCache(bundle, None, delay_seconds=0)
    try:
        cache.schedule({}, "messy-2")
        deadline = time.monotonic() + 1
        payload = None
        while time.monotonic() < deadline and payload is None:
            payload = cache.get({}, "messy-2")
            time.sleep(0.01)
        assert payload is not None
        assert payload["record"]["unique_id"] == "messy-2"
        assert payload["navigation"]["next_unique_id"] is None
    finally:
        cache.close()


def test_records_support_score_sorting_and_mismatch_filter(tmp_path: Path) -> None:
    api_bundle = _load_bundle(create_api_test_bundle(tmp_path / "api-bundle"))
    _ensure_state_database(api_bundle)

    descending = _records_payload(
        api_bundle,
        {"sort_by": ["splink_score"], "sort_order": ["desc"]},
    )
    ascending = _records_payload(
        api_bundle,
        {"sort_by": ["splink_score"], "sort_order": ["asc"]},
    )
    assert [row["unique_id"] for row in descending["rows"]] == [
        "messy-1",
        "messy-2",
    ]
    assert [row["unique_id"] for row in ascending["rows"]] == [
        "messy-2",
        "messy-1",
    ]
    assert descending["rows"][0]["splink_match_weight"] == 10.0

    mismatch_bundle = _load_bundle(create_test_bundle(tmp_path / "mismatch-bundle"))
    _ensure_state_database(mismatch_bundle)
    mismatches = _records_payload(mismatch_bundle, {"mismatches_only": ["true"]})
    assert mismatches["total_filtered"] == 1
    assert mismatches["rows"][0]["unique_id"] == "messy-1"
    assert (
        _records_payload(
            mismatch_bundle,
            {"mismatches_only": ["true"], "show_labelled": ["false"]},
        )["total_filtered"]
        == 1
    )

    unmatched_bundle = _load_bundle(
        create_api_test_bundle(tmp_path / "unmatched-bundle", include_unmatched=True)
    )
    _ensure_state_database(unmatched_bundle)
    mismatches = _records_payload(unmatched_bundle, {"mismatches_only": ["true"]})
    assert mismatches["total_filtered"] == 1
    assert mismatches["rows"][0]["unique_id"] == "messy-1"

    filtered_mismatches = _records_payload(
        unmatched_bundle,
        {
            "mismatches_only": ["true"],
            "address_query": ["unmatched"],
        },
    )
    assert filtered_mismatches["total_filtered"] == 0


def test_record_text_filters_are_grouped_with_other_filters(tmp_path: Path) -> None:
    bundle = _load_bundle(create_api_test_bundle(tmp_path / "filter-bundle"))
    _ensure_state_database(bundle)

    records = _records_payload(
        bundle,
        {
            "address_query": ["road"],
            "stage": ["splink"],
        },
    )

    assert [row["unique_id"] for row in records["rows"]] == ["messy-1"]


def test_labels_endpoint_validates_candidates_and_writes_csv(
    running_app: tuple[str, SessionState, Path],
) -> None:
    base_url, session, input_file = running_app
    invalid_payload = {
        "unique_id": "messy-1",
        "decision": "select_candidate",
        "ukam_label": "not-a-candidate",
        "selected_candidate_rank": 2,
    }

    status, payload = request(
        base_url,
        "/api/labels",
        token=session.token,
        method="POST",
        payload=invalid_payload,
    )
    assert status == 400
    assert "not one of the exported candidates" in payload["error"]

    status, payload = request(
        base_url,
        "/api/labels",
        token=session.token,
        method="POST",
        payload={
            "unique_id": "messy-1",
            "decision": "select_candidate",
            "ukam_label": "label-2",
            "selected_candidate_rank": 2,
        },
    )
    assert status == 201
    assert payload["decision"] == "select_candidate"
    assert payload["ukam_label"] == "label-2"

    connection = duckdb.connect()
    try:
        label = connection.execute(
            "SELECT review_label FROM read_csv_auto(?) WHERE unique_id = 'messy-1'",
            [str(input_file)],
        ).fetchone()[0]
    finally:
        connection.close()
    assert label == "label-2"

    status, payload = request(
        base_url,
        "/api/records?stage=splink&show_labelled=false",
        token=session.token,
    )
    assert status == 200
    assert payload["total_filtered"] == 0


def test_undo_restores_the_previous_input_label(
    running_app: tuple[str, SessionState, Path],
) -> None:
    base_url, session, input_file = running_app
    status, _ = request(
        base_url,
        "/api/labels",
        token=session.token,
        method="POST",
        payload={
            "unique_id": "messy-1",
            "decision": "select_candidate",
            "ukam_label": "label-2",
            "selected_candidate_rank": 2,
        },
    )
    assert status == 201

    status, payload = request(
        base_url,
        "/api/undo",
        token=session.token,
        method="POST",
        payload={},
    )
    assert status == 200
    assert payload["unique_id"] == "messy-1"
    assert payload["ukam_label"] is None

    connection = duckdb.connect()
    try:
        label = connection.execute(
            "SELECT review_label FROM read_csv_auto(?) WHERE unique_id = 'messy-1'",
            [str(input_file)],
        ).fetchone()[0]
    finally:
        connection.close()
    assert label is None


def test_load_bundle_rejects_missing_folder(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="does not exist"):
        _load_bundle(tmp_path / "missing")


def test_load_bundle_rejects_missing_manifest(tmp_path: Path) -> None:
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    with pytest.raises(FileNotFoundError, match="manifest"):
        _load_bundle(bundle)


def test_load_bundle_accepts_csv_data(tmp_path: Path) -> None:
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    data_file = bundle / "review_data.csv"
    columns = {
        "bundle_id": "bundle-1",
        "uk_address_matcher_version": "1.2.3",
        "created_at_utc": "2026-07-28T00:00:00Z",
        "unique_id": "messy-1",
        "messy_address": "1 TEST ROAD",
        "messy_cleaned_address": "1 TEST ROAD",
        "messy_postcode": "E1 1AA",
        "ukam_label": "",
        "has_existing_label": "false",
        "resolved_canonical_id": "canonical-1",
        "resolved_label_id": "label-1",
        "resolved_canonical_address": "1 TEST ROAD LONDON",
        "resolved_canonical_postcode": "E1 1AA",
        "match_reason": "splink",
        "match_stage": "splink",
        "is_matched": "true",
        "match_weight": "12.5",
        "distinguishability": "2.1",
        "candidate_count": "1",
        "top_candidates": "[]",
    }
    with data_file.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()
        writer.writerow(columns)
    (bundle / "manifest.json").write_text(
        json.dumps({"bundle_id": "bundle-1", "data_file": data_file.name}),
        encoding="utf-8",
    )

    loaded_bundle = _load_bundle(bundle)
    _ensure_state_database(loaded_bundle)

    assert loaded_bundle.data_file == data_file.resolve()
    row = _records_payload(loaded_bundle, {})["rows"][0]
    assert row["unique_id"] == "messy-1"
    assert row["messy_cleaned_address"] == "1 TEST ROAD"


def test_load_bundle_rejects_unsupported_data_format(tmp_path: Path) -> None:
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    data_file = bundle / "review_data.json"
    data_file.write_text("{}", encoding="utf-8")
    (bundle / "manifest.json").write_text(
        json.dumps({"data_file": data_file.name}),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="CSV or Parquet"):
        _load_bundle(bundle)


@pytest.mark.parametrize("port", [-1, 65_536])
def test_launch_rejects_invalid_port(port: int) -> None:
    with pytest.raises(ValueError, match="port"):
        _launch_labelling_app_beta(
            input_dataset_path="messy_addresses.parquet",
            port=port,
            open_browser=False,
        )


def test_launch_passes_additional_canonical_columns_to_source_loader(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class FakeServer:
        server_address = ("127.0.0.1", 12345)

        def __init__(self, address: object, handler: object) -> None:
            captured["address"] = address
            captured["handler"] = handler

        def serve_forever(self, poll_interval: float) -> None:
            return

        def server_close(self) -> None:
            return

    def fake_load_canonical_source(
        _path: object, *, additional_canonical_columns: tuple[str, ...]
    ) -> None:
        captured["additional_canonical_columns"] = additional_canonical_columns

    monkeypatch.setattr(server_module, "_load_bundle", lambda _path: object())
    monkeypatch.setattr(
        server_module,
        "_load_input_dataset",
        lambda _bundle, _path, label_column: object(),
    )
    monkeypatch.setattr(server_module, "_ensure_state_database", lambda _bundle: None)
    monkeypatch.setattr(
        server_module,
        "_handler_factory",
        lambda _bundle, _input_dataset, _session, _canonical_source: object(),
    )
    monkeypatch.setattr(server_module, "ThreadingHTTPServer", FakeServer)
    monkeypatch.setattr(
        server_module, "load_canonical_source", fake_load_canonical_source
    )

    _launch_labelling_app_beta(
        input_dataset_path="messy_addresses.parquet",
        canonical_address_path="canonical.parquet",
        additional_canonical_columns=("floorlevel",),
        open_browser=False,
    )

    assert captured["additional_canonical_columns"] == ("floorlevel",)


def test_session_state_expires_after_configured_interval() -> None:
    session = SessionState(idle_timeout_seconds=0.01)

    time.sleep(0.02)

    assert session.is_expired()


def test_bootstrap_only_returns_match_stages_present_in_bundle(tmp_path: Path) -> None:
    bundle = _load_bundle(create_test_bundle(tmp_path / "bundle"))
    _ensure_state_database(bundle)

    bootstrap = _bootstrap_payload(bundle, SessionState(idle_timeout_seconds=600))

    assert bootstrap["stage_counts"] == {"splink": 1}


def test_score_filters_keep_selected_non_splink_stages(tmp_path: Path) -> None:
    bundle_path = create_test_bundle(tmp_path / "bundle")
    data_file = bundle_path / "review_data.parquet"
    replacement_file = bundle_path / "replacement.parquet"
    connection = duckdb.connect()
    try:
        replacement_path = str(replacement_file).replace("'", "''")
        connection.execute(
            f"""COPY (
                SELECT * FROM read_parquet(?)
                UNION ALL
                SELECT * REPLACE (
                    'exact-1' AS unique_id,
                    'exact' AS match_reason,
                    'exact' AS match_stage,
                    NULL::DOUBLE AS match_weight,
                    NULL::DOUBLE AS distinguishability
                ) FROM read_parquet(?)
                UNION ALL
                SELECT * REPLACE (
                    'unmatched-1' AS unique_id,
                    'unmatched' AS match_reason,
                    'unmatched' AS match_stage,
                    FALSE AS is_matched,
                    NULL::DOUBLE AS match_weight,
                    NULL::DOUBLE AS distinguishability
                ) FROM read_parquet(?)
            ) TO '{replacement_path}' (FORMAT PARQUET)""",
            [str(data_file), str(data_file), str(data_file)],
        )
    finally:
        connection.close()
    replacement_file.replace(data_file)
    bundle = _load_bundle(bundle_path)
    _ensure_state_database(bundle)

    records = _records_payload(
        bundle,
        {
            "stage": ["splink", "exact", "unmatched"],
            "score_min": ["10"],
            "distinguishability_min": ["1"],
        },
    )

    assert {row["match_stage"] for row in records["rows"]} == {
        "splink",
        "exact",
        "unmatched",
    }


def test_handler_serves_static_html_bytes(tmp_path: Path) -> None:
    bundle = _load_bundle(create_test_bundle(tmp_path / "bundle"))
    input_dataset = InputDataset(
        tmp_path / "input.parquet", "unique_id", "ukam_label", True
    )
    session = SessionState(idle_timeout_seconds=600)
    server = ThreadingHTTPServer(
        ("127.0.0.1", 0), _handler_factory(bundle, input_dataset, session)
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with urlopen(
            f"http://127.0.0.1:{server.server_address[1]}/?token={session.token}"
        ) as response:
            assert response.headers.get_content_type() == "text/html"
            body = response.read()
            assert b"UKAM" in body
            assert b'id="review-no-candidates"' in body
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_saved_label_updates_input_dataset(tmp_path: Path) -> None:
    bundle_path = create_test_bundle(tmp_path / "bundle")
    bundle = _load_bundle(bundle_path)
    _ensure_state_database(bundle)
    input_file = tmp_path / "messy_addresses.parquet"
    connection = duckdb.connect()
    try:
        input_path = str(input_file).replace("'", "''")
        connection.execute(
            f"""
            COPY (
                SELECT 'messy-1'::VARCHAR AS unique_id,
                    NULL::VARCHAR AS ukam_label,
                    'unchanged'::VARCHAR AS other_value
            ) TO '{input_path}' (FORMAT PARQUET)
            """
        )
    finally:
        connection.close()

    input_dataset = _load_input_dataset(bundle, input_file)
    _save_label(
        bundle,
        {
            "unique_id": "messy-1",
            "decision": "accept_model",
            "ukam_label": "label-1",
            "selected_candidate_rank": 1,
        },
        input_dataset,
    )

    connection = duckdb.connect()
    try:
        row = connection.execute(
            "SELECT unique_id, ukam_label, other_value FROM read_parquet(?)",
            [str(input_file)],
        ).fetchone()
    finally:
        connection.close()

    assert row == ("messy-1", "label-1", "unchanged")


def test_saved_label_creates_missing_ukam_label_column(tmp_path: Path) -> None:
    bundle = _load_bundle(create_test_bundle(tmp_path / "bundle"))
    _ensure_state_database(bundle)
    input_file = tmp_path / "messy_addresses.parquet"
    connection = duckdb.connect()
    try:
        connection.execute(
            """COPY (
                SELECT 'messy-1'::VARCHAR AS source_identifier,
                    'unchanged'::VARCHAR AS other_value
            ) TO ? (FORMAT PARQUET)""",
            [str(input_file)],
        )
    finally:
        connection.close()

    input_dataset = _load_input_dataset(bundle, input_file, label_column="missing")
    _save_label(
        bundle,
        {
            "unique_id": "messy-1",
            "decision": "accept_model",
            "ukam_label": "label-1",
            "selected_candidate_rank": 1,
        },
        input_dataset,
    )

    connection = duckdb.connect()
    try:
        row = connection.execute(
            "SELECT source_identifier, ukam_label, other_value FROM read_parquet(?)",
            [str(input_file)],
        ).fetchone()
    finally:
        connection.close()
    assert row == ("messy-1", "label-1", "unchanged")


def test_canonical_search_api_and_selection_validation(tmp_path: Path) -> None:
    bundle = _load_bundle(create_test_bundle(tmp_path / "bundle"))
    _ensure_state_database(bundle)
    input_file = tmp_path / "input.csv"
    input_file.write_text("unique_id\nmessy-1\n", encoding="utf-8")
    input_dataset = _load_input_dataset(bundle, input_file)
    canonical_file = tmp_path / "canonical.parquet"
    connection = duckdb.connect()
    try:
        connection.execute(
            """COPY (
                SELECT 'canonical-1' AS unique_id,
                    '1 TEST ROAD' AS original_address_concat,
                    '1 TEST ROAD' AS clean_full_address, 'E1 1AA' AS postcode,
                    'RD06' AS classificationcode, '1' AS floorlevel
                UNION ALL
                SELECT 'canonical-2', '2 TEST ROAD', '2 TEST ROAD', 'E1 1AA',
                    'RD06', '2'
            ) TO ? (FORMAT PARQUET)""",
            [str(canonical_file)],
        )
    finally:
        connection.close()
    canonical_source = load_canonical_source(canonical_file)
    assert canonical_source is not None
    original_bytes = canonical_file.read_bytes()
    session = SessionState(idle_timeout_seconds=600)
    server = ThreadingHTTPServer(
        ("127.0.0.1", 0),
        _handler_factory(bundle, input_dataset, session, canonical_source),
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base_url = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        status, payload = request(
            base_url,
            "/api/bootstrap",
            token=session.token,
        )
        assert status == 200
        assert payload["canonical_search"] == {
            "available": True,
            "source_name": "canonical.parquet",
            "page_size": 100,
            "additional_canonical_columns": ["classificationcode", "floorlevel"],
            "warning": None,
        }

        status, payload = request(
            base_url,
            "/api/canonical-search?postcode=e11aa&address_query=test&page=1",
            token=session.token,
        )
        assert status == 200
        assert payload["postcode"] == "E1 1AA"
        assert [row["canonical_id"] for row in payload["rows"]] == [
            "canonical-1",
            "canonical-2",
        ]
        assert payload["rows"][0]["classificationcode"] == "RD06"
        assert payload["rows"][0]["floorlevel"] == "1"

        status, payload = request(
            base_url,
            "/api/canonical-search?unique_id_query=canonical-2",
            token=session.token,
        )
        assert status == 200
        assert payload["unique_id_query"] == "canonical-2"
        assert [row["canonical_id"] for row in payload["rows"]] == ["canonical-2"]

        status, payload = request(
            base_url,
            "/api/labels",
            token=session.token,
            method="POST",
            payload={
                "unique_id": "messy-1",
                "decision": "select_canonical",
                "ukam_label": "unknown",
                "selected_candidate_rank": None,
            },
        )
        assert status == 400
        assert "does not exist" in payload["error"]

        status, payload = request(
            base_url,
            "/api/labels",
            token=session.token,
            method="POST",
            payload={
                "unique_id": "messy-1",
                "decision": "select_canonical",
                "ukam_label": "canonical-2",
                "selected_candidate_rank": None,
            },
        )
        assert status == 201
        assert payload["decision"] == "select_canonical"
        assert payload["ukam_label"] == "canonical-2"

        status, payload = request(
            base_url,
            "/api/review-record?unique_id=messy-1&include_current=true",
            token=session.token,
        )
        assert status == 200
        assert payload["record"]["current_label"] == "canonical-2"
        assert payload["record"]["current_label_address"] == "2 TEST ROAD"
        assert payload["record"]["current_label_postcode"] == "E1 1AA"
        assert payload["record"]["current_label_additional_columns"] == {
            "classificationcode": "RD06",
            "floorlevel": "2",
        }
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
    assert canonical_file.read_bytes() == original_bytes


def test_canonical_search_api_is_unavailable_without_source(
    running_app: tuple[str, SessionState, Path],
) -> None:
    base_url, session, _ = running_app

    status, payload = request(
        base_url,
        "/api/canonical-search?postcode=E1%201AA",
        token=session.token,
    )

    assert status == 409
    assert payload == {
        "error": (
            "Canonical search is unavailable because no canonical_data_path was supplied."
        )
    }
