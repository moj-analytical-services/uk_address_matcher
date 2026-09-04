from __future__ import annotations

import json
import os
import re
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread

import duckdb
import pytest
from playwright.sync_api import Page, expect

from tests.labelling.test_updates import create_test_bundle
from uk_address_matcher.labelling.server import (
    _handler_factory,
    _local_files,
    _static_root,
)

pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_LABELLING_BROWSER_TESTS") != "1",
    reason="set RUN_LABELLING_BROWSER_TESTS=1 to run Chromium labelling tests",
)


def test_local_labelling_app_loads_configured_bundle(page: Page, tmp_path) -> None:
    bundle = create_test_bundle(tmp_path / "bundle")
    server = ThreadingHTTPServer(
        ("127.0.0.1", 0),
        _handler_factory(_local_files(bundle, None), _static_root()),
    )
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        page.goto(f"http://127.0.0.1:{server.server_address[1]}/")

        expect(page.locator("#dataset-loader")).to_be_hidden()
        expect(page.locator("#labelling-app")).to_be_visible()
        expect(page.locator("#bundle-name")).not_to_have_text("Loading bundle...")
        expect(page.locator("#session-countdown")).to_have_text(
            "Saved to bundle/labelled_review_data.parquet"
        )
        labelled_review_path = bundle / "labelled_review_data.parquet"
        expect(page.locator("#session-countdown")).to_have_attribute(
            "title",
            re.compile(f"Labelled review: {labelled_review_path}"),
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_review_does_not_advance_when_event_persistence_fails(
    page: Page, tmp_path
) -> None:
    bundle = create_test_bundle(tmp_path / "bundle")
    server = ThreadingHTTPServer(
        ("127.0.0.1", 0),
        _handler_factory(_local_files(bundle, None), _static_root()),
    )
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        page.goto(f"http://127.0.0.1:{server.server_address[1]}/")
        page.locator("button.review").first.click()
        expect(page.locator("#review-content")).to_be_visible()
        expect(page.locator("#review-current-decision-title")).to_have_text(
            "Not yet labelled"
        )
        review_url = page.url
        page.route(
            "**/api/events",
            lambda route: route.fulfill(
                status=500,
                body='{"error":"forced event failure"}',
                content_type="application/json",
            ),
        )

        page.locator("#review-no-match").click()

        expect(page.locator("#save-status")).to_have_text("Save failed")
        assert page.url == review_url
        expect(page.locator("#review-content")).to_be_visible()
        expect(page.locator("#review-current-decision-title")).to_have_text("No match")
        expect(page.locator("#review-current-decision-persistence")).to_have_text(
            "Save failed"
        )
        expect(page.locator("#review-no-match")).to_have_attribute("aria-pressed", "true")
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_review_save_persists_event_before_advancing(page: Page, tmp_path: Path) -> None:
    bundle = create_test_bundle(tmp_path / "bundle")
    server = ThreadingHTTPServer(
        ("127.0.0.1", 0),
        _handler_factory(_local_files(bundle, None), _static_root()),
    )
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        page.goto(f"http://127.0.0.1:{server.server_address[1]}/")
        page.locator("button.review").first.click()
        expect(page.locator("#review-content")).to_be_visible()

        page.locator("#review-no-match").click()

        expect(page.locator("#save-status")).to_have_text("Autosaved")
        expect(page.locator("#review-messy-id")).to_have_text("messy-2")
        page.evaluate("location.hash = '#review/messy-1'")
        expect(page.locator("#review-messy-id")).to_have_text("messy-1")
        expect(page.locator("#review-current-decision-title")).to_have_text("No match")
        expect(page.locator("#review-current-decision-persistence")).to_contain_text(
            "Saved"
        )
        expect(page.locator("#review-no-match")).to_have_attribute("aria-pressed", "true")
        events = json.loads((bundle / "labelling_updates.json").read_text())["events"]
        assert [(event["unique_id"], event["decision"]) for event in events] == [
            ("messy-1", "no_match")
        ]
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_overview_shows_current_label_details(page: Page, tmp_path: Path) -> None:
    bundle = create_test_bundle(tmp_path / "bundle")
    server = ThreadingHTTPServer(
        ("127.0.0.1", 0),
        _handler_factory(_local_files(bundle, None), _static_root()),
    )
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        page.goto(f"http://127.0.0.1:{server.server_address[1]}/")
        expect(page.locator("button.review").first).to_be_visible()
        page.locator("button.review").first.click()
        expect(page.locator("#review-accept")).to_be_visible()
        page.locator("#review-accept").click()
        expect(page.locator("#review-messy-id")).to_have_text("messy-2")
        page.evaluate("location.hash = '#review/messy-1'")
        expect(page.locator("#review-messy-id")).to_have_text("messy-1")
        expect(page.locator("#review-current-decision-title")).to_have_text(
            "Candidate match selected"
        )
        expect(page.locator("#review-current-decision-id")).to_have_text("canonical-1")
        expect(page.locator("#review-canonical-label")).to_have_text("canonical-1")
        expect(
            page.get_by_text("Predicted value - canonical-1", exact=True)
        ).to_be_attached()
        expect(page.get_by_text("Candidate 1 - canonical-1", exact=True)).to_be_attached()
        expect(page.locator("#review-accept")).to_have_attribute("aria-pressed", "true")

        page.locator('.tab[data-view="overview"]').click()

        first_row = page.locator("#records-body tr").first
        expect(first_row.locator(".current-label")).to_contain_text("canonical-1")
        expect(first_row.locator(".current-label")).to_contain_text("1 TEST ROAD LONDON")
        expect(first_row.locator(".current-label")).to_contain_text("E1 1AA")
        expect(first_row.locator(".model-suggestion .primary").first).to_have_text(
            "canonical-1"
        )
        expect(first_row.locator(".model-suggestion .primary").first).to_have_css(
            "color", "rgb(16, 24, 40)"
        )
        expect(first_row.locator(".current-label .primary").first).to_have_css(
            "color", "rgb(16, 24, 40)"
        )
        expect(first_row.locator(".current-label .primary").first).to_have_css(
            "font-weight", "400"
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_review_presents_imported_label_as_accepted_model_match(
    page: Page, tmp_path: Path
) -> None:
    bundle = create_test_bundle(tmp_path / "bundle", existing_label="canonical-1")
    server = ThreadingHTTPServer(
        ("127.0.0.1", 0),
        _handler_factory(_local_files(bundle, None), _static_root()),
    )
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        page.goto(f"http://127.0.0.1:{server.server_address[1]}/")
        expect(page.locator("button.review").first).to_be_visible()
        page.evaluate("location.hash = '#review/messy-1'")

        expect(page.locator("#review-current-decision-title")).to_have_text(
            "Model match accepted"
        )
        expect(page.locator("#review-current-decision-id")).to_have_text("canonical-1")
        expect(page.locator("#review-current-decision-icon")).to_have_text("✓")
        expect(page.locator("#review-current-decision")).to_have_class(
            "current-decision current-decision-accepted"
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


@pytest.mark.parametrize(
    ("current_label", "expected_rows"),
    [("canonical-1", 0), ("canonical-2", 1)],
)
def test_model_mismatches_filter_compares_current_and_model_output_labels(
    page: Page, tmp_path: Path, current_label: str, expected_rows: int
) -> None:
    bundle = create_test_bundle(tmp_path / "bundle", existing_label=current_label)
    server = ThreadingHTTPServer(
        ("127.0.0.1", 0),
        _handler_factory(_local_files(bundle, None), _static_root()),
    )
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        page.goto(f"http://127.0.0.1:{server.server_address[1]}/")
        expect(page.locator("#records-body tr")).to_have_count(2)

        page.locator("#mismatches-only").check()

        expect(page.locator("#records-body tr")).to_have_count(max(expected_rows, 1))
        if expected_rows:
            expect(page.locator("#records-body tr").first).to_contain_text("messy-1")
        else:
            expect(page.locator("#records-body tr").first).to_contain_text(
                "No records match"
            )
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_hosted_app_recovers_after_invalid_review_data(
    page: Page, tmp_path: Path
) -> None:
    bundle = create_test_bundle(tmp_path / "bundle")
    invalid_bundle = tmp_path / "invalid-bundle"
    invalid_bundle.mkdir()
    (invalid_bundle / "manifest.json").write_text(
        json.dumps({"bundle_id": "bundle-1", "data_file": "review_data.csv"}),
        encoding="utf-8",
    )
    (invalid_bundle / "review_data.csv").write_text(
        "bundle_id\nbundle-1\n", encoding="utf-8"
    )
    static_root = _static_root()
    handler = lambda *args, **kwargs: SimpleHTTPRequestHandler(
        *args, directory=static_root, **kwargs
    )
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        page.goto(f"http://127.0.0.1:{server.server_address[1]}/")
        expect(page.locator("#dataset-loader")).to_be_visible()
        assert page.locator('input[type="file"]').count() == 2
        expect(page.locator("#canonical-data-files")).to_be_visible()
        page.locator("#bundle-directory").set_input_files(invalid_bundle)
        page.locator("#load-dataset").click()
        expect(page.locator("#dataset-loader-status")).to_contain_text(
            "missing required columns"
        )

        page.locator("#bundle-directory").set_input_files(bundle)
        page.locator("#load-dataset").click()

        expect(page.locator("#labelling-app")).to_be_visible()
        expect(page.locator("#session-countdown")).to_have_text("Saved in browser")
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_hosted_app_labels_with_selected_canonical_and_downloads_updates(
    page: Page, tmp_path: Path
) -> None:
    bundle = create_test_bundle(tmp_path / "bundle")
    canonical = tmp_path / "canonical.parquet"
    connection = duckdb.connect()
    try:
        connection.execute(
            """
            COPY (
                SELECT 'canonical-row-1' AS ukam_address_id,
                    'canonical-external-1' AS unique_id,
                    '1 TEST ROAD LONDON' AS original_address_concat,
                    '1 TEST ROAD LONDON' AS clean_full_address,
                    'E1 1AA' AS postcode
            ) TO ? (FORMAT PARQUET)
            """,
            [str(canonical)],
        )
    finally:
        connection.close()
    static_root = _static_root()
    handler = lambda *args, **kwargs: SimpleHTTPRequestHandler(
        *args, directory=static_root, **kwargs
    )
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    api_requests: list[str] = []

    def abort_api(route) -> None:
        api_requests.append(route.request.url)
        route.abort()

    thread.start()
    try:
        page.route("**/api/**", abort_api)
        page.goto(f"http://127.0.0.1:{server.server_address[1]}/")
        page.locator("#bundle-directory").set_input_files(bundle)
        page.locator("#canonical-data-files").set_input_files(canonical)
        page.locator("#load-dataset").click()

        expect(page.locator("#labelling-app")).to_be_visible()
        page.locator("button.review").first.click()
        expect(page.locator("#canonical-content")).to_be_visible()
        page.locator("#canonical-postcode").fill("E1 1AA")
        page.locator("#canonical-search").click()
        expect(page.get_by_text("canonical-external-1", exact=True)).to_be_visible()

        page.locator("#review-no-match").click()
        expect(page.locator("#save-status")).to_have_text("Autosaved")
        with page.expect_download() as download_info:
            page.locator("#download-updates").click()
        download = download_info.value
        assert download.suggested_filename == "bundle-1-labelling-updates.json"
        updates = tmp_path / download.suggested_filename
        download.save_as(updates)
        payload = json.loads(updates.read_text(encoding="utf-8"))
        assert [(event["unique_id"], event["decision"]) for event in payload["events"]] == [
            ("messy-1", "no_match")
        ]
        assert all(request.endswith("/api/local-config") for request in api_requests)
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
