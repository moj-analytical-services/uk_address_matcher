from __future__ import annotations

import csv
import hashlib
import json
import os
import shutil
from contextlib import contextmanager
from http.server import ThreadingHTTPServer
from pathlib import Path
from threading import Thread

import duckdb
import pytest
from playwright.sync_api import Page, expect

from tests.labelling.utils import create_test_bundle
from uk_address_matcher.labelling.generator import generate_labelling_html
from uk_address_matcher.labelling.server import (
    _handler_factory,
    _local_files,
    _static_root,
)

pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_LABELLING_BROWSER_TESTS") != "1",
    reason="set RUN_LABELLING_BROWSER_TESTS=1 to run Chromium labelling tests",
)


@contextmanager
def local_app(bundle: Path, canonical: Path | None = None):
    server = ThreadingHTTPServer(
        ("127.0.0.1", 0),
        _handler_factory(_local_files(bundle, canonical), _static_root()),
    )
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}/"
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def open_generated_app(
    page: Page,
    bundle: Path,
    output: Path,
    canonical: Path | None = None,
) -> None:
    generate_labelling_html(output, open_browser=False)
    canonical_folder = output.parent / f"{output.stem}-canonical"
    canonical_folder.mkdir()
    address_path = canonical_folder / "ukam_canonical_addresses.parquet"
    if canonical is None:
        connection = duckdb.connect()
        try:
            connection.execute(
                """
                COPY (
                    SELECT 'canonical-1' AS unique_id,
                        '1 TEST ROAD LONDON' AS original_address_concat,
                        '1 TEST ROAD LONDON' AS clean_full_address,
                        'E1 1AA' AS postcode
                    UNION ALL
                    SELECT 'canonical-2', '2 TEST ROAD LONDON',
                        '2 TEST ROAD LONDON', 'E1 1AB'
                ) TO ? (FORMAT PARQUET)
                """,
                [str(address_path)],
            )
        finally:
            connection.close()
    else:
        shutil.copyfile(canonical, address_path)
    for name in ("ukam_term_frequencies.parquet", "ukam_inverted_index.parquet"):
        shutil.copyfile(address_path, canonical_folder / name)
    files = {}
    for path in canonical_folder.glob("*.parquet"):
        files[path.name] = {
            "size_bytes": path.stat().st_size,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "columns": [],
        }
    (canonical_folder / "ukam_manifest.json").write_text(
        json.dumps(
            {
                "ukam_version": "1.2.3",
                "created_at": "2024-01-01T00:00:00+00:00",
                "created_with_duckdb_version": "1.0.0",
                "row_counts": {"canonical_addresses": 2, "canonical_output_chunks": 1},
                "preparation_options": {"add_debug_features": False},
                "files": files,
            }
        ),
        encoding="utf-8",
    )
    page.goto(output.as_uri())
    page.locator("#bundle-directory").set_input_files(str(bundle))
    page.locator("#canonical-directory").set_input_files(str(canonical_folder))
    page.locator("#load-dataset").click()


def test_local_labelling_app_loads_generated_bundle(page: Page, tmp_path: Path) -> None:
    bundle = create_test_bundle(tmp_path / "bundle")
    open_generated_app(page, bundle, tmp_path / "review.html")

    expect(page.locator("#dataset-loader")).to_be_hidden()
    expect(page.locator("#labelling-app")).to_be_visible()
    expect(page.locator("#bundle-name")).not_to_have_text("Loading bundle...")
    expect(page.locator("#save-status")).to_have_text("Session only")


def test_generated_app_loads_bundle_without_canonical_data(
    page: Page, tmp_path: Path
) -> None:
    bundle = create_test_bundle(tmp_path / "bundle")
    output = tmp_path / "review.html"
    generate_labelling_html(output, open_browser=False)
    page.goto(output.as_uri())
    page.locator("#bundle-directory").set_input_files(str(bundle))
    page.locator("#load-dataset").click()

    expect(page.locator("#dataset-loader")).to_be_hidden()
    expect(page.locator("#labelling-app")).to_be_visible()


def test_local_labelling_app_persists_events_to_bundle(
    page: Page, tmp_path: Path
) -> None:
    bundle = create_test_bundle(tmp_path / "bundle")
    with local_app(bundle) as url:
        page.goto(url)
        expect(page.locator("#dataset-loader")).to_be_hidden()
        page.locator("button.review").first.click()
        page.locator("#review-no-match").click()
        expect(page.locator("#save-status")).to_have_text("Saved in session")

    updates = json.loads((bundle / "labelling_updates.json").read_text())
    assert len(updates["events"]) == 1
    assert updates["events"][0]["decision"] == "no_match"


def test_local_labelling_app_selects_server_searched_canonical_record(
    page: Page, tmp_path: Path
) -> None:
    bundle = create_test_bundle(tmp_path / "bundle")
    canonical = tmp_path / "canonical.parquet"
    connection = duckdb.connect()
    try:
        connection.execute(
            """
            COPY (
                SELECT 'canonical-row-1' AS unique_id,
                    '1 TEST ROAD LONDON' AS original_address_concat,
                    '1 TEST ROAD LONDON' AS clean_full_address,
                    'E1 1AA' AS postcode
            ) TO ? (FORMAT PARQUET)
            """,
            [str(canonical)],
        )
    finally:
        connection.close()

    with local_app(bundle, canonical) as url:
        page.goto(url)
        page.locator("button.review").first.click()
        page.locator("#canonical-postcode").fill("E1 1AA")
        page.locator("#canonical-search").click()
        page.locator(".use-canonical-button").click()
        expect(page.locator("#save-status")).to_have_text("Saved in session")

    updates = json.loads((bundle / "labelling_updates.json").read_text())
    assert updates["events"][0]["decision"] == "select_canonical"
    assert updates["events"][0]["ukam_label"] == "canonical-row-1"


def test_review_save_stays_in_memory_and_advances(page: Page, tmp_path: Path) -> None:
    bundle = create_test_bundle(tmp_path / "bundle")
    open_generated_app(page, bundle, tmp_path / "review.html")
    page.locator("button.review").first.click()
    expect(page.locator("#review-content")).to_be_visible()

    page.locator("#review-no-match").click()

    expect(page.locator("#save-status")).to_have_text("Saved in session")
    expect(page.locator("#review-messy-id")).to_have_text("messy-2")
    page.evaluate("location.hash = '#review/messy-1'")
    expect(page.locator("#review-messy-id")).to_have_text("messy-1")
    expect(page.locator("#review-current-decision-title")).to_have_text("No match")
    expect(page.locator("#review-current-decision-persistence")).to_contain_text("Saved")
    expect(page.locator("#review-no-match")).to_have_attribute("aria-pressed", "true")
    assert not (bundle / "labelling_updates.json").exists()
    assert not (bundle / "labelled_review_data.parquet").exists()


def test_overview_shows_current_label_details(page: Page, tmp_path: Path) -> None:
    bundle = create_test_bundle(tmp_path / "bundle")
    open_generated_app(page, bundle, tmp_path / "review.html")
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
    expect(page.get_by_text("Predicted value - canonical-1", exact=True)).to_be_attached()
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


def test_review_presents_imported_label_as_accepted_model_match(
    page: Page, tmp_path: Path
) -> None:
    bundle = create_test_bundle(tmp_path / "bundle", existing_label="canonical-1")
    open_generated_app(page, bundle, tmp_path / "review.html")
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


@pytest.mark.parametrize(
    ("current_label", "expected_rows"),
    [("canonical-1", 0), ("canonical-2", 1)],
)
def test_model_mismatches_filter_compares_current_and_model_output_labels(
    page: Page, tmp_path: Path, current_label: str, expected_rows: int
) -> None:
    bundle = create_test_bundle(tmp_path / "bundle", existing_label=current_label)
    open_generated_app(page, bundle, tmp_path / "review.html")

    expect(page.locator("#records-body tr")).to_have_count(2)
    page.locator("#mismatches-only").check()

    expect(page.locator("#records-body tr")).to_have_count(max(expected_rows, 1))
    if expected_rows:
        expect(page.locator("#records-body tr").first).to_contain_text("messy-1")
    else:
        expect(page.locator("#records-body tr").first).to_contain_text("No records match")


def test_generated_app_recovers_after_invalid_review_data(
    page: Page, tmp_path: Path
) -> None:
    bundle = create_test_bundle(tmp_path / "bundle")
    invalid_bundle = tmp_path / "invalid-bundle"
    invalid_bundle.mkdir()
    (invalid_bundle / "manifest.json").write_text(
        json.dumps(
            {
                "bundle_id": "bundle-1",
                "uk_address_matcher_version": "1.2.3",
                "data_file": "review_data.csv",
            }
        ),
        encoding="utf-8",
    )
    (invalid_bundle / "review_data.csv").write_text(
        "bundle_id\nbundle-1\n", encoding="utf-8"
    )
    open_generated_app(page, invalid_bundle, tmp_path / "invalid.html")
    expect(page.locator("#dataset-loader")).to_be_visible()
    expect(page.locator("#dataset-loader-status")).to_contain_text(
        "missing required columns"
    )

    open_generated_app(page, bundle, tmp_path / "valid.html")

    expect(page.locator("#labelling-app")).to_be_visible()
    expect(page.locator("#save-status")).to_have_text("Session only")


def test_generated_app_rejects_invalid_canonical_manifest(
    page: Page, tmp_path: Path
) -> None:
    bundle = create_test_bundle(tmp_path / "bundle")
    canonical = tmp_path / "invalid-canonical"
    canonical.mkdir()
    (canonical / "ukam_manifest.json").write_text("{}", encoding="utf-8")
    output = tmp_path / "review.html"
    generate_labelling_html(output, open_browser=False)
    page.goto(output.as_uri())
    page.locator("#bundle-directory").set_input_files(str(bundle))
    page.locator("#canonical-directory").set_input_files(str(canonical))
    page.locator("#load-dataset").click()

    expect(page.locator("#dataset-loader")).to_be_visible()
    expect(page.locator("#dataset-loader-status")).to_contain_text(
        "missing required provenance fields"
    )


def test_generated_app_labels_with_canonical_data_and_downloads_labels(
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

    api_requests: list[str] = []

    def record_api(route) -> None:
        api_requests.append(route.request.url)
        route.abort()

    page.route("**/api/**", record_api)
    open_generated_app(page, bundle, tmp_path / "review.html", canonical)
    page.locator("button.review").first.click()
    expect(page.locator("#canonical-content")).to_be_visible()
    page.locator("#canonical-postcode").fill("E1 1AA")
    page.locator("#canonical-search").click()
    expect(page.get_by_text("canonical-external-1", exact=True)).to_be_visible()

    page.locator("#review-no-match").click()
    expect(page.locator("#save-status")).to_have_text("Saved in session")
    page.locator("#review-accept").click()
    expect(page.locator("#save-status")).to_have_text("Saved in session")
    with page.expect_download() as download_info:
        page.locator("#download-labels").click()
    download = download_info.value
    assert download.suggested_filename == "bundle-1-labels.csv"
    labels_file = tmp_path / download.suggested_filename
    download.save_as(labels_file)
    rows = list(csv.DictReader(labels_file.open(newline="", encoding="utf-8")))
    assert list(rows[0]) == [
        "unique_id",
        "address_name",
        "ukam_label",
        "label_available",
    ]
    assert [
        (
            row["unique_id"],
            row["address_name"],
            row["ukam_label"],
            row["label_available"],
        )
        for row in rows
    ] == [
        ("messy-1", "1 TEST ROAD", "", "FALSE"),
        ("messy-2", "2 TEST ROAD", "label-3", "TRUE"),
    ]
    assert api_requests == []
