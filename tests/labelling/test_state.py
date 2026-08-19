from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest

from uk_address_matcher.labelling.server import (
    _ensure_state_database,
    _load_bundle,
    _records_payload,
    _review_record_payload,
    _save_label,
)


def create_test_bundle(root: Path) -> Path:
    root.mkdir()
    connection = duckdb.connect()
    data_file = root / "review_data.parquet"
    try:
        connection.execute(
            """COPY (SELECT 'bundle-1' bundle_id, '1.2.3' uk_address_matcher_version,
            CURRENT_TIMESTAMP created_at_utc, 'messy-1' unique_id,
            '1 TEST ROAD' messy_address,
            '1 TEST ROAD' messy_cleaned_address,
            'E1 1AA' messy_postcode, 'existing-1' ukam_label, TRUE has_existing_label,
            'canonical-1' resolved_canonical_id, 'label-1' resolved_label_id,
            '1 TEST ROAD LONDON' resolved_canonical_address,
            'E1 1AA' resolved_canonical_postcode, 'splink' match_reason,
            'splink' match_stage, TRUE is_matched, 12.5 match_weight,
            2.1 distinguishability, 1 candidate_count,
            [{'rank': 1::BIGINT, 'label_id': 'label-1'::VARCHAR},
             {'rank': 2::BIGINT, 'label_id': 'label-2'::VARCHAR}]
            top_candidates) TO ? (FORMAT PARQUET)""",
            [str(data_file)],
        )
    finally:
        connection.close()
    (root / "manifest.json").write_text(
        json.dumps({"bundle_id": "bundle-1", "data_file": "review_data.parquet"}),
        encoding="utf-8",
    )
    return root


def test_saved_label_is_persistent_and_hides_when_filtered(tmp_path: Path) -> None:
    bundle = _load_bundle(create_test_bundle(tmp_path / "bundle"))
    _ensure_state_database(bundle)
    saved = _save_label(
        bundle,
        {
            "unique_id": "messy-1",
            "decision": "select_candidate",
            "ukam_label": "label-2",
            "selected_candidate_rank": 2,
        },
    )
    assert saved["ukam_label"] == "label-2"
    response = _records_payload(
        bundle, {"page": ["1"], "page_size": ["20"], "show_labelled": ["false"]}
    )
    assert response["total_filtered"] == 0


def test_candidate_validation_and_page_sizes(tmp_path: Path) -> None:
    bundle = _load_bundle(create_test_bundle(tmp_path / "bundle"))
    _ensure_state_database(bundle)
    assert (
        _save_label(
            bundle,
            {
                "unique_id": "messy-1",
                "decision": "accept_model",
                "ukam_label": "label-1",
                "selected_candidate_rank": 1,
            },
        )["decision"]
        == "accept_model"
    )
    with pytest.raises(ValueError, match="not one of the exported candidates"):
        _save_label(
            bundle,
            {
                "unique_id": "messy-1",
                "decision": "select_candidate",
                "ukam_label": "bad",
                "selected_candidate_rank": 2,
            },
        )
    for page_size in (10, 20, 50, 100):
        assert (
            _records_payload(bundle, {"page_size": [str(page_size)]})["page_size"]
            == page_size
        )
    with pytest.raises(ValueError, match="page_size"):
        _records_payload(bundle, {"page_size": ["25"]})


def test_records_payload_exposes_model_target_for_label_selector(tmp_path: Path) -> None:
    bundle = _load_bundle(create_test_bundle(tmp_path / "bundle"))
    _ensure_state_database(bundle)

    row = _records_payload(bundle, {"page_size": ["20"]})["rows"][0]

    assert row["resolved_label_id"] == "label-1"
    assert row["top_candidates"][0]["label_id"] == "label-1"


def test_review_record_returns_candidates_and_navigation(tmp_path: Path) -> None:
    bundle = _load_bundle(create_test_bundle(tmp_path / "bundle"))
    _ensure_state_database(bundle)
    response = _review_record_payload(bundle, {"unique_id": ["messy-1"]})

    assert response["record"]["messy_cleaned_address"] == "1 TEST ROAD"
    assert [candidate["label_id"] for candidate in response["record"]["candidates"]] == [
        "label-1",
        "label-2",
    ]
    assert response["navigation"] == {
        "position": 1,
        "total": 1,
        "previous_unique_id": None,
        "next_unique_id": None,
    }


def test_review_record_rejects_records_outside_filters(tmp_path: Path) -> None:
    bundle = _load_bundle(create_test_bundle(tmp_path / "bundle"))
    _ensure_state_database(bundle)

    with pytest.raises(ValueError, match="current filtered review set"):
        _review_record_payload(
            bundle,
            {"unique_id": ["messy-1"], "stage": ["exact"]},
        )
