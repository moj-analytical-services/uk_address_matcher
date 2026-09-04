from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest

from uk_address_matcher.labelling.updates import apply_labelling_updates


def create_test_bundle(root: Path, existing_label: str | None = None) -> Path:
    root.mkdir()
    data_file = root / "review_data.parquet"
    escaped_label = existing_label.replace("'", "''") if existing_label else None
    imported_label = f"'{escaped_label}'" if escaped_label else "NULL::VARCHAR"
    has_existing_label = "TRUE" if existing_label else "FALSE"
    connection = duckdb.connect()
    try:
        connection.execute(
            """COPY (SELECT 'bundle-1' bundle_id, '1.2.3' uk_address_matcher_version,
            CURRENT_TIMESTAMP created_at_utc, 'messy-1' unique_id,
            '1 TEST ROAD' messy_address, '1 TEST ROAD' messy_cleaned_address,
                'E1 1AA' messy_postcode, __IMPORTED_LABEL__ ukam_label, __HAS_EXISTING_LABEL__ has_existing_label,
            'canonical-1' resolved_canonical_id, 'label-1' resolved_label_id,
            '1 TEST ROAD LONDON' resolved_canonical_address,
            'E1 1AA' resolved_canonical_postcode, 'splink' match_reason,
            'splink' match_stage, TRUE is_matched, 12.5 match_weight,
            2.1 distinguishability, 2 candidate_count,
            [{'rank': 1::BIGINT, 'label_id': 'label-1'::VARCHAR, 'canonical_id': 'canonical-1'::VARCHAR},
             {'rank': 2::BIGINT, 'label_id': 'label-2'::VARCHAR, 'canonical_id': 'canonical-2'::VARCHAR}] top_candidates
            UNION ALL
            SELECT 'bundle-1', '1.2.3', CURRENT_TIMESTAMP, 'messy-2',
            '2 TEST ROAD', '2 TEST ROAD', 'E1 1AB', NULL::VARCHAR, FALSE,
            'canonical-2', 'label-3', '2 TEST ROAD LONDON', 'E1 1AB', 'exact',
            'exact', TRUE, NULL::DOUBLE, NULL::DOUBLE, 1,
            [{'rank': 1::BIGINT, 'label_id': 'label-3'::VARCHAR, 'canonical_id': 'canonical-2'::VARCHAR}] top_candidates
                ) TO ? (FORMAT PARQUET)""".replace(
                "__IMPORTED_LABEL__", imported_label
            ).replace("__HAS_EXISTING_LABEL__", has_existing_label),
            [str(data_file)],
        )
    finally:
        connection.close()
    (root / "manifest.json").write_text(
        json.dumps({"bundle_id": "bundle-1", "data_file": "review_data.parquet"}),
        encoding="utf-8",
    )
    return root


def write_updates(path: Path, events: list[dict[str, object]]) -> None:
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "bundle_id": "bundle-1",
                "exported_at_utc": "2026-08-19T12:00:00Z",
                "events": events,
            }
        ),
        encoding="utf-8",
    )


def event(
    event_id: str,
    unique_id: str,
    decision: str,
    label: str | None,
    created_at: str,
    rank: int | None = None,
) -> dict[str, object]:
    return {
        "event_id": event_id,
        "bundle_id": "bundle-1",
        "unique_id": unique_id,
        "decision": decision,
        "ukam_label": label,
        "selected_candidate_rank": rank,
        "created_at_utc": created_at,
    }


def read_labels(path: Path) -> list[tuple[str, str | None]]:
    connection = duckdb.connect()
    try:
        return connection.execute(
            "SELECT unique_id, review_label FROM read_csv_auto(?) ORDER BY unique_id",
            [str(path)],
        ).fetchall()
    finally:
        connection.close()


def test_apply_updates_writes_latest_validated_labels_to_csv(tmp_path: Path) -> None:
    bundle = create_test_bundle(tmp_path / "bundle")
    input_file = tmp_path / "input.csv"
    input_file.write_text(
        "unique_id,review_label\nmessy-1,old\nmessy-2,keep\n", encoding="utf-8"
    )
    updates = tmp_path / "updates.json"
    write_updates(
        updates,
        [
            event(
                "event-1",
                "messy-1",
                "select_candidate",
                "label-2",
                "2026-08-19T11:00:00Z",
                2,
            ),
            event("event-2", "messy-1", "clear", None, "2026-08-19T12:00:00Z"),
            event("event-3", "messy-2", "no_match", None, "2026-08-19T12:00:00Z"),
        ],
    )

    output, count = apply_labelling_updates(
        bundle,
        updates,
        input_file,
        input_dataset_label_column="review_label",
    )

    assert output == input_file.resolve()
    assert count == 2
    assert read_labels(input_file) == [("messy-1", None), ("messy-2", None)]


def test_apply_updates_can_create_a_label_column_and_output_parquet(
    tmp_path: Path,
) -> None:
    bundle = create_test_bundle(tmp_path / "bundle")
    input_file = tmp_path / "input.csv"
    input_file.write_text("record_id\nmessy-1\nmessy-2\n", encoding="utf-8")
    updates = tmp_path / "updates.json"
    write_updates(
        updates,
        [
            event(
                "event-1",
                "messy-1",
                "accept_model",
                "label-1",
                "2026-08-19T12:00:00Z",
            )
        ],
    )
    output_file = tmp_path / "output.parquet"

    output, count = apply_labelling_updates(
        bundle,
        updates,
        input_file,
        input_dataset_label_column="review_label",
        output_path=output_file,
    )

    assert output == output_file.resolve()
    assert count == 1
    connection = duckdb.connect()
    try:
        assert connection.execute(
            "SELECT record_id, review_label FROM read_parquet(?) ORDER BY record_id",
            [str(output_file)],
        ).fetchall() == [("messy-1", "label-1"), ("messy-2", None)]
    finally:
        connection.close()


@pytest.mark.parametrize(
    ("mutator", "message"),
    [
        (lambda payload: payload.update(bundle_id="wrong"), "bundle_id"),
        (lambda payload: payload.update(schema_version=99), "schema version"),
    ],
)
def test_apply_updates_rejects_incompatible_metadata(
    tmp_path: Path, mutator: object, message: str
) -> None:
    bundle = create_test_bundle(tmp_path / "bundle")
    updates = tmp_path / "updates.json"
    payload: dict[str, object] = {
        "schema_version": 1,
        "bundle_id": "bundle-1",
        "events": [],
    }
    mutator(payload)
    updates.write_text(json.dumps(payload), encoding="utf-8")
    input_file = tmp_path / "input.csv"
    input_file.write_text("unique_id\nmessy-1\nmessy-2\n", encoding="utf-8")

    with pytest.raises(ValueError, match=message):
        apply_labelling_updates(bundle, updates, input_file)


def test_apply_updates_rejects_event_from_another_bundle(tmp_path: Path) -> None:
    bundle = create_test_bundle(tmp_path / "bundle")
    updates = tmp_path / "updates.json"
    write_updates(
        updates,
        [event("event-1", "messy-1", "no_match", None, "2026-08-19T12:00:00Z")],
    )
    payload = json.loads(updates.read_text(encoding="utf-8"))
    payload["events"][0]["bundle_id"] = "wrong"
    updates.write_text(json.dumps(payload), encoding="utf-8")
    input_file = tmp_path / "input.csv"
    input_file.write_text("unique_id\nmessy-1\nmessy-2\n", encoding="utf-8")

    with pytest.raises(ValueError, match="event bundle_id"):
        apply_labelling_updates(bundle, updates, input_file)
