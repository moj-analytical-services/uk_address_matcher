from __future__ import annotations

import json
from pathlib import Path

import duckdb


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
            'E1 1AA' messy_postcode,
            __IMPORTED_LABEL__ ukam_label,
            __HAS_EXISTING_LABEL__ has_existing_label,
            'canonical-1' resolved_canonical_id, 'label-1' resolved_label_id,
            '1 TEST ROAD LONDON' resolved_canonical_address,
            'E1 1AA' resolved_canonical_postcode, 'splink' match_reason,
            'splink' match_stage, TRUE is_matched, 12.5 match_weight,
            2.1 distinguishability, 2 candidate_count,
                [{'rank': 1::BIGINT, 'label_id': 'label-1'::VARCHAR,
                  'canonical_id': 'canonical-1'::VARCHAR},
                 {'rank': 2::BIGINT, 'label_id': 'label-2'::VARCHAR,
                  'canonical_id': 'canonical-2'::VARCHAR}] top_candidates
            UNION ALL
            SELECT 'bundle-1', '1.2.3', CURRENT_TIMESTAMP, 'messy-2',
            '2 TEST ROAD', '2 TEST ROAD', 'E1 1AB', NULL::VARCHAR, FALSE,
            'canonical-2', 'label-3', '2 TEST ROAD LONDON', 'E1 1AB', 'exact',
            'exact', TRUE, NULL::DOUBLE, NULL::DOUBLE, 1,
                        [{'rank': 1::BIGINT, 'label_id': 'label-3'::VARCHAR,
                            'canonical_id': 'canonical-2'::VARCHAR}] top_candidates
                ) TO ? (FORMAT PARQUET)""".replace(
                "__IMPORTED_LABEL__", imported_label
            ).replace("__HAS_EXISTING_LABEL__", has_existing_label),
            [str(data_file)],
        )
    finally:
        connection.close()
    (root / "manifest.json").write_text(
        json.dumps(
            {
                "bundle_id": "bundle-1",
                "uk_address_matcher_version": "1.2.3",
                "data_file": "review_data.parquet",
            }
        ),
        encoding="utf-8",
    )
    return root
