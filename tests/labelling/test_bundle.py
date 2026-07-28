from __future__ import annotations

import json
import logging

import duckdb
import pyarrow
import pytest

from uk_address_matcher import AddressMatcher, ExactMatchStage, SplinkStage
from uk_address_matcher.labelling import export_labelling_bundle


def _relation(con: duckdb.DuckDBPyConnection, records: list[dict[str, str]]):
    return con.from_arrow(pyarrow.Table.from_pylist(records))


def test_exports_default_bundle_with_deterministic_candidates(
    tmp_path,
    monkeypatch,
    caplog,
):
    con = duckdb.connect(database=":memory:")
    canonical = _relation(
        con,
        [
            {
                "unique_id": "canonical-1",
                "address_concat": "1 Fictional Street",
                "postcode": "AB1 2CD",
            }
        ],
    )
    messy = _relation(
        con,
        [
            {
                "unique_id": "messy-matched",
                "address_concat": "1 Fictional Street",
                "postcode": "AB1 2CD",
            },
            {
                "unique_id": "messy-unmatched",
                "address_concat": "9 Unknown Road",
                "postcode": "ZZ1 1ZZ",
            },
        ],
    )
    result = AddressMatcher(
        canonical_addresses=canonical,
        addresses_to_match=messy,
        con=con,
        stages=[ExactMatchStage()],
    ).match()

    monkeypatch.chdir(tmp_path)
    caplog.set_level(logging.INFO, logger="uk_address_matcher")
    bundle_path = result.export_labelling_bundle()

    assert bundle_path == (tmp_path / "ukam_labelling_bundle").resolve()
    assert (bundle_path / "review_data.parquet").is_file()
    assert (bundle_path / "manifest.json").is_file()

    manifest = json.loads((bundle_path / "manifest.json").read_text())
    assert manifest["uk_address_matcher_version"]
    assert "bundle_schema_version" not in manifest
    assert manifest["row_count"] == 2
    assert manifest["matched_row_count"] == 1
    assert manifest["unmatched_row_count"] == 1
    assert (
        f"Labelling bundle written to '{bundle_path}' (bundle_id={manifest['bundle_id']})"
    ) in caplog.messages

    with duckdb.connect() as fresh_con:
        rows = fresh_con.execute(
            """
            SELECT
                unique_id,
                messy_address,
                messy_cleaned_address,
                resolved_canonical_address,
                match_stage,
                candidate_count,
                top_candidates
            FROM read_parquet(?)
            ORDER BY unique_id
            """,
            [str(bundle_path / "review_data.parquet")],
        ).fetchall()

    assert rows[0][0] == "messy-matched"
    assert rows[0][1] == "1 Fictional Street"
    assert rows[0][2] == "1 FICTIONAL STREET"
    assert rows[0][3] == "1 FICTIONAL STREET"
    assert rows[0][4] == "exact"
    assert rows[0][5] == 1
    assert rows[0][6][0]["source"] == "exact: full match"
    assert rows[0][6][0]["canonical_address"] == "1 FICTIONAL STREET"
    assert rows[1][0] == "messy-unmatched"
    assert rows[1][4] == "unmatched"
    assert rows[1][5] == 0
    assert rows[1][6] == []

    custom_bundle_path = export_labelling_bundle(
        result,
        tmp_path / "custom_bundle",
    )
    assert custom_bundle_path == (tmp_path / "custom_bundle").resolve()
    con.close()


def test_exports_reranked_splink_candidates(tmp_path):
    con = duckdb.connect(database=":memory:")
    canonical = _relation(
        con,
        [
            {
                "unique_id": "canonical-1",
                "address_concat": "10 Fictional Street",
                "postcode": "AB1 2CD",
            },
            {
                "unique_id": "canonical-2",
                "address_concat": "11 Fictional Street",
                "postcode": "AB1 2CD",
            },
        ],
    )
    messy = _relation(
        con,
        [
            {
                "unique_id": "messy-1",
                "address_concat": "10 Fictional St",
                "postcode": "AB1 2CD",
            }
        ],
    )
    result = AddressMatcher(
        canonical_addresses=canonical,
        addresses_to_match=messy,
        con=con,
        stages=[
            ExactMatchStage(),
            SplinkStage(
                predict_threshold_match_weight=-50,
                improve_threshold_match_weight=-50,
                final_match_weight_threshold=-50,
                final_distinguishability_threshold=None,
            ),
        ],
    ).match()

    bundle_path = result.export_labelling_bundle(tmp_path / "splink_bundle")
    with duckdb.connect() as fresh_con:
        candidate_count, candidates = fresh_con.execute(
            """
            SELECT candidate_count, top_candidates
            FROM read_parquet(?)
            """,
            [str(bundle_path / "review_data.parquet")],
        ).fetchone()

    assert candidate_count >= len(candidates) > 0
    first_candidate = candidates[0]
    assert first_candidate["source"] == "splink"
    assert first_candidate["splink_match_weight"] is not None
    assert first_candidate["splink_match_probability"] is not None
    assert first_candidate["match_weight"] is not None
    assert first_candidate["rerank_adjustment"] == pytest.approx(
        first_candidate["match_weight"] - first_candidate["splink_match_weight"]
    )
    con.close()


def test_preserves_labels_with_fixed_default_schema(tmp_path):
    con = duckdb.connect(database=":memory:")
    canonical = _relation(
        con,
        [
            {
                "unique_id": "canonical-1",
                "uprn": "uprn-1",
                "classification": "residential",
                "address_concat": "1 Fictional Street",
                "postcode": "AB1 2CD",
            }
        ],
    )
    messy = _relation(
        con,
        [
            {
                "unique_id": "messy-1",
                "ukam_label": "existing-label",
                "local_authority": "Fictionshire",
                "address_concat": "1 Fictional Street",
                "postcode": "AB1 2CD",
            }
        ],
    )
    result = AddressMatcher(
        canonical_addresses=canonical,
        addresses_to_match=messy,
        con=con,
        stages=[ExactMatchStage()],
    ).match()

    bundle_path = result.export_labelling_bundle(tmp_path / "review_bundle")
    with duckdb.connect() as fresh_con:
        row = fresh_con.execute(
            """
            SELECT ukam_label, has_existing_label, resolved_label_id, top_candidates
            FROM read_parquet(?)
            """,
            [str(bundle_path / "review_data.parquet")],
        ).fetchone()
        columns = {
            column[0]
            for column in fresh_con.execute(
                "DESCRIBE SELECT * FROM read_parquet(?)",
                [str(bundle_path / "review_data.parquet")],
            ).fetchall()
        }

    assert row[:3] == ("existing-label", True, "canonical-1")
    assert row[3][0]["label_id"] == "canonical-1"
    assert "local_authority" not in columns
    assert "bundle_schema_version" not in columns
    assert "rerank_changed_winner" not in columns
    assert not any(column.startswith("top_candidate_") for column in columns)

    with pytest.raises(FileExistsError, match="already exists"):
        result.export_labelling_bundle(bundle_path)
    result.export_labelling_bundle(bundle_path, overwrite=True)
    con.close()
