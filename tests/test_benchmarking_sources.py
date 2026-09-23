from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from benchmarking.config.datasets import load_dataset
from benchmarking.config.sources import resolve_data_path


def test_resolve_data_path_accepts_s3_uri(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("UKAM_TEST_DATA_PATH", "s3://example-bucket/path/to/data")

    assert resolve_data_path("UKAM_TEST_DATA_PATH") == "s3://example-bucket/path/to/data/"


def test_resolve_data_path_accepts_existing_local_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    local_data_dir = tmp_path / "messy_data"
    local_data_dir.mkdir()
    monkeypatch.setenv("UKAM_TEST_DATA_PATH", str(local_data_dir))

    assert resolve_data_path("UKAM_TEST_DATA_PATH") == f"{local_data_dir.as_posix()}/"


def test_resolve_data_path_raises_for_missing_local_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    missing_dir = tmp_path / "does_not_exist"
    monkeypatch.setenv("UKAM_TEST_DATA_PATH", str(missing_dir))

    with pytest.raises(RuntimeError, match="does not exist"):
        resolve_data_path("UKAM_TEST_DATA_PATH")


def test_resolve_data_path_raises_for_local_file_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    local_file = tmp_path / "data.csv"
    local_file.write_text("x", encoding="utf-8")
    monkeypatch.setenv("UKAM_TEST_DATA_PATH", str(local_file))

    with pytest.raises(RuntimeError, match="does not exist"):
        resolve_data_path("UKAM_TEST_DATA_PATH")


def test_load_hackney_accepts_prepared_parquet(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_path = tmp_path / "HACKNEY_CTBANDS_ONSUD_202507.parquet"
    escaped_source_path = source_path.as_posix().replace("'", "''")
    source_con = duckdb.connect()
    source_con.sql(
        f"""
        COPY (
            SELECT *
            FROM (VALUES
                ('P1', '10 Example Road', 'E8 1AA', 'P1'),
                ('P2', 'Flat 2 Example Road', 'E8 1AB', 'P2')
            ) AS source(unique_id, address_concat, postcode, ukam_label)
        ) TO '{escaped_source_path}' (FORMAT PARQUET)
        """
    )
    monkeypatch.setenv("UKAM_HACKNEY_DATA_PATH", str(tmp_path))

    relation = load_dataset(duckdb.connect(), dataset_key="hackney")

    assert relation.columns == [
        "unique_id",
        "address_concat",
        "ukam_label",
        "postcode",
    ]
    assert relation.fetchall() == [
        ("P1", "10 example road", "P1", "E8 1AA"),
        ("P2", "flat 2 example road", "P2", "E8 1AB"),
    ]
