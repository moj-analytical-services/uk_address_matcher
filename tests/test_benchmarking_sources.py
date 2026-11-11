from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from benchmarking.config.datasets import _clean_output
from benchmarking.config.sources import resolve_data_path


def test_dataset_cleaning_retains_missing_postcodes() -> None:
    with duckdb.connect() as connection:
        source = connection.sql(
            "SELECT * FROM (VALUES "
            "('1', '12 Example Street', '101', NULL), "
            "('2', '8 Sample Road', '102', 'AB1 2CD'), "
            "(NULL, '4 Test Lane', '103', NULL), "
            "('3', ' ', '104', NULL)"
            ") AS source(unique_id, address_concat, ukam_label, postcode)"
        )
        assert _clean_output(connection, source).order("unique_id").fetchall() == [
            ("1", "12 example street", "101", None),
            ("2", "8 sample road", "102", "AB1 2CD"),
        ]


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
