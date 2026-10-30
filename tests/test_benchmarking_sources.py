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


def test_load_gwynedd_maps_uprn_and_concatenates_address_fields(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_file = tmp_path / "Gwynedd-domestic-property-list-060626.csv"
    source_file.write_text(
        "property_ref,uprn,band,addr1,addr2,addr3,addr4,postcode\n"
        "42,12345678901,A,7 TEST STREET,,TEST TOWN,,ZZ1 1ZZ\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("UKAM_GWYNEDD_DATA_PATH", str(tmp_path))

    connection = duckdb.connect(":memory:")
    try:
        rows = load_dataset(connection, "gwynedd").fetchall()
    finally:
        connection.close()

    assert rows == [
        ("12345678901", "7 test street test town", "12345678901", "ZZ1 1ZZ")
    ]
