import json
import tempfile
from pathlib import Path

import duckdb
import pyarrow
import pytest

from uk_address_matcher import prepare_canonical_folder
from uk_address_matcher.prepare_canonical import (
    _PreparedCanonical,
    load_prepared_canonical_data,
)

CANONICAL_RECORDS = [
    {
        "unique_id": "C1",
        "address_concat": "1 high street london",
        "postcode": "SW1A 1AA",
    },
    {
        "unique_id": "C2",
        "address_concat": "2 low street manchester",
        "postcode": "M1 1AA",
    },
    {
        "unique_id": "C3",
        "address_concat": "3 middle road birmingham",
        "postcode": "B1 1AA",
    },
]


@pytest.fixture
def con():
    return duckdb.connect(database=":memory:")


@pytest.fixture
def canonical_data(con):
    return con.from_arrow(pyarrow.Table.from_pylist(CANONICAL_RECORDS))


@pytest.fixture
def prepared_folder(con, canonical_data, tmp_path):
    """A ready-made prepared folder for tests that only need to read."""
    prepare_canonical_folder(
        canonical_data, output_folder=tmp_path, con=con, overwrite=True
    )
    return tmp_path


def test_prepare_creates_expected_files(prepared_folder):
    assert (prepared_folder / "ukam_canonical_addresses.parquet").exists()
    assert (prepared_folder / "ukam_term_frequencies.parquet").exists()
    assert (prepared_folder / "ukam_inverted_index.parquet").exists()
    assert (prepared_folder / "ukam_manifest.json").exists()


def test_prepare_overwrite_false_raises(con, canonical_data):
    with tempfile.TemporaryDirectory() as tmp:
        prepare_canonical_folder(
            canonical_data, output_folder=tmp, con=con, overwrite=True
        )
        with pytest.raises(FileExistsError):
            prepare_canonical_folder(
                canonical_data, output_folder=tmp, con=con, overwrite=False
            )


def test_prepare_overwrite_true_succeeds(con, canonical_data):
    with tempfile.TemporaryDirectory() as tmp:
        prepare_canonical_folder(
            canonical_data, output_folder=tmp, con=con, overwrite=True
        )
        # Should not raise on second write
        prepare_canonical_folder(
            canonical_data, output_folder=tmp, con=con, overwrite=True
        )


def test_overwrite_clears_stale_files(con, canonical_data):
    """overwrite=True should remove temp files left by a previous interrupted run."""
    with tempfile.TemporaryDirectory() as tmp:
        prepare_canonical_folder(
            canonical_data, output_folder=tmp, con=con, overwrite=True
        )

        stale = Path(tmp) / "ukam_manifest.json.tmp"
        stale.write_text("stale")
        assert stale.exists()

        prepare_canonical_folder(
            canonical_data, output_folder=tmp, con=con, overwrite=True
        )

        assert not stale.exists()
        assert (Path(tmp) / "ukam_manifest.json").exists()
        assert (Path(tmp) / "ukam_canonical_addresses.parquet").exists()


def test_manifest_contains_expected_fields(prepared_folder):
    manifest = json.loads((prepared_folder / "ukam_manifest.json").read_text())

    assert "ukam_version" in manifest
    assert "created_at" in manifest
    assert "created_with_duckdb_version" in manifest
    assert manifest["row_counts"]["canonical_addresses"] == 3

    # Per-file metadata
    assert "files" in manifest
    addr_meta = manifest["files"]["ukam_canonical_addresses.parquet"]
    assert "size_bytes" in addr_meta
    assert "sha256" in addr_meta
    assert "columns" in addr_meta
    assert isinstance(addr_meta["columns"], list)
    assert len(addr_meta["columns"]) > 0


def test_manifest_version_mismatch_warns(con, prepared_folder):
    manifest_path = prepared_folder / "ukam_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["ukam_version"] = "0.0.0"
    manifest_path.write_text(json.dumps(manifest))

    with pytest.warns(UserWarning, match="v0.0.0"):
        load_prepared_canonical_data(prepared_folder, con=con)


def test_missing_manifest_warns(con, prepared_folder):
    """Loading from a folder with no manifest should warn."""
    (prepared_folder / "ukam_manifest.json").unlink()

    with pytest.warns(UserWarning, match="No manifest file found"):
        load_prepared_canonical_data(prepared_folder, con=con)


def test_file_size_mismatch_warns(con, prepared_folder):
    """A recorded size that doesn't match the actual file should warn."""
    manifest_path = prepared_folder / "ukam_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["files"]["ukam_canonical_addresses.parquet"]["size_bytes"] = 1
    manifest_path.write_text(json.dumps(manifest))

    with pytest.warns(UserWarning, match="size.*bytes"):
        load_prepared_canonical_data(prepared_folder, con=con)


def test_load_returns_prepared_canonical(con, prepared_folder):
    result = load_prepared_canonical_data(prepared_folder, con=con)

    assert isinstance(result, _PreparedCanonical)
    assert isinstance(result.addresses, duckdb.DuckDBPyRelation)
    assert isinstance(result.term_frequencies, duckdb.DuckDBPyRelation)
    assert isinstance(result.inverted_index, duckdb.DuckDBPyRelation)


def test_load_prepared_data_has_expected_row_counts(con, prepared_folder):
    result = load_prepared_canonical_data(prepared_folder, con=con)

    assert result.addresses.count("*").fetchone()[0] == 3
    assert result.term_frequencies.count("*").fetchone()[0] > 0
    assert result.inverted_index.count("*").fetchone()[0] > 0


def test_load_accepts_string_path(con, prepared_folder):
    result = load_prepared_canonical_data(str(prepared_folder), con=con)
    assert result.addresses.count("*").fetchone()[0] == 3


def test_invalid_folder_raises(con):
    with pytest.raises(FileNotFoundError):
        load_prepared_canonical_data("/tmp/nonexistent_folder_xyz_123", con=con)


def test_corrupt_parquet_raises(con, prepared_folder):
    """A corrupt Parquet file should be caught during validation."""
    corrupt_path = prepared_folder / "ukam_canonical_addresses.parquet"
    corrupt_path.write_bytes(b"not a parquet file")

    with pytest.raises(FileNotFoundError, match="not a valid Parquet file"):
        load_prepared_canonical_data(prepared_folder, con=con)
