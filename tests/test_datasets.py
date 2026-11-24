from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from uk_address_matcher.datasets import DatasetSpec, UKAMDatasets
from uk_address_matcher.datasets.registry import build_default_specs


def _write_csv(path: Path, values: list[str]) -> None:
    lines = ["unique_id,address_concat,postcode", *values]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_dataset_path_download_and_cache(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
):
    source = tmp_path / "demo.csv"
    _write_csv(source, ["1,10 Demo Street,AB1 2CD"])

    datasets = UKAMDatasets(
        specs=[
            DatasetSpec(
                name="demo",
                file_name="demo.csv",
                base_url=source.parent.as_uri(),
            )
        ],
        cache_dir=tmp_path / "cache",
    )

    downloaded = datasets.path("demo")
    assert downloaded.exists()
    assert "10 Demo Street" in downloaded.read_text(encoding="utf-8")
    captured = capsys.readouterr()
    assert "downloading:" in captured.out
    assert source.as_uri() in captured.out

    _write_csv(source, ["1,20 Changed Street,AB1 2CD"])
    cached = datasets.path("demo")
    assert "10 Demo Street" in cached.read_text(encoding="utf-8")

    refreshed = datasets.path("demo", refresh=True)
    assert "20 Changed Street" in refreshed.read_text(encoding="utf-8")


def test_dataset_attribute_returns_duckdb_relation(tmp_path: Path):
    source = tmp_path / "messy.csv"
    _write_csv(
        source,
        [
            "m_1,10 Demo Street,AB1 2CD",
            "m_2,11 Demo Street,AB1 2CD",
        ],
    )

    datasets = UKAMDatasets(
        specs=[
            DatasetSpec(
                name="messy_example",
                file_name="messy.csv",
                base_url=source.parent.as_uri(),
            )
        ],
        cache_dir=tmp_path / "cache",
    )

    rel = datasets.messy_example
    row_count = rel.aggregate("COUNT(*) as c").fetchall()[0][0]
    assert row_count == 2

    con = duckdb.connect(database=":memory:")
    rel_with_con = datasets.as_relation("messy_example", con=con)
    first_id = rel_with_con.order("unique_id").limit(1).fetchall()[0][0]
    assert first_id == "m_1"


def test_unknown_dataset_errors(tmp_path: Path):
    datasets = UKAMDatasets(specs=[], cache_dir=tmp_path / "cache")

    with pytest.raises(KeyError, match="Unknown dataset"):
        datasets.path("missing")

    with pytest.raises(AttributeError, match="Unknown dataset"):
        _ = datasets.missing


def test_dataset_spec_invalid_format_raises():
    with pytest.raises(ValueError, match="data_format"):
        DatasetSpec(
            name="bad",
            file_name="bad.json",
            base_url="https://example.com",
            data_format="json",
        )


def test_catalog_and_info_expose_metadata(tmp_path: Path):
    source = tmp_path / "demo.csv"
    _write_csv(source, ["1,10 Demo Street,AB1 2CD"])

    datasets = UKAMDatasets(
        specs=[
            DatasetSpec(
                name="demo",
                file_name="demo.csv",
                base_url=source.parent.as_uri(),
                rows="1",
                unique_entities="1",
                description="Demo dataset",
                data_format="csv",
            )
        ],
        cache_dir=tmp_path / "cache",
    )

    info = datasets.info("demo")
    assert info.name == "demo"
    assert info.data_format == "csv"

    catalog = datasets.catalog()
    assert len(catalog) == 1
    assert catalog[0]["name"] == "demo"
    assert catalog[0]["description"] == "Demo dataset"


def test_default_connection_uses_in_memory_relation_cache(tmp_path: Path):
    source = tmp_path / "demo.csv"
    _write_csv(source, ["1,10 Demo Street,AB1 2CD"])

    datasets = UKAMDatasets(
        specs=[
            DatasetSpec(
                name="demo",
                file_name="demo.csv",
                base_url=source.parent.as_uri(),
                data_format="csv",
            )
        ],
        cache_dir=tmp_path / "cache",
    )

    rel_1 = datasets.as_relation("demo")
    rel_2 = datasets.as_relation("demo")

    assert rel_1 is rel_2

    rel_3 = datasets.as_relation("demo", refresh=True)
    assert rel_3 is not rel_1


def test_default_registry_includes_fictional_london_datasets():
    names = {spec.name for spec in build_default_specs()}
    assert "fictional_london_canonical" in names
    assert "fictional_london_messy" in names


def test_fictional_london_returns_messy_and_canonical_tuple(tmp_path: Path):
    canonical = tmp_path / "fictional_london_canonical.parquet"
    messy = tmp_path / "fictional_london_messy.parquet"

    con = duckdb.connect(database=":memory:")
    con.execute(
        f"""
        COPY (
            SELECT
                'c_1' AS unique_id,
                '10 Demo Road, Townton' AS address_concat,
                'AB1 2CD' AS postcode
        )
        TO '{canonical.as_posix()}'
        (FORMAT PARQUET)
        """
    ).fetchall()
    con.execute(
        f"""
        COPY (
            SELECT
                'm_1' AS unique_id,
                '10 Demo Rd, Townton' AS address_concat,
                'AB1 2CD' AS postcode
        )
        TO '{messy.as_posix()}'
        (FORMAT PARQUET)
        """
    ).fetchall()

    datasets = UKAMDatasets(
        specs=[
            DatasetSpec(
                name="fictional_london_canonical",
                file_name="fictional_london_canonical.parquet",
                base_url=tmp_path.as_uri(),
            ),
            DatasetSpec(
                name="fictional_london_messy",
                file_name="fictional_london_messy.parquet",
                base_url=tmp_path.as_uri(),
            ),
        ],
        cache_dir=tmp_path / "cache",
    )

    messy_rel, canonical_rel = datasets.fictional_london
    assert messy_rel.aggregate("COUNT(*) AS c").fetchall()[0][0] == 1
    assert canonical_rel.aggregate("COUNT(*) AS c").fetchall()[0][0] == 1
