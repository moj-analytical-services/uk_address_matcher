from __future__ import annotations

import duckdb

from benchmarking.config.datasets import (
    _load_cornwall,
    get_dataset_definition,
)


def test_cornwall_dataset_loads_minimal_matching_columns(
    monkeypatch,
    tmp_path,
) -> None:
    source_path = tmp_path / "CORNWALL_CTBANDS_ONSUD_202510.csv"
    source_path.write_text(
        "PROPREF,UPRN,ADDR1,ADDR2,ADDR3,ADDR4,POSTCODE\n"
        'REF-1,123456789.0,"1 Market Place",Truro,Cornwall,,TR1 1AA\n'
    )
    monkeypatch.setenv("UKAM_CORNWALL_DATA_PATH", str(source_path))

    definition = get_dataset_definition("cornwall")
    assert definition["file_name"] == "CORNWALL_CTBANDS_ONSUD_202510.csv"

    with duckdb.connect() as con:
        rows = _load_cornwall(con, str(source_path)).fetchall()

    assert rows == [
        (
            "REF-1",
            "1 market place truro cornwall",
            "123456789",
            "TR1 1AA",
        )
    ]
