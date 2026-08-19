from __future__ import annotations

import random
from pathlib import Path

import duckdb
import pytest

from uk_address_matcher.labelling.canonical import (
    CANONICAL_PAGE_SIZE,
    canonical_scan_sql,
    find_canonical_record,
    load_canonical_source,
    normalise_postcode_search,
    search_canonical_data,
)


def write_canonical_data(
    path: Path, *, include_cleaned: bool = True, include_long_postcode: bool = False
) -> None:
    records: list[tuple[str | None, str, str, str]] = []
    for index in range(200):
        canonical_id = f"CANON_{index:04d}"
        records.extend(
            [
                (
                    canonical_id,
                    f"{index} TEST STREET HACKNEY LONDON",
                    f"{index} TEST STREET HACKNEY LONDON",
                    "E5 8RY",
                ),
                (
                    canonical_id,
                    f"FLAT {index} TEST STREET HACKNEY LONDON",
                    f"FLAT {index} TEST STREET HACKNEY LONDON",
                    "E5 8RY",
                ),
            ]
        )
    records.extend(
        [
            ("PERCENT", "100% TEST ROAD", "100% TEST ROAD", "N16 5FD"),
            ("UNDERSCORE", "A_B TEST ROAD", "A_B TEST ROAD", "N16 5FD"),
            (None, "NULL ID STREET", "NULL ID STREET", "N16 5FD"),
        ]
    )
    if include_long_postcode:
        records.append(
            (
                "LONG_POSTCODE",
                "LONG POSTCODE TEST ROAD",
                "LONG POSTCODE TEST ROAD",
                "ME5 8RY",
            )
        )
    random.Random(42).shuffle(records)
    connection = duckdb.connect()
    try:
        connection.execute(
            """CREATE TABLE canonical_data (
                unique_id VARCHAR,
                original_address_concat VARCHAR,
                clean_full_address VARCHAR,
                postcode VARCHAR
            )"""
        )
        connection.executemany("INSERT INTO canonical_data VALUES (?, ?, ?, ?)", records)
        if include_cleaned:
            connection.execute("COPY canonical_data TO ? (FORMAT PARQUET)", [str(path)])
        else:
            connection.execute(
                """COPY (
                    SELECT
                        unique_id,
                        original_address_concat,
                        postcode
                    FROM canonical_data
                ) TO ? (FORMAT PARQUET)""",
                [str(path)],
            )
    finally:
        connection.close()


def write_classified_canonical_data(path: Path) -> None:
    source_path = path.with_name("base-canonical.parquet")
    write_canonical_data(source_path)
    source_sql = str(source_path).replace("'", "''")
    connection = duckdb.connect()
    try:
        connection.execute(
            f"""COPY (
                SELECT *,
                    CASE WHEN unique_id IS NULL THEN 'RD99' ELSE 'RD06' END
                        AS classificationcode,
                    CASE WHEN unique_id IS NULL THEN NULL ELSE '2' END
                        AS floorlevel
                FROM read_parquet('{source_sql}')
            ) TO ? (FORMAT PARQUET)""",
            [str(path)],
        )
    finally:
        connection.close()


def test_loads_direct_file_and_prepared_folder_layouts(tmp_path: Path) -> None:
    direct_file = tmp_path / "canonical.parquet"
    write_canonical_data(direct_file)

    direct_source = load_canonical_source(direct_file)
    assert direct_source is not None
    assert direct_source.display_address_column == "original_address_concat"
    assert direct_source.cleaned_address_column == "clean_full_address"
    assert direct_source.columns_to_retain() == (
        "unique_id",
        "postcode",
        "clean_full_address",
        "original_address_concat",
    )

    prepared_folder = tmp_path / "prepared"
    prepared_folder.mkdir()
    single_file = prepared_folder / "ukam_canonical_addresses.parquet"
    write_canonical_data(single_file)
    folder_source = load_canonical_source(prepared_folder)
    assert folder_source is not None
    assert folder_source.parquet_paths == (single_file,)

    chunked_folder = tmp_path / "chunked"
    chunks = chunked_folder / "ukam_canonical_addresses_chunks"
    chunks.mkdir(parents=True)
    first_chunk = chunks / "canonical_chunk_0001.parquet"
    second_chunk = chunks / "canonical_chunk_0002.parquet"
    write_canonical_data(second_chunk)
    write_canonical_data(first_chunk)
    chunked_source = load_canonical_source(chunked_folder)
    assert chunked_source is not None
    assert chunked_source.parquet_paths == (first_chunk, second_chunk)


def test_rejects_missing_cleaned_address_column(tmp_path: Path) -> None:
    path = tmp_path / "missing-cleaned.parquet"
    write_canonical_data(path, include_cleaned=False)

    with pytest.raises(ValueError, match="clean_full_address"):
        load_canonical_source(path)


def test_search_returns_stable_pages_and_literal_substrings(
    tmp_path: Path,
) -> None:
    path = tmp_path / "canonical.parquet"
    write_canonical_data(path)
    source = load_canonical_source(path)
    assert source is not None

    page_one = search_canonical_data(
        source, postcode="e58ry", address_query="street", page=1
    )
    page_two = search_canonical_data(
        source, postcode="E5 8RY", address_query="STREET", page=2
    )
    page_four = search_canonical_data(
        source, postcode="E5 8RY", address_query="STREET", page=4
    )
    partial_postcode = search_canonical_data(
        source, postcode="E5", address_query=None, page=1
    )
    tokenised_address = search_canonical_data(
        source, postcode=None, address_query="LONDON 199", page=1
    )

    assert len(page_one.rows) == CANONICAL_PAGE_SIZE
    assert len(page_two.rows) == CANONICAL_PAGE_SIZE
    assert len(page_four.rows) == CANONICAL_PAGE_SIZE
    assert (
        len(
            {
                (row["canonical_id"], row["cleaned_address"])
                for page in (page_one, page_two, page_four)
                for row in page.rows
            }
        )
        == CANONICAL_PAGE_SIZE * 3
    )
    assert page_one.has_previous is False
    assert page_one.has_next is True
    assert page_two.has_previous is True
    assert page_two.has_next is True
    assert page_four.has_previous is True
    assert page_four.has_next is False
    assert partial_postcode.has_next is True
    assert {row["canonical_postcode"] for row in partial_postcode.rows} == {"E5 8RY"}
    assert len(tokenised_address.rows) == 2
    duplicate_id_rows = search_canonical_data(
        source, postcode="E5 8RY", address_query="199 TEST STREET", page=1
    ).rows
    assert [row["canonical_id"] for row in duplicate_id_rows] == [
        "CANON_0199",
        "CANON_0199",
    ]
    assert [row["cleaned_address"] for row in duplicate_id_rows] == [
        "199 TEST STREET HACKNEY LONDON",
        "FLAT 199 TEST STREET HACKNEY LONDON",
    ]
    filtered_by_id = search_canonical_data(
        source, unique_id_query="percent", postcode=None, address_query=None, page=1
    )
    assert filtered_by_id.unique_id_query == "percent"
    assert [row["canonical_id"] for row in filtered_by_id.rows] == ["PERCENT"]
    assert (
        search_canonical_data(
            source, postcode="E5 8RY", address_query="street", page=1
        ).rows
        == page_one.rows
    )

    assert [
        row["canonical_id"]
        for row in search_canonical_data(
            source, postcode=None, address_query="%", page=1
        ).rows
    ] == ["PERCENT"]
    assert [
        row["canonical_id"]
        for row in search_canonical_data(
            source, postcode=None, address_query="_", page=1
        ).rows
    ] == ["UNDERSCORE"]


def test_search_validation_lookup_and_parquet_plan(tmp_path: Path) -> None:
    path = tmp_path / "canonical.parquet"
    write_canonical_data(path)
    source = load_canonical_source(path)
    assert source is not None

    assert normalise_postcode_search("n165fd") == "N16 5FD"
    assert find_canonical_record(source, "PERCENT")["canonical_postcode"] == "N16 5FD"
    assert find_canonical_record(source, "missing") is None
    with pytest.raises(ValueError, match="unique ID"):
        search_canonical_data(source, postcode=None, address_query=None, page=1)
    with pytest.raises(ValueError, match="at least 1"):
        search_canonical_data(source, postcode="E5 8RY", address_query=None, page=0)
    with pytest.raises(ValueError, match="100 characters"):
        search_canonical_data(source, postcode=None, address_query="x" * 101, page=1)

    connection = duckdb.connect()
    try:
        plan = connection.execute(
            f"EXPLAIN SELECT * FROM {canonical_scan_sql(source)}"
        ).fetchall()
    finally:
        connection.close()
    assert "PARQUET_SCAN" in str(plan).upper() or "READ_PARQUET" in str(plan).upper()


def test_valid_spaced_postcode_uses_exact_matching(tmp_path: Path) -> None:
    path = tmp_path / "canonical.parquet"
    write_canonical_data(path, include_long_postcode=True)
    source = load_canonical_source(path)
    assert source is not None

    exact_postcode = search_canonical_data(source, postcode="E5 8RY", page=1)
    unspaced_postcode = search_canonical_data(
        source, postcode="E58RY", address_query="LONG POSTCODE", page=1
    )
    partial_postcode = search_canonical_data(
        source, postcode="E5 8R", address_query="LONG POSTCODE", page=1
    )

    assert {row["canonical_postcode"] for row in exact_postcode.rows} == {"E5 8RY"}
    assert [row["canonical_postcode"] for row in unspaced_postcode.rows] == ["ME5 8RY"]
    assert [row["canonical_postcode"] for row in partial_postcode.rows] == ["ME5 8RY"]


def test_search_returns_additional_canonical_columns(tmp_path: Path) -> None:
    path = tmp_path / "classified-canonical.parquet"
    write_classified_canonical_data(path)

    source = load_canonical_source(path)
    assert source is not None
    assert source.additional_canonical_columns == ("classificationcode", "floorlevel")

    page = search_canonical_data(source, postcode="E5", page=1)

    assert page.rows
    assert page.additional_canonical_columns == ("classificationcode", "floorlevel")
    assert {row["classificationcode"] for row in page.rows} == {"RD06"}
    assert {row["floorlevel"] for row in page.rows} == {"2"}
    assert find_canonical_record(source, "CANON_0000")["classificationcode"] == "RD06"

    floor_only_source = load_canonical_source(
        path, additional_canonical_columns=("floorlevel", "missing")
    )
    assert floor_only_source is not None
    assert floor_only_source.additional_canonical_columns == ("floorlevel",)
