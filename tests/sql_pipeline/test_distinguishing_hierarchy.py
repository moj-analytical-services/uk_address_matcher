"""Synthetic examples and an independent complete-suffix support oracle."""

import json
from collections import defaultdict

import pytest

from uk_address_matcher.cleaning.chunking_strategies import (
    _materialise_canonical_distinguishing_features,
)
from uk_address_matcher.cleaning.hierarchy import _materialise_distinguishing_hierarchy
from uk_address_matcher.cleaning.materialisation import _CanonicalIntermediates
from uk_address_matcher.prepare_canonical import (
    load_prepared_canonical_data,
    prepare_canonical_folder,
)


@pytest.fixture
def con(duck_con):
    return duck_con


def _source(con):
    # Two repeated property rows and an alias must contribute support once.
    rows = [
        (1, "a", "9 FABLE LANE STORYVILLE COUNTY"),
        (2, "b", "7 FABLE LANE STORYVILLE COUNTY"),
        (3, "c", "5 FABLE LANE STORYVILLE COUNTY"),
        (4, "a", "9 FABLE LANE STORYVILLE COUNTY"),
        (5, "a", "OLD NAME FABLE LANE STORYVILLE COUNTY"),
        (6, "d", "1 OTHER ROAD STORYVILLE COUNTY"),
        (7, "e", "2 OTHER ROAD STORYVILLE COUNTY"),
        (8, "f", "1 WISH ROAD ELSEWHERE COUNTY"),
        (9, "g", "2 WISH ROAD ELSEWHERE COUNTY"),
        (10, "isolated", "ENTIRELY SEPARATE"),
        (11, None, "3 FABLE LANE STORYVILLE COUNTY"),
        (12, "empty", ""),
        (13, "null", None),
        (14, "unicode", "1 ÉCHO ROAD STORYVILLE COUNTY"),
        (15, "unicode2", "2 ÉCHO ROAD STORYVILLE COUNTY"),
    ]
    con.execute(
        "CREATE TABLE source(ukam_address_id BIGINT, unique_id VARCHAR, "
        "clean_full_address VARCHAR)"
    )
    con.executemany("INSERT INTO source VALUES (?, ?, ?)", rows)
    return con.table("source").project(
        "*, string_split(clean_full_address, ' ') AS clean_full_address_tokens"
    )


def _oracle(source_rows, adjacent_rows):
    properties = defaultdict(set)
    for uid, tokens in source_rows:
        if tokens is not None:
            for depth in range(1, len(tokens) + 1):
                if uid is not None:
                    properties[tuple(tokens[-depth:])].add(uid)
    result = {}
    for identifier, tokens, depth in adjacent_rows:
        parts = []
        if depth:
            tail = tuple(tokens)
            for _ in range(2):
                support = len(properties[tail])
                parent = next(
                    (
                        tail[-d:]
                        for d in range(len(tail) - 1, 0, -1)
                        if len(properties[tail[-d:]]) > support
                    ),
                    (),
                )
                parts.append(
                    {"tokens": list(tail[: len(tail) - len(parent)]), "support": support}
                )
                tail = parent
                if not tail:
                    break
        result[identifier] = (parts + [None, None])[:2]
    return result


@pytest.mark.parametrize("depth", [2, 3])
@pytest.mark.parametrize("storage_kind", ["native", "parquet"])
@pytest.mark.parametrize("batch_rows", [3, 100])
def test_global_support_and_original_fields(
    con, monkeypatch, depth, storage_kind, batch_rows
):
    import uk_address_matcher.cleaning.chunking_strategies as chunking

    monkeypatch.setattr(chunking, "_CANONICAL_ADJACENT_BATCH_ROWS", batch_rows)
    source = _source(con)
    with _CanonicalIntermediates(con) as storage:
        selected_storage = storage if storage_kind == "parquet" else None
        native = _materialise_canonical_distinguishing_features(
            con,
            source,
            "native_first_cut",
            _storage=selected_storage,
        )
        original_columns = native.columns
        original = native.order("ukam_address_id").fetchall()
        first = _materialise_canonical_distinguishing_features(
            con,
            source,
            "with_depth",
            _storage=selected_storage,
            include_shared_depth=True,
        )
        expected = _oracle(
            source.project("unique_id, clean_full_address_tokens").fetchall(),
            first.project(
                "ukam_address_id, common_adj_start_tokens, __shared_suffix_depth"
            ).fetchall(),
        )
        result = _materialise_distinguishing_hierarchy(
            con,
            source,
            "with_depth",
            depth=depth,
            _storage=selected_storage,
        )
        assert (
            result.project(", ".join(original_columns))
            .order("ukam_address_id")
            .fetchall()
            == original
        )
        assert "__shared_suffix_depth" not in result.columns
        new_columns = ["second_shared_part"] + (
            ["third_shared_part"] if depth == 3 else []
        )
        for row in result.project(
            ", ".join(["ukam_address_id", *new_columns])
        ).fetchall():
            assert list(row[1:]) == expected[row[0]][: depth - 1]
        assert not con.sql(
            "SELECT table_name FROM duckdb_tables() "
            "WHERE table_name LIKE '__ukam_hierarchy_%'"
        ).fetchall()
        assert not con.sql(
            "SELECT view_name FROM duckdb_views() "
            "WHERE view_name LIKE '__ukam_hierarchy_%'"
        ).fetchall()


def test_three_segments_example(con):
    source = _source(con)
    _materialise_canonical_distinguishing_features(
        con, source, "adjacent", include_shared_depth=True
    )
    result = _materialise_distinguishing_hierarchy(con, source, "adjacent", depth=3)
    row = (
        result.filter("ukam_address_id = 2")
        .project("distinguishing_adj_start_tokens, second_shared_part, third_shared_part")
        .fetchone()
    )
    assert row == (
        ["7"],
        {"tokens": ["FABLE", "LANE"], "support": 3},
        {"tokens": ["STORYVILLE"], "support": 7},
    )


@pytest.mark.parametrize("depth", [2, 3])
def test_folder_exports_requested_columns(con, tmp_path, depth):
    data = con.sql("""
        SELECT * FROM (VALUES
            ('a', '9 FABLE LANE STORYVILLE', 'ZZ1 1ZZ'),
            ('b', '7 FABLE LANE STORYVILLE', 'ZZ1 1ZZ'),
            ('c', '5 FABLE LANE STORYVILLE', 'ZZ1 1ZZ')
        ) t(unique_id, address_concat, postcode)
    """)
    prepare_canonical_folder(
        data,
        tmp_path,
        con=con,
        distinguishing_hierarchy_depth=depth,
        show_progress="off",
        _derive_road_catalogue=False,
    )
    manifest = json.loads((tmp_path / "ukam_manifest.json").read_text())
    assert manifest["preparation_options"]["distinguishing_hierarchy_depth"] == depth
    result = con.read_parquet(str(tmp_path / "ukam_canonical_addresses.parquet"))
    assert "second_shared_part" in result.columns
    assert ("third_shared_part" in result.columns) == (depth == 3)
    assert "__shared_suffix_depth" not in result.columns
    loaded = load_prepared_canonical_data(tmp_path, con=con).addresses
    columns = "unique_id, second_shared_part"
    if depth == 3:
        columns += ", third_shared_part"
    assert loaded.project(columns).order("unique_id").fetchall() == (
        result.project(columns).order("unique_id").fetchall()
    )
    assert (
        str(result.types[result.columns.index("second_shared_part")])
        == "STRUCT(tokens VARCHAR[], support BIGINT)"
    )


@pytest.mark.parametrize("depth, adjacent", [(0, True), (4, True), (2, False)])
def test_invalid_depth_rejected_before_cleaning(con, tmp_path, depth, adjacent):
    data = con.sql(
        "SELECT 'a' unique_id, '9 FABLE LANE' address_concat, 'ZZ1 1ZZ' postcode"
    )
    with pytest.raises(ValueError):
        prepare_canonical_folder(
            data,
            tmp_path,
            con=con,
            distinguishing_hierarchy_depth=depth,
            derive_distinguishing_wrt_adjacent_records=adjacent,
        )


@pytest.mark.parametrize("storage_kind", ["native", "parquet"])
def test_hierarchy_intermediates_removed_on_failure(con, monkeypatch, storage_kind):
    import uk_address_matcher.cleaning.hierarchy as hierarchy

    source = _source(con)
    with _CanonicalIntermediates(con) as storage:
        selected_storage = storage if storage_kind == "parquet" else None
        _materialise_canonical_distinguishing_features(
            con,
            source,
            "adjacent",
            _storage=selected_storage,
            include_shared_depth=True,
        )

        def fail():
            raise RuntimeError("Injected count failure")

        monkeypatch.setattr(hierarchy, "_hierarchy_support", fail)
        with pytest.raises(RuntimeError, match="Injected count failure"):
            _materialise_distinguishing_hierarchy(
                con,
                source,
                "adjacent",
                depth=3,
                _storage=selected_storage,
            )
        assert not con.sql(
            "SELECT table_name FROM duckdb_tables() "
            "WHERE table_name LIKE '__ukam_hierarchy_%'"
        ).fetchall()
        assert not con.sql(
            "SELECT view_name FROM duckdb_views() "
            "WHERE view_name LIKE '__ukam_hierarchy_%'"
        ).fetchall()


def test_parquet_batches_keep_rows_across_hierarchy_schema_change(
    con, monkeypatch, tmp_path
):
    # The first-cut files have one row group each; the supported output has two.
    # Exercise more than one output row group when transferring ownership from
    # several first-cut files to the supported output with a wider schema.
    import uk_address_matcher.cleaning.chunking_strategies as chunking

    monkeypatch.setattr(chunking, "_CANONICAL_ADJACENT_BATCH_ROWS", 35_000)
    source = con.sql("""
        SELECT i AS ukam_address_id, i::VARCHAR AS unique_id,
            concat(i, ' STREET', i % 100, ' LANE TOWN', i % 10,
                ' COUNTY') AS clean_full_address,
            string_split(clean_full_address, ' ') AS clean_full_address_tokens
        FROM range(1, 140001) t(i)
    """)
    with _CanonicalIntermediates(con) as storage:
        first = _materialise_canonical_distinguishing_features(
            con,
            source,
            "adjacent",
            _storage=storage,
            include_shared_depth=True,
        )
        expected = storage.write(
            first.project("* EXCLUDE (__shared_suffix_depth)"),
            "original_features",
        )
        result = _materialise_distinguishing_hierarchy(
            con,
            source,
            "adjacent",
            depth=3,
            _storage=storage,
        )
        assert result.count("*").fetchone()[0] == 140_000
        assert (
            result.aggregate("count(DISTINCT ukam_address_id)").fetchone()[0] == 140_000
        )
        actual = result.project(", ".join(expected.columns))
        assert actual.except_(expected).count("*").fetchone()[0] == 0
        assert expected.except_(actual).count("*").fetchone()[0] == 0
        assert (
            result.filter(
                "second_shared_part.support <> 1400 OR third_shared_part.support <> 14000"
            )
            .count("*")
            .fetchone()[0]
            == 0
        )

    # More than one row group in a TF chunk, with nullable nested support structs.
    # Carrying these structs through the TF stages triggered a DuckDB 1.5 vector
    # error in full London; attaching them after TF preserves its input schema.
    raw = source.project("""
        unique_id,
        CASE WHEN ukam_address_id % 113 = 0
            THEN concat('ONLY PLACE ', ukam_address_id)
            ELSE clean_full_address END AS address_concat,
        NULL::VARCHAR AS postcode
    """)
    prepare_canonical_folder(
        raw,
        tmp_path,
        con=con,
        num_of_chunks=1,
        distinguishing_hierarchy_depth=3,
        show_progress="off",
        _derive_road_catalogue=False,
    )
    loaded = load_prepared_canonical_data(tmp_path, con=con).addresses
    assert loaded.count("*").fetchone()[0] == 140_000
    assert loaded.filter("second_shared_part IS NULL").count("*").fetchone()[0] > 0
    assert loaded.filter("third_shared_part IS NOT NULL").count("*").fetchone()[0] > 0
