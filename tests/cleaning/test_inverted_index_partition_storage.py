from __future__ import annotations

from pathlib import Path

import pytest

from uk_address_matcher.cleaning import chunking_strategies as cs


@pytest.fixture
def tracked_directories(duck_con, monkeypatch, tmp_path):
    directories = []
    native_temporary_directory = cs.TemporaryDirectory

    def temporary_directory(**kwargs):
        directory = native_temporary_directory(dir=tmp_path, **kwargs)
        directories.append(Path(directory.name))
        return directory

    monkeypatch.setattr(cs, "TemporaryDirectory", temporary_directory)
    yield
    assert directories and all(not directory.exists() for directory in directories)
    assert not any(
        name.startswith("__ukam_index_keys_")
        for (name,) in duck_con.execute("SHOW TABLES").fetchall()
    )


@pytest.mark.parametrize("id_type", ["BIGINT", "VARCHAR", "HUGEINT"])
@pytest.mark.parametrize("empty_keys", [False, True])
def test_partitioned_index_matches_single_pass(
    duck_con, tracked_directories, id_type, empty_keys
):
    # Values beyond the precise DOUBLE range exercise the HUGEINT fallback.
    start = 2**100 if id_type == "HUGEINT" else 0
    tokens = "['ONE']" if empty_keys else "['ONE', 'EXAMPLE', 'ROAD']"
    source = duck_con.sql(f"""
        SELECT CAST(i + {start} AS {id_type}) AS unique_id,
            {tokens} AS clean_full_address_tokens
        FROM range(3) AS t(i)
        UNION ALL
        SELECT CAST({start} AS {id_type}), {tokens}
    """)
    baseline = cs.derive_inverted_index(source, duck_con, num_of_chunks=1)
    candidate = cs.derive_inverted_index(source, duck_con, num_of_chunks=5)
    assert candidate.columns == baseline.columns
    assert candidate.types == baseline.types

    def rows(relation):
        return sorted((key, sorted(ids), name) for key, ids, name in relation.fetchall())

    assert rows(candidate) == rows(baseline)


def test_partition_files_removed_after_aggregation_failure(
    duck_con, monkeypatch, tracked_directories
):
    def fail_aggregation(*args, **kwargs):
        raise RuntimeError("injected bucket aggregation failure")

    monkeypatch.setattr(cs, "_build_inverted_index_from_scalar_keys", fail_aggregation)
    source = duck_con.sql("""
        SELECT 1::BIGINT AS unique_id,
            ['ONE', 'EXAMPLE', 'ROAD'] AS clean_full_address_tokens
    """)
    with pytest.raises(RuntimeError, match="injected bucket aggregation failure"):
        cs.derive_inverted_index(source, duck_con, num_of_chunks=5)


@pytest.mark.parametrize(
    "ancestor", ["unique_id=999", "unique_id=sentinel", "key=SENTINEL", "key_bucket=999"]
)
def test_partitioned_index_ignores_ancestor_names(
    duck_con, monkeypatch, tmp_path, ancestor
):
    parent = tmp_path / ancestor
    parent.mkdir()
    temporary_directory = cs.TemporaryDirectory
    monkeypatch.setattr(
        cs, "TemporaryDirectory", lambda **kw: temporary_directory(dir=parent, **kw)
    )
    source = duck_con.sql(
        "SELECT 1::BIGINT unique_id, ['ONE', 'ROAD'] clean_full_address_tokens"
    )
    index = cs.derive_inverted_index(source, duck_con, num_of_chunks=3)
    assert index.fetchall() == [("ONE ROAD", [1], "bigram")]
    assert list(parent.iterdir()) == []


@pytest.mark.parametrize(
    "field, count", [("unique_id", 2), ("key", 2), ("key", 22), ("default", 22)]
)
def test_partitioned_index_preserves_collations(duck_con, monkeypatch, field, count):
    if field == "default":
        duck_con.execute("SET default_collation = 'nocase'").fetchall()
    identifier = (
        "CASE WHEN i = 0 THEN 'Aa' ELSE 'aa' END COLLATE NOCASE"
        if field == "unique_id"
        else "i + 1"
    )
    token = (
        "'ONE'"
        if field == "unique_id"
        else "CASE WHEN i % 2 = 0 THEN 'a' ELSE 'A' END COLLATE NOCASE"
    )
    source = duck_con.sql(
        f"SELECT {identifier} unique_id, "
        f"[{token}, 'EXAMPLE', 'ROAD'] clean_full_address_tokens "
        f"FROM range({count}) t(i)"
    )
    native = cs.derive_inverted_index(source, duck_con, num_of_chunks=1)
    materialisations = []
    original = cs._materialise_relation

    def record(con, relation, name, **kwargs):
        materialisations.append(name)
        return original(con, relation, name, **kwargs)

    monkeypatch.setattr(cs, "_materialise_relation", record)
    candidate = cs.derive_inverted_index(source, duck_con, num_of_chunks=3)

    def rows(index):
        return sorted(
            (key.lower(), sorted(str(i).lower() for i in ids), strategy)
            for key, ids, strategy in index.fetchall()
        )

    assert rows(candidate) == rows(native)
    assert any(name.startswith("__ukam_index_keys_") for name in materialisations)
