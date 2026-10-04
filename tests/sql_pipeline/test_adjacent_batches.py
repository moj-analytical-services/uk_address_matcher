import pytest

from uk_address_matcher.cleaning import chunking_strategies as cs


def _source(con, rows=60):
    return con.sql(f"""
        SELECT i + 1 AS ukam_address_id, (i // 2)::VARCHAR AS unique_id,
            i::VARCHAR || ' EXAMPLE ROAD ZZ1 1ZZ' AS clean_full_address,
            string_split(clean_full_address, ' ') AS clean_full_address_tokens
        FROM range({rows}) AS r(i)
    """)


@pytest.mark.parametrize("batch_rows", [12, 100])
def test_only_global_adjacent_input_is_materialised(duck_con, monkeypatch, batch_rows):
    original = cs.create_sql_pipeline
    inputs = []

    def capture(con, input_rel, stage_specs, **kwargs):
        inputs.append(input_rel)
        return original(con, input_rel, stage_specs, **kwargs)

    monkeypatch.setattr(cs, "create_sql_pipeline", capture)
    monkeypatch.setattr(cs, "_CANONICAL_ADJACENT_BATCH_ROWS", batch_rows)
    source = _source(duck_con).project("*, 'unused' AS unused_column")
    result = cs._materialise_canonical_distinguishing_features(duck_con, source, "result")
    assert result.count("*").fetchone()[0] == 60
    assert len(inputs) > 1 if batch_rows == 12 else len(inputs) == 1
    assert all(len(relation.columns) == 4 for relation in inputs)
    assert all(
        ("AS MATERIALIZED" in relation.sql_query().upper()) == (batch_rows == 100)
        for relation in inputs
    )


def test_adjacent_batches_preserve_interior_rows(duck_con, monkeypatch, tmp_path):
    source = _source(duck_con)
    baseline = cs._materialise_canonical_distinguishing_features(
        duck_con, source, "baseline"
    )
    original = cs._canonical_distinguishing_features
    batches = []

    def capture(con, batch, debug_options):
        batches.append(
            [
                r[0]
                for r in batch.order(
                    "reverse(clean_full_address), CAST(unique_id AS VARCHAR), "
                    "ukam_address_id"
                )
                .project("ukam_address_id")
                .fetchall()
            ]
        )
        return original(con, batch, debug_options)

    monkeypatch.setattr(cs, "_CANONICAL_ADJACENT_BATCH_ROWS", 12)
    monkeypatch.setattr(cs, "_canonical_distinguishing_features", capture)
    monkeypatch.setattr("tempfile.tempdir", str(tmp_path))
    result = cs._materialise_canonical_distinguishing_features(
        duck_con, source, "batched"
    )
    assert len(batches) > 1
    assert sorted(i for batch in batches for i in batch) == list(range(1, 61))
    boundary_ids = {i for batch in batches for i in (*batch[:3], *batch[-3:])}
    expected = {r[0]: r for r in baseline.fetchall()}
    assert result.columns == baseline.columns
    assert sorted(r[0] for r in result.fetchall()) == list(range(1, 61))
    assert all(r == expected[r[0]] for r in result.fetchall() if r[0] not in boundary_ids)
    assert not list(tmp_path.iterdir())
    assert not any(
        "__ukam_adjacent_" in name for (name,) in duck_con.sql("SHOW TABLES").fetchall()
    )


@pytest.mark.parametrize(
    "rows, id_type", [(0, "VARCHAR"), (1, "BIGINT"), (20, "INTEGER"), (20, "HUGEINT")]
)
def test_adjacent_batches_empty_small_and_skewed(duck_con, monkeypatch, rows, id_type):
    # A repeated address must not create missing-range errors or lose rows.
    source = _source(duck_con, rows).project(
        f"ukam_address_id, unique_id::{id_type} AS unique_id, "
        "'EXAMPLE ROAD' AS clean_full_address, "
        "['EXAMPLE', 'ROAD'] AS clean_full_address_tokens"
    )
    if id_type == "HUGEINT":

        def no_scatter(*args, **kwargs):
            pytest.fail("HUGEINT identifiers must retain native storage")

        monkeypatch.setattr(cs, "TemporaryDirectory", no_scatter)
    monkeypatch.setattr(cs, "_CANONICAL_ADJACENT_BATCH_ROWS", 4)
    result = cs._materialise_canonical_distinguishing_features(duck_con, source, "result")
    assert result.count("*").fetchone()[0] == rows


def test_adjacent_batch_failure_cleans_files_and_partial_output(
    duck_con, monkeypatch, tmp_path
):
    original = cs._canonical_distinguishing_features
    calls = 0

    def fail_second(con, batch, debug_options):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("synthetic batch failure")
        return original(con, batch, debug_options)

    monkeypatch.setattr(cs, "_CANONICAL_ADJACENT_BATCH_ROWS", 12)
    monkeypatch.setattr(cs, "_canonical_distinguishing_features", fail_second)
    monkeypatch.setattr("tempfile.tempdir", str(tmp_path))
    with pytest.raises(RuntimeError, match="synthetic batch failure"):
        cs._materialise_canonical_distinguishing_features(
            duck_con, _source(duck_con), "partial"
        )
    assert not list(tmp_path.iterdir())
    assert not any(
        name == "partial" or "__ukam_adjacent_" in name
        for (name,) in duck_con.sql("SHOW TABLES").fetchall()
    )
