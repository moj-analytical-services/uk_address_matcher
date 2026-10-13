import random

import pytest

from uk_address_matcher.cleaning import chunking_strategies as cs
from uk_address_matcher.cleaning.steps.token_parsing import (
    _derive_distinguishing_token_components,
    _separate_distinguishing_start_tokens_from_with_respect_to_adjacent_records,
)
from uk_address_matcher.sql_pipeline.runner import create_sql_pipeline


def _native(con, source, debug_options=None):
    return (
        create_sql_pipeline(
            con,
            source,
            [
                _separate_distinguishing_start_tokens_from_with_respect_to_adjacent_records(
                    include_input_columns=False, use_precomputed_tokens=True
                ),
                _derive_distinguishing_token_components,
            ],
        )
        .run(debug_options)
        .project(", ".join(("ukam_address_id", *cs.DISTINGUISHING_FEATURE_COLUMNS)))
    )


def _source(con):
    rng = random.Random(917)
    rows = [
        (
            i,
            str(rng.randrange(30)),
            f"{i % 9} {rng.choice(['EXAMPLE ROAD', 'OTHER LANE', 'CAFÉ WALK'])} "
            f"{rng.choice(['TEST TOWN', 'OTHER TOWN'])}",
        )
        for i in range(1, 181)
    ]
    rows += [(181 + i, "same", "9 EXAMPLE ROAD TEST TOWN") for i in range(8)]
    rows += [
        (189, None, "EXAMPLE ROAD"),
        (190, "empty", ""),
        (191, "null", None),
        (192, "suffix", "ROAD TEST TOWN"),
    ]
    con.execute(
        "CREATE TABLE fixture(ukam_address_id INTEGER, unique_id VARCHAR, "
        "clean_full_address VARCHAR)"
    )
    con.executemany("INSERT INTO fixture VALUES (?, ?, ?)", rows)
    return con.table("fixture").project(
        "*, string_split(clean_full_address, ' ') AS clean_full_address_tokens"
    )


@pytest.mark.parametrize("batch_rows", [20, 1000])
def test_optimised_features_match_native_with_fixed_ids(
    duck_con, monkeypatch, batch_rows
):
    source = _source(duck_con)
    monkeypatch.setattr(cs, "_CANONICAL_ADJACENT_BATCH_ROWS", batch_rows)
    expected = _native(duck_con, source).order("ukam_address_id").fetchall()
    assert (
        cs._canonical_distinguishing_features(duck_con, source, None)
        .order("ukam_address_id")
        .fetchall()
        == expected
    )
    with monkeypatch.context() as native:
        native.setattr(cs, "_canonical_distinguishing_features", _native)
        baseline = (
            cs._materialise_canonical_distinguishing_features(
                duck_con, source, "baseline"
            )
            .order("ukam_address_id")
            .fetchall()
        )
    candidate = (
        cs._materialise_canonical_distinguishing_features(duck_con, source, "candidate")
        .order("ukam_address_id")
        .fetchall()
    )
    assert candidate == baseline


@pytest.mark.parametrize(
    "address,tokens",
    [
        ("A B", ["CUSTOM"]),
        ("A  B", ["A", "", "B"]),
        ("A B", [None, "B"]),
        ("A\u0301 B", ["A\u0301", "B"]),
        ("X\x00Y Z", ["X\x00Y", "Z"]),
    ],
)
def test_non_native_tokens_and_reversal_edges_preserve_native(duck_con, address, tokens):
    duck_con.execute(
        "CREATE TABLE fixture(ukam_address_id INTEGER, unique_id VARCHAR, "
        "clean_full_address VARCHAR, clean_full_address_tokens VARCHAR[])"
    )
    duck_con.executemany(
        "INSERT INTO fixture VALUES (?, ?, ?, ?)",
        [(1, "one", address, tokens), (2, "two", address, tokens)],
    )
    source = duck_con.table("fixture")
    assert (
        cs._canonical_distinguishing_features(duck_con, source, None)
        .order("ukam_address_id")
        .fetchall()
        == _native(duck_con, source).order("ukam_address_id").fetchall()
    )


@pytest.mark.parametrize("collation", ["", "nocase"])
def test_collations_preserve_native(duck_con, collation):
    source = _source(duck_con)
    if collation:
        duck_con.execute("SET default_collation='nocase'")
    source = source.project(
        "ukam_address_id, unique_id, clean_full_address COLLATE nocase "
        "AS clean_full_address, clean_full_address_tokens"
    )
    assert (
        cs._canonical_distinguishing_features(duck_con, source, None)
        .order("ukam_address_id")
        .fetchall()
        == _native(duck_con, source).order("ukam_address_id").fetchall()
    )


def test_custom_numeric_token_cache_preserves_native(duck_con):
    source = duck_con.sql("""
        SELECT i::INTEGER AS ukam_address_id, i::VARCHAR AS unique_id,
            'EXAMPLE ROAD' AS clean_full_address,
            [1, 2]::INTEGER[] AS clean_full_address_tokens
        FROM range(1, 4) AS rows(i)
    """)
    assert (
        cs._canonical_distinguishing_features(duck_con, source, None)
        .order("ukam_address_id")
        .fetchall()
        == _native(duck_con, source).order("ukam_address_id").fetchall()
    )
