from __future__ import annotations

import pytest
from splink import SettingsCreator

from uk_address_matcher import (
    AddressMatcher,
    SplinkStage,
    prepare_canonical_folder,
    prepare_data_for_matching,
)
from uk_address_matcher.cleaning.local_keys import (
    add_local_key_features,
    prepare_canonical_local_keys,
    prepare_source_local_keys,
)
from uk_address_matcher.cleaning.steps.token_parsing import _derive_local_key_tokens
from uk_address_matcher.linking_model.matching.stages.splink import (
    _preserve_anchored_name_aliases as preserve_anchored_name_aliases,
)
from uk_address_matcher.linking_model.splink_model import (
    _align_postcode_name_columns,
    _get_model_settings_dict,
)
from uk_address_matcher.sql_pipeline.runner import create_sql_pipeline


@pytest.mark.parametrize("postcode", ["ZZ1 1ZZ", "ZZ2 1ZZ", "WR9 9XX", None, "missing"])
@pytest.mark.parametrize("prepared_index", [False, True])
def test_local_key_evidence_uses_canonical_postcode_rarity(
    duck_con, postcode, prepared_index
):
    canonical = duck_con.sql("""
        SELECT * FROM (VALUES
            ('001', 'a1', 'MEADOW COTTAGE', 'ZZ1 1ZZ'),
            ('002', 'b1', 'ROSE BANK', 'ZZ1 1ZZ'),
            ('003', 'c1', 'LAUREL GROVE', 'ZZ1 1ZZ'),
            ('004', 'd1', 'MEADOW COTTAGE', 'ZZ2 1ZZ'),
            ('005', 'e1', 'MEADOW COTTAGE', 'ZZ2 1ZZ'),
            ('006', 'f1', 'LAUREL GROVE', 'ZZ2 1ZZ')
        ) addresses(unique_id, ukam_address_id, clean_full_address, postcode)
    """)
    if prepared_index:
        canonical = prepare_canonical_local_keys(duck_con, canonical)
    source = duck_con.sql(
        """
        SELECT *, $postcode::VARCHAR AS postcode, []::VARCHAR[] AS numeric_tokens,
            false AS has_flat_indicator, false AS has_business_unit
        FROM (VALUES ('literal', 'MEADOW COTTAGE'), ('spacing', 'ROSEBANK'))
            queries(ukam_address_id, clean_full_address)
    """,
        params={"postcode": postcode},
    )
    if postcode == "missing":
        source = source.select("* EXCLUDE (postcode)")
    featured, _ = add_local_key_features(duck_con, source, canonical)
    assert featured.select("ukam_address_id, lk_level_by_alias, lk_anchor_uprns").order(
        "ukam_address_id"
    ).fetchall() == [
        ("literal", {"a1": 6}, ["001"])
        if prepared_index and postcode == "ZZ1 1ZZ"
        else ("literal", {}, []),
        ("spacing", {"b1": 2}, [])
        if prepared_index and postcode == "ZZ1 1ZZ"
        else ("spacing", {}, []),
    ]


def test_local_key_lookup_avoids_expanding_unrelated_postcodes(duck_con):
    duck_con.execute("SET memory_limit = '64MB'")
    duck_con.execute("SET max_temp_directory_size = '0B'")
    duck_con.execute("SET threads = 1")
    canonical = duck_con.sql("""
        SELECT address_id::VARCHAR AS ukam_address_id,
            address_id::VARCHAR AS unique_id,
            'ZZ' || (address_id // 2)::VARCHAR || ' 1ZZ' AS postcode,
            CASE WHEN address_id % 2 = 0 THEN 'MEADOW COTTAGE'
                ELSE 'ORCHARD HOUSE' END AS clean_full_address,
            [struct_pack(pos := 1, kind := 'word',
                key := CASE WHEN address_id % 2 = 0 THEN 'MEADOW' ELSE 'ORCHARD' END,
                df_uprns := 1::UTINYINT, n_uprns := 2::UINTEGER),
             struct_pack(pos := 2, kind := 'word',
                key := CASE WHEN address_id % 2 = 0 THEN 'COTTAGE' ELSE 'HOUSE' END,
                df_uprns := 1::UTINYINT, n_uprns := 2::UINTEGER),
             struct_pack(pos := 1, kind := 'phrase',
                key := clean_full_address,
                df_uprns := 1::UTINYINT, n_uprns := 2::UINTEGER)] AS local_key_index
        FROM range(200000) addresses(address_id)
    """)
    source = duck_con.sql("""
        SELECT *, 'ZZ0 1ZZ' AS postcode, []::VARCHAR[] AS numeric_tokens,
            false AS has_flat_indicator, false AS has_business_unit
        FROM (VALUES ('literal', 'MEADOW COTTAGE'), ('spacing', 'MEADOWCOTTAGE'))
            queries(ukam_address_id, clean_full_address)
    """)
    featured, returned_canonical = add_local_key_features(duck_con, source, canonical)
    assert featured.select("ukam_address_id, lk_level_by_alias, lk_anchor_uprns").order(
        "ukam_address_id"
    ).fetchall() == [("literal", {"0": 6}, ["0"]), ("spacing", {"0": 2}, [])]
    assert returned_canonical.count("*").fetchone()[0] == 200_000


def test_canonical_local_key_statistics_are_materialised_once(duck_con):
    duck_con.execute("""
        CREATE TEMP TABLE canonical_key_input AS
        SELECT *, 'ZZ1 1ZZ' AS postcode FROM (VALUES
            ('001', 'a1', 'MEADOW COTTAGE'),
            ('002', 'b1', 'ORCHARD HOUSE')
        ) addresses(unique_id, ukam_address_id, clean_full_address)
    """)
    canonical = duck_con.table("canonical_key_input")
    prepared = prepare_canonical_local_keys(duck_con, canonical)
    columns = "ukam_address_id, local_key_index"
    expected = prepared.select(columns).order("ukam_address_id").fetchall()
    duck_con.execute("UPDATE canonical_key_input SET clean_full_address = 'CHANGED NAME'")
    assert prepared.select(columns).order("ukam_address_id").fetchall() == expected
    rebuilt = prepare_canonical_local_keys(duck_con, canonical)
    assert rebuilt.select(columns).order("ukam_address_id").fetchall() != expected


@pytest.mark.parametrize("numericless_only", [True, False])
def test_preparation_keys_preserve_numeric_gaps(duck_con, numericless_only):
    addresses = duck_con.sql("""
        SELECT * FROM (VALUES
            ('source', 'MEADOW COTTAGE', []),
            ('numbered', 'MEADOW 54 COTTAGE', ['54']),
            ('stopwords', 'THE OF', []),
            ('empty', '', []),
            ('missing', NULL, NULL)
        ) addresses(unique_id, clean_full_address, numeric_tokens)
    """)
    prepared = create_sql_pipeline(
        duck_con,
        input_rel=addresses,
        stage_specs=[_derive_local_key_tokens(numericless_only=numericless_only)],
    ).run()
    keys = dict(prepared.select("unique_id, local_key_tokens").fetchall())
    assert keys["source"] == [
        {"pos": 1, "kind": "word", "key": "MEADOW"},
        {"pos": 2, "kind": "word", "key": "COTTAGE"},
        {"pos": 1, "kind": "phrase", "key": "MEADOW COTTAGE"},
    ]
    assert keys["numbered"] == (
        []
        if numericless_only
        else [
            {"pos": 1, "kind": "word", "key": "MEADOW"},
            {"pos": 3, "kind": "word", "key": "COTTAGE"},
        ]
    )
    assert keys["stopwords"] == keys["empty"] == keys["missing"] == []


@pytest.mark.parametrize(
    "numeric_tokens,has_keys", [([], True), (["54"], False), (None, False)]
)
def test_source_key_eligibility_uses_numeric_tokens(duck_con, numeric_tokens, has_keys):
    source = duck_con.sql(
        """
        SELECT 'MEADOW 54 COTTAGE' AS clean_full_address,
            $tokens::VARCHAR[] AS numeric_tokens
        """,
        params={"tokens": numeric_tokens},
    )
    prepared = prepare_source_local_keys(duck_con, source)
    keys = prepared.select("local_key_tokens").fetchone()[0]
    assert bool(keys) is has_keys
    assert all(key["kind"] == "word" for key in keys)
    assert (
        prepare_source_local_keys(duck_con, prepared)
        .select("local_key_tokens")
        .fetchone()[0]
        == keys
    )


def _features(duck_con, text, canonical_rows):
    source = duck_con.sql(
        """
        SELECT 'query' AS unique_id, 'source-alias' AS ukam_address_id,
            'ZZ1 1ZZ' AS postcode, $text AS clean_full_address,
            regexp_extract_all($text, '[0-9]+') AS numeric_tokens,
            false AS has_flat_indicator, false AS has_business_unit,
            NULL::VARCHAR AS resolved_canonical_id
    """,
        params={"text": text},
    )
    canonical = duck_con.sql(
        """
        SELECT entry[1] AS unique_id, entry[2] AS ukam_address_id,
            entry[3] AS clean_full_address, 'ZZ1 1ZZ' AS postcode
        FROM UNNEST($rows::VARCHAR[][]) AS entries(entry)
    """,
        params={"rows": canonical_rows},
    )
    canonical = prepare_canonical_local_keys(duck_con, canonical)
    return add_local_key_features(duck_con, source, canonical)


def test_canonical_local_keys_do_not_materialise_unrelated_columns(duck_con):
    memory_limit, threads, spill_limit = duck_con.execute("""
        SELECT current_setting('memory_limit'), current_setting('threads'),
            current_setting('max_temp_directory_size')
    """).fetchone()
    try:
        duck_con.execute("SET memory_limit = '96MB'")
        duck_con.execute("SET threads = 1")
        duck_con.execute("SET max_temp_directory_size = '0B'")
        canonical = duck_con.sql("""
            SELECT address_id::VARCHAR AS unique_id,
                address_id::VARCHAR AS ukam_address_id,
                'ZZ' || (address_id // 2)::VARCHAR || ' 1ZZ' AS postcode,
                CASE WHEN address_id % 2 = 0 THEN 'MEADOW COTTAGE'
                    ELSE 'ORCHARD HOUSE' END AS clean_full_address,
                repeat(md5(address_id::VARCHAR), 512) AS unrelated_payload
            FROM range(12000) addresses(address_id)
        """)
        prepared = prepare_canonical_local_keys(duck_con, canonical)
        assert prepared.aggregate("count(*), sum(len(local_key_index))").fetchone() == (
            12000,
            36000,
        )
        assert prepared.filter("ukam_address_id = '0'").select(
            "length(unrelated_payload)"
        ).fetchone() == (16384,)
    finally:
        duck_con.execute("SET memory_limit = ?", [memory_limit])
        duck_con.execute("SET threads = ?", [threads])
        duck_con.execute("SET max_temp_directory_size = ?", [spill_limit])


def test_sql_feature_views_keep_parameter_bound_calls_independent(duck_con):
    canonical_rows = [
        ["001", "a1", "MEADOW COTTAGE"],
        ["002", "b1", "LAUREL GROVE"],
    ]
    first, _ = _features(duck_con, "MEADOW COTTAGE", canonical_rows)
    second, _ = _features(duck_con, "LAUREL GROVE", canonical_rows)
    assert first.select("lk_level_by_alias, lk_anchor_uprns").fetchone() == (
        {"a1": 6},
        ["001"],
    )
    assert second.select("lk_level_by_alias, lk_anchor_uprns").fetchone() == (
        {"b1": 6},
        ["002"],
    )


@pytest.mark.parametrize("text", ["54 MEADOW COTTAGE", "MEADOW COTTAGE 54"])
def test_numbered_sources_skip_canonical_key_preparation(duck_con, monkeypatch, text):
    def unexpected_preparation(con, canonical):
        pytest.fail("Canonical keys must not be prepared without eligible sources")

    monkeypatch.setattr(
        "uk_address_matcher.cleaning.local_keys.prepare_canonical_local_keys",
        unexpected_preparation,
    )
    source, canonical = _features(
        duck_con,
        text,
        [
            ["001", "a1", "MEADOW COTTAGE"],
            ["002", "b1", "ORCHARD HOUSE"],
        ],
    )
    for relation in (source, canonical):
        assert all(
            row == ({}, [], False)
            for row in relation.select(
                "lk_level_by_alias, lk_anchor_uprns, lk_eligible"
            ).fetchall()
        )
    assert source.count("*").fetchone()[0] == 1
    assert canonical.count("*").fetchone()[0] == 2


def test_canonical_statistics_recompute_filtered_population(duck_con):
    canonical = duck_con.sql("""
        SELECT * FROM (VALUES
            ('001', 'a1', 'MEADOW COTTAGE 54 TEST ROAD'),
            ('001', 'a2', 'MEADOW COTTAGE'),
            ('002', 'b1', 'MEADOW HOUSE'),
            ('003', 'c1', 'THE OF'),
            ('004', 'd1', NULL)
        ) addresses(unique_id, ukam_address_id, clean_full_address)
    """).select("*, 'ZZ1 1ZZ' AS postcode")
    prepared = prepare_canonical_local_keys(duck_con, canonical)
    assert prepared.count("*").fetchone()[0] == 5
    index = (
        prepared.filter("ukam_address_id = 'a1'").select("local_key_index").fetchone()[0]
    )
    meadow = next(
        key for key in index if key["kind"] == "word" and key["key"] == "MEADOW"
    )
    assert (meadow["df_uprns"], meadow["n_uprns"]) == (2, 4)
    filtered = prepared.filter("unique_id <> '002'")
    rebuilt = prepare_canonical_local_keys(duck_con, filtered)
    index = (
        rebuilt.filter("ukam_address_id = 'a1'").select("local_key_index").fetchone()[0]
    )
    meadow = next(
        key for key in index if key["kind"] == "word" and key["key"] == "MEADOW"
    )
    assert (meadow["df_uprns"], meadow["n_uprns"]) == (1, 3)
    changed = prepared.select("* REPLACE ('CHANGED NAME' AS clean_full_address)")
    assert prepare_canonical_local_keys(duck_con, changed) is not changed


def test_source_cleaning_defers_keys_and_uses_current_text(duck_con):
    source = duck_con.sql("""
        SELECT 'query' AS unique_id, 'MEADOW COTTAGE' AS address_concat,
            'ZZ1 1ZZ' AS postcode
    """)
    cleaned = prepare_data_for_matching(source, duck_con, num_of_chunks=1)
    assert "local_key_tokens" not in cleaned.columns
    changed = cleaned.select("* REPLACE ('ORCHARD HOUSE' AS clean_full_address)")
    prepared = prepare_source_local_keys(duck_con, changed)
    keys = prepared.select("local_key_tokens").fetchone()[0]
    assert any(key["key"] == "ORCHARD HOUSE" for key in keys)
    assert all("MEADOW" not in key["key"] for key in keys)
    changed_numbers = cleaned.select("* REPLACE (['54'] AS numeric_tokens)")
    assert prepare_source_local_keys(duck_con, changed_numbers).select(
        "local_key_tokens"
    ).fetchone() == ([],)


def test_literal_rarity_counts_numbered_properties(duck_con):
    canonical = duck_con.sql("""
        SELECT * FROM (VALUES
            ('001', 'a1', 'ROSEBANK'),
            ('001', 'a2', 'ROSE BANK'),
            ('002', 'b1', 'THE OTHER PLACE ROSE BANK'),
            ('003', 'c1', '54 ROSEBANK'),
            ('004', 'd1', 'LAUREL GROVE')
        ) addresses(unique_id, ukam_address_id, clean_full_address)
    """).select("*, 'ZZ1 1ZZ' AS postcode")
    prepared = prepare_canonical_local_keys(duck_con, canonical)
    index = (
        prepared.filter("ukam_address_id = 'a1'").select("local_key_index").fetchone()[0]
    )
    rosebank = next(key for key in index if key["key"] == "ROSEBANK")
    assert (rosebank["df_uprns"], rosebank["n_uprns"]) == (2, 4)
    filtered = prepared.filter("unique_id NOT IN ('002', '003')")
    rebuilt = prepare_canonical_local_keys(duck_con, filtered)
    index = (
        rebuilt.filter("ukam_address_id = 'a1'").select("local_key_index").fetchone()[0]
    )
    rosebank = next(key for key in index if key["key"] == "ROSEBANK")
    assert (rosebank["df_uprns"], rosebank["n_uprns"]) == (1, 2)


def test_canonical_index_keeps_only_evidential_prefix_keys(duck_con):
    canonical = duck_con.sql("""
        SELECT * FROM (VALUES
            ('001', 'a1', 'MEADOW ROSEBANK'),
            ('002', 'b1', 'MEADOW LAUREL GROVE'),
            ('003', 'c1', 'MEADOW OTHER PLACE ROSE BANK'),
            ('004', 'd1', '54 MEADOW ROSE BANK')
        ) addresses(unique_id, ukam_address_id, clean_full_address)
    """).select("*, 'ZZ1 1ZZ' AS postcode")
    prepared = prepare_canonical_local_keys(duck_con, canonical)
    assert "local_key_tokens" not in prepared.columns
    indexes = prepared.select("local_key_index").fetchall()
    for (index,) in indexes:
        for key in index:
            assert key["pos"] <= 3
            assert key["n_uprns"] == 4
            assert key["key"] != "MEADOW"
            assert key["df_uprns"] <= 3
            assert key["df_uprns"] / key["n_uprns"] <= 0.5


def test_aliases_count_once_and_numbered_targets_remain(duck_con):
    source, canonical = _features(
        duck_con,
        "MEADOW COTTAGE",
        [
            ["001", "a1", "MEADOW COTTAGE 54 TEST ROAD"],
            ["001", "a2", "MEADOW COTTAGE"],
            ["002", "b1", "ORCHARD HOUSE"],
        ],
    )
    assert source.select(
        "lk_level_by_alias, lk_anchor_uprns, lk_eligible"
    ).fetchone() == (
        {"a1": 6, "a2": 6},
        ["001"],
        True,
    )
    assert canonical.select("lk_eligible").fetchall() == [(False,), (False,), (False,)]


def test_identical_text_uprns_are_not_unique(duck_con):
    source, _ = _features(
        duck_con,
        "MEADOW COTTAGE",
        [
            ["001", "a1", "MEADOW COTTAGE"],
            ["002", "a2", "MEADOW COTTAGE"],
            ["003", "b1", "ORCHARD HOUSE"],
            ["004", "b2", "PINE LODGE"],
        ],
    )
    assert source.select("lk_level_by_alias, lk_anchor_uprns").fetchone() == (
        {"a1": 4, "a2": 4},
        [],
    )


def test_phrase_does_not_bridge_numeric_gap(duck_con):
    source, _ = _features(
        duck_con,
        "MEADOW COTTAGE",
        [
            ["001", "a1", "MEADOW 54 COTTAGE"],
            ["002", "b1", "ORCHARD HOUSE"],
        ],
    )
    assert source.select("lk_level_by_alias").fetchone() == ({"a1": 5},)


@pytest.mark.parametrize("text", ["54 MEADOW COTTAGE", "THE OF", ""])
def test_no_evidence_is_neutral(duck_con, text):
    source, _ = _features(
        duck_con,
        text,
        [
            ["001", "a1", "MEADOW COTTAGE"],
            ["002", "b1", "ORCHARD HOUSE"],
        ],
    )
    assert source.select("lk_level_by_alias, lk_eligible").fetchone() == ({}, False)


@pytest.mark.parametrize(
    "column",
    [
        "has_flat_indicator",
        "has_business_unit",
        "resolved_canonical_id",
    ],
)
def test_guard_excludes_units_and_resolved_rows(duck_con, column):
    source, canonical = _features(
        duck_con,
        "MEADOW COTTAGE",
        [
            ["001", "a1", "MEADOW COTTAGE"],
            ["002", "b1", "ORCHARD HOUSE"],
        ],
    )
    source = source.select(
        "* EXCLUDE ("
        + ", ".join(
            [
                "lk_level_by_alias",
                "lk_anchor_uprns",
                "lk_eligible",
            ]
        )
        + ")"
    )
    value = "'001'" if column == "resolved_canonical_id" else "true"
    source = source.select(f"* REPLACE ({value} AS {column})")
    canonical = canonical.select(
        "* EXCLUDE ("
        + ", ".join(
            [
                "lk_level_by_alias",
                "lk_anchor_uprns",
                "lk_eligible",
            ]
        )
        + ")"
    )
    source, _ = add_local_key_features(duck_con, source, canonical)
    assert source.select("lk_eligible").fetchone() == (False,)


def test_canonical_position_gate_preserves_full_background(duck_con):
    source, _ = _features(
        duck_con,
        "MEADOW COTTAGE",
        [
            ["001", "a1", "ORCHARD HOUSE OFF MEADOW COTTAGE"],
            ["002", "b1", "PINE LODGE"],
        ],
    )
    assert source.select("lk_eligible").fetchone() == (False,)


def test_late_canonical_hits_still_count_towards_frequency(duck_con):
    source, _ = _features(
        duck_con,
        "MEADOW",
        [
            ["001", "a1", "MEADOW HOUSE"],
            ["002", "b1", "ORCHARD HOUSE OFF MEADOW"],
        ],
    )
    assert source.select("lk_eligible").fetchone() == (False,)


def test_model_includes_local_key_comparison_and_block():
    model = _get_model_settings_dict()
    comparison = model["comparisons"][-1]
    assert comparison["output_column_name"] == "postcode_distinguishing_name"
    assert "additional_columns_to_retain" not in model
    assert all(
        "coalesce" not in level["sql_condition"].lower()
        for level in comparison["comparison_levels"]
    )
    assert (
        "map_contains(r.lk_level_by_alias"
        in model["blocking_rules_to_generate_predictions"][-1]["blocking_rule"]
    )
    assert [
        level["m_probability"] / level["u_probability"]
        for level in comparison["comparison_levels"][1:]
    ] == ([0.25, 16, 8, 4, 2, 2, 1])


@pytest.mark.parametrize(
    "source_name,target_name,expected_bf",
    [("MEADOW COTTAGE", "MEADOW COTTAGE", 16.0), ("ROSE BANK", "ROSEBANK", 2.0)],
)
@pytest.mark.parametrize("prepared_index", [False, True])
def test_stage_executes_local_key_comparison(
    duck_con, tmp_path, source_name, target_name, expected_bf, prepared_index
):
    source = duck_con.sql(
        """
        SELECT 'query' AS unique_id, $name || ' TEST VILLAGE' AS address_concat,
            'ZZ1 1ZZ' AS postcode, '001' AS ukam_label
    """,
        params={"name": source_name},
    )
    canonical = duck_con.sql(
        """
        SELECT * FROM (VALUES
            ('001', $name || ' 54 TEST ROAD', 'ZZ1 1ZZ'),
            ('002', 'MEADOW HOUSE 56 TEST ROAD', 'ZZ1 1ZZ'),
            ('001', $source_name || ' 58 TEST ROAD', 'ZZ1 1ZZ')
        ) AS addresses(unique_id, address_concat, postcode)
    """,
        params={"name": target_name, "source_name": source_name},
    )
    if prepared_index:
        folder = tmp_path / "canonical"
        prepare_canonical_folder(
            canonical, con=duck_con, output_folder=folder, show_progress="off"
        )
        canonical = folder
    stage = SplinkStage(
        include_full_postcode_block=True,
        predict_threshold_match_weight=-100,
        retain_intermediate_calculation_columns=True,
    )
    result = AddressMatcher(
        con=duck_con,
        addresses_to_match=source,
        canonical_addresses=canonical,
        stages=[stage],
    ).match()
    raw = result._splink_predictions()
    target = raw.filter(
        "unique_id_l = '001' AND clean_full_address_l LIKE '% 54 TEST ROAD'"
    )
    assert target.select("bf_postcode_distinguishing_name").fetchone() == (
        expected_bf if prepared_index else 1.0,
    )
    assert not any(column.startswith("lk_") for column in raw.columns)
    improved = duck_con.table(stage.improved_predictions_table)
    assert not any(column.startswith("lk_") for column in improved.columns)
    assert (
        duck_con.sql("""
        SELECT view_name FROM duckdb_views()
        WHERE view_name LIKE 'local_key_source_names%'
    """).fetchall()
        == []
    )
    assert (
        duck_con.sql("""
        SELECT table_name FROM duckdb_tables()
        WHERE table_name LIKE '__ukam__name_alias_exclusions_%'
            OR table_name LIKE '__ukam__name_source_features_%'
    """).fetchall()
        == []
    )


@pytest.mark.parametrize(
    "text,expected_level",
    [
        ("MEADOW COTTAGE", 6),
        ("THE TEST PLACE MEADOW COTTAGE", 4),
    ],
)
def test_leading_names_receive_stronger_evidence(duck_con, text, expected_level):
    source, _ = _features(
        duck_con,
        text,
        [
            ["001", "a1", "MEADOW COTTAGE"],
            ["002", "b1", "ORCHARD HOUSE"],
        ],
    )
    assert source.select("lk_level_by_alias").fetchone() == ({"a1": expected_level},)


@pytest.mark.parametrize("populated_side", ["source", "canonical"])
def test_missing_postcode_name_columns_are_typed_and_neutral(duck_con, populated_side):
    blank = duck_con.sql("SELECT 'query' AS unique_id")
    populated = duck_con.sql("""
        SELECT '001' AS unique_id,
            MAP(['a1'], [6]) AS lk_level_by_alias,
            ['001'] AS lk_anchor_uprns, true AS lk_eligible
    """)
    inputs = (populated, blank) if populated_side == "source" else (blank, populated)
    aligned = _align_postcode_name_columns(*inputs)
    neutral = aligned[1] if populated_side == "source" else aligned[0]
    preserved = aligned[0] if populated_side == "source" else aligned[1]
    projection = "lk_level_by_alias, lk_anchor_uprns, lk_eligible"
    assert neutral.select(projection).fetchone() == ({}, [], False)
    assert preserved.select(projection).fetchone() == ({"a1": 6}, ["001"], True)
    assert [str(column_type) for column_type in neutral.select(projection).types] == [
        "MAP(VARCHAR, INTEGER)",
        "VARCHAR[]",
        "BOOLEAN",
    ]


@pytest.mark.parametrize("column", ["has_flat_indicator", "has_business_unit"])
def test_missing_unit_metadata_has_no_postcode_name_evidence(duck_con, column):
    source, canonical = _features(
        duck_con,
        "MEADOW COTTAGE",
        [["001", "a1", "MEADOW COTTAGE"], ["002", "b1", "ORCHARD HOUSE"]],
    )
    source = source.select(
        f"* EXCLUDE (lk_level_by_alias, lk_anchor_uprns, lk_eligible, {column})"
    )
    canonical = canonical.select(
        "* EXCLUDE (lk_level_by_alias, lk_anchor_uprns, lk_eligible)"
    )
    prepared, _ = add_local_key_features(duck_con, source, canonical)
    assert prepared.select(f"{column}, lk_level_by_alias, lk_eligible").fetchone() == (
        None,
        {},
        False,
    )


def test_shared_word_does_not_override_unique_phrase_anchor(duck_con):
    source, _ = _features(
        duck_con,
        "MEADOW COTTAGE",
        [
            ["001", "a1", "MEADOW HOUSE"],
            ["002", "b1", "MEADOW COTTAGE"],
            ["003", "c1", "ORCHARD LODGE"],
            ["004", "d1", "PINE LODGE"],
        ],
    )
    assert source.select("lk_anchor_uprns").fetchone() == (["002"],)


@pytest.mark.parametrize(
    "eligible,postcode,entity,evidence_level,expected",
    [
        (False, "ZZ1 1ZZ", "002", 6, None),
        (None, "ZZ1 1ZZ", "002", 6, None),
        (True, None, "002", 6, None),
        (True, "ZZ1 2ZZ", "002", 6, None),
        (True, "ZZ1 1ZZ", "001", None, 1.0),
        (True, "ZZ1 1ZZ", "001", 6, 16.0),
        (True, "ZZ1 1ZZ", "001", 5, 8.0),
        (True, "ZZ1 1ZZ", "001", 4, 4.0),
        (True, "ZZ1 1ZZ", "001", 3, 2.0),
        (True, "ZZ1 1ZZ", "001", 2, 2.0),
        (True, "ZZ1 1ZZ", "001", 0, 1.0),
        (True, "ZZ1 1ZZ", "002", 6, 0.25),
        (True, "ZZ1 1ZZ", "002", 2, 0.25),
    ],
)
def test_postcode_name_comparison_weights(
    duck_con,
    eligible,
    postcode,
    entity,
    evidence_level,
    expected,
):
    comparison = SettingsCreator.from_path_or_dict(
        _get_model_settings_dict()
    ).create_settings_dict("duckdb")["comparisons"][-1]
    pair = duck_con.sql(
        """
        SELECT false AS lk_eligible_l, $eligible::BOOLEAN AS lk_eligible_r,
            'ZZ1 1ZZ' AS postcode_l, $postcode::VARCHAR AS postcode_r,
            []::VARCHAR[] AS lk_anchor_uprns_l, ['001'] AS lk_anchor_uprns_r,
            $entity AS unique_id_l, 'query' AS unique_id_r,
            'omitting-alias' AS ukam_address_id_l, 'source-alias' AS ukam_address_id_r,
            MAP([]::VARCHAR[], []::INTEGER[]) AS lk_level_by_alias_l,
            MAP(['omitting-alias'], [$evidence_level::INTEGER]) AS lk_level_by_alias_r
    """,
        params={
            "eligible": eligible,
            "postcode": postcode,
            "entity": entity,
            "evidence_level": evidence_level,
        },
    )
    for level in comparison["comparison_levels"]:
        condition = level["sql_condition"]
        if condition == "ELSE" or pair.filter(condition).count("*").fetchone()[0]:
            bits = (
                None
                if level.get("is_null_level")
                else (level["m_probability"] / level["u_probability"])
            )
            assert bits == expected
            break


@pytest.mark.parametrize(
    "text,target,expected",
    [
        ("ROSE BANK", "ROSEBANK", {"a1": 2}),
        ("ROSEBANK", "ROSE BANK", {"a1": 2}),
        ("ROSE BANK", "54 ROSEBANK", {"a1": 2}),
        ("THE OTHER PLACE ROSE BANK", "ROSEBANK", {}),
        ("ROSE BANK", "THE OTHER PLACE ROSEBANK", {}),
        ("ROSE 54 BANK", "ROSEBANK", {}),
        ("OAK EL", "OAKEL", {}),
    ],
)
def test_spacing_support_is_weak_and_near_start(duck_con, text, target, expected):
    source, _ = _features(
        duck_con, text, [["001", "a1", target], ["002", "b1", "LAUREL GROVE"]]
    )
    assert source.select("lk_level_by_alias").fetchone()[0] == expected
    assert source.select("lk_anchor_uprns").fetchone()[0] == []
    assert source.select("lk_eligible").fetchone()[0] == bool(expected)


@pytest.mark.parametrize("collision", ["THE OTHER PLACE ROSE BANK", "54 ROSE BANK"])
def test_spacing_collisions_block_support_across_positions_and_kinds(duck_con, collision):
    source, _ = _features(
        duck_con, "ROSE BANK", [["001", "a1", "ROSEBANK"], ["002", "b1", collision]]
    )
    assert "a1" not in source.select("lk_level_by_alias").fetchone()[0]


def test_spacing_aliases_count_once_and_preserve_literal_anchors(duck_con):
    source, _ = _features(
        duck_con,
        "ROSE BANK",
        [
            ["001", "a1", "ROSEBANK"],
            ["001", "a2", "ROSEBANK"],
            ["002", "b1", "LAUREL GROVE"],
        ],
    )
    assert source.select("lk_level_by_alias").fetchone()[0] == {"a1": 2, "a2": 2}
    source, _ = _features(
        duck_con,
        "HEATH ROSE BANK",
        [["001", "a1", "MEADOW ROSEBANK"], ["002", "b1", "HEATH GROVE"]],
    )
    assert source.select("lk_level_by_alias, lk_anchor_uprns").fetchone() == (
        {"b1": 5},
        ["002"],
    )
    source, _ = _features(
        duck_con,
        "HEATH ROSE BANK",
        [["001", "a1", "HEATH ROSEBANK"], ["002", "b1", "LAUREL GROVE"]],
    )
    assert source.select("lk_level_by_alias, lk_anchor_uprns").fetchone() == (
        {"a1": 5},
        ["001"],
    )


@pytest.mark.parametrize(
    "anchors,other_weight,expected",
    [
        (["001"], -0.3, [1]),
        (["001"], 0.0, [1]),
        (["001"], -1.5, [1, 2]),
        ([], -0.3, [1, 2]),
        (["002"], -0.3, [1, 2]),
        (["001", "002"], -0.3, [1, 2]),
        (None, -0.3, [1, 2]),
    ],
)
def test_spacing_alias_cannot_displace_better_exact_anchored_alias(
    duck_con, anchors, other_weight, expected
):
    predictions = duck_con.sql(
        """
        SELECT aliases.*, 1 AS ukam_address_id_r,
            '001' AS unique_id_l, 'query' AS unique_id_r,
            MAP(['1', '2'], [5, 2]) AS lk_level_by_alias_r,
            $anchors::VARCHAR[] AS lk_anchor_uprns_r,
            2.0 AS bf_postcode_distinguishing_name
        FROM (VALUES (1, $other_weight::DOUBLE), (2, 0.0))
            aliases(ukam_address_id_l, match_weight)
    """,
        params={"anchors": anchors, "other_weight": other_weight},
    )
    source = predictions.select(
        "ukam_address_id_r AS ukam_address_id, "
        "lk_level_by_alias_r AS lk_level_by_alias, lk_anchor_uprns_r AS lk_anchor_uprns"
    ).distinct()
    predictions = predictions.select("* EXCLUDE (lk_level_by_alias_r, lk_anchor_uprns_r)")
    preserved = preserve_anchored_name_aliases(duck_con, predictions, source)
    assert preserved.columns == predictions.columns
    assert (
        preserved.order("ukam_address_id_l").select("ukam_address_id_l").fetchall()
    ) == [(alias,) for alias in expected]
    assert (
        preserved.filter("ukam_address_id_l = 1").select("match_weight").fetchone()
    ) == (other_weight,)


def test_spacing_only_alias_and_unfeatured_predictions_are_preserved(duck_con):
    predictions = duck_con.sql("""
        SELECT 2 AS ukam_address_id_l, 1 AS ukam_address_id_r,
            '001' AS unique_id_l, 'query' AS unique_id_r,
            0.0 AS match_weight, MAP(['2'], [2]) AS lk_level_by_alias_r,
            ['001'] AS lk_anchor_uprns_r, 2.0 AS bf_postcode_distinguishing_name
    """)
    source = predictions.select(
        "ukam_address_id_r AS ukam_address_id, lk_level_by_alias_r AS lk_level_by_alias, "
        "lk_anchor_uprns_r AS lk_anchor_uprns"
    )
    predictions = predictions.select("* EXCLUDE (lk_level_by_alias_r, lk_anchor_uprns_r)")
    assert preserve_anchored_name_aliases(duck_con, predictions, source).fetchall() == (
        predictions.fetchall()
    )
    plain = predictions.select("* EXCLUDE (bf_postcode_distinguishing_name)")
    assert preserve_anchored_name_aliases(duck_con, plain, source) is plain


@pytest.mark.parametrize(
    "factor,spacing_weight,expected",
    [
        (None, 0.0, [1, 2]),
        (0.5, 0.0, [1, 2]),
        (1.0, 0.0, [1, 2]),
        (1.5, 0.0, [1, 2]),
        (2.0, 0.0, [1]),
        (4.0, 1.0, [1]),
    ],
)
def test_alias_preservation_uses_actual_spacing_contribution(
    duck_con, factor, spacing_weight, expected
):
    predictions = duck_con.sql(
        """
        SELECT aliases.*, 1 AS ukam_address_id_r,
            '001' AS unique_id_l, 'query' AS unique_id_r,
            MAP(['1', '2'], [5, 2]) AS lk_level_by_alias_r,
            ['001'] AS lk_anchor_uprns_r,
            $factor::DOUBLE AS bf_postcode_distinguishing_name
        FROM (VALUES (1, -0.75::DOUBLE), (2, $spacing_weight::DOUBLE))
            aliases(ukam_address_id_l, match_weight)
    """,
        params={"factor": factor, "spacing_weight": spacing_weight},
    )
    source = predictions.select(
        "ukam_address_id_r AS ukam_address_id, "
        "lk_level_by_alias_r AS lk_level_by_alias, lk_anchor_uprns_r AS lk_anchor_uprns"
    ).distinct()
    predictions = predictions.select("* EXCLUDE (lk_level_by_alias_r, lk_anchor_uprns_r)")
    preserved = preserve_anchored_name_aliases(duck_con, predictions, source)
    assert preserved.order("ukam_address_id_l").select(
        "ukam_address_id_l"
    ).fetchall() == [(alias,) for alias in expected]
    without_contribution = predictions.select(
        "* EXCLUDE (bf_postcode_distinguishing_name)"
    )
    assert preserve_anchored_name_aliases(duck_con, without_contribution, source) is (
        without_contribution
    )


@pytest.mark.parametrize(
    "factor,tf_factor,expected",
    [
        (2.0, 0.5, [1, 2]),
        (1.5, 2.0, [1]),
        (2.0, None, [1]),
    ],
)
def test_alias_preservation_includes_custom_tf_contribution(
    duck_con, factor, tf_factor, expected
):
    predictions = duck_con.sql(
        """
        SELECT aliases.*, 1 AS ukam_address_id_r,
            '001' AS unique_id_l, 'query' AS unique_id_r,
            MAP(['1', '2'], [5, 2]) AS lk_level_by_alias_r,
            ['001'] AS lk_anchor_uprns_r,
            $factor::DOUBLE AS bf_postcode_distinguishing_name,
            $tf_factor::DOUBLE AS bf_tf_postcode_distinguishing_name
        FROM (VALUES (1, -0.75::DOUBLE), (2, 0.0::DOUBLE))
            aliases(ukam_address_id_l, match_weight)
    """,
        params={"factor": factor, "tf_factor": tf_factor},
    )
    source = predictions.select(
        "ukam_address_id_r AS ukam_address_id, "
        "lk_level_by_alias_r AS lk_level_by_alias, lk_anchor_uprns_r AS lk_anchor_uprns"
    ).distinct()
    predictions = predictions.select("* EXCLUDE (lk_level_by_alias_r, lk_anchor_uprns_r)")
    preserved = preserve_anchored_name_aliases(duck_con, predictions, source)
    assert preserved.columns == predictions.columns
    assert preserved.order("ukam_address_id_l").select(
        "ukam_address_id_l"
    ).fetchall() == [(alias,) for alias in expected]


def test_alias_preservation_uses_other_source_aliases_without_retaining_maps(duck_con):
    predictions = duck_con.sql("""
        SELECT * FROM (VALUES
            (1, 10, '001', 'query', -0.3::DOUBLE, 8.0),
            (2, 11, '001', 'query', 0.0, 2.0),
            (2, 12, '001', 'other', 0.0, 2.0)
        ) predictions(ukam_address_id_l, ukam_address_id_r, unique_id_l,
            unique_id_r, match_weight, bf_postcode_distinguishing_name)
    """)
    source = duck_con.sql("""
        SELECT * FROM (VALUES
            (10, MAP(['1'], [5]), []::VARCHAR[]),
            (11, MAP(['2'], [2]), ['001']),
            (12, MAP(['2'], [2]), ['001'])
        ) source(ukam_address_id, lk_level_by_alias, lk_anchor_uprns)
    """)
    existing_tables = {
        name
        for (name,) in duck_con.sql("SELECT table_name FROM duckdb_tables()").fetchall()
    }
    preserved = preserve_anchored_name_aliases(duck_con, predictions, source)
    assert preserved.columns == predictions.columns
    assert preserved.order("ukam_address_id_r").select(
        "ukam_address_id_l, ukam_address_id_r, match_weight"
    ).fetchall() == [(1, 10, -0.3), (2, 12, 0.0)]
    created_tables = {
        name
        for (name,) in duck_con.sql("SELECT table_name FROM duckdb_tables()").fetchall()
    } - existing_tables
    assert len(created_tables) == 1
    assert duck_con.table(created_tables.pop()).columns == [
        "ukam_address_id_l",
        "ukam_address_id_r",
    ]


def test_name_lookup_reuse_preserves_each_source_relation(duck_con):
    canonical = duck_con.sql("""
        SELECT * FROM (VALUES
            ('001', 'a1', 'MEADOW COTTAGE', 'ZZ1 1ZZ'),
            ('002', 'b1', 'LAUREL GROVE', 'ZZ1 1ZZ')
        ) canonical(unique_id, ukam_address_id, clean_full_address, postcode)
    """)
    canonical = prepare_canonical_local_keys(duck_con, canonical)
    source = duck_con.sql("""
        SELECT 'query' AS unique_id, 'q1' AS ukam_address_id,
            'MEADOW COTTAGE' AS clean_full_address, 'ZZ1 1ZZ' AS postcode,
            []::VARCHAR[] AS numeric_tokens, false AS has_flat_indicator,
            false AS has_business_unit, NULL::VARCHAR AS resolved_canonical_id
    """)
    first, _ = add_local_key_features(duck_con, source, canonical)
    second, _ = add_local_key_features(
        duck_con,
        source.select("* REPLACE ('LAUREL GROVE' AS clean_full_address)"),
        canonical,
    )
    assert first.select("clean_full_address, lk_level_by_alias").fetchone() == (
        "MEADOW COTTAGE",
        {"a1": 6},
    )
    assert second.select("clean_full_address, lk_level_by_alias").fetchone() == (
        "LAUREL GROVE",
        {"b1": 6},
    )
    lookups = duck_con.sql("""
        SELECT table_name FROM duckdb_tables()
        WHERE table_name LIKE '__ukam__name_source_features_%'
    """).fetchall()
    assert len(lookups) == 2
    for (lookup_name,) in lookups:
        assert duck_con.table(lookup_name).columns == [
            "alias_id",
            "lk_level_by_alias",
            "lk_anchor_uprns",
        ]
        assert duck_con.table(lookup_name).count("*").fetchone() == (1,)
