from uk_address_matcher.cleaning.chunking_strategies import (
    _add_canonical_road_blocking_keys,
    clean_data_pre_term_frequencies,
    derive_roadlike_places,
)
from uk_address_matcher.cleaning.steps.roadlike_places import (
    ROAD_FEATURE_COLUMNS,
    add_top_1_road_features,
    derive_rightmost_numeric_position_sql,
    derive_top_1_road_keys,
    roadlike_place_candidate_sql,
    roadlike_place_catalog_sql,
    roadlike_place_prepared_candidate_sql,
    roadlike_place_prepared_input_sql,
)


def _catalogue_from_source(duck_con, source):
    return derive_roadlike_places(source, duck_con, show_progress="off")


def test_roadlike_place_stage_extracts_terminal_first_candidates_and_catalogue(duck_con):
    source = duck_con.sql("""
        SELECT * FROM (VALUES
            ('1', '12 HIGH STREET', 'AB1 2CD', ['12']),
            ('2', '14 HIGH STREET', 'AB1 3CD', ['14']),
            ('3', '29 SHERWOOD STREET TELFORD SHOPPING CENTRE TELFORD', 'TF1 1AA', ['29'])
        ) AS rows(unique_id, clean_full_address, postcode, numeric_tokens)
    """)
    duck_con.register("roadlike_source", source)

    candidates = duck_con.sql(roadlike_place_candidate_sql("roadlike_source"))
    duck_con.register("roadlike_candidates", candidates)
    catalogue = duck_con.sql(roadlike_place_catalog_sql("roadlike_candidates"))

    assert candidates.order("address_id, candidate_phrase").fetchall() == [
        ("1", "AB12CD", "AB1", "12", 1, 2, 2, 2, 3, "HIGH STREET", "STREET"),
        ("2", "AB13CD", "AB1", "14", 1, 2, 2, 2, 3, "HIGH STREET", "STREET"),
        ("3", "TF11AA", "TF1", "29", 1, 2, 2, 2, 3, "SHERWOOD STREET", "STREET"),
    ]
    assert catalogue.order("candidate_phrase").fetchall() == [
        ("HIGH STREET", "STREET", 2, 2, 2, 2, 1, 3, 2),
        ("SHERWOOD STREET", "STREET", 1, 1, 1, 1, 1, 3, 2),
    ]


def test_prepared_roadlike_candidates_match_generic_candidates(duck_con):
    source = duck_con.sql("""
        SELECT * FROM (VALUES
            ('1', '12 HIGH STREET', 'AB1 2CD', ['12']),
            ('2', '29 SHERWOOD STREET TELFORD SHOPPING CENTRE TELFORD', 'TF1 1AA', ['29'])
        ) AS rows(unique_id, clean_full_address, postcode, numeric_tokens)
    """)
    duck_con.register("prepared_roadlike_source", source)
    generic_candidates = duck_con.sql(
        roadlike_place_candidate_sql("prepared_roadlike_source")
    ).order("address_id, candidate_phrase")
    prepared = duck_con.sql(roadlike_place_prepared_input_sql("prepared_roadlike_source"))
    duck_con.register("prepared_roadlike_input", prepared)
    prepared_candidates = duck_con.sql(
        roadlike_place_prepared_candidate_sql("prepared_roadlike_input")
    ).order("address_id, candidate_phrase")

    assert prepared_candidates.fetchall() == generic_candidates.fetchall()


def test_precomputed_numeric_position_matches_inline_preparation(duck_con):
    source = duck_con.sql("""
        SELECT * FROM (VALUES
            ('1', 'FLAT 2 14 HIGH STREET LONDON', 'AB1 2CD', ['2', '14']),
            ('2', 'UNIT 7 29 SHERWOOD STREET TELFORD', 'TF1 1AA', ['7', '29'])
        ) AS rows(unique_id, clean_full_address, postcode, numeric_tokens)
    """)
    duck_con.register("numeric_position_source", source)
    enriched = duck_con.sql(
        derive_rightmost_numeric_position_sql("numeric_position_source")
    )
    duck_con.register("numeric_position_enriched", enriched)

    inline = duck_con.sql(
        roadlike_place_prepared_input_sql("numeric_position_source")
    ).order("unique_id")
    precomputed = duck_con.sql(
        roadlike_place_prepared_input_sql(
            "numeric_position_enriched",
            use_precomputed_numeric_position=True,
        )
    ).order("unique_id")

    assert enriched.types[-1] == "SMALLINT"
    assert precomputed.fetchall() == inline.fetchall()


def test_derive_roadlike_places_batches_by_district_and_writes_parquet(
    duck_con, tmp_path
):
    source = duck_con.sql("""
        SELECT * FROM (VALUES
            ('1', '12 HIGH STREET', 'AB1 2CD'),
            ('2', '14 HIGH STREET', 'AB2 3CD'),
            ('3', '29 SHERWOOD STREET TELFORD SHOPPING CENTRE TELFORD', 'TF1 1AA')
        ) AS rows(unique_id, address_concat, postcode)
    """)
    output_path = tmp_path / "roadlike_places.parquet"
    cleaned_source = clean_data_pre_term_frequencies(source, duck_con, num_of_chunks=1)

    catalogue = derive_roadlike_places(
        cleaned_source,
        duck_con,
        output_path,
        postcode_districts_per_batch=1,
        show_progress="off",
    )

    assert output_path.is_file()
    assert catalogue.order("candidate_phrase").fetchall() == [
        ("HIGH STREET", "STREET", 2, 2, 2, 2, 2, 3, 2),
        ("SHERWOOD STREET", "STREET", 1, 1, 1, 1, 1, 3, 2),
    ]
    written_catalogue = duck_con.read_parquet(str(output_path)).order("candidate_phrase")
    assert written_catalogue.fetchall() == (
        catalogue.order("candidate_phrase").fetchall()
    )


def test_add_top_1_road_features_uses_supplied_catalogue_and_packaged_scorecard(
    duck_con,
):
    source = duck_con.sql("""
        SELECT * FROM (VALUES
            ('1', '12 HIGH STREET', 'AB1 2CD', ['12'], ['HIGH']),
            ('2', 'CARAVAN 7 RIVERSIDE ROAD', 'AB1 3CD', ['7'], [])
        ) AS rows(
            unique_id, clean_full_address, postcode, numeric_tokens, unusual_tokens_arr
        )
    """)

    features = add_top_1_road_features(
        duck_con,
        source,
        roadlike_places=_catalogue_from_source(duck_con, source),
    ).order("unique_id")

    assert features.columns[-5:] == list(ROAD_FEATURE_COLUMNS)
    rows = features.fetchall()
    assert rows[0][5] == "HIGH STREET"
    assert rows[0][6] > 0.0
    assert rows[0][7] == 2
    assert rows[0][8] >= 0.0
    assert rows[0][9] == ["HIGH"]
    assert rows[1][5:9] == (None, None, None, None)


def test_top_1_road_keys_can_require_catalogue_support(duck_con):
    source = duck_con.sql("""
        SELECT
            '1' AS unique_id,
            '12 XYZZY QUUX' AS clean_full_address,
            'AB1 2CD' AS postcode,
            ['12'] AS numeric_tokens
    """)

    catalogue = duck_con.sql("""
        SELECT * FROM (VALUES
            ('HIGH STREET', 'STREET', 1, 1, 1, 1, 1, 1, 1)
        ) AS rows(
            candidate_phrase,
            terminal_token,
            phrase_support,
            phrase_addresses,
            distinct_numbers,
            distinct_postcodes,
            distinct_districts,
            terminal_support,
            terminal_distinct_phrases
        )
    """)
    unrestricted = derive_top_1_road_keys(
        duck_con,
        source,
        roadlike_places=catalogue,
    )
    supported = derive_top_1_road_keys(
        duck_con,
        source,
        roadlike_places=catalogue,
        require_catalogue_support=True,
    )

    assert unrestricted.select("road_1_norm").fetchone() == ("XYZZY QUUX",)
    assert supported.count("*").fetchone() == (0,)


def test_top_1_road_keys_reuse_equivalent_post_number_tails(duck_con):
    source = duck_con.sql("""
        SELECT * FROM (VALUES
            ('1', '12 HIGH STREET', 'AB1 2CD', ['12']),
            ('2', '14 HIGH STREET', 'AB1 3CD', ['14'])
        ) AS rows(unique_id, clean_full_address, postcode, numeric_tokens)
    """)

    keys = derive_top_1_road_keys(
        duck_con,
        source,
        roadlike_places=_catalogue_from_source(duck_con, source),
    ).order("unique_id")

    assert keys.fetchall() == [("1", "HIGH STREET"), ("2", "HIGH STREET")]


def test_canonical_road_keys_use_preferred_row_and_rejoin_variants(duck_con):
    duck_con.execute("SET preserve_insertion_order = true")
    source = duck_con.sql("""
        SELECT * FROM (VALUES
            ('1', 1, '12 WRONG ROAD', 'AB1 2CD', ['12'], [], 'CUSTOM_LEVEL', '12'),
            (
                '1', 2, '12 HIGH STREET', 'AB1 2CD', ['12'], [],
                'add_gb_builtaddress.parquet', '12'
            )
        ) AS rows(
            unique_id,
            ukam_address_id,
            clean_full_address,
            postcode,
            numeric_tokens,
            unusual_tokens_arr,
            filename,
            numeric_token_1
        )
    """)

    rows = (
        _add_canonical_road_blocking_keys(
            source,
            duck_con,
            num_of_chunks=2,
            roadlike_places=_catalogue_from_source(duck_con, source),
        )
        .order("ukam_address_id")
        .fetchall()
    )

    assert len(rows) == 2
    assert [row[8] for row in rows] == ["HIGH STREET", "HIGH STREET"]
    assert duck_con.execute(
        "SELECT current_setting('preserve_insertion_order')"
    ).fetchone() == (True,)


def test_road_features_without_catalogue_are_neutral(duck_con):
    source = duck_con.sql("""
        SELECT
            '1' AS unique_id,
            '12 HIGH STREET' AS clean_full_address,
            'AB1 2CD' AS postcode,
            ['12'] AS numeric_tokens,
            ['HIGH'] AS unusual_tokens_arr
    """)

    features = add_top_1_road_features(duck_con, source)

    assert features.select(
        "road_1_norm, road_1_confidence, road_1_token_count, "
        "road_1_margin, road_1_distinctive_tokens"
    ).fetchone() == (None, None, None, None, None)
