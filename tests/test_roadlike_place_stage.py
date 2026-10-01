from contextlib import nullcontext

import pytest

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
    roadlike_place_prepared_candidate_sources_sql,
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


def test_supported_road_candidates_do_not_cross_districts(duck_con):
    source = duck_con.sql("""
        SELECT * FROM (VALUES
            ('1', '12 HIGH STREET', 'AB1 2CD', ['12']),
            ('2', '14 HIGH STREET', 'AB2 3CD', ['14'])
        ) AS rows(unique_id, clean_full_address, postcode, numeric_tokens)
    """)
    duck_con.register("district_candidate_input", source)
    prepared = duck_con.sql(roadlike_place_prepared_input_sql("district_candidate_input"))
    duck_con.register("district_candidate_prepared", prepared)
    duck_con.execute("""
        CREATE TEMPORARY TABLE district_catalogue AS
        SELECT 'AB1' AS postcode_district, 'HIGH STREET' AS candidate_phrase,
            'STREET' AS terminal_token
    """)

    candidates = duck_con.sql(
        roadlike_place_prepared_candidate_sql(
            "district_candidate_prepared",
            catalogue_width_relation="district_catalogue",
            catalogue_has_district=True,
        )
    )

    assert candidates.select("address_id, postcode_district").fetchall() == [("1", "AB1")]


def test_materialized_prepared_candidate_sources_match_inline_candidates(duck_con):
    source = duck_con.sql("""
        SELECT * FROM (VALUES
            ('1', '12 HIGH STREET', 'AB1 2CD', ['12']),
            ('2', '29 SHERWOOD STREET TELFORD SHOPPING CENTRE TELFORD', 'TF1 1AA', ['29'])
        ) AS rows(unique_id, clean_full_address, postcode, numeric_tokens)
    """)
    duck_con.register("materialized_candidate_source", source)
    prepared = duck_con.sql(
        roadlike_place_prepared_input_sql("materialized_candidate_source")
    )
    duck_con.register("materialized_candidate_prepared", prepared)
    duck_con.execute(
        """
        CREATE TEMPORARY TABLE materialized_candidate_sources AS
        SELECT * FROM (
            %s
        ) AS candidate_sources
    """
        % roadlike_place_prepared_candidate_sources_sql("materialized_candidate_prepared")
    )

    inline = duck_con.sql(
        roadlike_place_prepared_candidate_sql("materialized_candidate_prepared")
    ).order("address_id, candidate_phrase")
    materialized = duck_con.sql(
        roadlike_place_prepared_candidate_sql(
            "materialized_candidate_prepared",
            candidate_source_relation="materialized_candidate_sources",
        )
    ).order("address_id, candidate_phrase")

    assert materialized.fetchall() == inline.fetchall()


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
    assert catalogue.order("postcode_district, candidate_phrase").fetchall() == [
        ("AB1", "HIGH STREET", "STREET", 1, 1, 1, 1, 1, 1, 1),
        ("AB2", "HIGH STREET", "STREET", 1, 1, 1, 1, 1, 1, 1),
        ("TF1", "SHERWOOD STREET", "STREET", 1, 1, 1, 1, 1, 1, 1),
    ]
    written_catalogue = duck_con.read_parquet(str(output_path)).order(
        "postcode_district, candidate_phrase"
    )
    assert written_catalogue.fetchall() == (
        catalogue.order("postcode_district, candidate_phrase").fetchall()
    )


def test_roadlike_catalogue_keeps_district_evidence_separate_by_default(duck_con):
    source = duck_con.sql("""
        SELECT * FROM (VALUES
            ('1', '12 HIGH STREET', 'AB1 2CD', ['12']),
            ('2', '14 HIGH STREET', 'AB1 3CD', ['14']),
            ('3', '16 HIGH STREET', 'AB2 2CD', ['16'])
        ) AS rows(unique_id, clean_full_address, postcode, numeric_tokens)
    """)
    expected = [
        ("AB1", "HIGH STREET", 2, 1),
        ("AB2", "HIGH STREET", 1, 1),
    ]

    for options in ({}, {"postcode_districts_per_batch": 1}):
        catalogue = derive_roadlike_places(
            source, duck_con, show_progress="off", **options
        )
        assert (
            catalogue.select(
                "postcode_district, candidate_phrase, phrase_support, distinct_districts"
            )
            .order("postcode_district")
            .fetchall()
            == expected
        )


def test_single_roadlike_batch_does_not_write_partition_files(
    duck_con, monkeypatch, tmp_path
):
    import uk_address_matcher.cleaning.chunking_strategies as chunking

    source = duck_con.sql("""
        SELECT '1' AS unique_id, '12 HIGH STREET' AS clean_full_address,
            'AB1 2CD' AS postcode, ['12'] AS numeric_tokens
    """)
    monkeypatch.setattr(
        chunking, "TemporaryDirectory", lambda **kwargs: nullcontext(str(tmp_path))
    )

    catalogue = derive_roadlike_places(source, duck_con, show_progress="off")

    assert catalogue.count("*").fetchone() == (1,)
    assert list(tmp_path.iterdir()) == []


def test_roadlike_default_prepares_at_most_sixteen_districts_per_batch(
    duck_con, monkeypatch
):
    import uk_address_matcher.cleaning.chunking_strategies as chunking

    source = duck_con.sql("""
        SELECT CAST(district AS VARCHAR) AS unique_id,
            '12 HIGH STREET' AS clean_full_address,
            'AB' || CAST(district AS VARCHAR) || ' 1AA' AS postcode,
            ['12'] AS numeric_tokens
        FROM range(1, 18) AS districts(district)
    """)
    batch_rows = []
    original = chunking.roadlike_place_prepared_candidate_sources_sql

    def record_prepared_batch(table_name):
        batch_rows.append(
            duck_con.sql(f"SELECT count(*) FROM {table_name}").fetchone()[0]
        )
        return original(table_name)

    monkeypatch.setattr(
        chunking, "roadlike_place_prepared_candidate_sources_sql", record_prepared_batch
    )
    catalogue = derive_roadlike_places(source, duck_con, show_progress="off")

    assert batch_rows == [16, 1]
    assert catalogue.count("*").fetchone() == (17,)


def test_derive_roadlike_places_filters_before_district_batching(duck_con):
    source = duck_con.sql("""
        SELECT * FROM (VALUES
            ('1', '12 HIGH STREET', 'AB1 2CD'),
            ('2', '14 HIGH STREET', 'AB2 3CD'),
            ('3', '29 MAIN ROAD', 'TF1 1AA')
        ) AS rows(unique_id, address_concat, postcode)
    """)
    cleaned_source = clean_data_pre_term_frequencies(source, duck_con, num_of_chunks=1)

    catalogue = derive_roadlike_places(
        cleaned_source,
        duck_con,
        postcode_districts=["ab1", "AB2"],
        postcode_districts_per_batch=1,
        show_progress="off",
    )

    assert catalogue.select(
        "postcode_district, candidate_phrase, phrase_support, distinct_districts"
    ).order("postcode_district").fetchall() == [
        ("AB1", "HIGH STREET", 1, 1),
        ("AB2", "HIGH STREET", 1, 1),
    ]


def test_derive_roadlike_places_composes_district_and_classification_filters(duck_con):
    source = duck_con.sql("""
        SELECT * FROM (VALUES
            ('1', '12 HIGH STREET', 'AB1 2CD', ['12'], 'RD01'),
            ('2', '14 HIGH STREET', 'AB2 3CD', ['14'], 'RD01'),
            ('3', '29 MAIN ROAD', 'AB1 4CD', ['29'], 'CI01'),
            ('4', '31 MAIN ROAD', 'TF1 1AA', ['31'], 'RD01')
        ) AS rows(
            unique_id, clean_full_address, postcode, numeric_tokens, classificationcode
        )
    """)

    catalogue = derive_roadlike_places(
        source,
        duck_con,
        postcode_districts=["AB1", "AB2"],
        show_progress="off",
    )

    assert catalogue.select("postcode_district, candidate_phrase").order(
        "postcode_district"
    ).fetchall() == [("AB1", "HIGH STREET"), ("AB2", "HIGH STREET")]


def test_roadlike_catalogue_filters_non_residential_rows(duck_con):
    source = duck_con.sql("""
        SELECT * FROM (VALUES
            ('1', '12 HIGH STREET', 'AB1 2CD', ['12'], 'RD01'),
            ('2', '14 COMMERCIAL ROAD', 'AB1 3CD', ['14'], 'CI01')
        ) AS rows(
            unique_id, clean_full_address, postcode, numeric_tokens, classificationcode
        )
    """)

    catalogue = _catalogue_from_source(duck_con, source)
    assert catalogue.project("candidate_phrase").fetchall() == [("HIGH STREET",)]


def test_roadlike_catalogue_uses_all_rows_without_classificationcode(duck_con):
    source = duck_con.sql("""
        SELECT * FROM (VALUES
            ('1', '12 HIGH STREET', 'AB1 2CD', ['12']),
            ('2', '14 COMMERCIAL ROAD', 'AB1 3CD', ['14'])
        ) AS rows(unique_id, clean_full_address, postcode, numeric_tokens)
    """)

    catalogue = derive_roadlike_places(
        source,
        duck_con,
        show_progress="off",
    )

    assert catalogue.project("candidate_phrase").order("candidate_phrase").fetchall() == [
        ("COMMERCIAL ROAD",),
        ("HIGH STREET",),
    ]


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


def test_prepared_candidate_expansion_preserves_fallbacks_and_duplicate_ids(duck_con):
    source = duck_con.sql("""
        SELECT * FROM (VALUES
            ('1', '12 HIGH STREET LONDON', 'AB1 2CD', ['12']),
            ('1', '12 LONG HIGH STREET', 'AB1 2CD', ['12']),
            ('1', '12 GREEN MEADOW', 'AB1 2CD', ['12']),
            ('2', '14 OAK LANE', 'AB1 3CD', ['14']),
            ('3', '29 SHERWOOD STREET TELFORD SHOPPING CENTRE TELFORD',
             'TF1 1AA', ['29']),
            ('4', '7 GREEN MEADOW', 'AB1 2CD', ['7']),
            ('4', '9 GREEN', 'AB1 2CD', ['9']),
            ('5', '9 GREEN', 'AB1 2CD', ['9']),
            ('6', '11', 'AB1 2CD', ['11']),
            ('7', '13 HIGH STREET UNIT 2', 'AB1 2CD', ['13', '2']),
            ('8', '15 HIGH ROAD LOW STREET', NULL, ['15'])
        ) AS rows(unique_id, clean_full_address, postcode, numeric_tokens)
    """)
    duck_con.register("candidate_expansion_source", source)
    generic = duck_con.sql(roadlike_place_candidate_sql("candidate_expansion_source"))
    prepared = duck_con.sql(
        roadlike_place_prepared_input_sql("candidate_expansion_source")
    )
    duck_con.register("candidate_expansion_prepared", prepared)
    actual = duck_con.sql(
        roadlike_place_prepared_candidate_sql("candidate_expansion_prepared")
    )
    assert actual.filter("address_id != '4'").order("ALL").fetchall() == (
        generic.filter("address_id != '4'").order("ALL").fetchall()
    )
    # Prepared extraction intentionally retains truncated fallback windows.
    assert actual.filter("address_id = '4'").select(
        "candidate_phrase, candidate_width, terminal_token"
    ).order("ALL").fetchall() == [
        ("GREEN", 2, None),
        ("GREEN", 3, None),
        ("GREEN MEADOW", 2, "MEADOW"),
        ("GREEN MEADOW", 3, None),
        ("MEADOW", 2, None),
        ("MEADOW", 3, None),
    ]
    duck_con.sql(
        roadlike_place_prepared_candidate_sources_sql("candidate_expansion_prepared")
    ).create("candidate_expansion_sources")
    reused = duck_con.sql(
        roadlike_place_prepared_candidate_sql(
            "candidate_expansion_prepared",
            candidate_source_relation="candidate_expansion_sources",
        )
    )
    assert reused.order("ALL").fetchall() == actual.order("ALL").fetchall()


@pytest.mark.parametrize("id_type", ["VARCHAR", "BIGINT"])
def test_catalogue_road_tails_preserve_variant_support(duck_con, id_type):
    source = duck_con.sql("""
        SELECT * FROM (VALUES
            ('1', 'FLAT 2 14 HIGH STREET', 'AB1 2CD', ['2', '14']),
            ('1', '14 HIGH STREET', 'AB1 2CD', ['14']),
            ('2', '16 HIGH STREET', 'AB1 3CD', ['16']),
            ('3', '18 GREEN MEADOW', 'AB2 4CD', ['18']),
            ('3', '18 GREEN MEADOW', 'AB2 4CD', ['18'])
        ) AS rows(unique_id, clean_full_address, postcode, numeric_tokens)
    """)
    source = source.select(f"* REPLACE (CAST(unique_id AS {id_type}) AS unique_id)")
    duck_con.register("duplicate_tail_source", source)
    duck_con.sql(roadlike_place_prepared_input_sql("duplicate_tail_source")).create(
        "duplicate_tail_prepared"
    )
    candidates = duck_con.sql(
        roadlike_place_prepared_candidate_sql("duplicate_tail_prepared")
    )
    duck_con.register("duplicate_tail_candidates", candidates)
    expected = duck_con.sql(
        roadlike_place_catalog_sql("duplicate_tail_candidates", by_postcode_district=True)
    )
    actual = derive_roadlike_places(source, duck_con, show_progress="off")
    assert actual.types == expected.types
    assert actual.order("ALL").fetchall() == expected.order("ALL").fetchall()
    assert actual.filter("candidate_phrase = 'HIGH STREET'").select(
        "phrase_support, phrase_addresses"
    ).fetchone() == (3, 2)


def test_fallback_validity_is_shared_across_null_identifiers(duck_con):
    source = duck_con.sql("""
        SELECT * FROM (VALUES
            (NULL::VARCHAR, '18 GREEN MEADOW', 'AB1 2CD', ['18']),
            (NULL::VARCHAR, '20 GREEN', 'AB1 2CD', ['20'])
        ) AS rows(unique_id, clean_full_address, postcode, numeric_tokens)
    """)
    duck_con.register("null_fallback_source", source)
    duck_con.sql(roadlike_place_prepared_input_sql("null_fallback_source")).create(
        "null_fallback_prepared"
    )
    candidates = duck_con.sql(
        roadlike_place_prepared_candidate_sql("null_fallback_prepared")
    )
    assert candidates.select(
        "address_id, rightmost_numeric_value, candidate_phrase, "
        "candidate_width, terminal_token"
    ).order("rightmost_numeric_value, candidate_phrase, candidate_width").fetchall() == [
        (None, "18", "GREEN MEADOW", 2, "MEADOW"),
        (None, "18", "GREEN MEADOW", 3, None),
        (None, "18", "MEADOW", 2, None),
        (None, "18", "MEADOW", 3, None),
        (None, "20", "GREEN", 2, None),
        (None, "20", "GREEN", 3, None),
    ]


def test_terminal_templates_include_the_numeric_anchor(duck_con):
    source = duck_con.sql("""
        SELECT * FROM (VALUES
            ('1', '12 14 HIGH STREET', 'AB1 2CD', ['12']),
            ('2', '12 14 HIGH STREET', 'AB1 2CD', ['14'])
        ) AS rows(unique_id, clean_full_address, postcode, numeric_tokens)
    """)
    duck_con.register("anchor_template_source", source)
    duck_con.sql(roadlike_place_prepared_input_sql("anchor_template_source")).create(
        "anchor_template_prepared"
    )
    candidates = duck_con.sql(
        roadlike_place_prepared_candidate_sql("anchor_template_prepared")
    )
    assert candidates.select(
        "address_id, rightmost_numeric_value, numeric_anchor, "
        "candidate_phrase, candidate_width"
    ).order("address_id, candidate_phrase").fetchall() == [
        ("1", "12", 1, "14 HIGH STREET", 3),
        ("1", "12", 1, "HIGH STREET", 2),
        ("2", "14", 2, "HIGH STREET", 2),
    ]


def test_fallback_exclusion_preserves_duplicate_and_null_ids(duck_con):
    source = duck_con.sql("""
        SELECT * FROM (VALUES
            ('1', '12 HIGH STREET', 'AB1 2CD', ['12']),
            ('1', '12 HIGH STREET', 'AB1 2CD', ['12']),
            ('1', '14 GREEN MEADOW', 'AB1 2CD', ['14']),
            (NULL, '16 HIGH STREET', 'AB1 2CD', ['16']),
            (NULL, '18 GREEN MEADOW', 'AB1 2CD', ['18']),
            ('2', '20 GREEN MEADOW', 'AB1 2CD', ['20'])
        ) AS rows(unique_id, clean_full_address, postcode, numeric_tokens)
    """)
    duck_con.register("fallback_exclusion_source", source)
    duck_con.sql(roadlike_place_prepared_input_sql("fallback_exclusion_source")).create(
        "fallback_exclusion_prepared"
    )
    candidates = duck_con.sql(
        roadlike_place_prepared_candidate_sql("fallback_exclusion_prepared")
    )
    assert candidates.aggregate(
        "address_id, rightmost_numeric_value, count(*) AS candidates",
        "address_id, rightmost_numeric_value",
    ).order("rightmost_numeric_value").fetchall() == [
        ("1", "12", 2),
        (None, "16", 5),
        (None, "18", 4),
        ("2", "20", 4),
    ]


@pytest.mark.parametrize("preserve_order", [True, False])
def test_canonical_road_keys_restore_order_after_materialisation_error(
    duck_con, monkeypatch, preserve_order
):
    duck_con.execute(f"SET preserve_insertion_order = {str(preserve_order).lower()}")
    source = duck_con.sql("""
        SELECT '1' AS unique_id, 1 AS ukam_address_id,
            '12 HIGH STREET' AS clean_full_address, 'AB1 2CD' AS postcode,
            ['12'] AS numeric_tokens
    """)
    catalogue = _catalogue_from_source(duck_con, source)

    def fail_materialisation(*args):
        assert duck_con.sql(
            "SELECT current_setting('preserve_insertion_order')"
        ).fetchone() == (preserve_order,)
        raise RuntimeError("materialisation failed")

    monkeypatch.setattr(
        "uk_address_matcher.cleaning.chunking_strategies._materialise_relation",
        fail_materialisation,
    )
    with pytest.raises(RuntimeError, match="materialisation failed"):
        _add_canonical_road_blocking_keys(source, duck_con, roadlike_places=catalogue)
    assert duck_con.sql(
        "SELECT current_setting('preserve_insertion_order')"
    ).fetchone() == (preserve_order,)
    assert duck_con.sql("""
        SELECT count(*) FROM duckdb_tables()
        WHERE starts_with(table_name, '__ukam_canonical_road_')
    """).fetchone() == (0,)


def test_terminal_templates_include_numeric_anchor_and_district(duck_con):
    source = duck_con.sql("""
        SELECT * FROM (VALUES
            ('1', '12 14 HIGH STREET', 'AB1 2CD', ['12']),
            ('2', '12 14 HIGH STREET', 'AB1 2CD', ['14']),
            ('3', '12 14 HIGH STREET', 'AB2 2CD', ['12'])
        ) AS rows(unique_id, clean_full_address, postcode, numeric_tokens)
    """)
    duck_con.register("anchor_template_source", source)
    prepared_sql = roadlike_place_prepared_input_sql("anchor_template_source")
    catalogue = duck_con.sql("""
        SELECT * FROM (VALUES
            ('HIGH STREET', 'STREET', 'AB1'),
            ('14 HIGH STREET', 'STREET', 'AB2')
        ) AS rows(candidate_phrase, terminal_token, postcode_district)
    """)
    duck_con.register("template_width_catalogue", catalogue)
    candidates = duck_con.sql(
        roadlike_place_prepared_candidate_sql(
            f"({prepared_sql})",
            catalogue_width_relation="template_width_catalogue",
            catalogue_has_district=True,
        )
    )
    assert candidates.select(
        "address_id, rightmost_numeric_value, numeric_anchor, "
        "candidate_phrase, candidate_width"
    ).order("address_id, candidate_phrase").fetchall() == [
        ("1", "12", 1, "HIGH STREET", 2),
        ("2", "14", 2, "HIGH STREET", 2),
        ("3", "12", 1, "14 HIGH STREET", 3),
    ]
