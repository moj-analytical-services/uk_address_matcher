from uk_address_matcher.cleaning.local_keys import add_local_key_features


def test_word_support_deduplicates_words_and_aliases(duck_con):
    canonical = duck_con.sql("""
        SELECT * FROM (VALUES
            ('001', 'a1', 'ALDER MEADOW', 'ZZ1 1ZZ'),
            ('001', 'a2', 'ALDER MEADOW ALDER', 'ZZ1 1ZZ'),
            ('002', 'b1', 'ALDER LODGE', 'ZZ1 1ZZ'),
            ('003', 'c1', 'BIRCH HOUSE', 'ZZ1 1ZZ'),
            ('004', 'd1', 'PINE GROVE', 'ZZ1 1ZZ'),
            ('005', 'e1', 'ALDER MEADOW', 'ZZ2 1ZZ')
        ) addresses(unique_id, ukam_address_id, clean_full_address, postcode)
    """)
    source = duck_con.sql("""
        SELECT *, 'ZZ1 1ZZ' AS postcode, []::VARCHAR[] AS numeric_tokens,
            false AS has_flat_indicator, false AS has_business_unit
        FROM (VALUES ('literal', 'ALDER MEADOW ALDER'),
            ('spacing', 'ALDERMEADOW'), ('numbered', '12 ALDER MEADOW'),
            ('missing', NULL)) queries(ukam_address_id, clean_full_address)
    """)
    featured, returned_canonical = add_local_key_features(duck_con, source, canonical)
    assert dict(
        featured.select("ukam_address_id, lk_word_support_by_uprn").fetchall()
    ) == {"literal": {"001": 2, "002": 1}, "spacing": {}, "numbered": {}, "missing": {}}
    assert (
        returned_canonical.filter("cardinality(lk_word_support_by_uprn) > 0")
        .count("*")
        .fetchone()[0]
        == 0
    )


def test_word_support_is_empty_for_phrase_only_spacing_evidence(duck_con):
    canonical = duck_con.sql("""
        SELECT * FROM (VALUES
            ('001', 'a1', 'ALDER MEADOW', 'ZZ1 1ZZ'),
            ('002', 'b1', 'BIRCH HOUSE', 'ZZ1 1ZZ')
        ) addresses(unique_id, ukam_address_id, clean_full_address, postcode)
    """)
    source = duck_con.sql("""
        SELECT 'source' AS ukam_address_id, 'ALDERMEADOW' AS clean_full_address,
            'ZZ1 1ZZ' AS postcode, []::VARCHAR[] AS numeric_tokens,
            false AS has_flat_indicator, false AS has_business_unit
    """)
    featured, _ = add_local_key_features(duck_con, source, canonical)
    assert featured.select("lk_level_by_alias, lk_word_support_by_uprn").fetchone() == (
        {"a1": 2},
        {},
    )
