import duckdb

from uk_address_matcher.cleaning.chunking_strategies import (
    derive_inverted_index,
    prepare_data_for_matching,
)
from uk_address_matcher.cleaning.steps.inverted_index import (
    MESSY_INVERTED_INDEX_LOOKUP_STRATEGIES,
)
from uk_address_matcher.cleaning.steps.inverted_index_strategies import (
    NumericRangeIndexConfig,
    build_index_chunk_sql,
    canonical_range_slots_sql,
)


def _canonical_with_range(con: duckdb.DuckDBPyConnection):
    return con.sql("""
        SELECT
            'C1' AS unique_id,
            'FLAT 5 20-23 MY ROAD' AS clean_full_address,
            [struct_pack(
                raw := '20-23',
                lower := 20::UINTEGER,
                upper := 23::UINTEGER,
                width := 3::UINTEGER,
                lower_suffix := NULL::VARCHAR,
                upper_suffix := NULL::VARCHAR,
                role := 1::UTINYINT,
                flags := 0::UTINYINT,
                lower_tf := NULL::DOUBLE
            )] AS numeric_range_attributes
    """)


def test_range_keys_mask_the_range_token_not_an_adjacent_scalar():
    con = duckdb.connect()
    index = derive_inverted_index(_canonical_with_range(con), con)
    range_keys = index.filter("index_strategy LIKE 'numeric_range_%'").fetchall()

    keys = {key for key, _, _ in range_keys}
    expected_contexts = {
        "5 __R__",
        "__R__ MY",
        "FLAT 5 __R__",
        "5 __R__ MY",
        "__R__ MY ROAD",
    }

    assert {key.split("|")[1] for key in keys} == expected_contexts
    assert all("20-23" not in key for key in keys)
    assert all("postcode" not in key.lower() for key in keys)
    assert {strategy for _, _, strategy in range_keys} == {
        "numeric_range_context_bucket16",
        "numeric_range_exact",
    }
    assert all(
        "|B=1" in key
        for key, _, strategy in range_keys
        if strategy == "numeric_range_context_bucket16"
    )


def test_range_index_is_identical_across_key_hash_chunk_counts():
    con = duckdb.connect()
    source = _canonical_with_range(con)

    single_chunk = (
        derive_inverted_index(source, con, num_of_chunks=1)
        .order("index_strategy, key")
        .fetchall()
    )
    multiple_chunks = (
        derive_inverted_index(source, con, num_of_chunks=3)
        .order("index_strategy, key")
        .fetchall()
    )

    assert multiple_chunks == single_chunk


def test_range_families_do_not_change_ordinary_index_rows():
    con = duckdb.connect()
    ranged_source = _canonical_with_range(con)
    ordinary_source = ranged_source.select("unique_id, clean_full_address")

    ranged_index = derive_inverted_index(ranged_source, con)
    ordinary_index = derive_inverted_index(ordinary_source, con)

    ranged_ordinary = (
        ranged_index.filter("index_strategy IN ('bigram', 'trigram')")
        .order("index_strategy, key")
        .fetchall()
    )
    ordinary_rows = ordinary_index.order("index_strategy, key").fetchall()

    assert ranged_ordinary == ordinary_rows


def test_chunk_sql_filters_each_key_family_before_union():
    sql = build_index_chunk_sql(
        source_table="source_table",
        chunk_index=2,
        number_of_chunks=5,
    )

    assert "abs(hash(__key)) % 5" in sql
    assert "abs(hash(" in sql.split("range_bucket_occurrences AS", 1)[1]
    assert "abs(hash(" in sql.split("exact_range_occurrences AS", 1)[1]
    assert "(abs(hash(key)) % 5) = 2" not in sql


def test_chunk_sql_filters_range_source_before_context_expansion():
    sql = build_index_chunk_sql(
        source_table="source_table",
        chunk_index=0,
        number_of_chunks=1,
    )

    assert "range_source AS MATERIALIZED" in sql
    assert "FROM range_source" in sql
    assert "WHERE numeric_range_attributes IS NOT NULL" in sql


def test_canonical_range_slots_filter_source_before_tokenisation():
    sql = canonical_range_slots_sql(
        source_table="source_table",
        source_filter_sql="numeric_range_attributes IS NOT NULL",
    )

    assert "WHERE numeric_range_attributes IS NOT NULL" in sql
    assert "regexp_split_to_array" in sql


def test_scalar_bucket_probe_uses_integer_bucket_text():
    sql = _lookup_sql_for_range_probe()

    assert "floor(probe_value / 16)::BIGINT::VARCHAR" in sql
    assert "floor(probe_value / 16)::VARCHAR" not in sql


def test_scalar_probe_retrieves_canonical_range_bucket():
    con = duckdb.connect()
    canonical = con.sql("""
        SELECT
            'C1' AS unique_id,
            'HAPPINESS FORGETS UNIT B 1 8-9 HOXTON SQUARE' AS address_concat,
            'N1 6NU' AS postcode
    """)
    messy = con.sql("""
        SELECT
            'M1' AS unique_id,
            'FLAT B 5 AT 9 HOXTON SQUARE' AS address_concat,
            'N1 6NU' AS postcode
    """)

    canonical_clean = prepare_data_for_matching(
        canonical, con, num_of_chunks=1, show_progress="off"
    )
    inverted_index = derive_inverted_index(
        canonical_clean, con, num_of_chunks=1, show_progress="off"
    )
    messy_clean = prepare_data_for_matching(
        messy,
        con,
        num_of_chunks=1,
        inverted_index=inverted_index,
        _inverted_index_strategies=MESSY_INVERTED_INDEX_LOOKUP_STRATEGIES,
        inverted_index_n=1,
        dataset_role="messy",
        show_progress="off",
    )

    assert messy_clean.select("exploding_unique_ids").fetchone()[0] == ["C1"]


def test_exact_range_cap_allows_multi_flat_range_families():
    config = NumericRangeIndexConfig()

    assert config.context_posting_cap == 30
    assert config.exact_posting_cap == 100


def _lookup_sql_for_range_probe():
    from uk_address_matcher.cleaning.steps.inverted_index import (
        _lookup_keys_in_inverted_index,
    )

    stage = _lookup_keys_in_inverted_index(
        canonical_range_slots_table="canonical_range_slots"
    )
    return "\n".join(step.sql for step in stage().steps)
