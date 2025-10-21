import pytest

from uk_address_matcher.linking_model.exact_matching.exact_matching_model import (
    _annotate_exact_matches,
    _filter_unmatched_exact_matches,
    _resolve_with_trie,
)
from uk_address_matcher.sql_pipeline.match_reasons import MatchReason
from uk_address_matcher.sql_pipeline.runner import InputBinding, create_sql_pipeline


@pytest.fixture
def enum_values() -> str:
    return str(MatchReason.enum_values())


@pytest.fixture
def dedupe_input_bindings(duck_con, enum_values) -> list[InputBinding]:
    duck_con.execute(
        f"""
        CREATE OR REPLACE TABLE fuzzy_addresses AS
        SELECT *
        FROM (
            VALUES
                (
                    1,
                    '1 Imaginary Farm Lane',
                    'ZZ1 1ZZ',
                    NULL::BIGINT,
                    ARRAY['1', 'imaginary', 'farm', 'lane'],
                    'unmatched'::ENUM {enum_values}
                ),
                (
                    2,
                    '22 Placeholder Business Park North',
                    'YY9 9YY',
                    NULL::BIGINT,
                    ARRAY['22', 'placeholder', 'business', 'park', 'north'],
                    'unmatched'::ENUM {enum_values}
                )
        ) AS t(
            unique_id,
            original_address_concat,
            postcode,
            resolved_canonical_id,
            address_tokens,
            match_reason
        )
        """
    )

    duck_con.execute(
        """
        CREATE OR REPLACE TABLE canonical_addresses AS
        SELECT *
        FROM (
            VALUES
                (
                    100,
                    '1 Imaginary Farm Lane',
                    'ZZ1 1ZZ',
                    ARRAY['1', 'imaginary', 'farm', 'lane']
                ),
                (
                    101,
                    '1 Imaginary Farm Lane',
                    'ZZ1 1ZZ',
                    ARRAY['1', 'imaginary', 'farm', 'lane']
                ),
                (
                    200,
                    '22 Placeholder Business Park North',
                    'YY9 9YY',
                    ARRAY['22', 'placeholder', 'business', 'park', 'north']
                )
        ) AS t(
            unique_id,
            original_address_concat,
            postcode,
            address_tokens
        )
        """
    )

    return [
        InputBinding("fuzzy_addresses", duck_con.table("fuzzy_addresses")),
        InputBinding("canonical_addresses", duck_con.table("canonical_addresses")),
    ]


@pytest.fixture
def trie_input_bindings(duck_con, enum_values) -> list[InputBinding]:
    duck_con.execute(
        f"""
        CREATE OR REPLACE TABLE fuzzy_addresses AS
        SELECT *
        FROM (
            VALUES
                (
                    1,
                    '4 Sample Street',
                    'CC3 3CC',
                    NULL::BIGINT,
                    ARRAY['4', 'sample', 'street'],
                    'unmatched'::ENUM {enum_values}
                ),
                (
                    2,
                    '5 Demo Road',
                    'DD4 4DD',
                    NULL::BIGINT,
                    ARRAY['5', 'demo', 'road'],
                    'unmatched'::ENUM {enum_values}
                )
        ) AS t(
            unique_id,
            original_address_concat,
            postcode,
            resolved_canonical_id,
            address_tokens,
            match_reason
        )
        """
    )

    duck_con.execute(
        """
        CREATE OR REPLACE TABLE canonical_addresses AS
        SELECT *
        FROM (
            VALUES
                (
                    1000,
                    '4 Sample Street',
                    'CC3 3CC',
                    ARRAY['4', 'sample', 'street']
                ),
                (
                    2000,
                    '5 Demo Rd',
                    'DD4 4DD',
                    ARRAY['5', 'demo', 'rd']
                )
        ) AS t(
            unique_id,
            original_address_concat,
            postcode,
            address_tokens
        )
        """
    )

    return [
        InputBinding("fuzzy_addresses", duck_con.table("fuzzy_addresses")),
        InputBinding("canonical_addresses", duck_con.table("canonical_addresses")),
    ]


def test_exact_matching_deduplicates_canonical(duck_con, dedupe_input_bindings):
    pipeline = create_sql_pipeline(
        duck_con,
        dedupe_input_bindings,
        [_annotate_exact_matches],
    )

    results = pipeline.run()

    total_rows, distinct_unique_ids = results.aggregate(
        "COUNT(*) AS total_rows, COUNT(DISTINCT unique_id) AS distinct_unique_ids"
    ).fetchone()

    assert total_rows == 2
    assert distinct_unique_ids == 2

    first_resolved_id, first_match_reason = (
        results.filter("unique_id = 1")
        .project("resolved_canonical_id, match_reason")
        .fetchone()
    )
    assert first_resolved_id == 100
    assert first_match_reason == MatchReason.EXACT.value

    second_resolved_id, second_match_reason = (
        results.filter("unique_id = 2")
        .project("resolved_canonical_id, match_reason")
        .fetchone()
    )
    assert second_resolved_id == 200
    assert second_match_reason == MatchReason.EXACT.value


def test_unmatched_records_retain_original_unique_id(duck_con, trie_input_bindings):
    duck_con.execute(
        "CREATE OR REPLACE MACRO build_suffix_trie(canonical_id, address_tokens) AS ANY_VALUE(canonical_id)"
    )
    duck_con.execute(
        "CREATE OR REPLACE MACRO find_address(address_tokens, trie_value) AS trie_value"
    )

    pipeline = create_sql_pipeline(
        duck_con,
        trie_input_bindings,
        [_annotate_exact_matches, _filter_unmatched_exact_matches, _resolve_with_trie],
    )

    results = pipeline.run()

    columns = set(results.columns)
    assert "unique_id" in columns
    assert "resolved_canonical_id" in columns
    assert "fuzzy_unique_id" not in columns
    assert "exact_match_canonical_id_bigint" not in columns
    assert "trie_match_unique_id_bigint" not in columns
    assert "resolved_canonical_unique_id_bigint" not in columns

    rows = (
        results.project("unique_id, resolved_canonical_id, match_reason")
        .order("unique_id")
        .fetchall()
    )
    assert rows == [
        (1, 1000, MatchReason.EXACT.value),
        (2, 2000, MatchReason.TRIE.value),
    ]


@pytest.fixture
def trie_with_unmatched_input_bindings(duck_con, enum_values) -> list[InputBinding]:
    duck_con.execute(
        f"""
        CREATE OR REPLACE TABLE fuzzy_addresses AS
        SELECT *
        FROM (
            VALUES
                (
                    1,
                    '4 Sample Street',
                    'CC3 3CC',
                    NULL::BIGINT,
                    ARRAY['4', 'sample', 'street'],
                    'unmatched'::ENUM {enum_values}
                ),
                (
                    2,
                    '5 Demo Road',
                    'DD4 4DD',
                    NULL::BIGINT,
                    ARRAY['5', 'demo', 'road'],
                    'unmatched'::ENUM {enum_values}
                ),
                (
                    3,
                    '999 Mystery Lane',
                    'EE5 5EE',
                    NULL::BIGINT,
                    ARRAY['999', 'mystery', 'lane'],
                    'unmatched'::ENUM {enum_values}
                )
        ) AS t(
            unique_id,
            original_address_concat,
            postcode,
            resolved_canonical_id,
            address_tokens,
            match_reason
        )
        """
    )

    duck_con.execute(
        """
        CREATE OR REPLACE TABLE canonical_addresses AS
        SELECT *
        FROM (
            VALUES
                (
                    1000,
                    '4 Sample Street',
                    'CC3 3CC',
                    ARRAY['4', 'sample', 'street']
                ),
                (
                    2000,
                    '5 Demo Rd',
                    'DD4 4DD',
                    ARRAY['5', 'demo', 'road']
                )
        ) AS t(
            unique_id,
            original_address_concat,
            postcode,
            address_tokens
        )
        """
    )

    return [
        InputBinding("fuzzy_addresses", duck_con.table("fuzzy_addresses")),
        InputBinding("canonical_addresses", duck_con.table("canonical_addresses")),
    ]


def test_trie_stage_outputs_only_matched_rows_without_duplicates(duck_con, trie_with_unmatched_input_bindings):
    duck_con.execute(
        "CREATE OR REPLACE MACRO build_suffix_trie(canonical_id, address_tokens) AS ANY_VALUE(canonical_id)"
    )
    duck_con.execute(
        "CREATE OR REPLACE MACRO find_address(address_tokens, trie_value) AS trie_value"
    )

    pipeline = create_sql_pipeline(
        duck_con,
        trie_with_unmatched_input_bindings,
        [_annotate_exact_matches, _filter_unmatched_exact_matches, _resolve_with_trie],
    )

    results = pipeline.run()

    total_rows, distinct_unique_ids = results.aggregate(
        "COUNT(*) AS total_rows, COUNT(DISTINCT unique_id) AS distinct_unique_ids"
    ).fetchone()

    assert total_rows == 2
    assert distinct_unique_ids == 2

    unmatched_count = results.filter("resolved_canonical_id IS NULL").count("*")
    assert unmatched_count == 0

    unique_ids = [
        row[0] for row in results.project("unique_id").order("unique_id").fetchall()
    ]
    assert unique_ids == [1, 2]

    match_reasons = {
        row[0]: row[1] for row in results.project("unique_id, match_reason").fetchall()
    }
    assert match_reasons[1] == MatchReason.EXACT.value
    assert match_reasons[2] == MatchReason.TRIE.value
