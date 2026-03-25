import duckdb
import pytest

from uk_address_matcher import AddressMatcher, NgramJaccardStage
from uk_address_matcher.sql_pipeline.match_reasons import MatchReason


@pytest.fixture
def canonical_cases(duck_con: duckdb.DuckDBPyConnection):
    return duck_con.sql("""
        SELECT *
        FROM (
            VALUES
                ('C1', '10 high street london', 'SW1A 1AA', 'best_candidate'),
                ('C2', '88 random industrial estate', 'SW1A 1AA', 'best_candidate'),
                ('C3', 'flat 2 15 kingston road london', 'SW1A 2BB', 'practical'),
                ('C4', 'st johns court 7 elm road', 'M1 2AB', 'practical'),
                ('C5', 'unit 4 riverside business park', 'LS1 4CD', 'practical')
        ) AS t(unique_id, address_concat, postcode, case_group)
    """)


@pytest.fixture
def messy_cases(duck_con: duckdb.DuckDBPyConnection):
    return duck_con.sql("""
        SELECT *
        FROM (
            VALUES
                ('M1', '10 high st london', 'SW1A 1AA', 'best_candidate'),
                ('M2', '10 high street london', 'SW1A 1AB', 'fallback'),
                ('M3', '15 kingston rd flat 2 london', 'SW1A 2BB', 'practical'),
                ('M4', 'st. john''s court 7 elm rd', 'M1 2AB', 'practical'),
                ('M5', 'riverside business pk unit 4', 'LS1 4CD', 'practical'),
                ('M6', 'warehouse alpha block zulu', 'SW1A 1AA', 'threshold')
        ) AS t(unique_id, address_concat, postcode, case_group)
    """)


def test_ngram_jaccard_stage_selects_best_postcode_candidate(
    duck_con, canonical_cases, messy_cases
):
    canonical = duck_con.sql(
        f"""
        SELECT unique_id, address_concat, postcode
        FROM ({canonical_cases.sql_query()}) AS canonical_cases
        WHERE case_group = 'best_candidate'
        """
    )
    messy = duck_con.sql(
        f"""
        SELECT unique_id, address_concat, postcode
        FROM ({messy_cases.sql_query()}) AS messy_cases
        WHERE unique_id = 'M1'
        """
    )

    matcher = AddressMatcher(
        canonical_addresses=canonical,
        addresses_to_match=messy,
        con=duck_con,
        stages=[NgramJaccardStage(min_final_score=0.0)],
    )

    rows = (
        matcher.match()
        .matches()
        .select("unique_id, resolved_canonical_id, match_reason")
        .fetchall()
    )

    assert rows == [("M1", "C1", MatchReason.NGRAM_JACCARD.value)]


def test_ngram_jaccard_stage_respects_minimum_similarity_threshold(
    duck_con, canonical_cases, messy_cases
):
    canonical = duck_con.sql(
        f"""
        SELECT unique_id, address_concat, postcode
        FROM ({canonical_cases.sql_query()}) AS canonical_cases
        WHERE unique_id = 'C1'
        """
    )
    messy = duck_con.sql(
        f"""
        SELECT unique_id, address_concat, postcode
        FROM ({messy_cases.sql_query()}) AS messy_cases
        WHERE case_group = 'threshold'
        """
    )

    matcher = AddressMatcher(
        canonical_addresses=canonical,
        addresses_to_match=messy,
        con=duck_con,
        stages=[NgramJaccardStage(min_final_score=0.95)],
    )

    rows = matcher.match().matches().select("unique_id, resolved_canonical_id").fetchall()

    assert rows == [("M6", None)]


def test_ngram_jaccard_stage_postcode_fallback_matches_unresolved_records(
    duck_con, canonical_cases, messy_cases
):
    canonical = duck_con.sql(
        f"""
        SELECT unique_id, address_concat, postcode
        FROM ({canonical_cases.sql_query()}) AS canonical_cases
        WHERE unique_id = 'C1'
        """
    )
    messy = duck_con.sql(
        f"""
        SELECT unique_id, address_concat, postcode
        FROM ({messy_cases.sql_query()}) AS messy_cases
        WHERE case_group = 'fallback'
        """
    )

    no_fallback = AddressMatcher(
        canonical_addresses=canonical,
        addresses_to_match=messy,
        con=duck_con,
        stages=[NgramJaccardStage(min_final_score=0.0, use_postcode_fallback=False)],
    )
    no_fallback_rows = (
        no_fallback.match()
        .matches()
        .select("unique_id, resolved_canonical_id")
        .fetchall()
    )

    with_fallback = AddressMatcher(
        canonical_addresses=canonical,
        addresses_to_match=messy,
        con=duck_con,
        stages=[NgramJaccardStage(min_final_score=0.0, use_postcode_fallback=True)],
    )
    with_fallback_rows = (
        with_fallback.match()
        .matches()
        .select("unique_id, resolved_canonical_id, match_reason")
        .fetchall()
    )

    assert no_fallback_rows == [("M2", None)]
    assert with_fallback_rows == [("M2", "C1", MatchReason.NGRAM_JACCARD.value)]


def test_ngram_jaccard_stage_postcode_fallback_works_with_chunking(
    duck_con, canonical_cases, messy_cases
):
    canonical = duck_con.sql(
        f"""
        SELECT unique_id, address_concat, postcode
        FROM ({canonical_cases.sql_query()}) AS canonical_cases
        WHERE unique_id = 'C1'
        """
    )
    messy = duck_con.sql(
        f"""
        SELECT unique_id, address_concat, postcode
        FROM ({messy_cases.sql_query()}) AS messy_cases
        WHERE case_group = 'fallback'
        """
    )

    chunked_with_fallback = AddressMatcher(
        canonical_addresses=canonical,
        addresses_to_match=messy,
        con=duck_con,
        stages=[
            NgramJaccardStage(
                min_final_score=0.0,
                use_postcode_fallback=True,
                num_of_chunks=2,
            )
        ],
    )
    rows = (
        chunked_with_fallback.match()
        .matches()
        .select("unique_id, resolved_canonical_id, match_reason")
        .fetchall()
    )

    assert rows == [("M2", "C1", MatchReason.NGRAM_JACCARD.value)]


@pytest.mark.parametrize(
    "messy_id, expected_canonical_id",
    [
        ("M3", None),
        ("M4", "C4"),
        ("M5", "C5"),
    ],
)
def test_ngram_jaccard_stage_matches_practical_format_variants(
    duck_con,
    canonical_cases,
    messy_cases,
    messy_id,
    expected_canonical_id,
):
    canonical = duck_con.sql(
        f"""
        SELECT unique_id, address_concat, postcode
        FROM ({canonical_cases.sql_query()}) AS canonical_cases
        WHERE case_group = 'practical'
        """
    )
    messy = duck_con.sql(
        f"""
        SELECT unique_id, address_concat, postcode
        FROM ({messy_cases.sql_query()}) AS messy_cases
        WHERE unique_id = '{messy_id}'
        """
    )

    matcher = AddressMatcher(
        canonical_addresses=canonical,
        addresses_to_match=messy,
        con=duck_con,
        stages=[NgramJaccardStage(min_final_score=0.0)],
    )

    rows = (
        matcher.match()
        .matches()
        .select("unique_id, resolved_canonical_id, match_reason")
        .fetchall()
    )

    if expected_canonical_id is None:
        assert rows == [(messy_id, None, None)]
    else:
        assert rows == [
            (messy_id, expected_canonical_id, MatchReason.NGRAM_JACCARD.value)
        ]


def test_ngram_jaccard_chunked_mode_matches_non_chunked_results(duck_con):
    canonical = duck_con.sql("""
        WITH ids AS (
            SELECT i
            FROM range(1, 26) AS t(i)
        )
        SELECT
            'C' || CAST(i AS VARCHAR) AS unique_id,
            CAST(i AS VARCHAR) || ' high street london' AS address_concat,
            CASE WHEN i % 2 = 0 THEN 'SW1A 1AA' ELSE 'M1 1AA' END AS postcode
        FROM ids
    """)
    messy = duck_con.sql("""
        WITH ids AS (
            SELECT i
            FROM range(1, 26) AS t(i)
        )
        SELECT
            'M' || CAST(i AS VARCHAR) AS unique_id,
            CASE
                WHEN i % 3 = 0 THEN CAST(i AS VARCHAR) || ' high st london'
                ELSE CAST(i AS VARCHAR) || ' high street london'
            END AS address_concat,
            CASE WHEN i % 2 = 0 THEN 'SW1A 1AA' ELSE 'M1 1AA' END AS postcode
        FROM ids
    """)

    non_chunked = AddressMatcher(
        canonical_addresses=canonical,
        addresses_to_match=messy,
        con=duck_con,
        stages=[NgramJaccardStage(min_final_score=0.0, num_of_chunks=None)],
    )
    non_chunked_rows = sorted(
        non_chunked.match()
        .matches()
        .select("unique_id, resolved_canonical_id, match_reason")
        .fetchall()
    )

    chunked = AddressMatcher(
        canonical_addresses=canonical,
        addresses_to_match=messy,
        con=duck_con,
        stages=[
            NgramJaccardStage(
                min_final_score=0.0,
                num_of_chunks=4,
            )
        ],
    )
    chunked_rows = sorted(
        chunked.match()
        .matches()
        .select("unique_id, resolved_canonical_id, match_reason")
        .fetchall()
    )

    assert chunked_rows == non_chunked_rows


def test_ngram_jaccard_stage_blocks_primary_number_mismatch(
    duck_con,
):
    canonical = duck_con.sql("""
        SELECT *
        FROM (
            VALUES
                ('C1', '12 flat 2 alpha road london', 'SW1A 1AA')
        ) AS t(unique_id, address_concat, postcode)
    """)
    messy = duck_con.sql("""
        SELECT *
        FROM (
            VALUES
                ('M1', '10 flat 2 alpha road london', 'SW1A 1AA')
        ) AS t(unique_id, address_concat, postcode)
    """)

    matcher = AddressMatcher(
        canonical_addresses=canonical,
        addresses_to_match=messy,
        con=duck_con,
        stages=[NgramJaccardStage(min_final_score=0.0)],
    )

    rows = matcher.match().matches().select("unique_id, resolved_canonical_id").fetchall()

    assert rows == [("M1", None)]


def test_ngram_jaccard_stage_allows_matches_without_numeric_tokens(duck_con):
    canonical = duck_con.sql("""
        SELECT *
        FROM (
            VALUES
                ('C1', 'alpha quay business park', 'SW1A 1AA')
        ) AS t(unique_id, address_concat, postcode)
    """)
    messy = duck_con.sql("""
        SELECT *
        FROM (
            VALUES
                ('M1', 'alpha quay business pk', 'SW1A 1AA')
        ) AS t(unique_id, address_concat, postcode)
    """)

    matcher = AddressMatcher(
        canonical_addresses=canonical,
        addresses_to_match=messy,
        con=duck_con,
        stages=[NgramJaccardStage(min_final_score=0.0)],
    )

    rows = matcher.match().matches().select("unique_id, resolved_canonical_id").fetchall()

    assert rows == [("M1", "C1")]


def test_ngram_jaccard_stage_penalises_but_does_not_block_flat_letter_conflict(duck_con):
    canonical = duck_con.sql("""
        SELECT *
        FROM (
            VALUES
                ('C1', 'flat d 108 auckland hill london', 'SE27 9QQ')
        ) AS t(unique_id, address_concat, postcode)
    """)
    messy = duck_con.sql("""
        SELECT *
        FROM (
            VALUES
                ('M1', 'flat a 108 auckland hill london', 'SE27 9QQ')
        ) AS t(unique_id, address_concat, postcode)
    """)

    matcher = AddressMatcher(
        canonical_addresses=canonical,
        addresses_to_match=messy,
        con=duck_con,
        stages=[NgramJaccardStage(min_final_score=0.0)],
    )

    rows = matcher.match().matches().select("unique_id, resolved_canonical_id").fetchall()

    assert rows == [("M1", "C1")]


def test_ngram_jaccard_stage_blocks_leading_number_mismatch_for_alphanumeric_number(
    duck_con,
):
    canonical = duck_con.sql("""
        SELECT *
        FROM (
            VALUES
                ('C1', '31a lower marsh london', 'SE1 7RJ')
        ) AS t(unique_id, address_concat, postcode)
    """)
    messy = duck_con.sql("""
        SELECT *
        FROM (
            VALUES
                ('M1', '1a lower marsh london', 'SE1 7RJ')
        ) AS t(unique_id, address_concat, postcode)
    """)

    matcher = AddressMatcher(
        canonical_addresses=canonical,
        addresses_to_match=messy,
        con=duck_con,
        stages=[NgramJaccardStage(min_final_score=0.0)],
    )

    rows = matcher.match().matches().select("unique_id, resolved_canonical_id").fetchall()

    assert rows == [("M1", None)]


def test_ngram_jaccard_stage_strict_primary_number_blocks_sibling_number_mismatch(
    duck_con,
):
    canonical = duck_con.sql("""
        SELECT *
        FROM (
            VALUES
                ('C1', 'flat 3 57 clapham common south side london', 'SW4 9DA')
        ) AS t(unique_id, address_concat, postcode)
    """)
    messy = duck_con.sql("""
        SELECT *
        FROM (
            VALUES
                ('M1', 'flat 3 56 clapham common south side london', 'SW4 9DA')
        ) AS t(unique_id, address_concat, postcode)
    """)

    matcher = AddressMatcher(
        canonical_addresses=canonical,
        addresses_to_match=messy,
        con=duck_con,
        stages=[NgramJaccardStage(min_final_score=0.0)],
    )

    rows = matcher.match().matches().select("unique_id, resolved_canonical_id").fetchall()

    assert rows == [("M1", None)]


@pytest.mark.parametrize(
    "kwargs, message",
    [
        ({"num_of_chunks": 0}, "num_of_chunks must be at least 1 when provided"),
        ({"max_token_frequency": 0}, "max_token_frequency must be at least 1"),
    ],
)
def test_ngram_jaccard_stage_validates_chunking_parameters(kwargs, message):
    with pytest.raises(ValueError, match=message):
        NgramJaccardStage(**kwargs)


def test_ngram_jaccard_stage_score_gap_uses_runner_up_below_min_final_score(duck_con):
    canonical = duck_con.sql("""
        SELECT *
        FROM (
            VALUES
                ('C1', '10 high street london', 'SW1A 1AA'),
                ('C2', '10 high street lane london', 'SW1A 1AA')
        ) AS t(unique_id, address_concat, postcode)
    """)
    messy = duck_con.sql("""
        SELECT *
        FROM (
            VALUES
                ('M1', '10 high street london', 'SW1A 1AA')
        ) AS t(unique_id, address_concat, postcode)
    """)

    matcher = AddressMatcher(
        canonical_addresses=canonical,
        addresses_to_match=messy,
        con=duck_con,
        stages=[
            NgramJaccardStage(
                min_final_score=0.9,
                min_score_gap=0.5,
            )
        ],
    )

    rows = matcher.match().matches().select("unique_id, resolved_canonical_id").fetchall()

    assert rows == [("M1", None)]


def test_ngram_jaccard_stage_rejects_removed_min_jaccard_argument():
    with pytest.raises(TypeError):
        NgramJaccardStage(min_jaccard=0.8)  # type: ignore[call-arg]
