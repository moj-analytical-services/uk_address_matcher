import duckdb
import pytest

from uk_address_matcher.post_linkage.distinguishing_features.structural_evidence import (
    improve_predictions_using_structural_evidence,
)
from uk_address_matcher.post_linkage.identify_distinguishing_tokens import (
    improve_predictions_using_distinguishing_tokens,
)


def _predictions(con, rows):
    con.execute("""
        CREATE TABLE predictions (
            unique_id_r VARCHAR,
            unique_id_l VARCHAR,
            clean_full_address_r VARCHAR,
            postcode_r VARCHAR,
            flat_letter_r VARCHAR,
            flat_number_r VARCHAR,
            has_flat_indicator_r BOOLEAN,
            structural_bigram_reward DOUBLE,
            gamma_postcode INTEGER,
            gamma_address_without_numbers INTEGER,
            bigram_absence_penalty DOUBLE,
            match_weight DOUBLE
        )
    """)
    con.executemany(
        "INSERT INTO predictions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    return con.table("predictions")


def _canonical(con, rows):
    con.execute("""
        CREATE TABLE canonical (
            unique_id VARCHAR,
            clean_full_address VARCHAR,
            postcode VARCHAR,
            flat_letter VARCHAR
        )
    """)
    con.executemany("INSERT INTO canonical VALUES (?, ?, ?, ?)", rows)
    return con.table("canonical")


def _apply(con, prediction_rows, canonical_rows):
    return improve_predictions_using_structural_evidence(
        df_predict=_predictions(con, prediction_rows),
        df_canonical=_canonical(con, canonical_rows),
        con=con,
    ).df()


def _prediction_row(
    *,
    source_id="messy",
    candidate_id="candidate",
    address="FLAT 2 10 ALPHA ROAD",
    postcode="AA1 1AA",
    flat_letter=None,
    flat_number="2",
    has_flat_indicator=True,
    structural_bigram_reward=0.0,
    gamma_postcode=5,
    gamma_address_without_numbers=3,
    bigram_absence_penalty=1.15,
    match_weight=10.0,
):
    return (
        source_id,
        candidate_id,
        address,
        postcode,
        flat_letter,
        flat_number,
        has_flat_indicator,
        structural_bigram_reward,
        gamma_postcode,
        gamma_address_without_numbers,
        bigram_absence_penalty,
        match_weight,
    )


def test_reranker_classifies_only_all_structural_positive_bigrams():
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE reranker_input AS
        SELECT
            * EXCLUDE (common_end_tokens_hist_r),
            CAST(common_end_tokens_hist_r AS MAP(VARCHAR, INTEGER))
                AS common_end_tokens_hist_r
        FROM (VALUES
            (0.0, 0.5, 'candidate', 'messy',
             'FLAT 2 CHARDMORE ROAD', 'FLAT 2 CHARDMORE ROAD',
             'AA1 1AB', 'AA1 1AA', 1, 10, map([], [])),
            (0.0, 0.5, 'rival', 'messy',
             'FLAT 3 CHARDMORE ROAD', 'FLAT 2 CHARDMORE ROAD',
             'AA1 1AC', 'AA1 1AA', 2, 10, map([], []))
        ) AS input(
            match_weight,
            match_probability,
            unique_id_l,
            unique_id_r,
            clean_full_address_l,
            clean_full_address_r,
            postcode_l,
            postcode_r,
            ukam_address_id_l,
            ukam_address_id_r,
            common_end_tokens_hist_r
        )
    """)

    result = (
        improve_predictions_using_distinguishing_tokens(
            df_predict=con.table("reranker_input"),
            con=con,
            match_weight_threshold=-100,
        )
        .df()
        .set_index("unique_id_l")
    )

    candidate = result.loc["candidate"]
    assert candidate["structural_bigram_reward"] == pytest.approx(2.2)
    assert candidate["bigram_reward"] == pytest.approx(4.4)


@pytest.mark.parametrize(
    ("canonical_rows", "expected_adjustment"),
    [
        (
            [("candidate", "FLAT 2 10 ALPHA ROAD", "BB1 1BB", None)],
            -2.2,
        ),
        (
            [
                ("candidate", "FLAT 2 10 ALPHA ROAD", "BB1 1BB", None),
                ("candidate", "FLAT 2 10 ALPHA ROAD", "AA1 1AA", None),
            ],
            0.0,
        ),
    ],
)
def test_structural_bigram_reward_depends_on_any_exact_postcode_variant(
    canonical_rows,
    expected_adjustment,
):
    con = duckdb.connect()
    result = _apply(
        con,
        [_prediction_row(structural_bigram_reward=2.2)],
        canonical_rows,
    ).iloc[0]

    assert result["structural_bigram_adjustment"] == pytest.approx(expected_adjustment)
    assert result["bigram_absence_penalty"] == pytest.approx(1.15)
    assert result["match_weight"] == pytest.approx(10.0 + expected_adjustment)


def test_postcode_or_substantive_identity_evidence_is_aggregated_by_uprn():
    con = duckdb.connect()
    result = _apply(
        con,
        [
            _prediction_row(candidate_id="neither"),
            _prediction_row(candidate_id="postcode_variant"),
            _prediction_row(candidate_id="identity_variant"),
            _prediction_row(candidate_id="structural_only"),
        ],
        [
            ("neither", "FLAT 2 10 BETA STREET", "BB1 1BB", None),
            ("postcode_variant", "FLAT 2 10 BETA STREET", "BB1 1BB", None),
            ("postcode_variant", "FLAT 2 10 BETA STREET", "AA1 1AA", None),
            ("identity_variant", "FLAT 2 10 BETA STREET", "BB1 1BB", None),
            ("identity_variant", "ALPHA HOUSE", "BB1 1BC", None),
            ("structural_only", "FLAT 2 10 UNIT FLOOR", "BB1 1BD", None),
        ],
    ).set_index("unique_id_l")

    assert result.loc["neither", "no_postcode_or_identity_adjustment"] == -8.0
    assert result.loc["postcode_variant", "no_postcode_or_identity_adjustment"] == 0.0
    assert result.loc["identity_variant", "no_postcode_or_identity_adjustment"] == 0.0
    assert result.loc["structural_only", "no_postcode_or_identity_adjustment"] == -8.0


def test_flat_letter_contradiction_guards_and_variant_support():
    con = duckdb.connect()
    result = _apply(
        con,
        [
            _prediction_row(
                source_id="explicit",
                candidate_id="only_b",
                address="FLAT A 27 CHARDMORE ROAD",
                flat_letter="A",
                flat_number=None,
            ),
            _prediction_row(
                source_id="explicit",
                candidate_id="supports_a",
                address="FLAT A 27 CHARDMORE ROAD",
                flat_letter="A",
                flat_number=None,
            ),
            _prediction_row(
                source_id="explicit",
                candidate_id="a_variant",
                address="FLAT A 27 CHARDMORE ROAD",
                flat_letter="A",
                flat_number=None,
            ),
            _prediction_row(
                source_id="no_rival",
                candidate_id="only_b_no_rival",
                address="FLAT A 27 CHARDMORE ROAD",
                flat_letter="A",
                flat_number=None,
            ),
            _prediction_row(
                source_id="empty_letter",
                candidate_id="only_b_empty",
                flat_letter=None,
                flat_number=None,
            ),
            _prediction_row(
                source_id="numbered",
                candidate_id="only_b_numbered",
                address="FLAT 2 AT 1A HIGH ROAD",
                flat_letter="A",
                flat_number="2",
            ),
            _prediction_row(
                source_id="numbered",
                candidate_id="supports_a_numbered",
                address="FLAT 2 AT 1A HIGH ROAD",
                flat_letter="A",
                flat_number="2",
            ),
        ],
        [
            ("only_b", "FLAT B 27 CHARDMORE ROAD", "AA1 1AA", "B"),
            ("supports_a", "FLAT A 27 CHARDMORE ROAD", "AA1 1AA", "A"),
            ("a_variant", "FLAT B 27 CHARDMORE ROAD", "AA1 1AA", "B"),
            ("a_variant", "FLAT A 27 CHARDMORE ROAD", "AA1 1AA", "A"),
            ("only_b_no_rival", "FLAT B 27 CHARDMORE ROAD", "AA1 1AA", "B"),
            ("only_b_empty", "FLAT B 10 ALPHA ROAD", "AA1 1AA", "B"),
            ("only_b_numbered", "FLAT B 1 HIGH ROAD", "AA1 1AA", "B"),
            ("supports_a_numbered", "FLAT A 1 HIGH ROAD", "AA1 1AA", "A"),
        ],
    ).set_index(["unique_id_r", "unique_id_l"])

    adjustment = "flat_letter_conflict_adjustment"
    assert result.loc[("explicit", "only_b"), adjustment] == -3.0
    assert result.loc[("explicit", "supports_a"), adjustment] == 0.0
    assert result.loc[("explicit", "a_variant"), adjustment] == 0.0
    assert result.loc[("no_rival", "only_b_no_rival"), adjustment] == 0.0
    assert result.loc[("empty_letter", "only_b_empty"), adjustment] == 0.0
    assert result.loc[("numbered", "only_b_numbered"), adjustment] == 0.0


@pytest.mark.parametrize(
    (
        "gamma_postcode",
        "gamma_address_without_numbers",
        "canonical_postcode",
        "expected_adjustment",
    ),
    [
        (2, 0, "BB1 1BB", -3.0),
        (3, 0, "BB1 1BB", 0.0),
        (2, 1, "BB1 1BB", 0.0),
        (2, 0, "AA1 1AA", 0.0),
    ],
)
def test_weak_postcode_and_address_level_guards(
    gamma_postcode,
    gamma_address_without_numbers,
    canonical_postcode,
    expected_adjustment,
):
    con = duckdb.connect()
    result = _apply(
        con,
        [
            _prediction_row(
                gamma_postcode=gamma_postcode,
                gamma_address_without_numbers=gamma_address_without_numbers,
            )
        ],
        [("candidate", "FLAT 2 10 ALPHA ROAD", canonical_postcode, None)],
    ).iloc[0]

    assert result["weak_address_and_postcode_adjustment"] == expected_adjustment


def test_four_adjustments_are_additive_and_change_only_ranking():
    con = duckdb.connect()
    result = _apply(
        con,
        [
            _prediction_row(
                candidate_id="wrong",
                address="FLAT A 27 CHARDMORE ROAD",
                postcode="N16 6JA",
                flat_letter="A",
                flat_number=None,
                structural_bigram_reward=2.2,
                gamma_postcode=2,
                gamma_address_without_numbers=0,
                match_weight=20.0,
            ),
            _prediction_row(
                candidate_id="right",
                address="FLAT A 27 CHARDMORE ROAD",
                postcode="N16 6JA",
                flat_letter="A",
                flat_number=None,
                match_weight=10.0,
            ),
        ],
        [
            ("wrong", "FLAT B 27 FORBURG ROAD", "N16 6HP", "B"),
            ("right", "FLAT B 27 CHARDMORE ROAD", "N16 6HP", "B"),
            ("right", "FLAT A 27 CHARDMORE ROAD", "N16 6JA", "A"),
        ],
    ).set_index("unique_id_l")

    wrong = result.loc["wrong"]
    assert wrong["structural_bigram_adjustment"] == pytest.approx(-2.2)
    assert wrong["no_postcode_or_identity_adjustment"] == -8.0
    assert wrong["flat_letter_conflict_adjustment"] == -3.0
    assert wrong["weak_address_and_postcode_adjustment"] == -3.0
    assert wrong["match_weight"] == pytest.approx(3.8)

    right = result.loc["right"]
    assert right["match_weight"] == pytest.approx(10.0)
    assert len(result) == 2
    assert result["match_weight"].idxmax() == "right"
    assert (result["match_weight"] >= 0.0).any()
