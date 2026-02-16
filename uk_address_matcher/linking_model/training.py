# This turns out to not be really a training script
# since we hard code all the values!

import splink.comparison_level_library as cll
import splink.comparison_library as cl
from splink import SettingsCreator, block_on
from splink.internals.misc import match_weight_to_bayes_factor

from .blocking import old_blocking_rules

toggle_u_probability_fix = True
toggle_m_probability_fix = True

clean_full_address_comparison = cl.ExactMatch(
    "clean_full_address",
).configure(u_probabilities=[1, 2], m_probabilities=[15, 1])


def get_address_without_numbers_comparison(
    WEIGHT_EXACT=15,
    WEIGHT_JACCARD_HIGH=8,
    WEIGHT_JACCARD_MED=4,
    WEIGHT_JACCARD_LOW=2,
    WEIGHT_ELSE_M=1,
    WEIGHT_ELSE_U=2,
):
    """Compare addresses with all numeric tokens stripped.

    This comparison captures street/locality similarity independent of building
    and flat numbers.  We use DuckDB's built-in ``jaccard`` (character-bigram
    based) rather than Levenshtein because it is:

    - Robust to space insertion:  MIDLOTHIAN vs MID LOTHIAN → 0.90
    - Robust to character transposition:  GIPSY HILL vs GYPSY HILL → 1.0
    - Robust to truncation:  SHAKESPEARE vs SHAKESPEAR → 1.0
    - Still discriminating for genuine differences:  LOVE LANE vs LOVE LAND → 0.875

    Levenshtein treats a space insertion the same as a character change (both
    cost 1), so ``MIDLOTHIAN`` vs ``MID LOTHIAN`` is lev=1 — the same as
    ``LOVE LANE`` vs ``LOVE LAND``.  That conflates a harmless formatting
    difference with a possible location difference.  Jaccard on the character
    bigrams separates these cases neatly.
    """
    address_without_numbers_comparison = {
        "output_column_name": "address_without_numbers",
        "comparison_levels": [
            {
                "sql_condition": '"address_without_numbers_l" IS NULL OR "address_without_numbers_r" IS NULL',
                "label_for_charts": "address_without_numbers is NULL",
                "is_null_level": True,
            },
            {
                "sql_condition": '"address_without_numbers_l" = "address_without_numbers_r"',
                "label_for_charts": "Exact match on address_without_numbers",
                "m_probability": WEIGHT_EXACT,
                "u_probability": 1,
            },
            {
                "sql_condition": "jaccard(address_without_numbers_l, address_without_numbers_r) >= 0.95",
                "label_for_charts": "Jaccard >= 0.95 on address_without_numbers",
                "m_probability": WEIGHT_JACCARD_HIGH,
                "u_probability": 1,
            },
            {
                "sql_condition": "jaccard(address_without_numbers_l, address_without_numbers_r) >= 0.88",
                "label_for_charts": "Jaccard >= 0.88 on address_without_numbers",
                "m_probability": WEIGHT_JACCARD_MED,
                "u_probability": 1,
            },
            {
                "sql_condition": "jaccard(address_without_numbers_l, address_without_numbers_r) >= 0.80",
                "label_for_charts": "Jaccard >= 0.80 on address_without_numbers",
                "m_probability": WEIGHT_JACCARD_LOW,
                "u_probability": 1,
            },
            {
                "sql_condition": "ELSE",
                "label_for_charts": "All other comparisons",
                "m_probability": WEIGHT_ELSE_M,
                "u_probability": WEIGHT_ELSE_U,
            },
        ],
        "comparison_description": "Address without numbers comparison",
    }
    return address_without_numbers_comparison


def get_flat_identity_comparison(
    WEIGHT_ASYMMETRIC=-10,
    WEIGHT_MATCH=6.57,
    WEIGHT_SAME_POSITIONAL=-2,
    WEIGHT_CROSS_TYPE=-2,
    WEIGHT_BOTH_POSITIONAL_DIFFER=-8,
    WEIGHT_SAME_NUMBER_LETTER_ONESIDED=-2,
    WEIGHT_SAME_NUMBER_LETTERS_DIFFER=-8,
    WEIGHT_MISMATCH=-20,
):
    """Compare the composite flat identity (number + letter + positional).

    The cleaning pipeline produces ``flat_identity`` by concatenating
    ``flat_number``, ``flat_letter``, and ``flat_positional`` into a
    single string.  This means '14D GROUND FLOOR' vs '14A GROUND FLOOR'
    is a single mismatch rather than separate partial hits.

    Levels are evaluated top-to-bottom:

    1. **Early exit** — if ``has_flat_indicator`` differs between the
       two sides (one address mentions a flat, the other does not) we
       apply a strong penalty immediately.  This avoids the ambiguity
       of comparing NULL flat_identity against a populated one.
    2. **Both NULL** — neither side has flat info; neutral.
    3. **Exact match** — all components agree; positive evidence.
    4. **Same positional** — both sides share the same positional
       descriptor (e.g. both BASEMENT, both FIRST FLOOR) but the full
       flat identity differs because one side has an additional letter
       or number (e.g. ``__BASEMENT`` vs ``_A_BASEMENT``).  The floor
       is the same; only the sub-unit label differs, so a moderate
       penalty applies.
    5. **Cross-type** — one side uses a positional descriptor
       (BASEMENT, FIRST FLOOR, etc.) while the other uses a
       letter/number (FLAT A, FLAT 1).  These naming conventions
       are interchangeable in practice (BASEMENT = FLAT 1,
       GROUND FLOOR = FLAT A), so we treat this as near-neutral.
    6. **Both positional differ** — both sides have a positional
       descriptor but they name different floors (e.g. TOP FLOOR vs
       FIRST FLOOR, LOWER GROUND vs BASEMENT).  This is more
       penalising than cross-type but less severe than a complete
       mismatch because informal labels like TOP FLOOR can refer to
       different canonical floors.
    7. **Same number, letter one-sided** — both sides share the
       same flat number but only one side has a flat letter (e.g.
       FLAT 2 vs FLAT 2A).  Messy data commonly drops the letter,
       so this deserves a mild penalty rather than the full ELSE.
    8. **Same number, letters both differ** — both sides share the
       same flat number and both have a flat letter, but the letters
       differ (e.g. FLAT 2A vs FLAT 2B).  These are genuinely
       different flats in the same building so the penalty is
       heavier than one-sided but lighter than a full mismatch.
    9. **ELSE** — flat identities differ and no partial match
       applies (e.g. FLAT 1 vs FLAT 2); very heavy penalty.
    """
    # Same positional condition: both sides share the same positional
    # descriptor but the composite flat_identity strings differ (caught
    # here because exact match already passed).  Covers cases like
    # __BASEMENT vs _A_BASEMENT, __FIRST FLOOR vs 2__FIRST FLOOR.
    same_positional_sql = """flat_positional_l IS NOT NULL
    AND flat_positional_l = flat_positional_r"""

    # Cross-type condition: one side has a positional descriptor, the other
    # does not, but both sides DO have some flat identity populated.
    # This catches FIRST FLOOR vs FLAT A, BASEMENT vs FLAT 1, etc.
    cross_type_sql = """(
        (flat_positional_l IS NOT NULL AND flat_positional_r IS NULL)
        OR
        (flat_positional_r IS NOT NULL AND flat_positional_l IS NULL)
    )
    AND flat_identity_l IS NOT NULL AND flat_identity_r IS NOT NULL"""

    # Both-positional-differ condition: both sides have a positional
    # descriptor, but they name different floors.  E.g. TOP FLOOR vs
    # FIRST FLOOR, LOWER GROUND vs BASEMENT.
    both_positional_differ_sql = """flat_positional_l IS NOT NULL
    AND flat_positional_r IS NOT NULL
    AND flat_positional_l != flat_positional_r"""

    # Same flat number, letter one-sided: both sides share the same flat
    # number but only one has a flat letter.  E.g. FLAT 2 vs FLAT 2A.
    # Messy data commonly drops the letter, so a mild penalty applies.
    same_number_letter_onesided_sql = """flat_number_l IS NOT NULL
    AND flat_number_l = flat_number_r
    AND (
        (flat_letter_l IS NULL AND flat_letter_r IS NOT NULL)
        OR (flat_letter_l IS NOT NULL AND flat_letter_r IS NULL)
    )"""

    # Same flat number, letters both differ: both sides share the same
    # flat number and both have letters, but the letters are different.
    # E.g. FLAT 2A vs FLAT 2B.  By elimination: exact match, same
    # positional, cross-type, both positional differ, and letter
    # one-sided levels have all been evaluated already.
    same_number_letters_differ_sql = """flat_number_l IS NOT NULL
    AND flat_number_l = flat_number_r"""

    flat_identity_comparison = {
        "output_column_name": "flat_identity",
        "comparison_levels": [
            # Early exit: one side has flat info, other does not
            {
                "sql_condition": "has_flat_indicator_l != has_flat_indicator_r",
                "label_for_charts": "One has flat indicator, other does not",
                "m_probability": match_weight_to_bayes_factor(WEIGHT_ASYMMETRIC),
                "u_probability": 1,
                "fix_m_probability": toggle_m_probability_fix,
                "fix_u_probability": toggle_u_probability_fix,
            },
            # Both null → neutral
            {
                "sql_condition": '"flat_identity_l" IS NULL AND "flat_identity_r" IS NULL',
                "label_for_charts": "Both null (no flat info)",
                "is_null_level": True,
            },
            # Exact match on composite identity
            {
                "sql_condition": "flat_identity_l = flat_identity_r",
                "label_for_charts": "Exact match on flat identity",
                "m_probability": match_weight_to_bayes_factor(WEIGHT_MATCH),
                "u_probability": 1,
                "fix_m_probability": toggle_m_probability_fix,
                "fix_u_probability": toggle_u_probability_fix,
            },
            # Same positional: both sides name the same floor but the full
            # identity differs (extra letter/number on one side).
            # e.g. __BASEMENT vs _A_BASEMENT, __FIRST FLOOR vs 2__FIRST FLOOR
            {
                "sql_condition": same_positional_sql,
                "label_for_charts": "Same positional, different letter/number",
                "m_probability": match_weight_to_bayes_factor(WEIGHT_SAME_POSITIONAL),
                "u_probability": 1,
                "fix_m_probability": toggle_m_probability_fix,
                "fix_u_probability": toggle_u_probability_fix,
            },
            # Cross-type: positional on one side, letter/number on the other.
            # e.g. BASEMENT vs FLAT 1, FIRST FLOOR vs FLAT A.
            # Different naming conventions but could be the same flat.
            {
                "sql_condition": cross_type_sql,
                "label_for_charts": "Cross-type flat (positional vs letter/number)",
                "m_probability": match_weight_to_bayes_factor(WEIGHT_CROSS_TYPE),
                "u_probability": 1,
                "fix_m_probability": toggle_m_probability_fix,
                "fix_u_probability": toggle_u_probability_fix,
            },
            # Both sides have a positional descriptor but for different floors.
            # e.g. TOP FLOOR vs FIRST FLOOR, LOWER GROUND vs BASEMENT.
            # Penalising but less severe than a plain number/letter mismatch
            # because informal descriptors like TOP FLOOR are ambiguous.
            {
                "sql_condition": both_positional_differ_sql,
                "label_for_charts": "Both positional but different floors",
                "m_probability": match_weight_to_bayes_factor(
                    WEIGHT_BOTH_POSITIONAL_DIFFER
                ),
                "u_probability": 1,
                "fix_m_probability": toggle_m_probability_fix,
                "fix_u_probability": toggle_u_probability_fix,
            },
            # Same flat number, letter one-sided: one side has a letter,
            # other does not.  e.g. FLAT 2 vs FLAT 2A (messy dropped letter).
            {
                "sql_condition": same_number_letter_onesided_sql,
                "label_for_charts": "Same number, letter one-sided",
                "m_probability": match_weight_to_bayes_factor(
                    WEIGHT_SAME_NUMBER_LETTER_ONESIDED
                ),
                "u_probability": 1,
                "fix_m_probability": toggle_m_probability_fix,
                "fix_u_probability": toggle_u_probability_fix,
            },
            # Same flat number, letters both present but different.
            # e.g. FLAT 2A vs FLAT 2B.  Genuinely different flats but
            # same building number so less harsh than full mismatch.
            {
                "sql_condition": same_number_letters_differ_sql,
                "label_for_charts": "Same number, letters both differ",
                "m_probability": match_weight_to_bayes_factor(
                    WEIGHT_SAME_NUMBER_LETTERS_DIFFER
                ),
                "u_probability": 1,
                "fix_m_probability": toggle_m_probability_fix,
                "fix_u_probability": toggle_u_probability_fix,
            },
            # Flat identities differ with no partial match → wrong flat
            {
                "sql_condition": "ELSE",
                "label_for_charts": "Flat identity differs (same type)",
                "m_probability": match_weight_to_bayes_factor(WEIGHT_MISMATCH),
                "u_probability": 1,
                "fix_m_probability": toggle_m_probability_fix,
                "fix_u_probability": toggle_u_probability_fix,
            },
        ],
        "comparison_description": "Combined flat identity comparison",
    }
    return flat_identity_comparison


def get_first_n_tokens_comparison(
    WEIGHT_1=1,
    WEIGHT_2=0.5,
    WEIGHT_3=0,
    WEIGHT_4=0,
    WEIGHT_5=-0.2,
):
    regex_4_tokens = r"^(?:\S+\s+){3}\S+"
    regex_3_tokens = r"^(?:\S+\s+){2}\S+"
    regex_2_tokens = r"^(?:\S+\s+){1}\S+"
    regex_1_token = r"^\S+"

    first_n_tokens_comparison = {
        "output_column_name": "first_n_tokens",
        "comparison_levels": [
            {
                "sql_condition": f"""
                    regexp_extract(original_address_concat_l, '{regex_4_tokens}') = regexp_extract(original_address_concat_r, '{regex_4_tokens}')
                    and length(regexp_extract(original_address_concat_l, '{regex_4_tokens}')) > 1
                    and postcode_l = postcode_r

                """,
                "label_for_charts": "First 4 tokens match",
                "m_probability": match_weight_to_bayes_factor(WEIGHT_1),
                "u_probability": 1,
                "fix_m_probability": toggle_m_probability_fix,
                "fix_u_probability": toggle_u_probability_fix,
            },
            {
                "sql_condition": f"""
                    regexp_extract(original_address_concat_l, '{regex_3_tokens}') = regexp_extract(original_address_concat_r, '{regex_3_tokens}')
                    and length(regexp_extract(original_address_concat_l, '{regex_3_tokens}')) > 1
                    and postcode_l = postcode_r
                """,
                "label_for_charts": "First 3 tokens match",
                "m_probability": match_weight_to_bayes_factor(WEIGHT_2),
                "u_probability": 1,
                "fix_m_probability": toggle_m_probability_fix,
                "fix_u_probability": toggle_u_probability_fix,
            },
            {
                "sql_condition": f"""
                    regexp_extract(original_address_concat_l, '{regex_2_tokens}') = regexp_extract(original_address_concat_r, '{regex_2_tokens}')
                    and length(regexp_extract(original_address_concat_l, '{regex_2_tokens}')) > 1
                    and postcode_l = postcode_r
                """,
                "label_for_charts": "First 2 tokens match",
                "m_probability": match_weight_to_bayes_factor(WEIGHT_3),
                "u_probability": 1,
                "fix_m_probability": toggle_m_probability_fix,
                "fix_u_probability": toggle_u_probability_fix,
            },
            {
                "sql_condition": f"""
                    regexp_extract(original_address_concat_l, '{regex_1_token}') = regexp_extract(original_address_concat_r, '{regex_1_token}')
                    and length(regexp_extract(original_address_concat_l, '{regex_1_token}')) > 1
                    and postcode_l = postcode_r
                """,
                "label_for_charts": "First token match",
                "m_probability": match_weight_to_bayes_factor(WEIGHT_4),
                "u_probability": 1,
                "fix_m_probability": toggle_m_probability_fix,
                "fix_u_probability": toggle_u_probability_fix,
            },
            {
                "sql_condition": "ELSE",
                "label_for_charts": "All other comparisons",
                "m_probability": match_weight_to_bayes_factor(WEIGHT_5),
                "u_probability": 1,
            },
        ],
    }
    return first_n_tokens_comparison


def get_num_1_comparison(
    WEIGHT_1=6.57,
    WEIGHT_2=6.57,
    WEIGHT_3=2,
    WEIGHT_4=-13.29,
    WEIGHT_5=-4,
):
    """Compare the primary numeric token (typically the house/building number).

    Levels are evaluated top-to-bottom:

    1. **Both NULL** — neutral.
    2. **Exact match** — full string match (with TF adjustment).
    3. **Numeric part match** — the leading digits match even if
       suffixes differ (e.g. ``12A`` vs ``12``).
    4. **Inverted** — the value matches the *other* side's secondary
       numeric token, catching swapped house/flat numbers.
    5. **Both present but differ** — both sides have a primary number
       but the values are different.  Strong evidence of non-match.
    6. **ELSE** — one side has no primary number; mildly penalising.
    """
    num_1_comparison = {
        "output_column_name": "numeric_token_1",
        "comparison_levels": [
            cll.NullLevel("numeric_token_1"),
            {
                "sql_condition": '"numeric_token_1_l" = "numeric_token_1_r"',
                "label_for_charts": "Exact match",
                "m_probability": match_weight_to_bayes_factor(WEIGHT_1),
                "u_probability": 1,
                "tf_adjustment_column": "numeric_token_1",
                "tf_adjustment_weight": 0.1,
                "fix_m_probability": toggle_m_probability_fix,
                "fix_u_probability": toggle_u_probability_fix,
            },
            {
                "sql_condition": """
                            nullif(regexp_extract(numeric_token_1_l, '\\d+', 0), '')
                            = nullif(regexp_extract(numeric_token_1_r, '\\d+', 0), '')
                            """,
                "label_for_charts": "Numeric part matches",
                "m_probability": match_weight_to_bayes_factor(WEIGHT_2),
                "u_probability": 1,
                "tf_adjustment_column": "numeric_token_1",
                "tf_adjustment_weight": 0.1,
                "fix_m_probability": toggle_m_probability_fix,
                "fix_u_probability": toggle_u_probability_fix,
            },
            {
                "sql_condition": "numeric_token_2_l = numeric_token_1_r or numeric_token_1_l = numeric_token_2_r",
                "label_for_charts": "Exact match inverted numbers",
                "m_probability": match_weight_to_bayes_factor(WEIGHT_3),
                "u_probability": 1,
                "fix_m_probability": toggle_m_probability_fix,
                "fix_u_probability": toggle_u_probability_fix,
            },
            {
                "sql_condition": '"numeric_token_1_l" IS NOT NULL AND "numeric_token_1_r" IS NOT NULL AND "numeric_token_1_l" != "numeric_token_1_r"',
                "label_for_charts": "Primary numbers both present but differ",
                "m_probability": match_weight_to_bayes_factor(WEIGHT_4),
                "u_probability": 1,
                "fix_m_probability": toggle_m_probability_fix,
                "fix_u_probability": toggle_u_probability_fix,
            },
            cll.ElseLevel().configure(
                m_probability=match_weight_to_bayes_factor(WEIGHT_5),
                u_probability=1,
                fix_m_probability=True,
                fix_u_probability=True,
            ),
        ],
        "comparison_description": "numeric_token_1",
    }
    return num_1_comparison


def get_num_2_comparison(
    WEIGHT_1=6.57,
    WEIGHT_2=0,
    WEIGHT_3=-13.29,
    WEIGHT_4=-2,
    WEIGHT_5=-4,
):
    """Compare the secondary numeric token (often the house number when a flat is present).

    Levels are evaluated top-to-bottom:

    1. **Both NULL** — neutral.
    2. **Exact match** — full string match (with TF adjustment).
    3. **Inverted** — the value matches the *other* side's primary
       numeric token, catching swapped house/flat numbers.
    4. **Both present but differ** — both sides have a secondary number
       but the values are different (e.g. 92 vs 102).  Strong evidence
       of non-match, particularly for flatted addresses where
       numeric_token_1 is the flat number and numeric_token_2 is the
       building number.
    5. **One null** — one side has a secondary number, the other does
       not; mildly penalising.
    6. **ELSE** — fallback for remaining edge cases.
    """
    num_2_comparison = {
        "output_column_name": "numeric_token_2",
        "comparison_levels": [
            # Both null → neutral
            {
                "sql_condition": '"numeric_token_2_l" IS NULL AND "numeric_token_2_r" IS NULL',
                "label_for_charts": "Both null",
                "is_null_level": True,
            },
            {
                "sql_condition": '"numeric_token_2_l" = "numeric_token_2_r"',
                "label_for_charts": "Exact match",
                "m_probability": match_weight_to_bayes_factor(WEIGHT_1),
                "u_probability": 1,
                "tf_adjustment_column": "numeric_token_2",
                "tf_adjustment_weight": 0.1,
                "fix_m_probability": toggle_m_probability_fix,
                "fix_u_probability": toggle_u_probability_fix,
            },
            {
                "sql_condition": "numeric_token_1_l = numeric_token_2_r OR numeric_token_1_r = numeric_token_2_l",
                "label_for_charts": "Exact match inverted numbers",
                "m_probability": match_weight_to_bayes_factor(WEIGHT_2),
                "u_probability": 1,
                "fix_m_probability": toggle_m_probability_fix,
                "fix_u_probability": toggle_u_probability_fix,
            },
            # Both present but values differ — strong evidence of wrong address
            {
                "sql_condition": '"numeric_token_2_l" IS NOT NULL AND "numeric_token_2_r" IS NOT NULL AND "numeric_token_2_l" != "numeric_token_2_r"',
                "label_for_charts": "Secondary numbers both present but differ",
                "m_probability": match_weight_to_bayes_factor(WEIGHT_3),
                "u_probability": 1,
                "fix_m_probability": toggle_m_probability_fix,
                "fix_u_probability": toggle_u_probability_fix,
            },
            # One has a num_2 and the other does not
            {
                "sql_condition": '"numeric_token_2_l" IS NULL OR "numeric_token_2_r" IS NULL',
                "label_for_charts": "One null",
                "m_probability": match_weight_to_bayes_factor(WEIGHT_4),
                "u_probability": 1,
                "fix_m_probability": toggle_m_probability_fix,
                "fix_u_probability": toggle_u_probability_fix,
            },
            cll.ElseLevel().configure(
                m_probability=match_weight_to_bayes_factor(WEIGHT_5),
                u_probability=1,
                fix_m_probability=True,
                fix_u_probability=True,
            ),
        ],
        "comparison_description": "numeric_token_2",
    }
    return num_2_comparison


num_3_comparison = {
    "output_column_name": "numeric_token_3",
    "comparison_levels": [
        {
            "sql_condition": '"numeric_token_3_l" IS NULL AND "numeric_token_3_r" IS NULL',
            "label_for_charts": "Null",
            "is_null_level": True,
        },
        {
            "sql_condition": '"numeric_token_3_l" = "numeric_token_3_r"',
            "label_for_charts": "Exact match",
            "m_probability": 0.6,
            "u_probability": 0.0001,
            "tf_adjustment_column": "numeric_token_3",
            "tf_adjustment_weight": 0.5,
        },
        {
            "sql_condition": '"numeric_token_2_l" = "numeric_token_3_r"',
            "label_for_charts": "Exact match inverted",
            "m_probability": 0.3,
            "u_probability": 0.0025,
            "tf_adjustment_column": "numeric_token_3",
            "tf_adjustment_weight": 0.5,
        },
        # One has a num 3 and the other does not
        {
            "sql_condition": '"numeric_token_3_l" IS NULL OR "numeric_token_3_r" IS NULL',
            "label_for_charts": "Null",
            "m_probability": 1,
            "u_probability": 16,
        },
        cll.ElseLevel().configure(
            m_probability=1,
            u_probability=256,
            fix_m_probability=True,
            fix_u_probability=True,
        ),
    ],
    "comparison_description": "numeric_token_3",
}


def array_reduce_by_freq(column_name: str) -> str:
    """Generate SQL for reducing arrays by frequency.

    Args:
        column_name: Name of the column containing arrays to compare
        power: Power to raise the denominator to in the second reduction

    Returns:
        SQL string for comparing arrays by frequency
    """
    # First part - multiply frequencies of matching tokens
    matching_tokens = f"""
    list_reduce(
        list_prepend(
        1.0,
        list_filter(
            list_transform(
            flatten(
                list_transform(
                map_entries({column_name}_l),
                entry -> CASE
                            WHEN COALESCE({column_name}_r[entry.key], 0) > 0
                            THEN list_value(POW(entry.key.rel_freq, LEAST(entry.value, {column_name}_r[entry.key])))
                            ELSE list_value()
                        END
                )
            ),
            x -> x
            ),
            x -> x IS NOT NULL
        )
        ),
        (p, q) -> p * q
    )
    """

    # This current fails if experimental optimisation on splink==4.0.7.dev1 is enabled
    # https://github.com/moj-analytical-services/splink/pull/2630
    # It doesn't appear to improve accuracy anyway
    #
    # missing_tokens_product = f"""
    # list_reduce(
    #     list_prepend(
    #         1.0,
    #         list_concat(
    #             list_transform(
    #                 map_entries({column_name}_l),
    #                 entry -> POW(entry.key.rel_freq, GREATEST(entry.value::INTEGER - COALESCE({column_name}_r[entry.key], 0), 0))
    #             ),
    #             list_transform(
    #                 map_entries({column_name}_r),
    #                 entry -> POW(entry.key.rel_freq, GREATEST(entry.value::INTEGER - COALESCE({column_name}_l[entry.key], 0), 0))
    #             )
    #         )
    #     ),
    #     (p, q) -> p * q
    # )
    # """

    # return f"{matching_tokens} / POW({missing_tokens_product}, 0.33)"
    return f"{matching_tokens}"


def generate_arr_reduce_data(
    start_exp=4,
    start_weight=-4,
    segments=[8, 8, 8, 10],
    delta_weights_within_segments=[1, 1, 0.25, 0.25],
):
    data = []
    current_exp = start_exp
    current_weight = start_weight

    for segment, delta_weight in zip(segments, delta_weights_within_segments):
        arr_red_sql = array_reduce_by_freq("token_rel_freq_arr_hist")
        for _ in range(segment):
            if current_exp > 0:
                sql_cond = f"{arr_red_sql} < 1e{current_exp}"
                label = f" < 1e{current_exp}"
            else:
                sql_cond = f"{arr_red_sql} < 1e{current_exp}"
                label = f" < 1e{current_exp}"

            level = {
                "sql_condition": sql_cond,
                "label_for_charts": label,
                "m_probability": match_weight_to_bayes_factor(current_weight),
                "u_probability": 1,
                "fix_m_probability": toggle_m_probability_fix,
                "fix_u_probability": toggle_u_probability_fix,
            }
            data.append(level)
            current_weight += delta_weight
            current_exp -= 1

    return data[::-1]


def get_token_rel_freq_arr_comparison(
    START_EXP=4,
    START_WEIGHT=-4,
    SEGMENTS=[8, 8, 8, 10],
    DELTA_WEIGHTS_WITHIN_SEGMENTS=[1, 1, 0.25, 0.25],
):
    middle_conditions = generate_arr_reduce_data(
        START_EXP,
        START_WEIGHT,
        SEGMENTS,
        DELTA_WEIGHTS_WITHIN_SEGMENTS,
    )

    token_rel_freq_arr_comparison = {
        "output_column_name": "token_rel_freq_arr_hist",
        "comparison_levels": [
            *middle_conditions,
            {
                "sql_condition": "ELSE",
                "label_for_charts": "All other comparisons",
                "m_probability": 1,
                "u_probability": 256,
                "fix_m_probability": toggle_m_probability_fix,
                "fix_u_probability": toggle_u_probability_fix,
            },
        ],
        "comparison_description": "Token relative frequency array",
    }

    return token_rel_freq_arr_comparison


arr_red_sql = array_reduce_by_freq("common_end_tokens_hist")

common_end_tokens_comparison = {
    "output_column_name": "common_end_tokens",
    "comparison_levels": [
        {
            "sql_condition": '"common_end_tokens_hist_l" IS NULL OR "common_end_tokens_hist_r" IS NULL',
            "label_for_charts": "Null",
            "is_null_level": True,
        },
        {
            "sql_condition": f"{arr_red_sql} < 1e-2",
            "label_for_charts": "<1e-2",
            "m_probability": 4,
            "u_probability": 1,
            "fix_m_probability": toggle_m_probability_fix,
            "fix_u_probability": toggle_u_probability_fix,
        },
        {
            "sql_condition": "ELSE",
            "label_for_charts": "All other comparisons",
            "m_probability": 1,
            "u_probability": 1.5,
            "fix_m_probability": toggle_m_probability_fix,
            "fix_u_probability": toggle_u_probability_fix,
        },
    ],
    "comparison_description": "Array intersection",
}


postcode_comparison = {
    "output_column_name": "postcode",
    "comparison_levels": [
        {
            "sql_condition": "postcode_l IS NULL AND postcode_r IS NULL",
            "label_for_charts": "Null",
            "is_null_level": True,
        },
        {
            "sql_condition": "postcode_r IS NULL",
            "label_for_charts": "Postcode missing from messy table",
            "fix_m_probability": True,
            "fix_u_probability": True,
            "m_probability": 1024,
            "u_probability": 1,
        },
        {
            "sql_condition": "postcode_l = postcode_r",
            "label_for_charts": "Exact",
            "m_probability": 3e6,
            "u_probability": 1,
            "fix_m_probability": toggle_m_probability_fix,
            "fix_u_probability": toggle_u_probability_fix,
        },
        {
            "sql_condition": "levenshtein(postcode_l, postcode_r) <= 1",
            "label_for_charts": "Lev <= 1",
            "m_probability": 10000,
            "u_probability": 1,
            "fix_m_probability": toggle_m_probability_fix,
            "fix_u_probability": toggle_u_probability_fix,
        },
        {
            "sql_condition": "levenshtein(postcode_l, postcode_r) <= 2",
            "label_for_charts": "Lev <=2",
            "m_probability": 5000,
            "u_probability": 1,
            "fix_m_probability": toggle_m_probability_fix,
            "fix_u_probability": toggle_u_probability_fix,
        },
        {
            "sql_condition": "split_part(postcode_l, ' ', 1) = split_part(postcode_r, ' ', 1)",
            "label_for_charts": "District",
            "m_probability": 3000,
            "u_probability": 1,
            "fix_m_probability": toggle_m_probability_fix,
            "fix_u_probability": toggle_u_probability_fix,
        },
        {
            "sql_condition": "split_part(postcode_l, ' ', 2) = split_part(postcode_r, ' ', 2)",
            "label_for_charts": "Unit not District",
            "m_probability": 2000,
            "u_probability": 1,
            "fix_m_probability": toggle_m_probability_fix,
            "fix_u_probability": toggle_u_probability_fix,
        },
        {
            "sql_condition": "ELSE",
            "label_for_charts": "All other comparisons",
            "m_probability": 1,
            "u_probability": 64,
            "fix_m_probability": toggle_m_probability_fix,
            "fix_u_probability": toggle_u_probability_fix,
        },
    ],
}


blocking_rules = old_blocking_rules + [block_on("postcode")]


def get_settings_for_training(
    num_1_weights=None,
    num_2_weights=None,
    token_rel_freq_arr_weights=None,
    flat_identity_weights=None,
    address_without_numbers_weights=None,
    first_n_tokens_weights=None,
    include_first_n_tokens=False,
):
    num_1_weights = num_1_weights or {}
    num_2_weights = num_2_weights or {}
    token_rel_freq_arr_weights = token_rel_freq_arr_weights or {}
    flat_identity_weights = flat_identity_weights or {}
    address_without_numbers_weights = address_without_numbers_weights or {}
    first_n_tokens_weights = first_n_tokens_weights or {}

    comparisons = [
        clean_full_address_comparison,
        get_address_without_numbers_comparison(**address_without_numbers_weights),
        get_flat_identity_comparison(**flat_identity_weights),
        get_num_1_comparison(**num_1_weights),
        get_num_2_comparison(**num_2_weights),
        num_3_comparison,
        get_token_rel_freq_arr_comparison(**token_rel_freq_arr_weights),
        common_end_tokens_comparison,
        postcode_comparison,
    ]

    if include_first_n_tokens:
        comparisons.append(get_first_n_tokens_comparison(**first_n_tokens_weights))

    settings_for_training = SettingsCreator(
        probability_two_random_records_match=3e-8,
        link_type="link_only",
        blocking_rules_to_generate_predictions=blocking_rules,
        comparisons=comparisons,
        retain_intermediate_calculation_columns=True,
        unique_id_column_name="ukam_address_id",
    )
    return settings_for_training
