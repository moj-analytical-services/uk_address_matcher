import duckdb

from uk_address_matcher import prepare_data_for_matching
from uk_address_matcher.linking_model.splink_model import _get_linker
from uk_address_matcher.post_linkage.identify_distinguishing_tokens import (
    improve_predictions_using_distinguishing_tokens,
)

# (messy_id, messy_address, postcode, canonical_id, should_match)
TEST_CASES = [
    # Letter mismatches
    ("m_flat_a", "FLAT A 10 KINGS ROAD LONDON", "SW3 4ND", "c_flat_c", False),
    ("m_flat_b", "FLAT B 10 KINGS ROAD LONDON", "SW3 4ND", "c_flat_c", False),
    ("m_flat_c", "FLAT C 10 KINGS ROAD LONDON", "SW3 4ND", "c_flat_c", True),
    # Number mismatches
    ("m_flat_1", "FLAT 1 20 HIGH STREET LONDON", "E1 6AA", "c_flat_3", False),
    ("m_flat_2", "FLAT 2 20 HIGH STREET LONDON", "E1 6AA", "c_flat_3", False),
    ("m_flat_3", "FLAT 3 20 HIGH STREET LONDON", "E1 6AA", "c_flat_3", True),
    # Combined number and letter
    ("m_flat_1a", "FLAT 1A 30 PARK LANE LONDON", "W1K 1BE", "c_flat_2b", False),
    ("m_flat_2a", "FLAT 2A 30 PARK LANE LONDON", "W1K 1BE", "c_flat_2b", False),
    ("m_flat_2b", "FLAT 2B 30 PARK LANE LONDON", "W1K 1BE", "c_flat_2b", True),
    # Positional (first vs second)
    (
        "m_first_pos",
        "FIRST FLOOR FLAT 40 QUEEN ST",
        "EC4R 1AA",
        "c_second_pos",
        False,
    ),
    (
        "m_second_pos",
        "SECOND FLOOR FLAT 40 QUEEN ST",
        "EC4R 1AA",
        "c_second_pos",
        True,
    ),
]

# Canonical addresses that messy records match against
CANONICAL_ADDRESSES = [
    ("c_flat_c", "FLAT C 10 KINGS ROAD LONDON", "SW3 4ND"),
    ("c_flat_3", "FLAT 3 20 HIGH STREET LONDON", "E1 6AA"),
    ("c_flat_2b", "FLAT 2B 30 PARK LANE LONDON", "W1K 1BE"),
    ("c_second_pos", "SECOND FLOOR FLAT 40 QUEEN STREET LONDON", "EC4R 1AA"),
]


def test_flat_penalties():
    match_weight_threshold = 10.0
    con = duckdb.connect()

    # Build messy addresses relation - use test_id as unique_id for lookup
    messy_values = ", ".join(
        f"('{uid}'::VARCHAR, '{addr}'::VARCHAR, '{pc}'::VARCHAR)"
        for uid, addr, pc, _, _ in TEST_CASES
    )
    messy_rel = con.sql(f"""
        SELECT * FROM (VALUES {messy_values})
        AS t(unique_id, address_concat, postcode)
    """)

    # Build canonical addresses relation
    canon_values = ", ".join(
        f"('{uid}'::VARCHAR, '{addr}'::VARCHAR, '{pc}'::VARCHAR)"
        for uid, addr, pc in CANONICAL_ADDRESSES
    )
    canon_rel = con.sql(f"""
        SELECT * FROM (VALUES {canon_values})
        AS t(unique_id, address_concat, postcode)
    """)

    # Clean the data
    messy_cleaned = prepare_data_for_matching(messy_rel, con=con)
    canon_cleaned = prepare_data_for_matching(canon_rel, con=con)

    # Get linker and run predictions
    linker = _get_linker(
        messy_cleaned,
        canon_cleaned,
        con=con,
        include_full_postcode_block=True,
        include_outside_postcode_block=False,
    )
    predictions = linker.inference.predict(threshold_match_probability=0.00001)
    results_df = predictions.as_pandas_dataframe()

    # Build lookup: (messy_unique_id, canon_unique_id) -> match_weight
    match_weights = {}
    for _, row in results_df.iterrows():
        key = (row["unique_id_l"], row["unique_id_r"])
        match_weights[key] = row["match_weight"]
        # Also store reverse key since Splink can return either order
        key_rev = (row["unique_id_r"], row["unique_id_l"])
        match_weights[key_rev] = row["match_weight"]

    # Check all test cases
    failures = []
    for messy_id, _, _, canon_id, should_match in TEST_CASES:
        key = (messy_id, canon_id)
        mw = match_weights.get(key)

        if mw is None:
            if should_match:
                failures.append(
                    f"{messy_id} -> {canon_id}: no prediction found (expected match)"
                )
        elif should_match and mw < match_weight_threshold:
            failures.append(
                f"{messy_id} -> {canon_id}: MW={mw:.2f} < "
                f"{match_weight_threshold} (expected match)"
            )
        elif not should_match and mw >= match_weight_threshold:
            failures.append(
                f"{messy_id} -> {canon_id}: MW={mw:.2f} >= "
                f"{match_weight_threshold} (expected penalty)"
            )

    assert not failures, "Flat penalty failures:\n" + "\n".join(failures)


# ── One-sided NULL: flat present vs flat absent (Option C) ────────────────────

NULL_TEST_CASES = [
    # FLAT 3 vs bare address — should NOT match
    ("m_flat3_null", "FLAT 3 27 LOVE LANE LONDON", "EC2V 7AA", "c_bare_27", False),
    # Same bare address vs bare canonical — should match
    ("m_bare_27", "27 LOVE LANE LONDON", "EC2V 7AA", "c_bare_27", True),
    # FLAT 1 vs bare address
    (
        "m_flat1_null",
        "FLAT 1 50 CHURCH LANE MANCHESTER",
        "M1 1AA",
        "c_bare_50",
        False,
    ),
    # Bare vs bare
    ("m_bare_50", "50 CHURCH LANE MANCHESTER", "M1 1AA", "c_bare_50", True),
]

NULL_CANONICAL_ADDRESSES = [
    ("c_bare_27", "27 LOVE LANE LONDON", "EC2V 7AA"),
    ("c_bare_50", "50 CHURCH LANE MANCHESTER", "M1 1AA"),
]


def test_flat_one_sided_null_penalty():
    """Flat present in messy but absent in canonical should be penalised."""
    match_weight_threshold = 10.0
    con = duckdb.connect()

    messy_values = ", ".join(
        f"('{uid}'::VARCHAR, '{addr}'::VARCHAR, '{pc}'::VARCHAR)"
        for uid, addr, pc, _, _ in NULL_TEST_CASES
    )
    messy_rel = con.sql(f"""
        SELECT * FROM (VALUES {messy_values})
        AS t(unique_id, address_concat, postcode)
    """)

    canon_values = ", ".join(
        f"('{uid}'::VARCHAR, '{addr}'::VARCHAR, '{pc}'::VARCHAR)"
        for uid, addr, pc in NULL_CANONICAL_ADDRESSES
    )
    canon_rel = con.sql(f"""
        SELECT * FROM (VALUES {canon_values})
        AS t(unique_id, address_concat, postcode)
    """)

    messy_cleaned = prepare_data_for_matching(messy_rel, con=con)
    canon_cleaned = prepare_data_for_matching(canon_rel, con=con)

    linker = _get_linker(
        messy_cleaned,
        canon_cleaned,
        con=con,
        include_full_postcode_block=True,
        include_outside_postcode_block=False,
    )
    predictions = linker.inference.predict(threshold_match_probability=0.00001)
    results_df = predictions.as_pandas_dataframe()

    match_weights = {}
    for _, row in results_df.iterrows():
        key = (row["unique_id_l"], row["unique_id_r"])
        match_weights[key] = row["match_weight"]
        match_weights[(row["unique_id_r"], row["unique_id_l"])] = row["match_weight"]

    failures = []
    for messy_id, _, _, canon_id, should_match in NULL_TEST_CASES:
        key = (messy_id, canon_id)
        mw = match_weights.get(key)

        if mw is None:
            if should_match:
                failures.append(
                    f"{messy_id} -> {canon_id}: no prediction found (expected match)"
                )
        elif should_match and mw < match_weight_threshold:
            failures.append(
                f"{messy_id} -> {canon_id}: MW={mw:.2f} < {match_weight_threshold} "
                f"(expected match)"
            )
        elif not should_match and mw >= match_weight_threshold:
            failures.append(
                f"{messy_id} -> {canon_id}: MW={mw:.2f} >= {match_weight_threshold} "
                f"(expected penalty)"
            )

    assert not failures, "One-sided NULL flat failures:\n" + "\n".join(failures)


EQUIVALENCE_TEST_CASES = [
    (
        "m_flat_letter",
        "FLAT B 10 KINGS ROAD LONDON",
        "SW3 4ND",
        "c_flat_number_equiv",
        "c_flat_number_mismatch",
    ),
    (
        "m_ground_floor",
        "GROUND FLOOR FLAT 20 HIGH STREET LONDON",
        "E1 6AA",
        "c_lower_ground_equiv",
        "c_first_floor_mismatch",
    ),
]

EQUIVALENCE_CANONICAL_ADDRESSES = [
    ("c_flat_number_equiv", "FLAT 2 10 KINGS ROAD LONDON", "SW3 4ND"),
    ("c_flat_number_mismatch", "FLAT D 10 KINGS ROAD LONDON", "SW3 4ND"),
    ("c_lower_ground_equiv", "LOWER GROUND FLAT 20 HIGH STREET LONDON", "E1 6AA"),
    ("c_first_floor_mismatch", "FIRST FLOOR FLAT 20 HIGH STREET LONDON", "E1 6AA"),
]

ONE_SIDED_NUMBER_LETTER_MESSY = [
    ("m_fuzzy_reference", "FLAT B 10 KINGS ROAD LONDON", "SW3 4ND"),
    ("m_one_sided_2b", "FLAT 2B 10 KINGS ROAD LONDON", "SW3 4ND"),
]

ONE_SIDED_NUMBER_LETTER_CANONICAL = [
    ("c_flat_2_reference", "FLAT 2 10 KINGS ROAD LONDON", "SW3 4ND"),
]

SAME_LETTER_NUMBER_ONESIDED_MESSY = [
    ("m_missing_number_2a", "2A GARDENSVILLE THE ROAD LONDON", "EC1V 9NX"),
]

SAME_LETTER_NUMBER_ONESIDED_CANONICAL = [
    (
        "c_same_letter_match",
        "FLAT 2A GARDENSVILLE THE ROAD LONDON",
        "EC1V 9NX",
    ),
    (
        "c_letter_mismatch",
        "FLAT 2B GARDENSVILLE THE ROAD LONDON",
        "EC1V 9NX",
    ),
]

CARDIFF_ROOM_VS_BUILDING_MESSY = [
    (
        "m_cardiff_room_23",
        ("ROOM 23 MY COMPANY 2-6 TESTING STCARDIFF CITY AND COUNTY OF CARDIFF"),
        "CF22 1AA",
    ),
]

CARDIFF_ROOM_VS_BUILDING_CANONICAL = [
    (
        "c_cardiff_ambassador",
        "MY COMPANY, 2-6, TESTING ST, CARDIFF",
        "CF22 1AA",
    ),
]


def test_flat_equivalence_soft_boost():
    """Fuzzy flat equivalence should apply a modest uplift versus mismatches."""
    con = duckdb.connect()

    messy_values = ", ".join(
        f"('{uid}'::VARCHAR, '{addr}'::VARCHAR, '{pc}'::VARCHAR)"
        for uid, addr, pc, _, _ in EQUIVALENCE_TEST_CASES
    )
    messy_rel = con.sql(f"""
        SELECT * FROM (VALUES {messy_values})
        AS t(unique_id, address_concat, postcode)
    """)

    canon_values = ", ".join(
        f"('{uid}'::VARCHAR, '{addr}'::VARCHAR, '{pc}'::VARCHAR)"
        for uid, addr, pc in EQUIVALENCE_CANONICAL_ADDRESSES
    )
    canon_rel = con.sql(f"""
        SELECT * FROM (VALUES {canon_values})
        AS t(unique_id, address_concat, postcode)
    """)

    messy_cleaned = prepare_data_for_matching(messy_rel, con=con)
    canon_cleaned = prepare_data_for_matching(canon_rel, con=con)

    linker = _get_linker(
        messy_cleaned,
        canon_cleaned,
        con=con,
        include_full_postcode_block=True,
        include_outside_postcode_block=False,
    )
    predictions = linker.inference.predict(threshold_match_probability=0.00001)
    results_df = predictions.as_pandas_dataframe()

    match_weights = {}
    for _, row in results_df.iterrows():
        key = (row["unique_id_l"], row["unique_id_r"])
        match_weights[key] = row["match_weight"]
        match_weights[(row["unique_id_r"], row["unique_id_l"])] = row["match_weight"]

    failures = []
    for messy_id, _, _, equiv_id, mismatch_id in EQUIVALENCE_TEST_CASES:
        equiv_key = (messy_id, equiv_id)
        mismatch_key = (messy_id, mismatch_id)
        equiv_weight = match_weights.get(equiv_key)
        mismatch_weight = match_weights.get(mismatch_key)

        if equiv_weight is None:
            failures.append(f"{messy_id} -> {equiv_id}: no prediction found")
            continue
        if mismatch_weight is None:
            failures.append(f"{messy_id} -> {mismatch_id}: no prediction found")
            continue

        uplift = equiv_weight - mismatch_weight
        if uplift < 3.0:
            failures.append(
                f"{messy_id}: uplift={uplift:.2f} (equiv={equiv_weight:.2f}, "
                f"mismatch={mismatch_weight:.2f})"
            )

    assert not failures, "Flat equivalence boost failures:\n" + "\n".join(failures)


def test_flat_number_letter_one_sided_penalty_not_fuzzy_equivalence():
    """FLAT 2B vs FLAT 2 should get one-sided letter penalty, not fuzzy boost."""
    con = duckdb.connect()

    messy_values = ", ".join(
        f"('{uid}'::VARCHAR, '{addr}'::VARCHAR, '{pc}'::VARCHAR)"
        for uid, addr, pc in ONE_SIDED_NUMBER_LETTER_MESSY
    )
    messy_rel = con.sql(f"""
        SELECT * FROM (VALUES {messy_values})
        AS t(unique_id, address_concat, postcode)
    """)

    canon_values = ", ".join(
        f"('{uid}'::VARCHAR, '{addr}'::VARCHAR, '{pc}'::VARCHAR)"
        for uid, addr, pc in ONE_SIDED_NUMBER_LETTER_CANONICAL
    )
    canon_rel = con.sql(f"""
        SELECT * FROM (VALUES {canon_values})
        AS t(unique_id, address_concat, postcode)
    """)

    messy_cleaned = prepare_data_for_matching(messy_rel, con=con)
    canon_cleaned = prepare_data_for_matching(canon_rel, con=con)

    linker = _get_linker(
        messy_cleaned,
        canon_cleaned,
        con=con,
        include_full_postcode_block=True,
        include_outside_postcode_block=False,
        retain_intermediate_calculation_columns=True,
    )
    predictions = linker.inference.predict(threshold_match_probability=0.00001)
    results_df = predictions.as_pandas_dataframe()

    match_weights = {}
    for _, row in results_df.iterrows():
        key = (row["unique_id_l"], row["unique_id_r"])
        match_weights[key] = row["match_weight"]
        match_weights[(row["unique_id_r"], row["unique_id_l"])] = row["match_weight"]

    fuzzy_reference_weight = match_weights.get(
        ("m_fuzzy_reference", "c_flat_2_reference")
    )
    one_sided_weight = match_weights.get(("m_one_sided_2b", "c_flat_2_reference"))

    assert fuzzy_reference_weight is not None, (
        "m_fuzzy_reference -> c_flat_2_reference: no prediction found"
    )
    assert one_sided_weight is not None, (
        "m_one_sided_2b -> c_flat_2_reference: no prediction found"
    )

    one_sided_row = results_df[
        (results_df["unique_id_l"] == "m_one_sided_2b")
        & (results_df["unique_id_r"] == "c_flat_2_reference")
        | (
            (results_df["unique_id_r"] == "m_one_sided_2b")
            & (results_df["unique_id_l"] == "c_flat_2_reference")
        )
    ]
    fuzzy_reference_row = results_df[
        (results_df["unique_id_l"] == "m_fuzzy_reference")
        & (results_df["unique_id_r"] == "c_flat_2_reference")
        | (
            (results_df["unique_id_r"] == "m_fuzzy_reference")
            & (results_df["unique_id_l"] == "c_flat_2_reference")
        )
    ]

    assert not one_sided_row.empty, "Missing one-sided comparison row"
    assert not fuzzy_reference_row.empty, "Missing fuzzy reference comparison row"

    one_sided_bf = float(one_sided_row.iloc[0]["bf_flat_identity"])
    fuzzy_reference_bf = float(fuzzy_reference_row.iloc[0]["bf_flat_identity"])

    assert one_sided_bf == 0.25, (
        "Expected FLAT 2B vs FLAT 2 to hit 'Same number, letter one-sided' "
        f"(bf=0.25), got bf_flat_identity={one_sided_bf:.6f}."
    )
    assert fuzzy_reference_bf == 13.0, (
        "Expected FLAT B vs FLAT 2 to hit fuzzy letter-number equivalence "
        f"(bf=13), got bf_flat_identity={fuzzy_reference_bf:.6f}."
    )
    assert one_sided_bf < fuzzy_reference_bf, (
        "Expected one-sided letter case to score lower than fuzzy equivalence; "
        f"got one_sided={one_sided_bf:.6f}, "
        f"fuzzy_reference={fuzzy_reference_bf:.6f}."
    )


def test_same_letter_number_one_sided_scores_between_fuzzy_and_mismatch():
    """2A vs FLAT 2A should beat a letter mismatch"""
    con = duckdb.connect()

    messy_values = ", ".join(
        f"('{uid}'::VARCHAR, '{addr}'::VARCHAR, '{pc}'::VARCHAR)"
        for uid, addr, pc in SAME_LETTER_NUMBER_ONESIDED_MESSY
    )
    messy_rel = con.sql(f"""
        SELECT * FROM (VALUES {messy_values})
        AS t(unique_id, address_concat, postcode)
    """)

    canon_values = ", ".join(
        f"('{uid}'::VARCHAR, '{addr}'::VARCHAR, '{pc}'::VARCHAR)"
        for uid, addr, pc in SAME_LETTER_NUMBER_ONESIDED_CANONICAL
    )
    canon_rel = con.sql(f"""
        SELECT * FROM (VALUES {canon_values})
        AS t(unique_id, address_concat, postcode)
    """)

    messy_cleaned = prepare_data_for_matching(messy_rel, con=con)
    canon_cleaned = prepare_data_for_matching(canon_rel, con=con)

    linker = _get_linker(
        messy_cleaned,
        canon_cleaned,
        con=con,
        include_full_postcode_block=True,
        include_outside_postcode_block=False,
        retain_intermediate_calculation_columns=True,
    )
    predictions = linker.inference.predict(threshold_match_probability=0.00001)
    results_df = predictions.as_pandas_dataframe()

    def _row_for(canonical_id: str):
        row = results_df[
            (results_df["unique_id_l"] == "m_missing_number_2a")
            & (results_df["unique_id_r"] == canonical_id)
            | (
                (results_df["unique_id_r"] == "m_missing_number_2a")
                & (results_df["unique_id_l"] == canonical_id)
            )
        ]
        assert not row.empty, f"Missing comparison row for {canonical_id}"
        return row.iloc[0]

    same_letter_row = _row_for("c_same_letter_match")
    mismatch_row = _row_for("c_letter_mismatch")
    same_letter_bf = float(same_letter_row["bf_flat_identity"])
    mismatch_bf = float(mismatch_row["bf_flat_identity"])

    assert same_letter_bf > 1.0, (
        "Expected 2A vs FLAT 2A to receive a positive flat-identity signal; "
        f"got bf_flat_identity={same_letter_bf:.6f}."
    )
    assert same_letter_bf > mismatch_bf, (
        "Expected same-letter one-sided-number case to score above a true letter "
        f"mismatch; got same_letter={same_letter_bf:.6f}, mismatch={mismatch_bf:.6f}."
    )
    assert same_letter_bf < 13.0, (
        "Expected same-letter one-sided-number case to stay below full fuzzy "
        f"equivalence; got bf_flat_identity={same_letter_bf:.6f}."
    )


def test_cardiff_room_vs_ambassador_scores_plus_five_or_more():
    """Fictional Cardiff-style room vs building comparison should score +5+."""
    con = duckdb.connect()

    messy_values = ", ".join(
        f"('{uid}'::VARCHAR, '{addr}'::VARCHAR, '{pc}'::VARCHAR)"
        for uid, addr, pc in CARDIFF_ROOM_VS_BUILDING_MESSY
    )
    messy_rel = con.sql(f"""
        SELECT * FROM (VALUES {messy_values})
        AS t(unique_id, address_concat, postcode)
    """)

    canon_values = ", ".join(
        f"('{uid}'::VARCHAR, '{addr}'::VARCHAR, '{pc}'::VARCHAR)"
        for uid, addr, pc in CARDIFF_ROOM_VS_BUILDING_CANONICAL
    )
    canon_rel = con.sql(f"""
        SELECT * FROM (VALUES {canon_values})
        AS t(unique_id, address_concat, postcode)
    """)

    messy_cleaned = prepare_data_for_matching(messy_rel, con=con)
    canon_cleaned = prepare_data_for_matching(canon_rel, con=con)

    linker = _get_linker(
        messy_cleaned,
        canon_cleaned,
        con=con,
        include_full_postcode_block=True,
        include_outside_postcode_block=False,
        retain_intermediate_calculation_columns=True,
    )
    predictions = linker.inference.predict(threshold_match_probability=0.00001)
    results_df = predictions.as_pandas_dataframe()

    row = results_df[
        (
            (results_df["unique_id_l"] == "m_cardiff_room_23")
            & (results_df["unique_id_r"] == "c_cardiff_ambassador")
        )
        | (
            (results_df["unique_id_r"] == "m_cardiff_room_23")
            & (results_df["unique_id_l"] == "c_cardiff_ambassador")
        )
    ]

    assert not row.empty, "Missing Cardiff room-vs-building prediction row"

    match_weight = float(row.iloc[0]["match_weight"])
    assert match_weight >= 5.0, (
        "Expected Cardiff room-vs-building case to score at least +5; "
        f"got match_weight={match_weight:.6f}."
    )


def test_sub_premise_location_discrimination_in_reranker():
    """Positional descriptors (LEFT/RIGHT/...) discriminate siblings in the reranker.

    The sub-premise location signal now lives in the reranker (phase 2) rather than
    in the Splink model: a messy record stating a position should rank a canonical
    candidate sharing that position above an otherwise-identical sibling that states a
    conflicting position, with a position-silent sibling sitting in between.
    """
    con = duckdb.connect()

    messy = "FLAT 4 FIRST FLOOR RIGHT 20 HIGH STREET LONDON"
    candidates = {
        "c_location_right": "FLAT 4 FIRST FLOOR RIGHT 20 HIGH STREET LONDON",
        "c_location_missing": "FLAT 4 FIRST FLOOR 20 HIGH STREET LONDON",
        "c_location_left": "FLAT 4 FIRST FLOOR LEFT 20 HIGH STREET LONDON",
    }

    rows = []
    for i, (canon_id, canon_addr) in enumerate(candidates.items(), start=1):
        rows.append(
            {
                "match_weight": 0.0,
                "match_probability": 0.5,
                "unique_id_l": canon_id,
                "unique_id_r": "m_location_right",
                "original_address_concat_l": canon_addr,
                "original_address_concat_r": messy,
                "clean_full_address_l": canon_addr,
                "clean_full_address_r": messy,
                "postcode_l": "E1 6AA",
                "postcode_r": "E1 6AA",
                "ukam_address_id_l": i,
                "ukam_address_id_r": 100,
            }
        )

    con.execute(
        """
        CREATE TEMP TABLE df (
            match_weight DOUBLE,
            match_probability DOUBLE,
            unique_id_l VARCHAR,
            unique_id_r VARCHAR,
            original_address_concat_l VARCHAR,
            original_address_concat_r VARCHAR,
            clean_full_address_l VARCHAR,
            clean_full_address_r VARCHAR,
            postcode_l VARCHAR,
            postcode_r VARCHAR,
            ukam_address_id_l INTEGER,
            ukam_address_id_r INTEGER
        )
        """
    )
    con.executemany(
        "INSERT INTO df VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                r["match_weight"],
                r["match_probability"],
                r["unique_id_l"],
                r["unique_id_r"],
                r["original_address_concat_l"],
                r["original_address_concat_r"],
                r["clean_full_address_l"],
                r["clean_full_address_r"],
                r["postcode_l"],
                r["postcode_r"],
                r["ukam_address_id_l"],
                r["ukam_address_id_r"],
            )
            for r in rows
        ],
    )
    df_predict = con.sql(
        """
        SELECT
            *,
            CAST(map([], []) AS MAP(VARCHAR, INTEGER)) AS common_end_tokens_hist_r
        FROM df
        """
    )

    df_improved = improve_predictions_using_distinguishing_tokens(
        df_predict=df_predict,
        con=con,
        match_weight_threshold=-100,
        top_n_matches=5,
        use_bigrams=True,
    )
    results = df_improved.df().set_index("unique_id_l")

    mw_right = float(results.loc["c_location_right", "match_weight"])
    mw_missing = float(results.loc["c_location_missing", "match_weight"])
    mw_left = float(results.loc["c_location_left", "match_weight"])

    # Matching position ranks above a silent sibling, which ranks above a conflict.
    assert mw_right > mw_missing, (
        f"Expected RIGHT-vs-RIGHT ({mw_right:.3f}) to outrank position-silent "
        f"({mw_missing:.3f})."
    )
    assert mw_missing > mw_left, (
        f"Expected position-silent ({mw_missing:.3f}) to outrank conflicting LEFT "
        f"({mw_left:.3f})."
    )
    # The LEFT/RIGHT conflict should cost roughly a full positional penalty relative
    # to the otherwise-identical position-silent sibling (proving the term is active).
    assert mw_missing - mw_left >= 5.0, (
        "Expected the positional conflict to demote the LEFT sibling by ~one penalty; "
        f"got gap={mw_missing - mw_left:.3f}."
    )
