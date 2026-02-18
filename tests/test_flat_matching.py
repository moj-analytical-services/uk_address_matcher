import duckdb

from uk_address_matcher import prepare_data_for_matching
from uk_address_matcher.linking_model.splink_model import _get_linker

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
    linker = _get_linker(messy_cleaned, canon_cleaned, con=con)
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

    linker = _get_linker(messy_cleaned, canon_cleaned, con=con)
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
