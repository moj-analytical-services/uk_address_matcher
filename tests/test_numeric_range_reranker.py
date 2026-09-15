import duckdb

from uk_address_matcher import AddressMatcher, SplinkStage
from uk_address_matcher.post_linkage.distinguishing_features.numeric_range import (
    NumericRangeRerankerConfig,
    build_numeric_range_adjustments,
    build_numeric_range_candidate_pool,
    ensure_numeric_range_struct,
)

_RANGE_TYPE = (
    "STRUCT(raw VARCHAR, lower UINTEGER, upper UINTEGER, width UINTEGER, "
    "lower_suffix VARCHAR, upper_suffix VARCHAR, role UTINYINT, "
    "flags UTINYINT, lower_tf DOUBLE)"
)


def _attribute(
    raw: str,
    lower: int,
    upper: int,
    *,
    lower_suffix: str | None = None,
    upper_suffix: str | None = None,
    role: int = 1,
    flags: int = 0,
    lower_tf: float | None = 0.1,
) -> str:
    lower_suffix_sql = "NULL::VARCHAR" if lower_suffix is None else f"'{lower_suffix}'"
    upper_suffix_sql = "NULL::VARCHAR" if upper_suffix is None else f"'{upper_suffix}'"
    lower_tf_sql = "NULL::DOUBLE" if lower_tf is None else str(lower_tf)
    return (
        "struct_pack("
        f"raw := '{raw}', lower := {lower}::UINTEGER, "
        f"upper := {upper}::UINTEGER, width := {max(0, upper - lower)}::UINTEGER, "
        f"lower_suffix := {lower_suffix_sql}, upper_suffix := {upper_suffix_sql}, "
        f"role := {role}::UTINYINT, flags := {flags}::UTINYINT, "
        f"lower_tf := {lower_tf_sql})"
    )


def test_numeric_range_struct_is_preserved_for_reranking():
    con = duckdb.connect()
    relation = con.sql("""
        SELECT
            'C1' AS unique_id,
            ['20'] AS numeric_tokens,
            struct_pack(
                raw := '20-23',
                lower := 20::UINTEGER,
                upper := 23::UINTEGER,
                width := 3::UINTEGER,
                lower_suffix := NULL::VARCHAR,
                upper_suffix := NULL::VARCHAR,
                role := 1::UTINYINT,
                flags := 0::UTINYINT,
                lower_tf := NULL::DOUBLE
            ) AS numeric_range
    """)

    normalised = ensure_numeric_range_struct(relation)
    numeric_range = normalised.select("numeric_range").fetchone()[0]

    assert numeric_range["lower"] == 20
    assert numeric_range["upper"] == 23


def test_missing_numeric_range_is_normalised_to_typed_null():
    con = duckdb.connect()
    relation = con.sql("SELECT 'C1' AS unique_id")

    normalised = ensure_numeric_range_struct(relation)
    numeric_range = normalised.select("numeric_range").fetchone()[0]

    assert numeric_range is None
    range_type = normalised.types[normalised.columns.index("numeric_range")]
    assert "lower UINTEGER" in str(range_type)
    assert "lower_tf DOUBLE" in str(range_type)


def test_splink_owns_numeric_range_reranker_configuration():
    assert "numeric_range_reranker" not in SplinkStage.__dataclass_fields__


def _range_candidates(con):
    empty = f"[]::{_RANGE_TYPE}[]"
    suffixed_range = (
        f"[{_attribute('20A-23B', 20, 23, lower_suffix='A', upper_suffix='B', flags=4)}]"
    )
    rows = [
        (
            "endpoint",
            1,
            f"[{_attribute('20-23', 20, 23)}]",
            0,
            empty,
            "[20]",
            "[NULL]",
            "[0]",
            1.0,
            20.0,
            "NULL",
            "'OTHER'",
        ),
        (
            "interior",
            1,
            f"[{_attribute('20-23', 20, 23)}]",
            0,
            empty,
            "[21]",
            "[NULL]",
            "[0]",
            1.0,
            20.0,
            "NULL",
            "'OTHER'",
        ),
        (
            "outside",
            1,
            f"[{_attribute('20-23', 20, 23)}]",
            0,
            empty,
            "[24]",
            "[NULL]",
            "[0]",
            1.0,
            20.0,
            "NULL",
            "'OTHER'",
        ),
        (
            "suffix_conflict",
            1,
            f"[{_attribute('20-23', 20, 23)}]",
            0,
            empty,
            "[20]",
            "['A']",
            "[0]",
            1.0,
            20.0,
            "NULL",
            "'OTHER'",
        ),
        (
            "suffix_endpoint",
            1,
            suffixed_range,
            0,
            empty,
            "[20]",
            "['A']",
            "[0]",
            1.0,
            20.0,
            "NULL",
            "'OTHER'",
        ),
        (
            "suffix_interior",
            1,
            suffixed_range,
            0,
            empty,
            "[21]",
            "[NULL]",
            "[0]",
            1.0,
            20.0,
            "NULL",
            "'OTHER'",
        ),
        (
            "reversed",
            1,
            f"[{_attribute('23-20', 23, 20, flags=1)}]",
            0,
            empty,
            "[23]",
            "[NULL]",
            "[0]",
            1.0,
            20.0,
            "NULL",
            "'OTHER'",
        ),
        (
            "reference",
            1,
            f"[{_attribute('20-23', 20, 23, role=3, flags=16)}]",
            0,
            empty,
            "[20]",
            "[NULL]",
            "[0]",
            1.0,
            20.0,
            "NULL",
            "'OTHER'",
        ),
        (
            "overwide",
            1,
            f"[{_attribute('1-30', 1, 30, flags=8)}]",
            0,
            empty,
            "[15]",
            "[NULL]",
            "[0]",
            1.0,
            20.0,
            "NULL",
            "'OTHER'",
        ),
        (
            "flat_conflict",
            1,
            f"[{_attribute('20-23', 20, 23)}]",
            0,
            empty,
            "[20]",
            "[NULL]",
            "[0]",
            1.0,
            20.0,
            "'FLAT_A'",
            "'FLAT_B'",
        ),
        (
            "null_tf",
            1,
            f"[{_attribute('20-23', 20, 23, lower_tf=None)}]",
            0,
            empty,
            "[20]",
            "[NULL]",
            "[0]",
            1.0,
            20.0,
            "NULL",
            "'OTHER'",
        ),
        (
            "strong_legacy",
            1,
            f"[{_attribute('20-23', 20, 23)}]",
            0,
            empty,
            "[20]",
            "[NULL]",
            "[0]",
            10.0,
            20.0,
            "NULL",
            "'OTHER'",
        ),
        (
            "symmetric",
            0,
            empty,
            1,
            f"[{_attribute('20-23', 20, 23)}]",
            "[20]",
            "[NULL]",
            "[0]",
            1.0,
            20.0,
            "NULL",
            "'OTHER'",
        ),
        (
            "multiple_ranges",
            2,
            f"[{_attribute('20-23', 20, 23)}, {_attribute('30-31', 30, 31)}]",
            0,
            empty,
            "[20]",
            "[NULL]",
            "[0]",
            1.0,
            20.0,
            "NULL",
            "'OTHER'",
        ),
    ]
    value_rows = []
    for index, row in enumerate(rows, start=1):
        (
            case_id,
            count_l,
            attrs_l,
            count_r,
            attrs_r,
            scalars_l,
            suffixes_l,
            roles_l,
            legacy,
            weight,
            flat_l,
            flat_r,
        ) = row
        scalar_value = scalars_l.strip("[]")
        suffix_value = suffixes_l.strip("[]").strip("'")
        numeric_token = (
            scalar_value if suffix_value == "NULL" else scalar_value + suffix_value
        )
        numeric_tokens_sql = f"['{numeric_token}']::VARCHAR[]"
        range_l_sql = (
            f"NULL::{_RANGE_TYPE}" if count_l == 0 else f"list_extract({attrs_l}, 1)"
        )
        range_r_sql = (
            f"NULL::{_RANGE_TYPE}" if count_r == 0 else f"list_extract({attrs_r}, 1)"
        )
        value_rows.append(
            f"('{case_id}', 'L{index}', 'R{index}', {index}, {index + 1000}, "
            f"{range_l_sql}, {range_r_sql}, "
            f"{numeric_tokens_sql}, {numeric_tokens_sql}, "
            f"{flat_l}::VARCHAR, {flat_r}::VARCHAR, {legacy}::DOUBLE, {weight}::DOUBLE)"
        )
    relation = con.sql(
        f"""
        SELECT * FROM (VALUES
            {", ".join(value_rows)}
        ) AS candidates(
            case_id, unique_id_l, unique_id_r,
            ukam_address_id_l, ukam_address_id_r,
            numeric_range_l, numeric_range_r,
            numeric_tokens_l, numeric_tokens_r,
            flat_identity_l, flat_identity_r,
            legacy_numeric_bits, match_weight
        )
        """
    )
    return relation


def test_numeric_range_adjustments_are_postcode_agnostic_and_guarded():
    con = duckdb.connect()
    relation = _range_candidates(con)
    result = build_numeric_range_adjustments(
        con,
        relation,
        NumericRangeRerankerConfig(),
    )
    rows = {
        row[0]: row[1:]
        for row in result.select(
            "unique_id_l, numeric_range_relationship, numeric_range_guard_passed, "
            "numeric_range_adjustment"
        ).fetchall()
    }
    con.close()

    assert rows["L1"][0:2] == ("scalar_range_endpoint", True)
    assert rows["L1"][2] > 0.0
    assert rows["L2"][0:2] == ("scalar_range_interior", True)
    assert rows["L2"][2] > 0.0
    assert rows["L3"] == ("neutral", False, 0.0)
    assert rows["L4"] == ("neutral", False, 0.0)
    assert rows["L5"][0:2] == ("scalar_range_endpoint", True)
    assert rows["L6"] == ("neutral", False, 0.0)
    assert rows["L7"] == ("neutral", False, 0.0)
    assert rows["L8"] == ("neutral", False, 0.0)
    assert rows["L9"] == ("neutral", False, 0.0)
    assert rows["L10"] == ("scalar_range_endpoint", False, 0.0)
    assert rows["L11"] == ("scalar_range_endpoint", True, 5.0)
    assert rows["L12"] == ("scalar_range_endpoint", True, 0.0)
    assert rows["L13"][0:2] == ("scalar_range_endpoint", True)
    assert rows["L14"][0:2] == ("scalar_range_endpoint", True)


def test_numeric_range_adjustment_respects_configured_cap():
    con = duckdb.connect()
    relation = _range_candidates(con).filter("case_id = 'endpoint'")
    relation = con.sql(
        "SELECT * REPLACE (-100.0::DOUBLE AS legacy_numeric_bits) "
        f"FROM ({relation.sql_query()}) AS candidates"
    )
    result = build_numeric_range_adjustments(
        con,
        relation,
        NumericRangeRerankerConfig(endpoint_match_bits=40.0),
    )
    assert result.select("numeric_range_adjustment").fetchone()[0] == 20.0
    con.close()


def test_numeric_range_candidate_pool_can_rescue_raw_rank_six():
    con = duckdb.connect()
    empty = f"NULL::{_RANGE_TYPE}"
    candidate_rows = []
    for rank in range(1, 7):
        numeric_range = _attribute("20-23", 20, 23) if rank == 6 else empty
        candidate_rows.append(
            f"('L{rank}', 'M', {rank}, 100, {21.0 - rank}::DOUBLE, "
            f"{numeric_range}, {empty}, ['20']::VARCHAR[], ['20']::VARCHAR[], "
            "NULL::VARCHAR, NULL::VARCHAR, "
            "2.0::DOUBLE, 1.0::DOUBLE, 1.0::DOUBLE, "
            "1.0::DOUBLE, 1.0::DOUBLE, 1.0::DOUBLE)"
        )
    relation = con.sql(f"""
        WITH candidates AS (
            SELECT * FROM (VALUES {", ".join(candidate_rows)}) AS rows(
                unique_id_l, unique_id_r, ukam_address_id_l, ukam_address_id_r,
                match_weight, numeric_range_l, numeric_range_r,
                numeric_tokens_l, numeric_tokens_r, flat_identity_l, flat_identity_r,
                bf_numeric_token_1, bf_numeric_token_2, bf_numeric_token_3,
                bf_tf_adj_numeric_token_1, bf_tf_adj_numeric_token_2,
                bf_tf_adj_numeric_token_3
            )
        )
        SELECT * FROM candidates
    """)

    result = build_numeric_range_candidate_pool(
        con,
        relation,
        NumericRangeRerankerConfig(lower_endpoint_tf_weight=0.0),
        top_n_matches=5,
        numeric_candidate_slots=1,
        numeric_search_depth=6,
    )
    rows = (
        result.select(
            "unique_id_l, match_weight + numeric_range_adjustment AS adjusted_weight"
        )
        .order("adjusted_weight DESC, unique_id_l DESC")
        .fetchall()
    )
    con.close()

    assert len(rows) == 5
    assert rows[0][0] == "L6"
    assert {row[0] for row in rows} == {"L1", "L2", "L3", "L4", "L6"}


def test_numeric_window_metadata_is_derived_only_from_supported_tokens():
    con = duckdb.connect()
    addresses = con.sql(
        "SELECT * FROM (VALUES "
        "('W', '20-23 HIGH STREET', 'ST1 1AA'), "
        "('T', '20 TO 23 HIGH STREET', 'ST1 1AA'), "
        "('A', '20A-23B HIGH STREET', 'ST1 1AA')"
        ") AS t(unique_id, address_concat, postcode)"
    )
    matcher = AddressMatcher(
        canonical_addresses=addresses,
        addresses_to_match=addresses.limit(1),
        con=con,
        stages=[SplinkStage(final_match_weight_threshold=-50)],
        show_progress=False,
    )
    matcher._resolve_canonical_data()
    rows = (
        matcher._canonical_clean.project("unique_id, numeric_range")
        .order("unique_id")
        .fetchall()
    )

    assert rows[0][1]["lower"] == 20
    assert rows[0][1]["upper"] == 23
    assert rows[1][1] is None
    assert rows[2][1]["lower"] == 20
    assert rows[2][1]["upper"] == 23
    con.close()


def test_enabled_splink_path_collapses_intermediate_numeric_columns():
    con = duckdb.connect()
    canonical = con.sql(
        "SELECT * FROM (VALUES "
        "('C', '20-23 HIGH STREET', 'ST1 1AA')"
        ") AS t(unique_id, address_concat, postcode)"
    )
    messy = con.sql(
        "SELECT * FROM (VALUES "
        "('M', '21 HIGH STREET', 'ST1 1AA')"
        ") AS t(unique_id, address_concat, postcode)"
    )
    stage = SplinkStage(
        final_match_weight_threshold=-50,
        final_distinguishability_threshold=None,
    )
    matcher = AddressMatcher(
        canonical_addresses=canonical,
        addresses_to_match=messy,
        con=con,
        stages=[stage],
        show_progress=False,
    )
    matcher.match()
    prediction_columns = con.table(stage.predictions_table).columns

    assert "legacy_numeric_bits" not in prediction_columns
    assert "numeric_range_l" in prediction_columns
    assert "numeric_range_r" in prediction_columns
    assert not any(column.startswith("bf_") for column in prediction_columns)
    assert not any(column.startswith("gamma_") for column in prediction_columns)
    assert not any(column.startswith("tf_") for column in prediction_columns)
    con.close()


def test_enabled_debug_retention_keeps_factors_without_range_bits():
    con = duckdb.connect()
    canonical = con.sql(
        "SELECT * FROM (VALUES "
        "('C', '20-23 HIGH STREET', 'ST1 1AA')"
        ") AS t(unique_id, address_concat, postcode)"
    )
    messy = con.sql(
        "SELECT * FROM (VALUES "
        "('M', '21 HIGH STREET', 'ST1 1AA')"
        ") AS t(unique_id, address_concat, postcode)"
    )
    stage = SplinkStage(
        retain_intermediate_calculation_columns=True,
        final_match_weight_threshold=-50,
        final_distinguishability_threshold=None,
    )
    matcher = AddressMatcher(
        canonical_addresses=canonical,
        addresses_to_match=messy,
        con=con,
        stages=[stage],
        show_progress=False,
    )
    matcher.match()
    predictions = con.table(stage.predictions_table)
    assert "legacy_numeric_bits" not in predictions.columns
    assert "bf_numeric_token_1" in predictions.columns
    assert "bf_tf_adj_numeric_token_1" in predictions.columns
    con.close()


def test_splink_always_collapses_numeric_factors():
    con = duckdb.connect()
    canonical = con.sql(
        "SELECT * FROM (VALUES "
        "('C', '20-23 HIGH STREET', 'ST1 1AA')"
        ") AS t(unique_id, address_concat, postcode)"
    )
    messy = con.sql(
        "SELECT * FROM (VALUES "
        "('M', '21 HIGH STREET', 'ST1 1AA')"
        ") AS t(unique_id, address_concat, postcode)"
    )
    stage = SplinkStage(final_match_weight_threshold=-50)
    matcher = AddressMatcher(
        canonical_addresses=canonical,
        addresses_to_match=messy,
        con=con,
        stages=[stage],
        show_progress=False,
    )
    matcher.match()
    prediction_columns = con.table(stage.predictions_table).columns

    assert "legacy_numeric_bits" not in prediction_columns
    assert not any(column.startswith("bf_") for column in prediction_columns)
    con.close()
