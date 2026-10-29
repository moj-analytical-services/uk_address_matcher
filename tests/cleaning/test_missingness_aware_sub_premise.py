import json
import math
from pathlib import Path

import duckdb
import pytest

from uk_address_matcher.cleaning.chunking_strategies import prepare_data_for_matching
from uk_address_matcher.cleaning.steps import (
    _derive_missingness_aware_sub_premise_features,
    _parse_out_business_unit,
    _parse_out_flat_position_and_letter,
    _parse_out_numbers,
)
from uk_address_matcher.linking_model.splink_model import (
    _get_directional_premise_role_settings,
    _get_linker,
    _get_missing_marker_recovery_settings,
    _get_model_settings_dict,
)
from uk_address_matcher.sql_pipeline.runner import DebugOptions, DuckDBPipeline


def test_derives_known_unknown_and_absent_sub_premise_evidence():
    connection = duckdb.connect()
    input_relation = connection.sql(
        """
        SELECT * FROM (VALUES
            ('FLAT 7 42 FICTIONAL ROAD', 'FLAT 7 42 FICTIONAL ROAD'),
            ('7 42 FICTIONAL ROAD', '7 42 FICTIONAL ROAD'),
            ('FLT 7 42 FICTIONAL ROAD', 'FLT 7 42 FICTIONAL ROAD'),
            ('UNIT 7 ACME WORKS', 'UNIT 7 ACME WORKS'),
            ('7 ACME WORKS', '7 ACME WORKS'),
            ('HIGH STREET', 'HIGH STREET')
        ) AS t(clean_full_address, original_address_concat)
        """
    )
    pipeline = DuckDBPipeline(connection, input_relation)
    pipeline.add_step(_parse_out_flat_position_and_letter())
    pipeline.add_step(_parse_out_business_unit())
    pipeline.add_step(_parse_out_numbers())
    pipeline.add_step(_derive_missingness_aware_sub_premise_features())
    result = pipeline.run(DebugOptions(pretty_print_sql=False))

    assert "sub_premise_marker_state" not in result.columns
    actual = result.project(
        """
        clean_full_address,
        sub_premise_role,
        sub_premise_identifier,
        sub_premise_marker_token
        """
    ).fetchall()

    assert actual == [
        ("FLAT 7 42 FICTIONAL ROAD", "FLAT", "7", "FLAT"),
        ("7 42 FICTIONAL ROAD", None, "7", None),
        ("FLT 7 42 FICTIONAL ROAD", None, "7", "FLT"),
        ("UNIT 7 ACME WORKS", "BUSINESS_UNIT", "7", "UNIT"),
        ("7 ACME WORKS", None, "7", None),
        ("HIGH STREET", None, None, None),
    ]


def test_splink_scores_marker_missingness_and_identifier_conflict(duck_con):
    canonical = duck_con.sql(
        """
        SELECT * FROM (VALUES
            ('c_flat_2', 'FLAT 2 42 FICTIONAL ROAD', 'N1 1AA'),
            ('c_flat_3', 'FLAT 3 42 FICTIONAL ROAD', 'N1 1AA')
        ) AS t(unique_id, address_concat, postcode)
        """
    )
    messy = duck_con.sql(
        """
        SELECT * FROM (VALUES
            ('m_missing', '2 42 FICTIONAL ROAD', 'N1 1AA'),
            ('m_typo', 'FLET 2 42 FICTIONAL ROAD', 'N1 1AA'),
            ('m_conflict', 'FLAT 3 42 FICTIONAL ROAD', 'N1 1AA')
        ) AS t(unique_id, address_concat, postcode)
        """
    )

    canonical_clean = prepare_data_for_matching(
        canonical,
        con=duck_con,
        num_of_chunks=1,
        derive_distinguishing_wrt_adjacent_records=True,
        dataset_role="canonical",
        show_progress=False,
    )
    messy_clean = prepare_data_for_matching(
        messy,
        con=duck_con,
        num_of_chunks=1,
        dataset_role="messy",
        show_progress=False,
    )
    assert canonical_clean.project(
        "unique_id, sub_premise_role, sub_premise_identifier"
    ).order("unique_id").fetchall() == [
        ("c_flat_2", "FLAT", "2"),
        ("c_flat_3", "FLAT", "3"),
    ]
    assert messy_clean.project(
        "unique_id, sub_premise_role, sub_premise_identifier"
    ).order("unique_id").fetchall() == [
        ("m_conflict", "FLAT", "3"),
        ("m_missing", None, "2"),
        ("m_typo", None, "2"),
    ]
    linker = _get_linker(
        messy_clean,
        canonical_clean,
        con=duck_con,
        include_full_postcode_block=True,
        include_outside_postcode_block=False,
        retain_intermediate_calculation_columns=True,
    )
    predictions = linker.inference.predict(threshold_match_weight=-100)
    rows = predictions.as_pandas_dataframe()

    def score(messy_id, canonical_id):
        matching_rows = rows[
            ((rows["unique_id_l"] == messy_id) & (rows["unique_id_r"] == canonical_id))
            | ((rows["unique_id_l"] == canonical_id) & (rows["unique_id_r"] == messy_id))
        ]
        assert len(matching_rows) == 1
        return math.log2(float(matching_rows.iloc[0]["bf_sub_premise_identifier"]))

    assert score("m_missing", "c_flat_2") == pytest.approx(2)
    assert score("m_typo", "c_flat_2") == pytest.approx(3)
    assert score("m_conflict", "c_flat_2") == pytest.approx(-8)


def test_cleaning_derives_premise_and_floor_key_sets(duck_con):
    addresses = duck_con.sql(
        """
        SELECT * FROM (VALUES
            ('m_roles', 'SUITE F ROOM 1', 'N1 1AA'),
            ('m_plural', 'UNITS A AND B', 'N1 1AA'),
            ('m_repeated', 'UNIT A UNIT A', 'N1 1AA'),
            ('m_partial', 'UNIT A AND PART UNIT B AND UNIT D AND E', 'N1 1AA'),
            ('m_range', 'UNIT 1 TO 3', 'N1 1AA'),
            ('m_hyphen_range', 'UNITS 1-3', 'N1 1AA'),
            ('m_range_list', 'UNITS 1 AND 3 TO 5 AND UNIT 7', 'N1 1AA'),
            ('m_exclusion', 'UNIT A EXCLUDING UNIT B', 'N1 1AA'),
            ('m_floors', 'GROUND AND FIRST FLOORS', 'N1 1AA'),
            ('m_floor_range', 'FIRST TO THIRD FLOORS', 'N1 1AA'),
            ('m_ordinal_floor', '1ST AND 2ND FLOORS', 'N1 1AA'),
            ('m_scalar', 'UNIT 7 ACME WORKS', 'N1 1AA'),
            ('m_order', 'OFFICE B UNIT A ROOM 2', 'N1 1AA'),
            ('m_wide', 'UNIT 12345 SUITE 2', 'N1 1AA'),
            ('m_excluded_first', 'EXCLUDING UNIT B UNIT A', 'N1 1AA')
        ) AS t(unique_id, address_concat, postcode)
        """
    )
    cleaned = prepare_data_for_matching(
        addresses,
        con=duck_con,
        num_of_chunks=1,
        dataset_role="messy",
        show_progress=False,
    )
    rows = {
        row[0]: row[1:]
        for row in cleaned.project(
            """
            unique_id,
            premise_keys,
            premise_scope_uncertain,
            floor_keys,
            floor_scope_uncertain,
            sub_premise_role,
            sub_premise_identifier,
            business_unit_type,
            business_unit_id
            """
        ).fetchall()
    }

    assert rows["m_roles"][0] == {"SUITE": ["F"], "ROOM": ["1"]}
    assert rows["m_plural"][0] == {"UNIT": ["A", "B"]}
    assert rows["m_repeated"][0] == {"UNIT": ["A"]}
    assert rows["m_partial"][0] == {"UNIT": ["A", "B", "D", "E"]}
    assert rows["m_partial"][1] is True
    assert rows["m_range"][0] == {"UNIT": ["1", "2", "3"]}
    assert rows["m_range"][1] is True
    assert rows["m_range"][6:] == ("UNIT", "1")
    assert rows["m_hyphen_range"][0] == {"UNIT": ["1", "2", "3"]}
    assert rows["m_range_list"][0] == {"UNIT": ["1", "3", "4", "5", "7"]}
    assert rows["m_range_list"][1] is True
    assert rows["m_exclusion"][0] == {"UNIT": ["A", "B"]}
    assert rows["m_exclusion"][1] is True
    assert rows["m_floors"][2] == ["FIRST", "GROUND"]
    assert rows["m_floors"][3] is False
    assert rows["m_floor_range"][3] is True
    assert rows["m_ordinal_floor"][2] == ["FIRST", "SECOND"]
    assert rows["m_ordinal_floor"][3] is False
    assert rows["m_scalar"][0] == {"UNIT": ["7"]}
    assert rows["m_scalar"][4:6] == ("BUSINESS_UNIT", "7")
    assert rows["m_order"][0] == {"OFFICE": ["B"], "UNIT": ["A"], "ROOM": ["2"]}
    assert rows["m_order"][6:] == ("OFFICE", "B")
    assert rows["m_wide"][0] == {"UNIT": ["12345"], "SUITE": ["2"]}
    assert rows["m_wide"][6:] == ("SUITE", "2")
    assert rows["m_excluded_first"][0] == {"UNIT": ["A", "B"]}
    assert rows["m_excluded_first"][1] is True
    assert rows["m_excluded_first"][6:] == ("UNIT", "A")


def test_packaged_directional_levels_remain_opt_in():
    baseline = _get_model_settings_dict()
    candidate = _get_model_settings_dict(include_directional_premise_roles=True)
    model_path = (
        Path(__file__).parents[2] / "uk_address_matcher" / "data" / "splink_model.json"
    )
    assert candidate == json.loads(model_path.read_text(encoding="utf-8"))
    comparison = next(
        comparison
        for comparison in candidate["comparisons"]
        if comparison["output_column_name"] == "sub_premise_identifier"
    )
    assert all(
        0 <= level["m_probability"] <= 1 and 0 <= level["u_probability"] <= 1
        for level in comparison["comparison_levels"][:4]
    )
    comparison["comparison_levels"] = comparison["comparison_levels"][4:]
    assert candidate == baseline
    assert "comparison_variants" not in baseline
    assert "sql_templates" not in baseline


def test_recovery_ablation_removes_redundant_exact_identifier_level():
    settings = _get_missing_marker_recovery_settings().create_settings_dict("duckdb")
    comparison = next(
        comparison
        for comparison in settings["comparisons"]
        if comparison["output_column_name"] == "sub_premise_identifier"
    )
    labels = {level["label_for_charts"] for level in comparison["comparison_levels"]}

    assert "Exact known sub-premise identifier" not in labels
    assert "Identifier agrees with a missing marker" in labels
    assert "Identifier agrees with a fuzzy marker" in labels


def test_directional_premise_role_model_uses_ordered_set_weights(duck_con):
    canonical = duck_con.sql(
        """
        SELECT * FROM (VALUES
            ('c_room', 'SUITE F ROOM 1 42 FICTIONAL ROAD', 'N1 1AA'),
            ('c_roles', 'UNIT 1 SUITE F 43 FICTIONAL ROAD', 'N1 1AA'),
            ('c_over', 'UNIT A AND UNIT B 44 FICTIONAL ROAD', 'N1 1AA'),
            ('c_under', 'UNIT A 45 FICTIONAL ROAD', 'N1 1AA'),
            ('c_missing', 'UNIT A SUITE F 46 FICTIONAL ROAD', 'N1 1AA'),
            ('c_uncertain', 'UNIT 1 TO 3 SUITE F 47 FICTIONAL ROAD', 'N1 1AA'),
            ('c_exclusion',
             'UNIT A AND UNIT B EXCLUDING UNIT C SUITE F 49 FICTIONAL ROAD', 'N1 1AA'),
            ('c_repeated', 'UNIT A UNIT A 48 FICTIONAL ROAD', 'N1 1AA')
        ) AS t(unique_id, address_concat, postcode)
        """
    )
    messy = duck_con.sql(
        """
        SELECT * FROM (VALUES
            ('m_room', 'SUITE F ROOM 2 42 FICTIONAL ROAD', 'N1 1AA'),
            ('m_roles', 'SUITE F UNIT 1 43 FICTIONAL ROAD', 'N1 1AA'),
            ('m_over', 'UNIT A 44 FICTIONAL ROAD', 'N1 1AA'),
            ('m_under', 'UNIT A AND UNIT B 45 FICTIONAL ROAD', 'N1 1AA'),
            ('m_missing', 'UNIT A 46 FICTIONAL ROAD', 'N1 1AA'),
            ('m_uncertain', 'UNIT 1 SUITE F 47 FICTIONAL ROAD', 'N1 1AA'),
            ('m_exclusion',
             'UNIT A AND UNIT B AND UNIT C SUITE F 49 FICTIONAL ROAD', 'N1 1AA'),
            ('m_repeated', 'UNIT A 48 FICTIONAL ROAD', 'N1 1AA')
        ) AS t(unique_id, address_concat, postcode)
        """
    )
    canonical_clean = prepare_data_for_matching(
        canonical,
        con=duck_con,
        num_of_chunks=1,
        dataset_role="canonical",
        derive_distinguishing_wrt_adjacent_records=True,
        show_progress=False,
    )
    messy_clean = prepare_data_for_matching(
        messy,
        con=duck_con,
        num_of_chunks=1,
        dataset_role="messy",
        show_progress=False,
    )
    directional_settings = _get_directional_premise_role_settings()
    linker_options = dict(
        con=duck_con,
        include_full_postcode_block=True,
        include_outside_postcode_block=False,
        retain_intermediate_calculation_columns=True,
    )
    old_canonical = canonical_clean.select(
        "* EXCLUDE (premise_keys, premise_scope_uncertain)"
    )
    with pytest.raises(ValueError, match="Rebuild the prepared canonical"):
        _get_linker(
            messy_clean, old_canonical, settings=directional_settings, **linker_options
        )
    scores = []
    for settings in (None, directional_settings):
        linker = _get_linker(
            messy_clean, canonical_clean, settings=settings, **linker_options
        )
        predictions = linker.inference.predict(
            threshold_match_weight=-100
        ).as_pandas_dataframe()
        assert predictions["unique_id_l"].str.startswith("c_").all()
        assert not predictions.duplicated(["unique_id_l", "unique_id_r"]).any()
        scores.append(
            {
                (row.unique_id_r, row.unique_id_l): math.log2(
                    row.bf_sub_premise_identifier
                )
                for row in predictions.itertuples()
            }
        )
    baseline, candidate = scores
    assert candidate["m_room", "c_room"] == pytest.approx(-8)
    assert candidate["m_roles", "c_roles"] == pytest.approx(8)
    assert candidate["m_over", "c_over"] == pytest.approx(-4)
    assert candidate["m_under", "c_under"] == pytest.approx(0)
    assert candidate["m_missing", "c_missing"] == pytest.approx(0)
    assert candidate["m_uncertain", "c_uncertain"] == pytest.approx(0)
    assert candidate["m_exclusion", "c_exclusion"] == pytest.approx(0)
    assert candidate["m_repeated", "c_repeated"] == pytest.approx(
        baseline["m_repeated", "c_repeated"]
    )
