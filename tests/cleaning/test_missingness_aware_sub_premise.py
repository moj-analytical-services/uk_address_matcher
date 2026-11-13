import math
import re

import duckdb
import pytest

from uk_address_matcher.cleaning.chunking_strategies import prepare_data_for_matching
from uk_address_matcher.cleaning.steps import (
    _derive_missingness_aware_sub_premise_features,
    _parse_out_numbers,
    _parse_out_sub_premise,
)
from uk_address_matcher.linking_model.splink_model import (
    _get_linker,
    _get_missing_marker_recovery_settings,
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
            ('UNIT G 3 AT 55-57 TEST ROAD', 'UNIT G 3 AT 55-57 TEST ROAD'),
            ('UNIT C 25 TEST ROAD', 'UNIT C 25 TEST ROAD'),
            ('UNIT GROUND FLOOR REAR N 4 AT 2-4 TEST ROAD',
             'UNIT GROUND FLOOR REAR N 4 AT 2-4 TEST ROAD'),
            ('UNIT GROUND FLOOR N 5 2-4 TEST ROAD',
             'UNIT GROUND FLOOR N 5 2-4 TEST ROAD'),
            ('UNIT REAR OF NORTH 4 2-4 TEST ROAD',
             'UNIT REAR OF NORTH 4 2-4 TEST ROAD'),
            ('UNIT FIRST FLOOR C 25 TEST ROAD', 'UNIT FIRST FLOOR C 25 TEST ROAD'),
            ('UNIT FIRST FLOOR 2B 110 TEST ROAD', 'UNIT FIRST FLOOR 2B 110 TEST ROAD'),
            ('UNIT 2 B AT 110 TEST ROAD', 'UNIT 2 B AT 110 TEST ROAD'),
            ('UNIT GROUND FLOOR 42 TEST ROAD', 'UNIT GROUND FLOOR 42 TEST ROAD'),
            ('STUDIO 8 42A TEST ROAD', 'STUDIO 8 42A TEST ROAD'),
            ('FLAT 8 ACME STUDIOS 42 TEST ROAD', 'FLAT 8 ACME STUDIOS 42 TEST ROAD'),
            ('FLAT 8 UNIT N 4 42 TEST ROAD', 'FLAT 8 UNIT N 4 42 TEST ROAD'),
            ('UNIT A 42 TEST ROAD PHASE 3', 'UNIT A 42 TEST ROAD PHASE 3'),
            ('UNIT A 42 44 TEST ROAD', 'UNIT A 42 44 TEST ROAD'),
            ('FIRST AND SECOND FLOOR UNIT 4 42 TEST ROAD',
             'FIRST AND SECOND FLOOR UNIT 4 42 TEST ROAD'),
            ('STUDIOS 8 42 TEST ROAD', 'STUDIOS 8 42 TEST ROAD'),
            ('1 FICTIONAL APARTMENTS 33-43 TEST ROAD',
             '1 FICTIONAL APARTMENTS 33-43 TEST ROAD'),
            ('2 FICTIONAL MAISONETTES 10 TEST ROAD',
             '2 FICTIONAL MAISONETTES 10 TEST ROAD'),
            ('STUDIO FLAT D FIRST FLOOR FRONT 56 TEST ROAD',
             'STUDIO FLAT D FIRST FLOOR FRONT 56 TEST ROAD'),
            ('LOWER MAISONETTE 55 TEST ROAD', 'LOWER MAISONETTE 55 TEST ROAD'),
            ('FLAT 5 AT 54A TEST ROAD', 'FLAT 5 AT 54A TEST ROAD'),
            ('FLAT A1 AT 156 TEST ROAD', 'FLAT A1 AT 156 TEST ROAD'),
            ('FLAT A 1 AT 156 TEST ROAD', 'FLAT A 1 AT 156 TEST ROAD'),
            ('7 ACME WORKS', '7 ACME WORKS'),
            ('HIGH STREET', 'HIGH STREET')
        ) AS t(clean_full_address, original_address_concat)
        """
    )
    pipeline = DuckDBPipeline(connection, input_relation)
    pipeline.add_step(_parse_out_sub_premise())
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
        ("UNIT G 3 AT 55-57 TEST ROAD", "BUSINESS_UNIT", "G3", "UNIT"),
        ("UNIT C 25 TEST ROAD", "BUSINESS_UNIT", "C", "UNIT"),
        ("UNIT GROUND FLOOR REAR N 4 AT 2-4 TEST ROAD", "BUSINESS_UNIT", "N4", "UNIT"),
        ("UNIT GROUND FLOOR N 5 2-4 TEST ROAD", "BUSINESS_UNIT", "N5", "UNIT"),
        ("UNIT REAR OF NORTH 4 2-4 TEST ROAD", "BUSINESS_UNIT", "N4", "UNIT"),
        ("UNIT FIRST FLOOR C 25 TEST ROAD", "BUSINESS_UNIT", "C", "UNIT"),
        ("UNIT FIRST FLOOR 2B 110 TEST ROAD", "BUSINESS_UNIT", "2B", "UNIT"),
        ("UNIT 2 B AT 110 TEST ROAD", "BUSINESS_UNIT", "2B", "UNIT"),
        ("UNIT GROUND FLOOR 42 TEST ROAD", "FLAT", None, None),
        ("STUDIO 8 42A TEST ROAD", "BUSINESS_UNIT", "8", "STUDIO"),
        ("FLAT 8 ACME STUDIOS 42 TEST ROAD", "FLAT", "8", "FLAT"),
        ("FLAT 8 UNIT N 4 42 TEST ROAD", "FLAT", "8", "FLAT"),
        ("UNIT A 42 TEST ROAD PHASE 3", "BUSINESS_UNIT", "A", "UNIT"),
        ("UNIT A 42 44 TEST ROAD", "BUSINESS_UNIT", "A", "UNIT"),
        ("FIRST AND SECOND FLOOR UNIT 4 42 TEST ROAD", "BUSINESS_UNIT", "4", "UNIT"),
        ("STUDIOS 8 42 TEST ROAD", "BUSINESS_UNIT", "8", "STUDIO"),
        ("1 FICTIONAL APARTMENTS 33-43 TEST ROAD", None, "1", None),
        ("2 FICTIONAL MAISONETTES 10 TEST ROAD", None, "2", None),
        ("STUDIO FLAT D FIRST FLOOR FRONT 56 TEST ROAD", "FLAT", "D", "FLAT"),
        ("LOWER MAISONETTE 55 TEST ROAD", "FLAT", "55", "MAISONETTE"),
        ("FLAT 5 AT 54A TEST ROAD", "FLAT", "5A", "FLAT"),
        ("FLAT A1 AT 156 TEST ROAD", "FLAT", None, "FLAT"),
        ("FLAT A 1 AT 156 TEST ROAD", "FLAT", "A", "FLAT"),
        ("7 ACME WORKS", None, "7", None),
        ("HIGH STREET", None, None, None),
    ]
    assert "sub_premise_details" not in result.columns
    assert not any(column.startswith("__") for column in result.columns)
    assert result.filter("""
        clean_full_address = 'UNIT GROUND FLOOR REAR N 4 AT 2-4 TEST ROAD'
    """).project("""
        sub_premise_building_number, flat_positional
    """).fetchall() == [("2-4", "GROUND FLOOR")]
    assert result.filter("""
        clean_full_address IN (
            'UNIT G 3 AT 55-57 TEST ROAD', 'STUDIO 8 42A TEST ROAD',
            'FLAT 8 ACME STUDIOS 42 TEST ROAD', 'UNIT A 42 TEST ROAD PHASE 3'
        )
    """).project("""
        sub_premise_identifier, business_unit_type, business_unit_id,
        sub_premise_building_number
    """).fetchall() == [
        ("G3", "UNIT", "G3", "55-57"),
        ("8", "STUDIO", "8", "42A"),
        ("8", None, None, "42"),
        ("A", "UNIT", "A", None),
    ]


def test_splink_scores_marker_missingness_and_identifier_conflict(duck_con):
    canonical = duck_con.sql(
        """
        SELECT * FROM (VALUES
            ('c_flat_2', 'FLAT 2 42 FICTIONAL ROAD', 'N1 1AA'),
            ('c_flat_3', 'FLAT 3 42 FICTIONAL ROAD', 'N1 1AA'),
            ('c_studio_2', 'STUDIO 2 42 FICTIONAL ROAD', 'N1 1AA')
        ) AS t(unique_id, address_concat, postcode)
        """
    )
    messy = duck_con.sql(
        """
        SELECT * FROM (VALUES
            ('m_missing', '2 42 FICTIONAL ROAD', 'N1 1AA'),
            ('m_typo', 'FLET 2 42 FICTIONAL ROAD', 'N1 1AA'),
            ('m_conflict', 'FLAT 3 42 FICTIONAL ROAD', 'N1 1AA'),
            ('m_floor_only', 'FIRST FLOOR 42 FICTIONAL ROAD', 'N1 1AA')
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
        ("c_studio_2", "BUSINESS_UNIT", "2"),
    ]
    assert messy_clean.project(
        "unique_id, sub_premise_role, sub_premise_identifier"
    ).order("unique_id").fetchall() == [
        ("m_conflict", "FLAT", "3"),
        ("m_floor_only", "FLAT", None),
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
    assert score("m_floor_only", "c_studio_2") == pytest.approx(-1)


def test_explicit_unit_evidence_outweighs_misleading_flat_and_floor_matches(duck_con):
    canonical = duck_con.sql("""
        SELECT * FROM (VALUES
            ('c_unit_4', 'GROUND FLOOR REAR UNIT 4 19A FICTIONAL ROAD'),
            ('c_flat_4', 'FLAT 4 19-19A FICTIONAL ROAD'),
            ('c_unit_2b', 'UNIT 2B 110 FICTIONAL ROAD'),
            ('c_flat_2', 'SECOND FLAT 2 110-112 FICTIONAL ROAD'),
            ('c_unit_4_floor', 'FIRST UNIT 4 2 FICTIONAL ROAD'),
            ('c_flat_2_floor', 'FIRST FLAT 2 2 FICTIONAL ROAD'),
            ('c_unit_2_floor', 'FIRST UNIT 2 FIRST FLOOR 2 FICTIONAL ROAD'),
            ('c_unit_n4', 'GROUNED FLOOR UNIT REAR OF NORTH 4 2-4 FICTIONAL ROAD'),
            ('c_unit_n5', 'UNIT GROUND FLOOR N 5 2-4 FICTIONAL ROAD'),
            ('c_flat_8', 'FLAT 8 42 FICTIONAL ROAD'),
            ('c_studio_11', 'STUDIO 11 FICTIONAL BUILDING 11-13 FICTIONAL ROAD')
        ) AS t(unique_id, address_concat)
    """).select("*, 'N1 1AA' AS postcode")
    messy = duck_con.sql("""
        SELECT * FROM (VALUES
            ('m_unit_4', 'UNIT 4 AT 19-19A FICTIONAL ROAD'),
            ('m_unit_2b', 'UNIT 2B AT 110-112 FICTIONAL ROAD'),
            ('m_unit_4_floor', 'UNIT 4 1ST FLR AT 2 FICTIONAL ROAD'),
            ('m_unit_n4', 'UNIT GND FLR REAR N4 AT 2-4 FICTIONAL ROAD'),
            ('m_studio_8', 'STUDIO 8 42 FICTIONAL ROAD'),
            ('m_flat_g07', 'FLAT G.07 AT 11-13 FICTIONAL ROAD')
        ) AS t(unique_id, address_concat)
    """).select("*, 'N1 1AA' AS postcode")
    canonical_clean = prepare_data_for_matching(
        canonical,
        con=duck_con,
        num_of_chunks=1,
        derive_distinguishing_wrt_adjacent_records=True,
        dataset_role="canonical",
        show_progress=False,
    ).select("*, 'FICTIONAL ROAD' AS road_1_norm")
    messy_clean = prepare_data_for_matching(
        messy,
        con=duck_con,
        num_of_chunks=1,
        dataset_role="messy",
        show_progress=False,
    ).select("*, 'FICTIONAL ROAD' AS road_1_norm")
    linker = _get_linker(
        messy_clean,
        canonical_clean,
        con=duck_con,
        include_full_postcode_block=True,
        include_outside_postcode_block=False,
        retain_intermediate_calculation_columns=True,
    )
    rows = linker.inference.predict(threshold_match_weight=-100).as_pandas_dataframe()
    scores = {}
    for row in rows.itertuples():
        pair = tuple(sorted((row.unique_id_l, row.unique_id_r)))
        scores[pair] = row

    for messy_id, correct_id, incorrect_id in (
        ('m_unit_4', 'c_unit_4', 'c_flat_4'),
        ('m_unit_2b', 'c_unit_2b', 'c_flat_2'),
        ('m_unit_4_floor', 'c_unit_4_floor', 'c_flat_2_floor'),
        ('m_unit_4_floor', 'c_unit_4_floor', 'c_unit_2_floor'),
        ('m_unit_n4', 'c_unit_n4', 'c_unit_n5'),
    ):
        correct = scores[(correct_id, messy_id)]
        incorrect = scores[(incorrect_id, messy_id)]
        assert correct.match_weight > incorrect.match_weight
        assert correct.bf_sub_premise_identifier == pytest.approx(4)

    same_identifier = scores[('c_flat_8', 'm_studio_8')]
    assert math.log2(same_identifier.bf_sub_premise_identifier) == pytest.approx(1)
    assert same_identifier.bf_flat_identity == pytest.approx(1)

    conflicting_identifier = scores[('c_studio_11', 'm_flat_g07')]
    assert math.log2(
        conflicting_identifier.bf_sub_premise_identifier
    ) == pytest.approx(-10)


def test_recovery_ablation_removes_redundant_exact_identifier_level():
    settings = _get_missing_marker_recovery_settings().create_settings_dict("duckdb")
    for comparison in settings["comparisons"]:
        if comparison["output_column_name"] in (
            "flat_identity", "sub_premise_identifier"
        ):
            assert all(
                re.search(r"\bclean_full_address_[lr]\b", level["sql_condition"]) is None
                for level in comparison["comparison_levels"]
            )
    comparison = next(
        comparison
        for comparison in settings["comparisons"]
        if comparison["output_column_name"] == "sub_premise_identifier"
    )
    labels = {level["label_for_charts"] for level in comparison["comparison_levels"]}

    assert "Exact known sub-premise identifier" not in labels
    assert "Identifier agrees with a missing marker" in labels
    assert "Identifier agrees with a fuzzy marker" in labels
