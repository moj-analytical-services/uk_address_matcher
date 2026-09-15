import math

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
