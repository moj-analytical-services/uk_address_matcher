import ast
import inspect
import json
import re
from dataclasses import replace
from pathlib import Path

import duckdb
import pytest

from uk_address_matcher.cleaning.chunking_strategies import prepare_data_for_matching
from uk_address_matcher.cleaning.steps import (
    _derive_distinguishing_token_components,
    _derive_numeric_context_roles,
)
from uk_address_matcher.sql_pipeline.runner import DebugOptions, DuckDBPipeline

_EXPECTED_NUMERIC_MARKERS = {
    "PARKING_SPACE": ("asset|PARKING_SPACE", "asset", "PARKING_SPACE"),
    "CONTAINER": ("asset|CONTAINER", "asset", "CONTAINER"),
    "PLATFORM": ("asset|PLATFORM", "asset", "PLATFORM"),
    "MAST": ("asset|MAST", "asset", "MAST"),
    "PLOT": ("asset|PLOT", "asset", "PLOT"),
    "GARAGE": ("asset|GARAGE", "asset", "GARAGE"),
    "YARD": ("asset|YARD", "asset", "YARD"),
    "SHOP": ("asset|SHOP", "asset", "SHOP"),
    "BAY": ("asset|BAY", "asset", "BAY"),
    "FLOOR": ("floor|FLOOR", "floor", "FLOOR"),
    "UNIT": ("unit|UNIT", "unit", "UNIT"),
}


def _marker_cases_from_stage():
    source = inspect.getsource(inspect.unwrap(_derive_numeric_context_roles))
    function = ast.parse(source).body[0]
    for node in ast.walk(function):
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "marker_cases"
            for target in node.targets
        ):
            return ast.literal_eval(node.value)
    raise AssertionError("Could not find marker_cases in _derive_numeric_context_roles")


def _marker_alternatives():
    return [
        pytest.param(
            marker,
            alternative,
            id=f"{marker.lower()}-{alternative.lower().replace(' ', '-')}",
        )
        for marker, pattern in _marker_cases_from_stage()
        for alternative in pattern.split("|")
    ]


def _run_numeric_role_stage(connection, relation, stage, table_name):
    pipeline = DuckDBPipeline(connection, relation)
    pipeline.add_step(stage)
    result = pipeline.run(DebugOptions(pretty_print_sql=False))
    result.create(table_name)
    return connection.table(table_name)


def test_numeric_role_gate_vocabulary_tracks_marker_cases():
    marker_cases = _marker_cases_from_stage()
    marker_tokens = sorted(
        {
            token
            for _marker, pattern in marker_cases
            for alternative in pattern.split("|")
            for token in alternative.split()
        }
    )
    marker_tokens_sql = ", ".join(
        "'" + token.replace("'", "''") + "'" for token in marker_tokens
    )
    stage_sql = _derive_numeric_context_roles().steps[0].sql

    assert f"[{marker_tokens_sql}]::VARCHAR[]" in stage_sql


@pytest.mark.parametrize(("marker", "alternative"), _marker_alternatives())
def test_numeric_role_gate_covers_every_marker_alternative(marker, alternative):
    assert marker in _EXPECTED_NUMERIC_MARKERS, (
        f"Add explicit expected output for new marker {marker!r}"
    )
    role_prefix, broad_role, specific_marker = _EXPECTED_NUMERIC_MARKERS[marker]
    tokens = alternative.split() + ["20"]
    connection = duckdb.connect()
    try:
        connection.execute(
            "CREATE TABLE numeric_marker_input ("
            "stable_input_row_id BIGINT, "
            "clean_full_address_tokens VARCHAR[], "
            "numeric_tokens VARCHAR[])"
        )
        connection.execute(
            "INSERT INTO numeric_marker_input VALUES (1, ?, ['20']::VARCHAR[])",
            [tokens],
        )
        input_relation = connection.table("numeric_marker_input")
        result = DuckDBPipeline(connection, input_relation)
        result.add_step(_derive_numeric_context_roles())
        rows = (
            result.run(DebugOptions(pretty_print_sql=False))
            .project("numeric_role_keys, numeric_broad_roles, numeric_specific_markers")
            .fetchall()
        )
    finally:
        connection.close()

    assert rows == [([f"{role_prefix}|20"], [broad_role], [specific_marker])]


def test_numeric_role_gate_preserves_null_empty_and_nonmatching_outputs():
    connection = duckdb.connect()
    try:
        input_relation = connection.sql(
            """
            SELECT * FROM (VALUES
                (1, NULL::VARCHAR[], ['7']::VARCHAR[]),
                (2, []::VARCHAR[], ['7']::VARCHAR[]),
                (3, ['UNIT', 'LONG', 'ADDRESS', '7']::VARCHAR[], ['7']::VARCHAR[]),
                (4, ['UNIT']::VARCHAR[], ['8']::VARCHAR[]),
                (5, ['20', 'UNIT']::VARCHAR[], ['20']::VARCHAR[]),
                (6, ['UNIT', '7']::VARCHAR[], NULL::VARCHAR[]),
                (7, ['UNIT', '7']::VARCHAR[], []::VARCHAR[]),
                (8, NULL::VARCHAR[], NULL::VARCHAR[]),
                (9, []::VARCHAR[], []::VARCHAR[]),
                (10, [NULL]::VARCHAR[], ['7']::VARCHAR[]),
                (11, ['UNIT', NULL, '7']::VARCHAR[], [NULL, '7']::VARCHAR[]),
                (12, ['UNITSX', '7']::VARCHAR[], ['7']::VARCHAR[]),
                (13, ['PARKING', 'LOT', '20']::VARCHAR[], ['20']::VARCHAR[]),
                (14, ['CAR', 'PARK', 'MALL', '20']::VARCHAR[], ['20']::VARCHAR[]),
                (15, ['SPACE', '20']::VARCHAR[], ['20']::VARCHAR[]),
                (16, ['PARK', 'SPACE', '20']::VARCHAR[], ['20']::VARCHAR[]),
                (17, ['12A', 'FICTIONAL']::VARCHAR[], ['12A']::VARCHAR[])
            ) AS source(
                stable_input_row_id,
                clean_full_address_tokens,
                numeric_tokens
            )
            """
        )
        candidate_stage = _derive_numeric_context_roles()
        candidate_sql = candidate_stage.steps[0].sql
        baseline_sql, replacements = re.subn(
            r"list_has_any\(\s*clean_full_address_tokens,\s*\[.*?\]::VARCHAR\[\]\s*\)",
            "TRUE",
            candidate_sql,
            count=1,
            flags=re.DOTALL,
        )
        assert replacements == 1
        baseline_stage = replace(
            candidate_stage,
            steps=(replace(candidate_stage.steps[0], sql=baseline_sql),),
        )
        _run_numeric_role_stage(
            connection,
            input_relation,
            candidate_stage,
            "numeric_role_candidate",
        )
        _run_numeric_role_stage(
            connection,
            input_relation,
            baseline_stage,
            "numeric_role_baseline",
        )

        output_columns = (
            "stable_input_row_id, numeric_role_keys, "
            "numeric_broad_roles, numeric_specific_markers"
        )
        expected = [
            (1, ["location|ADDRESS_NUMBER|7"], ["location"], ["ADDRESS_NUMBER"]),
            (2, ["location|ADDRESS_NUMBER|7"], ["location"], ["ADDRESS_NUMBER"]),
            (3, ["location|ADDRESS_NUMBER|7"], ["location"], ["ADDRESS_NUMBER"]),
            (4, ["location|ADDRESS_NUMBER|8"], ["location"], ["ADDRESS_NUMBER"]),
            (5, ["location|ADDRESS_NUMBER|20"], ["location"], ["ADDRESS_NUMBER"]),
            (6, None, None, None),
            (7, [], [], []),
            (8, None, None, None),
            (9, [], [], []),
            (10, ["location|ADDRESS_NUMBER|7"], ["location"], ["ADDRESS_NUMBER"]),
            (
                11,
                [None, "location|ADDRESS_NUMBER|7"],
                ["location", "location"],
                ["ADDRESS_NUMBER", "ADDRESS_NUMBER"],
            ),
            (12, ["location|ADDRESS_NUMBER|7"], ["location"], ["ADDRESS_NUMBER"]),
            (13, ["location|ADDRESS_NUMBER|20"], ["location"], ["ADDRESS_NUMBER"]),
            (14, ["location|ADDRESS_NUMBER|20"], ["location"], ["ADDRESS_NUMBER"]),
            (15, ["location|ADDRESS_NUMBER|20"], ["location"], ["ADDRESS_NUMBER"]),
            (16, ["location|ADDRESS_NUMBER|20"], ["location"], ["ADDRESS_NUMBER"]),
            (17, ["location|ADDRESS_NUMBER|12A"], ["location"], ["ADDRESS_NUMBER"]),
        ]
        actual = connection.execute(
            f"SELECT {output_columns} FROM numeric_role_candidate "
            "ORDER BY stable_input_row_id"
        ).fetchall()
        assert actual == expected

        candidate_only = connection.execute(
            f"SELECT count(*) FROM (SELECT {output_columns} "
            "FROM numeric_role_candidate EXCEPT ALL "
            f"SELECT {output_columns} FROM numeric_role_baseline)"
        ).fetchone()[0]
        baseline_only = connection.execute(
            f"SELECT count(*) FROM (SELECT {output_columns} "
            "FROM numeric_role_baseline EXCEPT ALL "
            f"SELECT {output_columns} FROM numeric_role_candidate)"
        ).fetchone()[0]
    finally:
        connection.close()

    assert candidate_only == 0
    assert baseline_only == 0


def test_derives_lexical_residuals_and_numeric_roles():
    connection = duckdb.connect()
    input_relation = connection.sql(
        """
        SELECT *, regexp_split_to_array(clean_full_address, '\\s+')::VARCHAR[]
            AS clean_full_address_tokens
        FROM (VALUES
            (
                'ACME UNIT 7',
                ['ACME', 'UNIT', '7']::VARCHAR[],
                ['7']::VARCHAR[]
            ),
            (
                'CAR PARK SPACE 20',
                ['CAR', 'PARK', 'SPACE', '20']::VARCHAR[],
                ['20']::VARCHAR[]
            ),
            (
                '42 FICTIONAL ROAD',
                ['42', 'FICTIONAL', 'ROAD']::VARCHAR[],
                ['42']::VARCHAR[]
            )
        ) AS t(
            clean_full_address,
            distinguishing_adj_start_tokens,
            numeric_tokens
        )
        """
    )
    pipeline = DuckDBPipeline(connection, input_relation)
    pipeline.add_step(_derive_distinguishing_token_components())
    pipeline.add_step(_derive_numeric_context_roles())
    result = pipeline.run(DebugOptions(pretty_print_sql=False))

    assert result.project(
        "distinguishing_lexical_tokens, numeric_role_keys, "
        "numeric_broad_roles, numeric_specific_markers"
    ).fetchall() == [
        (["ACME"], ["unit|UNIT|7"], ["unit"], ["UNIT"]),
        ([], ["asset|PARKING_SPACE|20"], ["asset"], ["PARKING_SPACE"]),
        (
            ["42", "FICTIONAL", "ROAD"],
            ["location|ADDRESS_NUMBER|42"],
            ["location"],
            ["ADDRESS_NUMBER"],
        ),
    ]


def test_numeric_roles_handle_repeated_tokens_and_ranges():
    connection = duckdb.connect()
    input_relation = connection.sql(
        """
        SELECT *, regexp_split_to_array(clean_full_address, '\\s+')::VARCHAR[]
            AS clean_full_address_tokens
        FROM (VALUES
            (
                'UNIT 7 UNIT 7',
                ['7', '7']::VARCHAR[]
            ),
            (
                'LEVEL 12-14 GARAGE 12-14',
                ['12-14', '12-14']::VARCHAR[]
            ),
            (
                'CAR PARK SPACE 20 BAY 20',
                ['20', '20']::VARCHAR[]
            )
        ) AS t(clean_full_address, numeric_tokens)
        """
    )
    pipeline = DuckDBPipeline(connection, input_relation)
    pipeline.add_step(_derive_numeric_context_roles())
    result = pipeline.run(DebugOptions(pretty_print_sql=False))

    assert result.project(
        "numeric_role_keys, numeric_broad_roles, numeric_specific_markers"
    ).fetchall() == [
        (
            ["unit|UNIT|7", "unit|UNIT|7"],
            ["unit", "unit"],
            ["UNIT", "UNIT"],
        ),
        (
            ["asset|GARAGE|12-14", "asset|GARAGE|12-14"],
            ["asset", "asset"],
            ["GARAGE", "GARAGE"],
        ),
        (
            ["asset|PARKING_SPACE|20", "asset|PARKING_SPACE|20"],
            ["asset", "asset"],
            ["PARKING_SPACE", "PARKING_SPACE"],
        ),
    ]


def test_packaged_settings_include_promoted_commercial_features():
    settings = json.loads(
        (
            Path(__file__).parents[2]
            / "uk_address_matcher"
            / "data"
            / "splink_model.json"
        ).read_text(encoding="utf-8")
    )
    comparisons = {
        comparison["output_column_name"]: comparison
        for comparison in settings["comparisons"]
    }

    sub_premise_labels = {
        level["label_for_charts"]
        for level in comparisons["sub_premise_identifier"]["comparison_levels"]
    }
    assert "Exact known sub-premise identifier" not in sub_premise_labels
    assert "Identifier agrees with a fuzzy marker" in sub_premise_labels
    assert "commercial_distinguishing_structural_tokens" not in comparisons

    lexical_levels = {
        level["label_for_charts"]: level
        for level in comparisons["distinguishing_lexical_tokens"]["comparison_levels"]
    }
    assert (
        lexical_levels["No lexical distinguishing tokens present (-2)"]["m_probability"]
        == 1.0
    )
    assert (
        lexical_levels["No lexical distinguishing tokens present (-2)"]["u_probability"]
        == 4.0
    )

    numeric_cap_levels = {
        level["label_for_charts"]: level
        for level in comparisons["numeric_token_1"]["comparison_levels"]
    }
    assert (
        numeric_cap_levels["Numeric 1 agrees but role marker conflicts (cap 6)"][
            "m_probability"
        ]
        == 64.0
    )

    contradiction_levels = {
        level["label_for_charts"]: level
        for level in comparisons["commercial_same_role_numeric_contradiction"][
            "comparison_levels"
        ]
    }
    assert (
        contradiction_levels["Confident same-role numeric contradiction (-6)"][
            "m_probability"
        ]
        == 0.015625
    )

    numeric_context_levels = {
        level["label_for_charts"]: level
        for level in comparisons["address_structure_numeric_context"]["comparison_levels"]
    }
    assert (
        numeric_context_levels["Exact numeric structure with strong context (+2)"][
            "m_probability"
        ]
        == 4.0
    )
    assert (
        numeric_context_levels["Numeric overlap with strong context (+1)"][
            "m_probability"
        ]
        == 2.0
    )
    assert (
        numeric_context_levels["Numeric conflict with strong context (-4)"][
            "m_probability"
        ]
        == 0.0625
    )

    numeric_context_conditions = [
        level["sql_condition"]
        for level in comparisons["address_structure_numeric_context"]["comparison_levels"]
        if "regexp_replace(regexp_replace(clean_full_address_l" in level["sql_condition"]
    ]
    assert len(numeric_context_conditions) == 3
    assert all(
        "numeric_tokens_r IS NOT NULL" in condition
        for condition in numeric_context_conditions
    )
    assert all(
        "lower(clean_full_address" not in condition
        for condition in numeric_context_conditions
    )
    assert all(
        "clean_full_address_numeric_context" not in condition
        for condition in numeric_context_conditions
    )
    assert all(
        "clean_full_address_r" in condition for condition in numeric_context_conditions
    )


def test_canonical_preparation_carries_distinguishing_lexical_tokens():
    connection = duckdb.connect()
    addresses = connection.sql(
        """
        SELECT * FROM (VALUES
            ('c_7', 'COMMERCIAL PARK UNIT 7 ACME ROAD', 'N1 1AA'),
            ('c_8', 'COMMERCIAL PARK UNIT 8 ACME ROAD', 'N1 1AA')
        ) AS t(unique_id, address_concat, postcode)
        """
    )

    prepared = prepare_data_for_matching(
        addresses,
        con=connection,
        num_of_chunks=1,
        derive_distinguishing_wrt_adjacent_records=True,
        dataset_role="canonical",
        show_progress=False,
    )

    assert "distinguishing_lexical_tokens" in prepared.columns
    assert prepared.project("unique_id, distinguishing_lexical_tokens").order(
        "unique_id"
    ).fetchall() == [
        ("c_7", ["COMMERCIAL"]),
        ("c_8", ["COMMERCIAL"]),
    ]
