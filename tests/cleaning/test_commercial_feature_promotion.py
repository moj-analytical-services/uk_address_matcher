import json
from pathlib import Path

import duckdb

from uk_address_matcher.cleaning.chunking_strategies import prepare_data_for_matching
from uk_address_matcher.cleaning.steps import (
    _derive_distinguishing_token_components,
    _derive_numeric_context_roles,
)
from uk_address_matcher.sql_pipeline.runner import DebugOptions, DuckDBPipeline


def test_derives_lexical_residuals_and_numeric_roles():
    connection = duckdb.connect()
    input_relation = connection.sql(
        """
        SELECT * FROM (VALUES
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

    lexical_levels = {
        level["label_for_charts"]: level
        for level in comparisons["commercial_distinguishing_lexical_tokens"][
            "comparison_levels"
        ]
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
