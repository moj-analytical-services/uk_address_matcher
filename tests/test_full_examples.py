import os
import subprocess

import duckdb

from uk_address_matcher import AddressMatcher, ExactMatchStage, SplinkStage


def test_example_matching():
    env = os.environ.copy()

    # Set flag to limit the number of records for testing
    env["TEST_LIMIT"] = "1"
    timeout_seconds = int(env.get("EXAMPLE_SCRIPT_TIMEOUT", "30"))

    result = subprocess.run(
        ["uv", "run", "python", "examples/example_matching.py"],
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
    )

    assert result.returncode == 0, (
        f"Script failed!\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
    )


def test_example_analysis_outputs_render_on_dummy_data():
    con = duckdb.connect(database=":memory:")
    canonical_rel = con.sql(
        """
        SELECT *
        FROM (
            VALUES
                ('c1', '10 Downing Street, London', 'SW1A 2AA'),
                ('c2', '22 Baker Street, London', 'NW1 6XE')
        ) AS t(unique_id, address_concat, postcode)
        """
    )
    messy_rel = con.sql(
        """
        SELECT *
        FROM (
            VALUES
                ('m1', '10 Downing Street, London', 'SW1A 2AA', 'c1'),
                ('m2', '22 Bakar Street, London', 'NW1 6XE', 'c2')
        ) AS t(unique_id, address_concat, postcode, ukam_label)
        """
    )

    matcher = AddressMatcher(
        canonical_addresses=canonical_rel,
        addresses_to_match=messy_rel,
        con=con,
        stages=[
            ExactMatchStage(),
            SplinkStage(
                predict_threshold_match_weight=-20,
                final_match_weight_threshold=10,
                include_full_postcode_block=True,
            ),
        ],
    )

    match_result = matcher.match()

    stage_diagnostics = match_result._stage_diagnostics
    diagnostics_table = match_result._stage_diagnostics_table()
    diagnostics_df = diagnostics_table.df()

    # Ensure tabular rendering path executes in tests.
    match_result._accuracy_table().show(max_width=50000)
    splink_matches = match_result._compare_splink_model_results(
        baseline_match_weight=10,
        splink_comparison_weights=[8],
        precision_at_metrics=[1, 3, 5],
    )

    headline_df = splink_matches.headline_table.df()
    delta_df = splink_matches.delta_table.df()

    assert stage_diagnostics is not None
    assert len(stage_diagnostics) > 0
    assert not diagnostics_df.empty
    assert "stage" in diagnostics_df.columns
    assert "elapsed_seconds" in diagnostics_df.columns
    assert splink_matches.total_input_rows == 2
    assert "scenario" in headline_df.columns
    assert "match_outcome" in headline_df.columns
    assert "threshold" not in headline_df.columns
    assert "precision_at_k" in headline_df.columns
    assert "average_true_rank" in headline_df.columns
    assert "scenario" in delta_df.columns
    assert "precision_at_k_delta" in delta_df.columns
