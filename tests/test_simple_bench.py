from __future__ import annotations

import duckdb
import pytest

from benchmarking.config.datasets import _clean_output
from benchmarking.insights.diagnostics import build_dataset_diagnostics
from benchmarking.insights.reporting import print_diagnostics
from benchmarking.insights.types import BenchmarkOutputOptions
from benchmarking.runner import BenchmarkRunResult, resolve_dataset_selection


def test_resolve_dataset_selection_all() -> None:
    selected = resolve_dataset_selection("all")

    assert "hackney" in selected
    assert "lambeth_council_tax" in selected
    assert "lambeth_electoral_register" in selected
    assert "lambeth_llpg" in selected


def test_resolve_dataset_selection_errors_on_unknown() -> None:
    with pytest.raises(ValueError):
        resolve_dataset_selection(["hackney", "unknown_dataset"])


def test_build_dataset_diagnostics_filters_unmatchable_incorrect_rows() -> None:
    con = duckdb.connect(database=":memory:")
    con.sql(
        """
        CREATE TABLE matches AS
        SELECT *
        FROM (
            VALUES
                (
                    's1', '100', '100', 'exact',
                    '10 HIGH ST', '10 HIGH STREET', '10 HIGH ST', 20.0
                ),
                (
                    'i1', '101', '999', 'splink',
                    '11 HIGH ST', '11 HIGH STREET', '99 OTHER ST', 5.0
                ),
                (
                    'i2', '404', '888', 'splink',
                    '12 HIGH ST', '12 HIGH STREET', '88 OTHER ST', 3.0
                ),
                ('u1', '102', NULL, NULL, '13 HIGH ST', '13 HIGH STREET', NULL, NULL)
        ) AS t(
            unique_id,
            ukam_label,
            resolved_canonical_id,
            match_reason,
            original_address_concat,
            clean_full_address,
            original_address_concat_canonical,
            match_weight
        )
        """
    )
    messy = con.sql(
        """
        SELECT *
        FROM (
            VALUES
                ('s1', '10 HIGH STREET'),
                ('i1', '11 HIGH STREET'),
                ('i2', '12 HIGH STREET'),
                ('u1', '13 HIGH STREET')
        ) AS t(unique_id, clean_full_address)
        """
    )
    canonical = con.sql(
        """
        SELECT *
        FROM (
            VALUES
                ('100', '10 HIGH STREET'),
                ('101', '11 HIGH STREET'),
                ('102', '13 HIGH STREET')
        ) AS t(unique_id, clean_full_address)
        """
    )

    diagnostics = build_dataset_diagnostics(
        con,
        matches_table_name="matches",
        messy_relation=messy,
        canonical_relation=canonical,
        splink_predictions=None,
    )

    successful_ids = {row[1] for row in diagnostics.successful_matches.fetchall()}
    successful_rows = diagnostics.successful_matches.fetchall()
    incorrect_ids = {row[1] for row in diagnostics.incorrect_matches.fetchall()}
    unmatched_ids = {row[0] for row in diagnostics.unmatched_records.fetchall()}

    assert successful_ids == {"s1"}
    assert successful_rows[0][7] == ["10 HIGH STREET"]
    assert incorrect_ids == {"i1"}
    assert "i2" not in incorrect_ids
    assert unmatched_ids == {"u1"}
    assert diagnostics.unmatched_top_splink is None
    assert diagnostics.splink_available is False


def test_build_dataset_diagnostics_adds_top_splink_for_unmatched() -> None:
    con = duckdb.connect(database=":memory:")
    con.sql(
        """
        CREATE TABLE matches AS
        SELECT *
        FROM (
            VALUES
                ('u1', '200', NULL, NULL, '1 MAIN ROAD', '1 MAIN ROAD', NULL, NULL),
                ('u2', '201', NULL, NULL, '2 MAIN ROAD', '2 MAIN ROAD', NULL, NULL)
        ) AS t(
            unique_id,
            ukam_label,
            resolved_canonical_id,
            match_reason,
            original_address_concat,
            clean_full_address,
            original_address_concat_canonical,
            match_weight
        )
        """
    )
    messy = con.sql(
        """
        SELECT *
        FROM (
            VALUES
                ('u1', '1 MAIN ROAD'),
                ('u2', '2 MAIN ROAD')
        ) AS t(unique_id, clean_full_address)
        """
    )
    canonical = con.sql(
        """
        SELECT *
        FROM (
            VALUES
                ('canon-1', '1 MAIN ROAD CANDIDATE'),
                ('canon-2', '2 MAIN ROAD CANDIDATE')
        ) AS t(unique_id, clean_full_address)
        """
    )
    splink_predictions = con.sql(
        """
        SELECT *
        FROM (
            VALUES
                ('canon-1', 'u1', 0.10, -3.0),
                ('canon-1', 'u1', 0.85, 4.0),
                ('canon-2', 'u2', 0.55, 2.0)
        ) AS t(unique_id_l, unique_id_r, match_probability, match_weight)
        """
    )

    diagnostics = build_dataset_diagnostics(
        con,
        matches_table_name="matches",
        messy_relation=messy,
        canonical_relation=canonical,
        splink_predictions=splink_predictions,
    )

    assert diagnostics.unmatched_top_splink is not None

    columns = diagnostics.unmatched_top_splink.columns
    rows = diagnostics.unmatched_top_splink.fetchall()
    by_unique_id = {row[0]: row for row in rows}

    assert columns == [
        "unique_id",
        "ukam_address_id",
        "original_address_concat",
        "cleaned_full_address",
        "highest_splink_comparison",
        "match_weight",
    ]
    assert diagnostics.splink_available is True
    assert float(by_unique_id["u1"][4]) == pytest.approx(0.85, rel=1e-6)
    assert float(by_unique_id["u1"][5]) == pytest.approx(4.0, rel=1e-6)
    assert float(by_unique_id["u2"][4]) == pytest.approx(0.55, rel=1e-6)


def test_build_dataset_diagnostics_adds_top_splink_for_unmatched_by_ukam_id() -> None:
    con = duckdb.connect(database=":memory:")
    con.sql(
        """
        CREATE TABLE matches AS
        SELECT *
        FROM (
            VALUES
                (
                    'u1', '200', NULL, NULL,
                    '1 MAIN ROAD', '1 MAIN ROAD', NULL, NULL, 'addr-1'
                ),
                (
                    'u2', '201', NULL, NULL,
                    '2 MAIN ROAD', '2 MAIN ROAD', NULL, NULL, 'addr-2'
                )
        ) AS t(
            unique_id,
            ukam_label,
            resolved_canonical_id,
            match_reason,
            original_address_concat,
            clean_full_address,
            original_address_concat_canonical,
            match_weight,
            ukam_address_id
        )
        """
    )
    messy = con.sql(
        """
        SELECT *
        FROM (
            VALUES
                ('u1', '1 MAIN ROAD'),
                ('u2', '2 MAIN ROAD')
        ) AS t(unique_id, clean_full_address)
        """
    )
    canonical = con.sql(
        """
        SELECT *
        FROM (
            VALUES
                ('200', 'unused')
        ) AS t(unique_id, clean_full_address)
        """
    )
    splink_predictions = con.sql(
        """
        SELECT *
        FROM (
            VALUES
                ('addr-1', 0.10, -3.0),
                ('addr-1', 0.90, 6.0),
                ('addr-2', 0.42, 1.5)
        ) AS t(ukam_address_id_r, match_probability, match_weight)
        """
    )

    diagnostics = build_dataset_diagnostics(
        con,
        matches_table_name="matches",
        messy_relation=messy,
        canonical_relation=canonical,
        splink_predictions=splink_predictions,
    )

    assert diagnostics.unmatched_top_splink is not None
    rows = diagnostics.unmatched_top_splink.fetchall()
    by_unique_id = {row[0]: row for row in rows}
    assert float(by_unique_id["u1"][4]) == pytest.approx(0.90, rel=1e-6)
    assert float(by_unique_id["u2"][4]) == pytest.approx(0.42, rel=1e-6)
    assert float(by_unique_id["u1"][5]) == pytest.approx(6.0, rel=1e-6)
    assert float(by_unique_id["u2"][5]) == pytest.approx(1.5, rel=1e-6)


def test_build_dataset_diagnostics_rolls_up_canonical_variants() -> None:
    con = duckdb.connect(database=":memory:")
    con.sql(
        """
        CREATE TABLE matches AS
        SELECT *
        FROM (
            VALUES
                (
                    'i1', '101', '999', 'splink',
                    'THREE SISTERS 35 QUEENSDOWN ROAD LONDON',
                    'THREE SISTERS 35 QUEENSDOWN ROAD LONDON',
                    NULL,
                    5.0
                )
        ) AS t(
            unique_id,
            ukam_label,
            resolved_canonical_id,
            match_reason,
            original_address_concat,
            clean_full_address,
            original_address_concat_canonical,
            match_weight
        )
        """
    )
    messy = con.sql(
        """
        SELECT *
        FROM (
            VALUES
                ('i1', 'THREE SISTERS 35 QUEENSDOWN ROAD LONDON')
        ) AS t(unique_id, clean_full_address)
        """
    )
    canonical = con.sql(
        """
        SELECT *
        FROM (
            VALUES
                (
                    '101',
                    'THREE SISTERS 35 QUEENSDOWN ROAD LONDON',
                    'THREE SISTERS 35 QUEENSDOWN ROAD LONDON'
                ),
                (
                    '999',
                    'THREE SISTERS 35 QUEENSDOWN ROAD LONDON',
                    'THREE SISTERS 35 QUEENSDOWN ROAD LONDON'
                ),
                (
                    '999',
                    'STAR BY HACKNEY DOWNS THREE SISTERS QUEENSDOWN ROAD HACKNEY LONDON',
                    'STAR BY HACKNEY DOWNS THREE SISTERS QUEENSDOWN ROAD HACKNEY LONDON'
                )
        ) AS t(unique_id, clean_full_address, original_address_concat)
        """
    )

    diagnostics = build_dataset_diagnostics(
        con,
        matches_table_name="matches",
        messy_relation=messy,
        canonical_relation=canonical,
        splink_predictions=None,
    )

    row = diagnostics.incorrect_matches.fetchall()[0]
    canonical_variants = row[7]
    assert isinstance(canonical_variants, list)
    assert "THREE SISTERS 35 QUEENSDOWN ROAD LONDON" in canonical_variants
    assert (
        "STAR BY HACKNEY DOWNS THREE SISTERS QUEENSDOWN ROAD HACKNEY LONDON"
        in canonical_variants
    )


def test_build_dataset_diagnostics_handles_missing_optional_match_columns() -> None:
    con = duckdb.connect(database=":memory:")
    con.sql(
        """
        CREATE TABLE matches AS
        SELECT *
        FROM (
            VALUES
                ('s1', '100', '100', 'exact', '10 HIGH ST', '10 HIGH STREET'),
                ('i1', '101', '999', 'exact', '11 HIGH ST', '11 HIGH STREET'),
                ('u1', '102', NULL, NULL, '12 HIGH ST', '12 HIGH STREET')
        ) AS t(
            unique_id,
            ukam_label,
            resolved_canonical_id,
            match_reason,
            original_address_concat,
            clean_full_address
        )
        """
    )
    messy = con.sql(
        """
        SELECT *
        FROM (
            VALUES
                ('s1', '10 HIGH STREET'),
                ('i1', '11 HIGH STREET'),
                ('u1', '12 HIGH STREET')
        ) AS t(unique_id, clean_full_address)
        """
    )
    canonical = con.sql("SELECT * FROM (VALUES ('100'), ('101')) AS t(unique_id)")

    diagnostics = build_dataset_diagnostics(
        con,
        matches_table_name="matches",
        messy_relation=messy,
        canonical_relation=canonical,
        splink_predictions=None,
    )

    incorrect_rows = diagnostics.incorrect_matches.fetchall()
    assert len(incorrect_rows) == 1
    assert incorrect_rows[0][1] == "i1"
    assert incorrect_rows[0][7] is None
    assert incorrect_rows[0][8] is None


def test_clean_output_lowercases_address_concat() -> None:
    con = duckdb.connect(database=":memory:")
    relation = con.sql(
        """
        SELECT *
        FROM (
            VALUES
                ('u1', ' 10 High STREET  ', '10', 'SW1A 2AA')
        ) AS t(unique_id, address_concat, ukam_label, postcode)
        """
    )

    cleaned = _clean_output(con, relation)
    row = cleaned.select("address_concat").fetchone()
    assert row[0] == "10 high street"


def test_benchmark_output_options_defaults_match_expected_sections() -> None:
    options = BenchmarkOutputOptions()

    assert options.show_splink_comparisons is True
    assert options.show_successful_matches is False
    assert options.show_incorrect_matches is True
    assert options.show_similarity_score_checks is True
    assert options.show_unmatched_records is False
    assert options.enable_diagnostics() is True


def test_benchmark_output_options_can_disable_all_diagnostics() -> None:
    options = BenchmarkOutputOptions(
        show_successful_matches=False,
        show_incorrect_matches=False,
        show_similarity_score_checks=False,
        show_unmatched_records=False,
    )

    assert options.enable_diagnostics() is False


def test_print_diagnostics_respects_output_toggles(
    capsys: pytest.CaptureFixture[str],
) -> None:
    con = duckdb.connect(database=":memory:")

    con.sql(
        """
        CREATE TABLE matches_for_output_toggle AS
        SELECT *
        FROM (
            VALUES
                (
                    's1', '100', '100', 'exact',
                    '10 HIGH ST', '10 HIGH STREET', '10 HIGH ST', 20.0
                ),
                (
                    'i1', '101', '999', 'splink',
                    '11 HIGH ST', '11 HIGH STREET', '99 OTHER ST', 5.0
                ),
                ('u1', '102', NULL, NULL, '12 HIGH ST', '12 HIGH STREET', NULL, NULL)
        ) AS t(
            unique_id,
            ukam_label,
            resolved_canonical_id,
            match_reason,
            original_address_concat,
            clean_full_address,
            original_address_concat_canonical,
            match_weight
        )
        """
    )

    diagnostics = build_dataset_diagnostics(
        con,
        matches_table_name="matches_for_output_toggle",
        messy_relation=con.sql(
            """
            SELECT *
            FROM (
                VALUES
                    ('s1', '10 HIGH STREET'),
                    ('i1', '11 HIGH STREET'),
                    ('u1', '12 HIGH STREET')
            ) AS t(unique_id, clean_full_address)
            """
        ),
        canonical_relation=con.sql(
            """
            SELECT *
            FROM (
                VALUES
                    ('100', '10 HIGH STREET'),
                    ('101', '11 HIGH STREET'),
                    ('102', '12 HIGH STREET')
            ) AS t(unique_id, clean_full_address)
            """
        ),
        splink_predictions=None,
    )

    run_result = BenchmarkRunResult(
        dataset_key="hackney",
        dataset_label="Hackney",
        total_rows=3,
        matched_rows=2,
        correct_matches=1,
        precision=0.5,
        recall=1 / 3,
        match_reason_breakdown=con.sql("SELECT 1 AS x"),
        timings={"data_load": 0.1, "match_pipeline": 0.2, "total_runtime": 0.3},
        con=con,
        diagnostics=diagnostics,
    )

    options = BenchmarkOutputOptions(
        show_successful_matches=False,
        show_unmatched_records=False,
    )
    print_diagnostics([run_result], output_options=options)
    output = capsys.readouterr().out

    assert "---- SUCCESSFUL MATCHES ----" not in output
    assert "Diagnostics: unmatched records with highest Splink comparison" not in output
    assert "---- INCORRECT MATCHES ----" in output
    assert "Diagnostics: similarity score checks" in output
    assert "Diagnostics: suspicious incorrect-match summary" in output
