from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from benchmarking.insights.run_persistence import (
    compare_persisted_runs,
    generate_benchmark_run_id,
    persist_benchmark_run,
    write_overlay_precision_recall_chart_html,
)
from benchmarking.runner import (
    BenchmarkRunResult,
    run_selected_datasets,
    run_single_dataset,
)
from uk_address_matcher.post_linkage.match_result.result import MatchResult


@dataclass(frozen=True)
class DummyStage:
    final_match_weight_threshold: float = 0.5


def _accuracy_table_relation(con: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
    return con.sql(
        """
        SELECT *
        FROM (
            VALUES
                ('overall', 10, 8, 7, 1, 3, 0.875, 0.7)
        ) AS t(
            match_reason,
            total_rows,
            matched_rows,
            correct_matches,
            wrong_matches,
            unmatched_rows,
            precision,
            recall
        )
        """
    )


def _stage_diagnostics_relation(
    con: duckdb.DuckDBPyConnection,
) -> duckdb.DuckDBPyRelation:
    return con.sql(
        """
        SELECT *
        FROM (
            VALUES
                ('exact_match', 0.123, 10, 8)
        ) AS t(stage, elapsed_seconds, input_rows, output_rows)
        """
    )


def test_generate_benchmark_run_id_hashes_timestamp_seed() -> None:
    timestamp = datetime(2026, 4, 9, 18, 34, 9, 950154, tzinfo=timezone.utc)
    timestamp_seed = "20260409T183409950154Z"

    assert (
        generate_benchmark_run_id(at=timestamp)
        == hashlib.sha256(timestamp_seed.encode("utf-8")).hexdigest()[:16]
    )
    assert generate_benchmark_run_id(at=timestamp) != timestamp_seed


def test_overlay_html_uses_matching_vega_lite_runtime_and_export_actions(
    tmp_path: Path,
) -> None:
    values = [
        {
            "recall": 0.8,
            "precision": 0.95,
            "truth_threshold": 8.0,
            "match_probability": 0.996,
            "fp": 5,
        }
    ]
    output_path = tmp_path / "overlay.html"

    write_overlay_precision_recall_chart_html(
        path=output_path,
        baseline_chart={"data": {"values": values}},
        comparison_charts={"data": {"values": values}},
    )

    html = output_path.read_text(encoding="utf-8")
    assert "https://cdn.jsdelivr.net/npm/vega@5" in html
    assert "https://cdn.jsdelivr.net/npm/vega-lite@5" in html
    assert "https://cdn.jsdelivr.net/npm/vega-embed@6" in html
    assert '"$schema": "https://vega.github.io/schema/vega-lite/v5.json"' in html
    assert "{ actions: true, renderer: 'svg' }" in html


def test_persist_benchmark_run_records_run_id_and_legacy_hash(tmp_path: Path) -> None:
    con = duckdb.connect(database=":memory:")
    run_id = "c7337f8f317d3734"

    persisted = persist_benchmark_run(
        run_id=run_id,
        dataset_key="hackney",
        dataset_label="Hackney",
        stages=[DummyStage()],
        accuracy_table=_accuracy_table_relation(con),
        stage_diagnostics_table=_stage_diagnostics_relation(con),
        timings={"total_runtime": 1.234},
        total_rows=10,
        matched_rows=8,
        correct_matches=7,
        precision=0.875,
        recall=0.7,
        results_root=str(tmp_path),
        enable_chart_exports=False,
    )

    manifest = json.loads(Path(persisted.manifest_path).read_text(encoding="utf-8"))
    history = json.loads((tmp_path / "run_history.json").read_text(encoding="utf-8"))

    assert persisted.run_id == run_id
    assert persisted.run_hash == manifest["run_hash"]
    assert manifest["run_id"] == run_id
    assert history["runs_by_hash"][persisted.run_hash]["run_id"] == run_id
    assert persisted.run_hash != run_id
    assert run_id in persisted.manifest_path
    assert persisted.deduplicated is False


def test_persist_benchmark_run_deduplicates_by_legacy_hash(tmp_path: Path) -> None:
    con = duckdb.connect(database=":memory:")

    first = persist_benchmark_run(
        run_id="c7337f8f317d3734",
        dataset_key="hackney",
        dataset_label="Hackney",
        stages=[DummyStage()],
        accuracy_table=_accuracy_table_relation(con),
        stage_diagnostics_table=_stage_diagnostics_relation(con),
        timings={"total_runtime": 1.234},
        total_rows=10,
        matched_rows=8,
        correct_matches=7,
        precision=0.875,
        recall=0.7,
        results_root=str(tmp_path),
        enable_chart_exports=False,
    )
    second = persist_benchmark_run(
        run_id="3f763df1ae74435a",
        dataset_key="hackney",
        dataset_label="Hackney",
        stages=[DummyStage()],
        accuracy_table=_accuracy_table_relation(con),
        stage_diagnostics_table=_stage_diagnostics_relation(con),
        timings={"total_runtime": 9.999},
        total_rows=10,
        matched_rows=8,
        correct_matches=7,
        precision=0.875,
        recall=0.7,
        results_root=str(tmp_path),
        enable_chart_exports=False,
    )

    history = json.loads((tmp_path / "run_history.json").read_text(encoding="utf-8"))

    assert second.deduplicated is True
    assert second.run_hash == first.run_hash
    assert second.manifest_path == first.manifest_path
    assert second.run_id == first.run_id
    assert list(history["runs_by_hash"]) == [first.run_hash]


def test_persist_benchmark_run_compares_against_baseline_run_id(tmp_path: Path) -> None:
    con = duckdb.connect(database=":memory:")

    baseline = persist_benchmark_run(
        run_id="c7337f8f317d3734",
        dataset_key="hackney",
        dataset_label="Hackney",
        stages=[DummyStage()],
        accuracy_table=con.sql(
            """
            SELECT *
            FROM (
                VALUES
                    ('overall', 10, 8, 7, 1, 3, 0.875, 0.7)
            ) AS t(
                match_reason,
                total_rows,
                matched_rows,
                correct_matches,
                wrong_matches,
                unmatched_rows,
                precision,
                recall
            )
            """
        ),
        stage_diagnostics_table=_stage_diagnostics_relation(con),
        timings={"total_runtime": 1.234},
        total_rows=10,
        matched_rows=8,
        correct_matches=7,
        precision=0.875,
        recall=0.7,
        results_root=str(tmp_path),
        enable_chart_exports=False,
    )

    comparison = persist_benchmark_run(
        run_id="3f763df1ae74435a",
        dataset_key="hackney",
        dataset_label="Hackney",
        stages=[DummyStage()],
        accuracy_table=con.sql(
            """
            SELECT *
            FROM (
                VALUES
                    ('overall', 10, 9, 8, 1, 2, 0.8889, 0.8)
            ) AS t(
                match_reason,
                total_rows,
                matched_rows,
                correct_matches,
                wrong_matches,
                unmatched_rows,
                precision,
                recall
            )
            """
        ),
        stage_diagnostics_table=_stage_diagnostics_relation(con),
        timings={"total_runtime": 1.111},
        total_rows=10,
        matched_rows=9,
        correct_matches=8,
        precision=0.8889,
        recall=0.8,
        comparison_baseline_run_id=baseline.run_id,
        results_root=str(tmp_path),
        enable_chart_exports=False,
    )

    assert comparison.comparison is not None
    assert comparison.comparison.baseline_run_id == baseline.run_id
    assert comparison.comparison.current_run_id == comparison.run_id
    assert baseline.run_id in comparison.comparison.summary_path
    assert comparison.run_id in comparison.comparison.summary_path


def test_compare_persisted_runs_accepts_run_ids(tmp_path: Path) -> None:
    con = duckdb.connect(database=":memory:")

    baseline = persist_benchmark_run(
        run_id="c7337f8f317d3734",
        dataset_key="hackney",
        dataset_label="Hackney",
        stages=[DummyStage()],
        accuracy_table=con.sql(
            """
            SELECT *
            FROM (
                VALUES
                    ('overall', 10, 8, 7, 1, 3, 0.875, 0.7)
            ) AS t(
                match_reason,
                total_rows,
                matched_rows,
                correct_matches,
                wrong_matches,
                unmatched_rows,
                precision,
                recall
            )
            """
        ),
        stage_diagnostics_table=_stage_diagnostics_relation(con),
        timings={"total_runtime": 1.234},
        total_rows=10,
        matched_rows=8,
        correct_matches=7,
        precision=0.875,
        recall=0.7,
        results_root=str(tmp_path),
        enable_chart_exports=False,
    )
    current = persist_benchmark_run(
        run_id="3f763df1ae74435a",
        dataset_key="hackney",
        dataset_label="Hackney",
        stages=[DummyStage()],
        accuracy_table=con.sql(
            """
            SELECT *
            FROM (
                VALUES
                    ('overall', 10, 9, 8, 1, 2, 0.8889, 0.8)
            ) AS t(
                match_reason,
                total_rows,
                matched_rows,
                correct_matches,
                wrong_matches,
                unmatched_rows,
                precision,
                recall
            )
            """
        ),
        stage_diagnostics_table=_stage_diagnostics_relation(con),
        timings={"total_runtime": 1.111},
        total_rows=10,
        matched_rows=9,
        correct_matches=8,
        precision=0.8889,
        recall=0.8,
        results_root=str(tmp_path),
        enable_chart_exports=False,
    )

    comparison_run = compare_persisted_runs(
        results_root=str(tmp_path),
        dataset_key="hackney",
        baseline_run_id=baseline.run_id,
        comparison_run_id=current.run_id,
        export_charts=False,
    )

    assert comparison_run.comparison is not None
    assert comparison_run.comparison.baseline_run_id == baseline.run_id
    assert comparison_run.comparison.current_run_id == current.run_id


def test_run_selected_datasets_passes_shared_run_id(monkeypatch) -> None:
    captured_run_ids: list[str | None] = []
    shared_run_id = "c7337f8f317d3734"

    def fake_run_single_dataset(**kwargs):
        captured_run_ids.append(kwargs["run_id"])
        return BenchmarkRunResult(
            dataset_key=kwargs["dataset_key"],
            dataset_label=kwargs["dataset_key"].title(),
            total_rows=1,
            matched_rows=1,
            correct_matches=1,
            precision=1.0,
            recall=1.0,
            match_reason_breakdown=object(),
            timings={"total_runtime": 0.01},
            con=object(),
        )

    monkeypatch.setattr(
        "benchmarking.runner.resolve_dataset_selection",
        lambda selection: ["hackney", "lambeth_llpg"],
    )
    monkeypatch.setattr("benchmarking.runner.setup_connection", lambda: object())
    monkeypatch.setattr("benchmarking.runner.run_single_dataset", fake_run_single_dataset)

    results = run_selected_datasets(
        selected_datasets=["hackney", "lambeth_llpg"],
        canonical_path="canonical.parquet",
        stages=[DummyStage()],
        run_id=shared_run_id,
    )

    assert [result.dataset_key for result in results] == ["hackney", "lambeth_llpg"]
    assert captured_run_ids == [shared_run_id, shared_run_id]


def test_run_selected_datasets_generates_shared_run_id_once(monkeypatch) -> None:
    captured_run_ids: list[str | None] = []
    generated_run_ids: list[str] = []
    shared_run_id = "3f763df1ae74435a"

    def fake_generate_benchmark_run_id() -> str:
        generated_run_ids.append(shared_run_id)
        return shared_run_id

    def fake_run_single_dataset(**kwargs):
        captured_run_ids.append(kwargs["run_id"])
        return BenchmarkRunResult(
            dataset_key=kwargs["dataset_key"],
            dataset_label=kwargs["dataset_key"].title(),
            total_rows=1,
            matched_rows=1,
            correct_matches=1,
            precision=1.0,
            recall=1.0,
            match_reason_breakdown=object(),
            timings={"total_runtime": 0.01},
            con=object(),
        )

    monkeypatch.setattr(
        "benchmarking.runner.resolve_dataset_selection",
        lambda selection: ["hackney", "lambeth_llpg"],
    )
    monkeypatch.setattr("benchmarking.runner.setup_connection", lambda: object())
    monkeypatch.setattr(
        "benchmarking.runner.generate_benchmark_run_id",
        fake_generate_benchmark_run_id,
    )
    monkeypatch.setattr("benchmarking.runner.run_single_dataset", fake_run_single_dataset)

    results = run_selected_datasets(
        selected_datasets=["hackney", "lambeth_llpg"],
        canonical_path="canonical.parquet",
        stages=[DummyStage()],
    )

    assert [result.dataset_key for result in results] == ["hackney", "lambeth_llpg"]
    assert generated_run_ids == [shared_run_id]
    assert captured_run_ids == [shared_run_id, shared_run_id]


def test_run_selected_datasets_prints_persistence_summary_when_enabled(
    monkeypatch,
) -> None:
    captured_results: list[list[BenchmarkRunResult]] = []

    def fake_run_single_dataset(**kwargs):
        return BenchmarkRunResult(
            dataset_key=kwargs["dataset_key"],
            dataset_label=kwargs["dataset_key"].title(),
            total_rows=1,
            matched_rows=1,
            correct_matches=1,
            precision=1.0,
            recall=1.0,
            match_reason_breakdown=object(),
            timings={"total_runtime": 0.01},
            con=object(),
            persisted_run=object(),
        )

    def fake_print_run_persistence_summary(results: list[BenchmarkRunResult]) -> None:
        captured_results.append(results)

    monkeypatch.setattr(
        "benchmarking.runner.resolve_dataset_selection",
        lambda selection: ["hackney", "lambeth_llpg"],
    )
    monkeypatch.setattr("benchmarking.runner.setup_connection", lambda: object())
    monkeypatch.setattr("benchmarking.runner.run_single_dataset", fake_run_single_dataset)
    monkeypatch.setattr(
        "benchmarking.runner.print_run_persistence_summary",
        fake_print_run_persistence_summary,
    )

    results = run_selected_datasets(
        selected_datasets=["hackney", "lambeth_llpg"],
        canonical_path="canonical.parquet",
        stages=[DummyStage()],
        persist_results=True,
    )

    assert [result.dataset_key for result in results] == ["hackney", "lambeth_llpg"]
    assert captured_results == [results]


def test_run_single_dataset_supports_deterministic_only_match_results(
    monkeypatch,
) -> None:
    con = duckdb.connect(database=":memory:")
    messy = con.sql(
        """
        SELECT *
        FROM (
            VALUES
                ('src-1', '1 MAIN ROAD', 1),
                ('src-2', '2 MAIN ROAD', 2)
        ) AS t(unique_id, clean_full_address, ukam_address_id)
        """
    )
    canonical = con.sql(
        """
        SELECT *
        FROM (
            VALUES
                ('canon-1', '1 MAIN ROAD', 101)
        ) AS t(unique_id, clean_full_address, ukam_address_id)
        """
    )

    class FakeAddressMatcher:
        def __init__(self, *args, **kwargs):
            self.con = kwargs["con"]

        def match(self) -> MatchResult:
            relation = self.con.sql(
                """
                SELECT *
                FROM (
                    VALUES
                        (1, 'src-1', 'canon-1', 'canon-1', 101, 'exact: full match'),
                        (2, 'src-2', NULL, NULL, NULL, NULL)
                ) AS t(
                    ukam_address_id,
                    unique_id,
                    ukam_label,
                    resolved_canonical_id,
                    canonical_ukam_address_id,
                    match_reason
                )
                """
            )
            return MatchResult(
                _relation=relation,
                con=self.con,
                _canonical_relation=canonical,
                _stage_diagnostics=[
                    {
                        "stage": "exact_match",
                        "unmatched_before": 2,
                        "matched_this_stage": 1,
                        "remaining_after": 1,
                        "matched_pct_of_unmatched": 0.5,
                        "matched_pct_of_input": 0.5,
                        "elapsed_seconds": 0.01,
                    }
                ],
            )

    monkeypatch.setattr(
        "benchmarking.runner.get_dataset_definition",
        lambda dataset_key: {"label": dataset_key.title()},
    )
    monkeypatch.setattr(
        "benchmarking.runner.load_dataset",
        lambda con, dataset_key, sample_mode: messy,
    )
    monkeypatch.setattr("benchmarking.runner.AddressMatcher", FakeAddressMatcher)
    monkeypatch.setattr(
        "benchmarking.runner.fetch_overall_summary",
        lambda con, accuracy_rel, total_input_rows: (2, 1, 1, 1.0, 1.0),
    )

    result = run_single_dataset(
        con=con,
        dataset_key="rhondda",
        canonical_path="ignored",
        stages=[DummyStage()],
        persist_results=False,
    )

    assert result.precision_recall_curve_records is not None
    thresholds = {
        float(row["truth_threshold"]) for row in result.precision_recall_curve_records
    }
    assert -999.0 in thresholds
    assert 999.0 in thresholds
    assert result.splink_available is False
