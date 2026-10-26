from __future__ import annotations

from pathlib import Path

import pytest

import benchmarking.runner as benchmark_runner
from benchmarking.config.sources import resolve_data_path
from uk_address_matcher import ExactMatchStage, prepare_canonical_folder


def test_resolve_data_path_accepts_s3_uri(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("UKAM_TEST_DATA_PATH", "s3://example-bucket/path/to/data")

    assert resolve_data_path("UKAM_TEST_DATA_PATH") == "s3://example-bucket/path/to/data/"


def test_resolve_data_path_accepts_existing_local_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    local_data_dir = tmp_path / "messy_data"
    local_data_dir.mkdir()
    monkeypatch.setenv("UKAM_TEST_DATA_PATH", str(local_data_dir))

    assert resolve_data_path("UKAM_TEST_DATA_PATH") == f"{local_data_dir.as_posix()}/"


def test_resolve_data_path_raises_for_missing_local_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    missing_dir = tmp_path / "does_not_exist"
    monkeypatch.setenv("UKAM_TEST_DATA_PATH", str(missing_dir))

    with pytest.raises(RuntimeError, match="does not exist"):
        resolve_data_path("UKAM_TEST_DATA_PATH")


def test_resolve_data_path_raises_for_local_file_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    local_file = tmp_path / "data.csv"
    local_file.write_text("x", encoding="utf-8")
    monkeypatch.setenv("UKAM_TEST_DATA_PATH", str(local_file))

    with pytest.raises(RuntimeError, match="does not exist"):
        resolve_data_path("UKAM_TEST_DATA_PATH")


@pytest.mark.parametrize("matching_num_chunks", [1, 3])
def test_benchmark_chunked_reports_survive_return(
    duck_con, tmp_path, monkeypatch, matching_num_chunks
):
    canonical = duck_con.sql("""
        SELECT * FROM (VALUES
            ('C1', '1 FICTIONAL HIGH STREET', 'SW1A 1AA'),
            ('C2', '2 FICTIONAL HIGH STREET', 'SW1A 1AA')
        ) AS addresses(unique_id, address_concat, postcode)
    """)
    folder = tmp_path / "prepared"
    prepare_canonical_folder(
        canonical, output_folder=folder, con=duck_con, show_progress="off"
    )
    messy = canonical.select("*, unique_id AS ukam_label")
    monkeypatch.setattr(benchmark_runner, "load_dataset", lambda *args, **kwargs: messy)

    result = benchmark_runner.run_single_dataset(
        con=duck_con,
        dataset_key="pooled_councils",
        canonical_path=str(folder),
        stages=[ExactMatchStage()],
        matching_num_chunks=matching_num_chunks,
        persist_results=True,
        results_root=str(tmp_path / "results"),
        enable_comparison_charts=False,
    )

    assert result.total_rows == 2
    assert result.correct_matches == 2
    assert result.accuracy_table.fetchall()
    assert result.stage_diagnostics_table.fetchall()
    assert Path(result.persisted_run.manifest_path).exists()
