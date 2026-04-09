from __future__ import annotations

import hashlib
import json
import subprocess
from dataclasses import fields, is_dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, Any

from benchmarking.comparisons.comparison_artifacts import (
    build_accuracy_comparison_rows,
    build_comparison_summary,
    build_stage_diagnostics_comparison_rows,
)
from benchmarking.constants import BENCHMARK_RESULTS_ROOT
from benchmarking.insights.types import (
    BenchmarkComparisonSummary,
    PersistedBenchmarkRun,
)
from uk_address_matcher.analysis import (
    build_precision_recall_chart_definition,
    overlay_precision_recall_charts,
    write_overlay_precision_recall_chart_html,
)

if TYPE_CHECKING:
    import duckdb


_RESULTS_ROOT = Path(BENCHMARK_RESULTS_ROOT)
_REPO_ROOT = _RESULTS_ROOT.parent.parent
_HISTORY_FILE = "run_history.json"
_LEGACY_RESULTS_PREFIXES = (
    Path("benchmarking/results"),
    Path("results"),
)
_PRECISION_RECALL_CURVE_FILE = "precision_recall_curve.json"


def _safe_path_segment(value: str) -> str:
    lowered = value.strip().lower().replace(" ", "_")
    safe = "".join(ch if (ch.isalnum() or ch in {"-", "_"}) else "_" for ch in lowered)
    return safe or "unknown_dataset"


def _now_utc_iso() -> str:
    return datetime.now(UTC).isoformat()


def _normalise_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, Path):
        return value.as_posix()
    if isinstance(value, Enum):
        return _normalise_value(value.value)
    if isinstance(value, Decimal):
        return round(float(value), 10)
    if isinstance(value, float):
        if value != value:
            return "NaN"
        if value == float("inf"):
            return "Infinity"
        if value == float("-inf"):
            return "-Infinity"
        return round(value, 10)
    if isinstance(value, list):
        return [_normalise_value(v) for v in value]
    if isinstance(value, tuple):
        return [_normalise_value(v) for v in value]
    if isinstance(value, (set, frozenset)):
        normalised = [_normalise_value(v) for v in value]
        return sorted(normalised, key=lambda item: json.dumps(item, sort_keys=True))
    if isinstance(value, dict):
        return {str(k): _normalise_value(v) for k, v in sorted(value.items())}
    as_dict = getattr(value, "as_dict", None)
    if callable(as_dict):
        try:
            return _normalise_value(as_dict())
        except TypeError:
            pass
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        try:
            return _normalise_value(to_dict())
        except TypeError:
            pass

    # Fallback for complex runtime-only objects (e.g. Splink linker instances).
    return {"__type__": f"{value.__class__.__module__}.{value.__class__.__name__}"}


def _relation_to_records(
    relation: duckdb.DuckDBPyRelation,
    *,
    exclude_columns: set[str] | None = None,
) -> list[dict[str, Any]]:
    excluded = exclude_columns or set()
    columns = [
        (index, column)
        for index, column in enumerate(relation.columns)
        if column not in excluded
    ]
    rows = relation.fetchall()
    records = [
        {column: _normalise_value(row[index]) for index, column in columns}
        for row in rows
    ]
    return sorted(records, key=lambda item: json.dumps(item, sort_keys=True))


def _normalise_hash_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, Path):
        return value.as_posix()
    if isinstance(value, Enum):
        return _normalise_hash_value(value.value)
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        return str(value)
    if isinstance(value, float):
        if value != value:
            return "NaN"
        if value == float("inf"):
            return "Infinity"
        if value == float("-inf"):
            return "-Infinity"
        return repr(value)
    if isinstance(value, list):
        return [_normalise_hash_value(v) for v in value]
    if isinstance(value, tuple):
        return [_normalise_hash_value(v) for v in value]
    if isinstance(value, (set, frozenset)):
        normalised = [_normalise_hash_value(v) for v in value]
        return sorted(normalised, key=lambda item: json.dumps(item, sort_keys=True))
    if isinstance(value, dict):
        return {str(k): _normalise_hash_value(v) for k, v in sorted(value.items())}
    as_dict = getattr(value, "as_dict", None)
    if callable(as_dict):
        try:
            return _normalise_hash_value(as_dict())
        except TypeError:
            pass
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        try:
            return _normalise_hash_value(to_dict())
        except TypeError:
            pass

    return {"__type__": f"{value.__class__.__module__}.{value.__class__.__name__}"}


def _relation_to_hash_records(
    relation: duckdb.DuckDBPyRelation,
    *,
    exclude_columns: set[str] | None = None,
) -> list[dict[str, Any]]:
    excluded = exclude_columns or set()
    columns = [
        (index, column)
        for index, column in enumerate(relation.columns)
        if column not in excluded
    ]
    rows = relation.fetchall()
    records = [
        {column: _normalise_hash_value(row[index]) for index, column in columns}
        for row in rows
    ]
    return sorted(records, key=lambda item: json.dumps(item, sort_keys=True))


def _serialisable_stage(stage: Any) -> dict[str, Any]:
    params: dict[str, Any] = {}
    if is_dataclass(stage):
        for field in fields(stage):
            if not field.init:
                continue
            value = getattr(stage, field.name)
            if callable(value):
                continue
            params[field.name] = _normalise_value(value)
    elif hasattr(stage, "__dict__"):
        for key, value in sorted(stage.__dict__.items()):
            if key.startswith("_"):
                continue
            if callable(value):
                continue
            params[key] = _normalise_value(value)

    return {
        "module": stage.__class__.__module__,
        "name": stage.__class__.__name__,
        "params": params,
    }


def _stage_fingerprint(stages: list[Any]) -> tuple[str, list[dict[str, Any]]]:
    serialised = [_serialisable_stage(stage) for stage in stages]
    payload = json.dumps(serialised, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
    return digest, serialised


def _run_hash(
    *,
    dataset_key: str,
    stage_fingerprint: str,
    accuracy_rows: list[dict[str, Any]],
    stage_rows: list[dict[str, Any]],
    precision_recall_curve_rows: list[dict[str, Any]] | None = None,
) -> str:
    payload = {
        "dataset_key": dataset_key,
        "stage_fingerprint": stage_fingerprint,
        "accuracy_table": accuracy_rows,
        "stage_diagnostics": stage_rows,
    }
    if precision_recall_curve_rows is not None:
        payload["precision_recall_curve"] = precision_recall_curve_rows
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


def _atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("w", encoding="utf-8", delete=False, dir=path.parent) as tmp:
        json.dump(payload, tmp, indent=2, sort_keys=True)
        temp_path = Path(tmp.name)
    temp_path.replace(path)


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _load_history(path: Path) -> dict[str, Any]:
    history = _read_json(path)
    if not history:
        return {
            "version": 1,
            "runs_by_hash": {},
            "latest_by_group": {},
        }
    history.setdefault("runs_by_hash", {})
    history.setdefault("latest_by_group", {})
    return history


def _git_commit_hash() -> str | None:
    try:
        output = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError:
        return None
    if output.returncode != 0:
        return None
    commit = output.stdout.strip()
    return commit or None


def _to_relative(path: Path) -> str:
    try:
        return path.relative_to(_REPO_ROOT).as_posix()
    except ValueError:
        pass
    return path.as_posix()


def _history_hint(results_root: Path) -> str:
    return _to_relative(results_root / _HISTORY_FILE)


def _strip_results_prefix(path: Path) -> Path:
    for prefix in _LEGACY_RESULTS_PREFIXES:
        prefix_parts = prefix.parts
        if path.parts[: len(prefix_parts)] == prefix_parts:
            return Path(*path.parts[len(prefix_parts) :])
    return path


def _build_group_key(*, dataset_key: str, stage_fingerprint: str) -> str:
    return f"{dataset_key}:{stage_fingerprint}"


def _records_to_hash_records(
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    records = [
        {str(key): _normalise_hash_value(value) for key, value in sorted(row.items())}
        for row in rows
    ]
    return sorted(records, key=lambda item: json.dumps(item, sort_keys=True))


def _parse_created_at(value: str | None) -> datetime:
    if not value:
        return datetime.min.replace(tzinfo=UTC)

    normalised = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalised)
    except ValueError:
        return datetime.min.replace(tzinfo=UTC)

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _sorted_dataset_runs(
    *,
    runs_by_hash: dict[str, Any],
    dataset_key: str,
    exclude_hashes: set[str] | None = None,
) -> list[dict[str, Any]]:
    excluded = exclude_hashes or set()
    dataset_runs = [
        run_info
        for run_hash, run_info in runs_by_hash.items()
        if run_hash not in excluded and run_info.get("dataset_key") == dataset_key
    ]
    return sorted(
        dataset_runs,
        key=lambda run_info: (
            _parse_created_at(run_info.get("created_at_utc")),
            str(run_info.get("run_hash", "")),
        ),
    )


def resolve_latest_dataset_hashes(
    *,
    results_root: str = BENCHMARK_RESULTS_ROOT,
    dataset_key: str,
) -> tuple[str, str]:
    root = Path(results_root)
    history = _load_history(root / _HISTORY_FILE)
    runs_by_hash: dict[str, Any] = history.get("runs_by_hash", {})
    dataset_runs = _sorted_dataset_runs(
        runs_by_hash=runs_by_hash,
        dataset_key=dataset_key,
    )

    if len(dataset_runs) < 2:
        available = sorted(
            {str(run_info.get("dataset_key", "")) for run_info in runs_by_hash.values()}
        )
        raise ValueError(
            "Need at least two persisted runs for dataset "
            f"'{dataset_key}'. Available datasets in history: {', '.join(available)}"
        )

    baseline_info = dataset_runs[-2]
    comparison_info = dataset_runs[-1]
    return str(baseline_info["run_hash"]), str(comparison_info["run_hash"])


def _manifest_artifact_path(
    run_info: dict[str, Any],
    *,
    results_root: Path,
    artifact_key: str,
) -> Path | None:
    direct_path = run_info.get(artifact_key)
    if direct_path:
        return _resolve_artifact_path(str(direct_path), results_root=results_root)

    manifest_path_value = run_info.get("manifest_path")
    if not manifest_path_value:
        return None

    manifest_path = _resolve_artifact_path(
        str(manifest_path_value),
        results_root=results_root,
    )
    manifest = _read_json(manifest_path)
    artifact_path = manifest.get("artifacts", {}).get(artifact_key)
    if not artifact_path:
        return None

    return _resolve_artifact_path(str(artifact_path), results_root=results_root)


def _load_run_rows(
    run_info: dict[str, Any],
    *,
    results_root: Path,
    artifact_key: str,
) -> list[dict[str, Any]]:
    artifact_path = _manifest_artifact_path(
        run_info,
        results_root=results_root,
        artifact_key=artifact_key,
    )
    if artifact_path is None:
        return []

    payload = _read_json(artifact_path)
    rows = payload.get("rows", [])
    if not isinstance(rows, list):
        return []
    return rows


def _write_precision_recall_curve_artifact(
    *,
    run_dir: Path,
    curve_rows: list[dict[str, Any]],
) -> Path:
    charts_dir = run_dir / "charts"
    charts_dir.mkdir(parents=True, exist_ok=True)
    curve_path = charts_dir / _PRECISION_RECALL_CURVE_FILE
    _atomic_write_json(curve_path, {"rows": curve_rows})
    return curve_path


def _backfill_precision_recall_curve_artifact(
    *,
    run_info: dict[str, Any],
    curve_rows: list[dict[str, Any]] | None,
    results_root: Path,
) -> dict[str, Any]:
    if not curve_rows:
        return run_info

    existing_curve_path = _manifest_artifact_path(
        run_info,
        results_root=results_root,
        artifact_key="precision_recall_curve_path",
    )
    if existing_curve_path is not None:
        return run_info

    run_dir = _resolve_artifact_path(str(run_info["run_dir"]), results_root=results_root)
    manifest_path = _resolve_artifact_path(
        str(run_info["manifest_path"]),
        results_root=results_root,
    )
    manifest = _read_json(manifest_path)
    curve_path = _write_precision_recall_curve_artifact(
        run_dir=run_dir,
        curve_rows=curve_rows,
    )
    manifest.setdefault("artifacts", {})["precision_recall_curve_path"] = _to_relative(
        curve_path
    )
    _atomic_write_json(manifest_path, manifest)

    updated_run_info = dict(run_info)
    updated_run_info["precision_recall_curve_path"] = _to_relative(curve_path)
    return updated_run_info


def _normalise_comparison_baseline_hash(
    comparison_baseline_hash: str | None,
) -> str | None:
    if comparison_baseline_hash is None:
        return None

    normalised = comparison_baseline_hash.strip()
    if not normalised:
        return None
    if normalised.lower() == "latest":
        return "latest"
    return normalised


def _resolve_requested_baseline_info(
    *,
    runs_by_hash: dict[str, Any],
    dataset_key: str,
    comparison_baseline_hash: str | None,
    current_run_hash: str,
) -> tuple[dict[str, Any] | None, str | None]:
    normalised_baseline_hash = _normalise_comparison_baseline_hash(
        comparison_baseline_hash
    )

    if normalised_baseline_hash == current_run_hash:
        raise ValueError(
            "comparison_baseline_hash must differ from the current run hash."
        )

    if normalised_baseline_hash == "latest":
        dataset_runs = _sorted_dataset_runs(
            runs_by_hash=runs_by_hash,
            dataset_key=dataset_key,
            exclude_hashes={current_run_hash},
        )
        if dataset_runs:
            return dataset_runs[-1], None
        return (
            None,
            "Comparison skipped: no earlier persisted run exists for the requested "
            f"'latest' baseline on dataset '{dataset_key}'.",
        )

    if normalised_baseline_hash is None:
        return None, None

    baseline_info = runs_by_hash.get(normalised_baseline_hash)
    if baseline_info is None:
        raise ValueError(
            f"Unknown comparison_baseline_hash '{normalised_baseline_hash}'."
        )
    return baseline_info, None


def persist_benchmark_run(
    *,
    dataset_key: str,
    dataset_label: str,
    stages: list[Any],
    accuracy_table: duckdb.DuckDBPyRelation,
    stage_diagnostics_table: duckdb.DuckDBPyRelation,
    timings: dict[str, float],
    total_rows: int,
    matched_rows: int,
    correct_matches: int,
    precision: float | None,
    recall: float | None,
    precision_recall_curve_rows: list[dict[str, Any]] | None = None,
    comparison_baseline_hash: str | None = None,
    results_root: str = BENCHMARK_RESULTS_ROOT,
    enable_chart_exports: bool = True,
) -> PersistedBenchmarkRun:
    root = Path(results_root)
    history_path = root / _HISTORY_FILE

    created_at = _now_utc_iso()
    stage_fingerprint, stage_definition = _stage_fingerprint(stages)
    group_key = _build_group_key(
        dataset_key=dataset_key,
        stage_fingerprint=stage_fingerprint,
    )

    accuracy_rows = _relation_to_records(accuracy_table)
    accuracy_hash_rows = _relation_to_hash_records(accuracy_table)
    stage_rows = _relation_to_records(stage_diagnostics_table)
    stage_hash_rows = _relation_to_hash_records(
        stage_diagnostics_table,
        exclude_columns={"elapsed_seconds"},
    )
    precision_recall_curve_hash_rows = (
        _records_to_hash_records(precision_recall_curve_rows)
        if precision_recall_curve_rows is not None
        else None
    )
    run_hash = _run_hash(
        dataset_key=dataset_key,
        stage_fingerprint=stage_fingerprint,
        accuracy_rows=accuracy_hash_rows,
        stage_rows=stage_hash_rows,
        precision_recall_curve_rows=precision_recall_curve_hash_rows,
    )
    comparison_baseline_hash = _normalise_comparison_baseline_hash(
        comparison_baseline_hash
    )

    history = _load_history(history_path)
    runs_by_hash: dict[str, Any] = history["runs_by_hash"]
    latest_by_group: dict[str, str] = history["latest_by_group"]

    existing = runs_by_hash.get(run_hash)
    if existing is not None:
        updated_existing = _backfill_precision_recall_curve_artifact(
            run_info=existing,
            curve_rows=precision_recall_curve_rows,
            results_root=root,
        )
        comparison = None
        baseline_info, comparison_warning = _resolve_requested_baseline_info(
            runs_by_hash=runs_by_hash,
            dataset_key=dataset_key,
            comparison_baseline_hash=comparison_baseline_hash,
            current_run_hash=run_hash,
        )
        if baseline_info is not None:
            comparison = _build_persisted_run_comparison(
                results_root=root,
                baseline_info=baseline_info,
                current_info=updated_existing,
                export_charts=enable_chart_exports,
            )
        runs_by_hash[run_hash] = updated_existing
        latest_by_group[group_key] = run_hash
        history["runs_by_hash"] = runs_by_hash
        history["latest_by_group"] = latest_by_group
        _atomic_write_json(history_path, history)
        return PersistedBenchmarkRun(
            run_hash=run_hash,
            group_key=group_key,
            created_at_utc=updated_existing.get("created_at_utc", created_at),
            run_dir=updated_existing["run_dir"],
            manifest_path=updated_existing["manifest_path"],
            history_path=_to_relative(history_path),
            deduplicated=True,
            git_commit_hash=updated_existing.get("git_commit_hash"),
            comparison=comparison,
            comparison_warning=comparison_warning,
        )

    date_bucket = created_at.split("T", 1)[0]
    dataset_segment = _safe_path_segment(dataset_key)
    run_dir = root / dataset_segment / date_bucket / run_hash
    charts_dir = run_dir / "charts"
    run_dir.mkdir(parents=True, exist_ok=True)
    charts_dir.mkdir(parents=True, exist_ok=True)

    accuracy_path = run_dir / "accuracy_table.json"
    stage_path = run_dir / "stage_diagnostics.json"
    manifest_path = run_dir / "manifest.json"

    _atomic_write_json(accuracy_path, {"rows": accuracy_rows})
    _atomic_write_json(stage_path, {"rows": stage_rows})
    precision_recall_curve_path: Path | None = None
    if precision_recall_curve_rows is not None:
        precision_recall_curve_path = _write_precision_recall_curve_artifact(
            run_dir=run_dir,
            curve_rows=precision_recall_curve_rows,
        )

    commit_hash = _git_commit_hash()

    previous_dataset_runs = _sorted_dataset_runs(
        runs_by_hash=runs_by_hash,
        dataset_key=dataset_key,
    )
    comparison = None
    comparison_warning: str | None = None
    baseline_info, comparison_warning = _resolve_requested_baseline_info(
        runs_by_hash=runs_by_hash,
        dataset_key=dataset_key,
        comparison_baseline_hash=comparison_baseline_hash,
        current_run_hash=run_hash,
    )
    if baseline_info is None and previous_dataset_runs:
        baseline_info = previous_dataset_runs[-1]

    if baseline_info is not None:
        comparison = _build_persisted_run_comparison(
            results_root=root,
            baseline_info=baseline_info,
            current_info={
                "run_hash": run_hash,
                "run_dir": _to_relative(run_dir),
                "manifest_path": _to_relative(manifest_path),
                "dataset_key": dataset_key,
                "dataset_label": dataset_label,
                "created_at_utc": created_at,
                "group_key": group_key,
                "stage_fingerprint": stage_fingerprint,
                "accuracy_table_path": _to_relative(accuracy_path),
                "stage_diagnostics_path": _to_relative(stage_path),
                "precision_recall_curve_path": (
                    _to_relative(precision_recall_curve_path)
                    if precision_recall_curve_path is not None
                    else None
                ),
                "timings": {k: round(float(v), 6) for k, v in timings.items()},
                "git_commit_hash": commit_hash,
            },
            export_charts=enable_chart_exports,
        )

    manifest = {
        "run_hash": run_hash,
        "dataset_key": dataset_key,
        "dataset_label": dataset_label,
        "created_at_utc": created_at,
        "group_key": group_key,
        "stage_fingerprint": stage_fingerprint,
        "stage_definition": stage_definition,
        "git_commit_hash": commit_hash,
        "timings": {k: round(float(v), 6) for k, v in timings.items()},
        "summary": {
            "total_rows": total_rows,
            "matched_rows": matched_rows,
            "correct_matches": correct_matches,
            "precision": precision,
            "recall": recall,
        },
        "artifacts": {
            "accuracy_table_path": _to_relative(accuracy_path),
            "stage_diagnostics_path": _to_relative(stage_path),
            "precision_recall_curve_path": (
                _to_relative(precision_recall_curve_path)
                if precision_recall_curve_path is not None
                else None
            ),
            "comparison_summary_path": (
                comparison.summary_path if comparison is not None else None
            ),
            "comparison_markdown_path": (
                comparison.markdown_report_path if comparison is not None else None
            ),
            "chart_paths": comparison.chart_paths if comparison is not None else [],
        },
        "comparison_warning": comparison_warning,
    }
    _atomic_write_json(manifest_path, manifest)

    runs_by_hash[run_hash] = {
        "run_hash": run_hash,
        "dataset_key": dataset_key,
        "dataset_label": dataset_label,
        "created_at_utc": created_at,
        "group_key": group_key,
        "stage_fingerprint": stage_fingerprint,
        "run_dir": _to_relative(run_dir),
        "manifest_path": _to_relative(manifest_path),
        "accuracy_table_path": _to_relative(accuracy_path),
        "stage_diagnostics_path": _to_relative(stage_path),
        "precision_recall_curve_path": (
            _to_relative(precision_recall_curve_path)
            if precision_recall_curve_path is not None
            else None
        ),
        "timings": {k: round(float(v), 6) for k, v in timings.items()},
        "git_commit_hash": commit_hash,
    }
    latest_by_group[group_key] = run_hash

    history["runs_by_hash"] = runs_by_hash
    history["latest_by_group"] = latest_by_group
    _atomic_write_json(history_path, history)

    return PersistedBenchmarkRun(
        run_hash=run_hash,
        group_key=group_key,
        created_at_utc=created_at,
        run_dir=_to_relative(run_dir),
        manifest_path=_to_relative(manifest_path),
        history_path=_to_relative(history_path),
        deduplicated=False,
        git_commit_hash=commit_hash,
        comparison=comparison,
        comparison_warning=comparison_warning,
    )


def _resolve_artifact_path(path_value: str, *, results_root: Path) -> Path:
    path = Path(path_value)
    if path.is_absolute():
        return path
    if path.exists():
        return path
    return results_root / _strip_results_prefix(path)


def _comparison_artifact_paths(
    *,
    current_run_dir: Path,
    baseline_hash: str,
    current_hash: str,
    export_charts: bool,
) -> dict[str, Path]:
    charts_dir = current_run_dir / "charts"
    if export_charts:
        charts_dir.mkdir(parents=True, exist_ok=True)

    file_suffix = f"{baseline_hash}_vs_{current_hash}"
    return {
        "summary_path": current_run_dir / f"comparison_summary_{file_suffix}.json",
        "markdown_report_path": (current_run_dir / f"comparison_report_{file_suffix}.md"),
        "overlay_chart_html": charts_dir / f"precision_recall_overlay_{file_suffix}.html",
        "overlay_chart_spec": (
            charts_dir / f"precision_recall_overlay_{file_suffix}.vl.json"
        ),
    }


def _validate_comparison_pair(
    *,
    baseline_info: dict[str, Any],
    current_info: dict[str, Any],
) -> None:
    baseline_dataset_key = str(baseline_info.get("dataset_key", ""))
    current_dataset_key = str(current_info.get("dataset_key", ""))
    if baseline_dataset_key != current_dataset_key:
        raise ValueError(
            "Cannot compare persisted runs from different datasets: "
            f"baseline='{baseline_dataset_key}', comparison='{current_dataset_key}'."
        )


def _build_overlay_chart(
    *,
    baseline_info: dict[str, Any],
    baseline_curve_rows: list[dict[str, Any]],
    current_info: dict[str, Any],
    current_curve_rows: list[dict[str, Any]],
    output_html_path: Path,
    output_spec_path: Path,
) -> Path | None:
    if not baseline_curve_rows or not current_curve_rows:
        return None

    try:
        baseline_chart = build_precision_recall_chart_definition(baseline_curve_rows)
        current_chart = build_precision_recall_chart_definition(current_curve_rows)
        overlay_chart = overlay_precision_recall_charts(
            baseline_chart=baseline_chart,
            comparison_charts=current_chart,
            baseline_label=f"Baseline {baseline_info['run_hash']}",
            comparison_labels=f"Comparison {current_info['run_hash']}",
        )
        chart_definition = (
            overlay_chart.to_dict()
            if hasattr(overlay_chart, "to_dict")
            else overlay_chart
        )
        if not isinstance(chart_definition, dict):
            return None
        chart_definition["title"] = (
            "Precision-Recall Curve Comparison: "
            f"{baseline_info['run_hash']} vs {current_info['run_hash']}"
        )
        _atomic_write_json(output_spec_path, chart_definition)
        write_overlay_precision_recall_chart_html(
            path=output_html_path,
            baseline_chart=baseline_chart,
            comparison_charts=current_chart,
            baseline_label=f"Baseline {baseline_info['run_hash']}",
            comparison_labels=f"Comparison {current_info['run_hash']}",
            title=str(chart_definition["title"]),
        )
    except (TypeError, ValueError):
        return None
    return output_html_path


def _build_persisted_run_comparison(
    *,
    results_root: Path,
    baseline_info: dict[str, Any],
    current_info: dict[str, Any],
    export_charts: bool,
) -> BenchmarkComparisonSummary:
    _validate_comparison_pair(baseline_info=baseline_info, current_info=current_info)

    baseline_hash = str(baseline_info["run_hash"])
    current_hash = str(current_info["run_hash"])

    baseline_accuracy_rows = _load_run_rows(
        baseline_info,
        results_root=results_root,
        artifact_key="accuracy_table_path",
    )
    baseline_stage_rows = _load_run_rows(
        baseline_info,
        results_root=results_root,
        artifact_key="stage_diagnostics_path",
    )
    baseline_curve_rows = _load_run_rows(
        baseline_info,
        results_root=results_root,
        artifact_key="precision_recall_curve_path",
    )

    current_accuracy_rows = _load_run_rows(
        current_info,
        results_root=results_root,
        artifact_key="accuracy_table_path",
    )
    current_stage_rows = _load_run_rows(
        current_info,
        results_root=results_root,
        artifact_key="stage_diagnostics_path",
    )
    current_curve_rows = _load_run_rows(
        current_info,
        results_root=results_root,
        artifact_key="precision_recall_curve_path",
    )

    current_run_dir = _resolve_artifact_path(
        str(current_info["run_dir"]),
        results_root=results_root,
    )
    artifact_paths = _comparison_artifact_paths(
        current_run_dir=current_run_dir,
        baseline_hash=baseline_hash,
        current_hash=current_hash,
        export_charts=export_charts,
    )

    accuracy_comparison_rows = build_accuracy_comparison_rows(
        baseline_rows=baseline_accuracy_rows,
        current_rows=current_accuracy_rows,
        baseline_hash=baseline_hash,
        current_hash=current_hash,
        baseline_git_commit_hash=baseline_info.get("git_commit_hash"),
        current_git_commit_hash=current_info.get("git_commit_hash"),
    )
    stage_diagnostics_comparison_rows = build_stage_diagnostics_comparison_rows(
        baseline_rows=baseline_stage_rows,
        current_rows=current_stage_rows,
        baseline_hash=baseline_hash,
        current_hash=current_hash,
        baseline_git_commit_hash=baseline_info.get("git_commit_hash"),
        current_git_commit_hash=current_info.get("git_commit_hash"),
    )

    chart_paths: list[Path] = []
    if export_charts:
        overlay_chart = _build_overlay_chart(
            baseline_info=baseline_info,
            baseline_curve_rows=baseline_curve_rows,
            current_info=current_info,
            current_curve_rows=current_curve_rows,
            output_html_path=artifact_paths["overlay_chart_html"],
            output_spec_path=artifact_paths["overlay_chart_spec"],
        )
        if overlay_chart is not None:
            chart_paths.append(overlay_chart)

    return build_comparison_summary(
        baseline_hash=baseline_hash,
        current_hash=current_hash,
        baseline_accuracy_rows=baseline_accuracy_rows,
        current_accuracy_rows=current_accuracy_rows,
        baseline_stage_rows=baseline_stage_rows,
        current_stage_rows=current_stage_rows,
        baseline_total_runtime_seconds=baseline_info.get("timings", {}).get(
            "total_runtime"
        ),
        current_total_runtime_seconds=current_info.get("timings", {}).get(
            "total_runtime"
        ),
        summary_path=artifact_paths["summary_path"],
        chart_paths=chart_paths,
        markdown_report_path=artifact_paths["markdown_report_path"],
        dataset_label=str(current_info.get("dataset_label") or ""),
        baseline_git_commit_hash=baseline_info.get("git_commit_hash"),
        current_git_commit_hash=current_info.get("git_commit_hash"),
        baseline_created_at_utc=baseline_info.get("created_at_utc"),
        current_created_at_utc=current_info.get("created_at_utc"),
        accuracy_comparison_rows=accuracy_comparison_rows,
        stage_diagnostics_comparison_rows=stage_diagnostics_comparison_rows,
    )


def compare_persisted_runs(
    *,
    results_root: str = BENCHMARK_RESULTS_ROOT,
    comparison_hash: str,
    baseline_hash: str,
    export_charts: bool = True,
) -> PersistedBenchmarkRun:
    """Compare two explicit persisted runs by hash.

    ``baseline_hash`` provides baseline values, and ``comparison_hash`` is compared
    against it.
    """
    root = Path(results_root)
    history_path = root / _HISTORY_FILE
    history = _load_history(history_path)
    runs_by_hash: dict[str, Any] = history.get("runs_by_hash", {})

    if not runs_by_hash:
        raise ValueError(f"No persisted benchmark runs found under '{results_root}'.")

    if comparison_hash not in runs_by_hash:
        raise ValueError(
            f"Unknown comparison_hash '{comparison_hash}'. "
            f"Check {_history_hint(root)} for available hashes."
        )
    if baseline_hash not in runs_by_hash:
        raise ValueError(
            f"Unknown baseline_hash '{baseline_hash}'. "
            f"Check {_history_hint(root)} for available hashes."
        )

    current_info = runs_by_hash[comparison_hash]
    baseline_info = runs_by_hash[baseline_hash]
    comparison = _build_persisted_run_comparison(
        results_root=root,
        baseline_info=baseline_info,
        current_info=current_info,
        export_charts=export_charts,
    )

    return PersistedBenchmarkRun(
        run_hash=comparison_hash,
        group_key=str(current_info.get("group_key", "")),
        created_at_utc=str(current_info.get("created_at_utc", "")),
        run_dir=str(current_info.get("run_dir", "")),
        manifest_path=str(current_info.get("manifest_path", "")),
        history_path=_to_relative(history_path),
        deduplicated=False,
        git_commit_hash=current_info.get("git_commit_hash"),
        comparison=comparison,
    )
