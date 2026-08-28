from __future__ import annotations

import hashlib
import json
import subprocess
from collections.abc import Sequence
from dataclasses import fields, is_dataclass
from datetime import datetime, timezone
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
from uk_address_matcher.analysis.accuracy_analysis import (
    build_precision_recall_chart_definition,
)
from uk_address_matcher.analysis.overlay_precision_recall_charts import (
    _overlay_precision_recall_charts,
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


def _chart_to_definition(chart: Any) -> dict[str, Any]:
    if isinstance(chart, dict):
        return chart

    to_dict = getattr(chart, "to_dict", None)
    if callable(to_dict):
        chart_definition = to_dict()
        if isinstance(chart_definition, dict):
            return chart_definition

    raise TypeError("chart must be a Vega-Lite definition dict or chart object")


def _vega_lite_html(*, title: str, chart_definition: dict[str, Any]) -> str:
    return f"""<!doctype html>
<html lang=\"en\">
<head>
    <meta charset=\"utf-8\" />
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
    <title>{title}</title>
    <script src=\"https://cdn.jsdelivr.net/npm/vega@5\"></script>
    <script src=\"https://cdn.jsdelivr.net/npm/vega-lite@5\"></script>
    <script src=\"https://cdn.jsdelivr.net/npm/vega-embed@6\"></script>
    <style>
        body {{
            font-family: ui-sans-serif, -apple-system, BlinkMacSystemFont,
                \"Segoe UI\", sans-serif;
            margin: 24px;
            background: #f8fafc;
            color: #0f172a;
        }}
        .card {{
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            background: #ffffff;
            border: 1px solid #e2e8f0;
            border-radius: 12px;
            box-shadow: 0 10px 30px rgba(15, 23, 42, 0.08);
        }}
        h1 {{
            margin: 0 0 8px;
            font-size: 1.5rem;
        }}
        p {{
            margin: 0 0 18px;
            color: #475569;
        }}
    </style>
</head>
<body>
    <div class=\"card\">
        <h1>{title}</h1>
        <p>Overlayed precision-recall curves with recall-aligned reductions in false positives.</p>
        <div id=\"chart\"></div>
    </div>
    <script>
        const spec = {json.dumps(chart_definition)};
        vegaEmbed('#chart', spec, {{ actions: true, renderer: 'svg' }});
    </script>
</body>
</html>
"""


def overlay_precision_recall_charts(
    baseline_chart: Any,
    comparison_charts: Any | list[Any],
    *,
    baseline_label: str | None = None,
    comparison_labels: str | Sequence[str] | None = None,
) -> Any:
    return _overlay_precision_recall_charts(
        baseline_chart,
        comparison_charts,
        baseline_label=baseline_label,
        comparison_labels=comparison_labels,
    )


def write_overlay_precision_recall_chart_html(
    *,
    path: Path,
    baseline_chart: Any,
    comparison_charts: Any | list[Any],
    baseline_label: str | None = None,
    comparison_labels: str | Sequence[str] | None = None,
    title: str | None = None,
) -> None:
    chart = overlay_precision_recall_charts(
        baseline_chart,
        comparison_charts,
        baseline_label=baseline_label,
        comparison_labels=comparison_labels,
    )
    chart_definition = _chart_to_definition(chart)
    chart_title = title or str(
        chart_definition.get("title") or "Precision-Recall Curve Comparison"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        _vega_lite_html(title=chart_title, chart_definition=chart_definition),
        encoding="utf-8",
    )


def _safe_path_segment(value: str) -> str:
    lowered = value.strip().lower().replace(" ", "_")
    safe = "".join(ch if (ch.isalnum() or ch in {"-", "_"}) else "_" for ch in lowered)
    return safe or "unknown_dataset"


def generate_benchmark_run_id(*, at: datetime | None = None) -> str:
    timestamp = at or datetime.now(timezone.utc)
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    else:
        timestamp = timestamp.astimezone(timezone.utc)
    seed = timestamp.strftime("%Y%m%dT%H%M%S%fZ")
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()[:16]


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


def _normalise_run_id(*, run_id: str | None, fallback_at: datetime) -> str:
    if run_id is None:
        return generate_benchmark_run_id(at=fallback_at)

    normalised = run_id.strip()
    if not normalised:
        return generate_benchmark_run_id(at=fallback_at)
    return normalised


def _run_info_run_id(
    run_info: dict[str, Any],
    *,
    fallback_hash: str | None = None,
) -> str | None:
    run_id = run_info.get("run_id")
    if run_id is None:
        return fallback_hash

    normalised = str(run_id).strip()
    return normalised or fallback_hash


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
        return datetime.min.replace(tzinfo=timezone.utc)

    normalised = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalised)
    except ValueError:
        return datetime.min.replace(tzinfo=timezone.utc)

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


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


def _find_dataset_run_by_run_id(
    *,
    runs_by_hash: dict[str, Any],
    dataset_key: str,
    run_id: str,
) -> dict[str, Any] | None:
    normalised_run_id = run_id.strip()
    if not normalised_run_id:
        return None

    for run_info in reversed(
        _sorted_dataset_runs(
            runs_by_hash=runs_by_hash,
            dataset_key=dataset_key,
        )
    ):
        if _run_info_run_id(run_info) == normalised_run_id:
            return run_info
    return None


def _resolve_requested_baseline_info(
    *,
    runs_by_hash: dict[str, Any],
    dataset_key: str,
    comparison_baseline_run_id: str | None,
    current_run_id: str,
    current_run_hash: str,
) -> tuple[dict[str, Any] | None, str | None]:
    if comparison_baseline_run_id is None:
        normalised_baseline_run_id = None
    else:
        normalised_baseline_run_id = comparison_baseline_run_id.strip() or None
        if normalised_baseline_run_id is not None:
            normalised_baseline_run_id = normalised_baseline_run_id.lower()
            if normalised_baseline_run_id != "latest":
                normalised_baseline_run_id = comparison_baseline_run_id.strip()

    if normalised_baseline_run_id == current_run_id:
        raise ValueError(
            "comparison_baseline_run_id must differ from the current run_id."
        )

    if normalised_baseline_run_id == "latest":
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

    if normalised_baseline_run_id is None:
        return None, None

    baseline_info = _find_dataset_run_by_run_id(
        runs_by_hash=runs_by_hash,
        dataset_key=dataset_key,
        run_id=normalised_baseline_run_id,
    )
    if baseline_info is not None:
        return baseline_info, None

    return (
        None,
        "Comparison skipped: unknown comparison_baseline_run_id "
        f"'{normalised_baseline_run_id}' for dataset '{dataset_key}'.",
    )


def persist_benchmark_run(
    *,
    run_id: str | None = None,
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
    comparison_baseline_run_id: str | None = None,
    results_root: str = BENCHMARK_RESULTS_ROOT,
    enable_chart_exports: bool = True,
) -> PersistedBenchmarkRun:
    root = Path(results_root)
    history_path = root / _HISTORY_FILE

    created_at_dt = datetime.now(timezone.utc)
    created_at = created_at_dt.isoformat()
    run_id = _normalise_run_id(run_id=run_id, fallback_at=created_at_dt)
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
            comparison_baseline_run_id=comparison_baseline_run_id,
            current_run_id=run_id,
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
            run_id=_run_info_run_id(updated_existing, fallback_hash=run_hash),
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
    run_dir = root / dataset_segment / date_bucket / run_id
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
        comparison_baseline_run_id=comparison_baseline_run_id,
        current_run_id=run_id,
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
                "run_id": run_id,
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
        "run_id": run_id,
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
        "run_id": run_id,
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
        run_id=run_id,
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
    baseline_run_id: str,
    current_run_id: str,
    export_charts: bool,
) -> dict[str, Path]:
    charts_dir = current_run_dir / "charts"
    if export_charts:
        charts_dir.mkdir(parents=True, exist_ok=True)

    file_suffix = (
        f"{_safe_path_segment(baseline_run_id)}_vs_{_safe_path_segment(current_run_id)}"
    )
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
        baseline_run_id = _run_info_run_id(
            baseline_info,
            fallback_hash=str(baseline_info["run_hash"]),
        )
        current_run_id = _run_info_run_id(
            current_info,
            fallback_hash=str(current_info["run_hash"]),
        )
        baseline_chart = build_precision_recall_chart_definition(baseline_curve_rows)
        current_chart = build_precision_recall_chart_definition(current_curve_rows)
        overlay_chart = overlay_precision_recall_charts(
            baseline_chart=baseline_chart,
            comparison_charts=current_chart,
            baseline_label=f"Baseline {baseline_run_id}",
            comparison_labels=f"Comparison {current_run_id}",
        )
        chart_definition = (
            overlay_chart.to_dict()
            if hasattr(overlay_chart, "to_dict")
            else overlay_chart
        )
        if not isinstance(chart_definition, dict):
            return None
        chart_definition["title"] = (
            f"Precision-Recall Curve Comparison: {baseline_run_id} vs {current_run_id}"
        )
        _atomic_write_json(output_spec_path, chart_definition)
        write_overlay_precision_recall_chart_html(
            path=output_html_path,
            baseline_chart=baseline_chart,
            comparison_charts=current_chart,
            baseline_label=f"Baseline {baseline_run_id}",
            comparison_labels=f"Comparison {current_run_id}",
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

    baseline_run_id = _run_info_run_id(
        baseline_info,
        fallback_hash=str(baseline_info["run_hash"]),
    )
    current_run_id = _run_info_run_id(
        current_info,
        fallback_hash=str(current_info["run_hash"]),
    )
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
        baseline_run_id=baseline_run_id,
        current_run_id=current_run_id,
        export_charts=export_charts,
    )

    accuracy_comparison_rows = build_accuracy_comparison_rows(
        baseline_rows=baseline_accuracy_rows,
        current_rows=current_accuracy_rows,
        baseline_run_id=baseline_run_id,
        current_run_id=current_run_id,
        baseline_hash=baseline_hash,
        current_hash=current_hash,
        baseline_git_commit_hash=baseline_info.get("git_commit_hash"),
        current_git_commit_hash=current_info.get("git_commit_hash"),
    )
    stage_diagnostics_comparison_rows = build_stage_diagnostics_comparison_rows(
        baseline_rows=baseline_stage_rows,
        current_rows=current_stage_rows,
        baseline_run_id=baseline_run_id,
        current_run_id=current_run_id,
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
        baseline_run_id=baseline_run_id,
        current_run_id=current_run_id,
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
    dataset_key: str,
    comparison_run_id: str,
    baseline_run_id: str,
    export_charts: bool = True,
) -> PersistedBenchmarkRun:
    """Compare two explicit persisted runs by dataset-scoped run_id.

    ``baseline_run_id`` provides baseline values, and ``comparison_run_id`` is
    compared against it.
    """
    root = Path(results_root)
    history_path = root / _HISTORY_FILE
    history = _load_history(history_path)
    runs_by_hash: dict[str, Any] = history.get("runs_by_hash", {})

    if not runs_by_hash:
        raise ValueError(f"No persisted benchmark runs found under '{results_root}'.")

    current_info = _find_dataset_run_by_run_id(
        runs_by_hash=runs_by_hash,
        dataset_key=dataset_key,
        run_id=comparison_run_id,
    )
    if current_info is None:
        raise ValueError(
            f"Unknown comparison_run_id '{comparison_run_id}' for dataset "
            f"'{dataset_key}'. Check {_history_hint(root)} for available run_ids."
        )

    baseline_info = _find_dataset_run_by_run_id(
        runs_by_hash=runs_by_hash,
        dataset_key=dataset_key,
        run_id=baseline_run_id,
    )
    if baseline_info is None:
        raise ValueError(
            f"Unknown baseline_run_id '{baseline_run_id}' for dataset "
            f"'{dataset_key}'. Check {_history_hint(root)} for available run_ids."
        )

    comparison = _build_persisted_run_comparison(
        results_root=root,
        baseline_info=baseline_info,
        current_info=current_info,
        export_charts=export_charts,
    )

    return PersistedBenchmarkRun(
        run_id=_run_info_run_id(
            current_info,
            fallback_hash=str(current_info.get("run_hash", "")),
        ),
        run_hash=str(current_info.get("run_hash", "")),
        group_key=str(current_info.get("group_key", "")),
        created_at_utc=str(current_info.get("created_at_utc", "")),
        run_dir=str(current_info.get("run_dir", "")),
        manifest_path=str(current_info.get("manifest_path", "")),
        history_path=_to_relative(history_path),
        deduplicated=False,
        git_commit_hash=current_info.get("git_commit_hash"),
        comparison=comparison,
    )
