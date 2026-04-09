from __future__ import annotations

from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent

BENCHMARK_PROJECT_ROOT = _PROJECT_ROOT.as_posix()
BENCHMARK_RESULTS_ROOT = (_PROJECT_ROOT / "benchmarking" / "results").as_posix()

# Optional Splink-only comparison table shown after benchmark summary.
# Set to `None` to disable threshold-comparison output.
SPLINK_BASELINE_WEIGHT: float | None = 10.0
SPLINK_COMPARISON_WEIGHTS: list[float] | None = None
TOP_K_PRECISION_AT_METRICS: list[int] = [1, 3, 5]

APPLY_CANONICAL_FILTER = True
CLEANING_NUM_CHUNKS = 1
