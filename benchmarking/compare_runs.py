from __future__ import annotations

from benchmarking.comparisons.console_report import print_comparison_report
from benchmarking.insights.run_persistence import compare_persisted_runs

# Edit these values directly before running the script.
RESULTS_ROOT = "benchmarking/results"
BASELINE_HASH = "a4b5ed29a00f8360"
COMPARISON_HASH = "a92dcab15534d295"
EXPORT_CHARTS = True


def _validate_hash_inputs(*, baseline_hash: str, comparison_hash: str) -> None:
    if not baseline_hash.strip():
        raise ValueError("BASELINE_HASH must be set to a persisted run hash.")
    if not comparison_hash.strip():
        raise ValueError("COMPARISON_HASH must be set to a persisted run hash.")
    if baseline_hash == comparison_hash:
        raise ValueError("BASELINE_HASH and COMPARISON_HASH must be different.")


_validate_hash_inputs(
    baseline_hash=BASELINE_HASH,
    comparison_hash=COMPARISON_HASH,
)

persisted = compare_persisted_runs(
    results_root=RESULTS_ROOT,
    baseline_hash=BASELINE_HASH,
    comparison_hash=COMPARISON_HASH,
    export_charts=EXPORT_CHARTS,
)

if persisted.comparison is None:
    raise RuntimeError("No comparison summary was produced.")

comparison = persisted.comparison
print_comparison_report(
    comparison=comparison,
    history_path=persisted.history_path,
)
