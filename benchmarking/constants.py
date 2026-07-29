from __future__ import annotations

from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent

BENCHMARK_PROJECT_ROOT = _PROJECT_ROOT.as_posix()
BENCHMARK_RESULTS_ROOT = (_PROJECT_ROOT / "benchmarking" / "results").as_posix()
DUCKDB_MAX_MEMORY = "16GB"

RESIDENTIAL_ONLY_CANONICAL_FILTER_SQL = "substr(classificationcode, 1, 1) = 'R'"
RESIDENTIAL_WITHOUT_GARAGES_OR_PARENT_SHELLS_CANONICAL_FILTER_SQL = (
    "substr(classificationcode, 1, 1) = 'R' "
    "AND substr(classificationcode, 1, 2) <> 'RG' "
    "AND substr(classificationcode, 1, 2) <> 'PP'"
)
RESIDENTIAL_WITHOUT_ANCILLARY_PREFIXES_CANONICAL_FILTER_SQL = (
    "substr(classificationcode, 1, 1) = 'R' "
    "AND clean_full_address NOT LIKE 'CAR PARK SPACE%' "
    "AND clean_full_address NOT LIKE 'GARAGE %'"
)
RESIDENTIAL_CORE_CANONICAL_FILTER_SQL = (
    "substr(classificationcode, 1, 1) = 'R' "
    "AND substr(classificationcode, 1, 2) <> 'RG' "
    "AND substr(classificationcode, 1, 2) <> 'PP' "
    "AND clean_full_address NOT LIKE 'CAR PARK SPACE%' "
    "AND clean_full_address NOT LIKE 'GARAGE %'"
)

# Optional Splink-only comparison table shown after benchmark summary.
# Set to `None` to disable threshold-comparison output.
SPLINK_BASELINE_WEIGHT: float | None = 10.0
SPLINK_COMPARISON_WEIGHTS: list[float] | None = None
TOP_K_PRECISION_AT_METRICS: list[int] = [1, 3, 5]

# Set this to one of the recipe constants above to enable a benchmark filter.
CANONICAL_FILTER_SQL: str | None = (
    RESIDENTIAL_WITHOUT_ANCILLARY_PREFIXES_CANONICAL_FILTER_SQL
)
APPLY_CANONICAL_FILTER = CANONICAL_FILTER_SQL is not None
CLEANING_NUM_CHUNKS = 1
