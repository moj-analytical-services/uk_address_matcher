from __future__ import annotations

from pathlib import Path
from typing import TypeAlias, TypedDict

import duckdb


class _ColumnAlias(TypedDict):
    """Rename an input column in joined match results"""

    input_column: str
    alias: str


class StageDiagnosticRow(TypedDict):
    stage: str
    unmatched_before: int
    matched_this_stage: int
    remaining_after: int
    matched_pct_of_unmatched: float
    matched_pct_of_input: float
    elapsed_seconds: float


StageDiagnostics: TypeAlias = list[StageDiagnosticRow]

PrepareCanonicalInput: TypeAlias = duckdb.DuckDBPyRelation | str | Path | list[str | Path]
