from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb


@dataclass(frozen=True)
class DatasetDiagnostics:
    successful_matches: duckdb.DuckDBPyRelation
    incorrect_matches: duckdb.DuckDBPyRelation
    lowest_similarity_incorrect: duckdb.DuckDBPyRelation
    highest_similarity_incorrect: duckdb.DuckDBPyRelation
    suspicious_incorrect_summary: duckdb.DuckDBPyRelation
    suspicious_incorrect_records: duckdb.DuckDBPyRelation
    unmatched_records: duckdb.DuckDBPyRelation
    unmatched_top_splink: duckdb.DuckDBPyRelation | None
    splink_available: bool


@dataclass(frozen=True)
class BenchmarkOutputOptions:
    show_splink_comparisons: bool = True
    show_successful_matches: bool = False
    show_incorrect_matches: bool = True
    show_similarity_score_checks: bool = True
    show_unmatched_records: bool = False
    incorrect_match_sample_size: int = 10

    def enable_diagnostics(self) -> bool:
        return any(
            (
                self.show_successful_matches,
                self.show_incorrect_matches,
                self.show_similarity_score_checks,
                self.show_unmatched_records,
            )
        )
