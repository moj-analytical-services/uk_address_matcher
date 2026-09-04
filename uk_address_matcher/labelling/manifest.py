from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from uk_address_matcher.post_linkage.match_result.result import MatchResult


def build_manifest(
    *,
    match_result: MatchResult,
    bundle_id: str,
    created_at_utc: str,
    uk_address_matcher_version: str,
    parquet_validation: dict[str, object],
    top_n_candidates: int,
    canonical_label_column: str,
    canonical_data_file: str | None = None,
    messy_columns: tuple[str, ...],
    canonical_columns: tuple[str, ...],
) -> dict[str, object]:
    """Build JSON-safe authoritative metadata for a written bundle."""
    return {
        "bundle_id": bundle_id,
        "created_at_utc": created_at_utc,
        "uk_address_matcher_version": uk_address_matcher_version,
        "data_file": "review_data.parquet",
        "row_count": parquet_validation["row_count"],
        "matched_row_count": parquet_validation["matched_row_count"],
        "unmatched_row_count": parquet_validation["unmatched_row_count"],
        "rows_with_candidates": parquet_validation["rows_with_candidates"],
        "rows_with_existing_labels": parquet_validation["rows_with_existing_labels"],
        "top_n_candidates": top_n_candidates,
        "canonical_label_column": canonical_label_column,
        "canonical_data_file": canonical_data_file,
        "messy_columns": list(messy_columns),
        "canonical_columns": list(canonical_columns),
        "match_reasons": _match_reasons(match_result),
        "splink": _splink_configuration(match_result),
        "parquet_schema": parquet_validation["schema"],
    }


def _match_reasons(match_result: MatchResult) -> list[str]:
    return [
        str(row[0])
        for row in match_result.con.execute(
            f"""
            SELECT DISTINCT CAST(match_reason AS VARCHAR)
            FROM ({match_result._relation.sql_query()}) AS matches
            WHERE match_reason IS NOT NULL
            ORDER BY 1
            """
        ).fetchall()
    ]


def _splink_configuration(match_result: MatchResult) -> dict[str, object]:
    stage = match_result._splink_stage
    if stage is None:
        return {"configured": False, "ran": False}
    return {
        "configured": True,
        "ran": stage.linker is not None,
        "predict_threshold_match_weight": float(stage.predict_threshold_match_weight),
        "improve_threshold_match_weight": float(stage.improve_threshold_match_weight),
        "improve_top_n_matches": int(stage.improve_top_n_matches),
        "final_match_weight_threshold": float(stage.final_match_weight_threshold),
        "final_distinguishability_threshold": (
            None
            if stage.final_distinguishability_threshold is None
            else float(stage.final_distinguishability_threshold)
        ),
    }
