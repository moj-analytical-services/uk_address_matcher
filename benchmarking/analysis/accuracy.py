from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb


def calculate_accuracy_metrics(
    matches: duckdb.DuckDBPyRelation,
) -> duckdb.DuckDBPyRelation:
    dataset_column: str | None = None
    if "dataset_name" in matches.columns:
        dataset_column = "dataset_name"
    elif "source_dataset" in matches.columns:
        dataset_column = "source_dataset"

    if dataset_column is None:
        sql = """
        WITH matched_records AS (
            SELECT
                unique_id,
                resolved_canonical_id,
                match_reason,
                CASE WHEN unique_id = resolved_canonical_id THEN 1 ELSE 0 END AS is_correct
            FROM matches
            WHERE match_reason IS NOT NULL
        )
        SELECT
            CASE WHEN GROUPING(match_reason) = 1 THEN 'OVERALL' ELSE match_reason END AS match_reason,
            COUNT(*) AS total_matched,
            SUM(is_correct) AS correct_matches,
            COUNT(*) - SUM(is_correct) AS incorrect_matches,
            ROUND(COALESCE(100.0 * SUM(is_correct) / NULLIF(COUNT(*), 0), 0), 2) AS accuracy_pct
        FROM matched_records
        GROUP BY GROUPING SETS ((match_reason), ())
        ORDER BY
            CASE WHEN GROUPING(match_reason) = 1 THEN 0 ELSE 1 END,
            total_matched DESC
        """
        return matches.query("accuracy", sql)

    sql = f"""
    WITH matched_records AS (
        SELECT
            unique_id,
            resolved_canonical_id,
            match_reason,
            COALESCE({dataset_column}, 'UNKNOWN_DATASET') AS dataset_name,
            CASE WHEN unique_id = resolved_canonical_id THEN 1 ELSE 0 END AS is_correct
        FROM matches
        WHERE match_reason IS NOT NULL
    )
    SELECT
        CASE WHEN GROUPING(dataset_name) = 1 THEN 'ALL_DATASETS' ELSE dataset_name END AS dataset_name,
        CASE WHEN GROUPING(match_reason) = 1 THEN 'OVERALL' ELSE match_reason END AS match_reason,
        COUNT(*) AS total_matched,
        SUM(is_correct) AS correct_matches,
        COUNT(*) - SUM(is_correct) AS incorrect_matches,
        ROUND(COALESCE(100.0 * SUM(is_correct) / NULLIF(COUNT(*), 0), 0), 2) AS accuracy_pct
    FROM matched_records
    GROUP BY GROUPING SETS ((dataset_name, match_reason), (dataset_name), ())
    ORDER BY
        CASE
            WHEN GROUPING(dataset_name) = 1 THEN 0
            WHEN GROUPING(match_reason) = 1 THEN 1
            ELSE 2
        END,
        dataset_name,
        total_matched DESC
    """

    return matches.query("accuracy", sql)
