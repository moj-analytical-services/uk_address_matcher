from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb


def calculate_accuracy_metrics(
    ukam_matches: duckdb.DuckDBPyRelation,
    ukam_canonical: duckdb.DuckDBPyRelation | None = None,
) -> duckdb.DuckDBPyRelation:
    """Calculate accuracy metrics from matching results.

    Parameters
    ----------
    ukam_matches:
        Match results containing ukam_label (ground truth), resolved_canonical_id,
        and match_reason columns. The ukam_label column must be present for
        accuracy calculation.
    ukam_canonical:
        Optional canonical dataset. If provided, records where the ground truth
        ukam_label doesn't exist in the canonical data will be separately reported
        as 'unmatchable' rather than counted as incorrect matches.

    Returns
    -------
    duckdb.DuckDBPyRelation
        Accuracy metrics broken down by match_reason (and dataset if present).

    Raises
    ------
    ValueError
        If ukam_label column is not present in the input data.
    """
    # Check if ukam_label exists - required for accuracy calculation
    if "ukam_label" not in ukam_matches.columns:
        raise ValueError(
            "Cannot calculate accuracy metrics: "
            "'ukam_label' column not found in match results. "
            "Accuracy calculation requires ground truth labels. "
            "If you're running in inference "
            "mode without labels, skip the accuracy calculation step."
        )

    dataset_column: str | None = None
    if "dataset_name" in ukam_matches.columns:
        dataset_column = "dataset_name"
    elif "source_dataset" in ukam_matches.columns:
        dataset_column = "source_dataset"

    # Build the canonical availability check if canonical data is provided
    # Use ukam_label column (ground truth UPRN) to check if record is matchable
    if ukam_canonical is not None:
        canonical_join = """
        LEFT JOIN ukam_canonical AS c
          ON m.ukam_label = c.unique_id
        """
        matchable_case = (
            "CASE WHEN c.unique_id IS NOT NULL THEN 1 ELSE 0 END AS is_matchable,"
        )
    else:
        canonical_join = ""
        matchable_case = "1 AS is_matchable,"

    if dataset_column is None:
        sql = f"""
        WITH matched_records AS (
            SELECT
                m.unique_id,
                m.ukam_label,
                m.resolved_canonical_id,
                m.match_reason,
                {matchable_case}
                CASE
                    WHEN m.ukam_label = m.resolved_canonical_id THEN 1
                    ELSE 0
                END AS is_correct
            FROM ukam_matches AS m
            {canonical_join}
            WHERE m.match_reason IS NOT NULL
        )
        SELECT
            CASE
                WHEN GROUPING(match_reason) = 1 THEN 'OVERALL'
                ELSE match_reason
            END AS match_reason,
            COUNT(*) AS total_matched,
            SUM(is_correct) AS correct_matches,
            SUM(
                CASE WHEN is_correct = 0 AND is_matchable = 1 THEN 1 ELSE 0 END
            ) AS incorrect_matches,
            SUM(CASE WHEN is_matchable = 0 THEN 1 ELSE 0 END) AS unmatchable_records,
            ROUND(
                COALESCE(100.0 * SUM(is_correct) / NULLIF(COUNT(*), 0), 0),
                2
            ) AS accuracy_pct,
            ROUND(
                COALESCE(100.0 * SUM(is_correct) / NULLIF(SUM(is_matchable), 0), 0),
                2
            ) AS accuracy_pct_matchable
        FROM matched_records
        GROUP BY GROUPING SETS ((match_reason), ())
        ORDER BY
            CASE WHEN GROUPING(match_reason) = 1 THEN 0 ELSE 1 END,
            total_matched DESC
        """
        return ukam_matches.query("accuracy", sql)

    sql = f"""
    WITH matched_records AS (
        SELECT
            m.unique_id,
            m.ukam_label,
            m.resolved_canonical_id,
            m.match_reason,
            COALESCE(m.{dataset_column}, 'UNKNOWN_DATASET') AS dataset_name,
            {matchable_case}
            CASE
                WHEN m.ukam_label = m.resolved_canonical_id THEN 1
                ELSE 0
            END AS is_correct
        FROM ukam_matches AS m
        {canonical_join}
        WHERE m.match_reason IS NOT NULL
    ),
    aggregated AS (
        SELECT
            CASE
                WHEN GROUPING(dataset_name) = 1 THEN 'ALL_DATASETS'
                ELSE dataset_name
            END AS dataset_name,
            CASE
                WHEN GROUPING(match_reason) = 1 THEN 'OVERALL'
                ELSE match_reason
            END AS match_reason,
            COUNT(*) AS total_matched,
            SUM(is_correct) AS correct_matches,
            SUM(
                CASE WHEN is_correct = 0 AND is_matchable = 1 THEN 1 ELSE 0 END
            ) AS incorrect_matches,
            SUM(CASE WHEN is_matchable = 0 THEN 1 ELSE 0 END) AS unmatchable_records,
            ROUND(
                COALESCE(100.0 * SUM(is_correct) / NULLIF(COUNT(*), 0), 0),
                2
            ) AS accuracy_pct,
            ROUND(
                COALESCE(100.0 * SUM(is_correct) / NULLIF(SUM(is_matchable), 0), 0),
                2
            ) AS accuracy_pct_matchable,
            GROUPING(dataset_name) AS dataset_grouping,
            GROUPING(match_reason) AS reason_grouping
        FROM matched_records
        GROUP BY GROUPING SETS ((dataset_name, match_reason), (dataset_name), ())
    )
    SELECT
        dataset_name,
        match_reason,
        total_matched,
        correct_matches,
        incorrect_matches,
        unmatchable_records,
        accuracy_pct,
        accuracy_pct_matchable
    FROM aggregated
    ORDER BY
        CASE
            WHEN dataset_grouping = 1 THEN 0
            ELSE 1
        END,
        CASE
            WHEN dataset_name = 'ALL_DATASETS' THEN 0
            ELSE 1
        END,
        dataset_name,
        CASE
            WHEN reason_grouping = 1 THEN 0
            ELSE 1
        END,
        total_matched DESC
    """

    return ukam_matches.query("accuracy", sql)
