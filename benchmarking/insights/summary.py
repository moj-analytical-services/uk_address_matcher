from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb


def fetch_overall_summary(
    con: duckdb.DuckDBPyConnection,
    accuracy_relation: duckdb.DuckDBPyRelation,
    *,
    total_input_rows: int,
) -> tuple[int, int, int, float | None, float | None]:
    """Return overall benchmark summary tuple from the accuracy table."""
    rows = con.sql(
        f"""
        SELECT
            rows_matched_in_stage,
            correct_matches,
            precision,
            recall
        FROM ({accuracy_relation.sql_query()}) AS accuracy
        WHERE stage = 'overall'
        """
    ).limit(1).fetchall()

    if not rows:
        raise ValueError("Expected an overall row in the accuracy table.")

    row = rows[0]
    return (
        int(total_input_rows),
        int(row[0]),
        int(row[1]),
        float(row[2]) if row[2] is not None else None,
        float(row[3]) if row[3] is not None else None,
    )
