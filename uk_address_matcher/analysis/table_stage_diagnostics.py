from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import uuid4

from uk_address_matcher._typing import StageDiagnosticRow, StageDiagnostics

if TYPE_CHECKING:
    import duckdb


def build_stage_diagnostics_relation(
    con: duckdb.DuckDBPyConnection,
    stage_diagnostics: StageDiagnostics | None,
) -> duckdb.DuckDBPyRelation:
    if not stage_diagnostics:
        raise ValueError("No stage diagnostics data available to build relation.")

    table_name = f"__ukam_stage_diagnostics_{uuid4().hex}"
    con.execute(
        f'''
        CREATE TEMP TABLE "{table_name}" (
            stage_order BIGINT,
            stage VARCHAR,
            unmatched_before BIGINT,
            matched_this_stage BIGINT,
            remaining_after BIGINT,
            matched_pct_of_unmatched DOUBLE,
            matched_pct_of_input DOUBLE,
            elapsed_seconds DOUBLE
        )
        '''
    ).fetchall()
    insert_sql = f'INSERT INTO "{table_name}" VALUES (?, ?, ?, ?, ?, ?, ?, ?)'

    stage_diagnostics_signature = tuple(StageDiagnosticRow.__annotations__.keys())
    for index, row in enumerate(stage_diagnostics):
        if tuple(row.keys()) != stage_diagnostics_signature:
            raise ValueError(
                "Stage diagnostics row keys must match StageDiagnosticRow: "
                + ", ".join(stage_diagnostics_signature)
            )
        # index indicates the order of the stage in the pipeline,
        # which is useful for ordering in the final diagnostics table.
        con.execute(insert_sql, [index, *row.values()]).fetchall()

    return con.sql(
        f'''
        SELECT
            stage,
            stage_order,
            unmatched_before AS rows_entering_stage,
            matched_this_stage AS rows_matched_in_stage,
            matched_pct_of_unmatched AS stage_match_rate,
            matched_pct_of_input AS share_of_total_input_matched,
            elapsed_seconds
        FROM "{table_name}"
        '''
    )


def build_stage_diagnostics_table(
    con: duckdb.DuckDBPyConnection,
    stage_diagnostics_relation: duckdb.DuckDBPyRelation,
) -> duckdb.DuckDBPyRelation:
    return con.sql(
        f"""
        WITH base_rows AS (
            SELECT
                d.stage,
                d.stage_order,
                d.rows_entering_stage,
                d.rows_matched_in_stage,
                ROUND(100.0 * d.stage_match_rate, 2) AS stage_match_rate,
                ROUND(100.0 * d.share_of_total_input_matched, 2)
                    AS share_of_total_input_matched,
                ROUND(d.elapsed_seconds, 4) AS elapsed_seconds
            FROM ({stage_diagnostics_relation.sql_query()}) AS d
        )
        SELECT
            stage,
            stage_order,
            rows_entering_stage,
            rows_matched_in_stage,
            stage_match_rate,
            share_of_total_input_matched,
            elapsed_seconds
        FROM base_rows
        ORDER BY stage_order
        """
    )
