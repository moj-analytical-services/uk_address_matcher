"""Shared helper for stages that use the SQL pipeline framework."""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from uk_address_matcher.sql_pipeline.runner import InputBinding, create_sql_pipeline

if TYPE_CHECKING:
    import duckdb

    from uk_address_matcher.sql_pipeline.runner import DebugOptions
    from uk_address_matcher.sql_pipeline.steps import Stage


def run_sql_pipeline(
    con: duckdb.DuckDBPyConnection,
    pipeline_stages: list[Stage],
    stage_name: str,
    df_unmatched: duckdb.DuckDBPyRelation,
    df_canonical: duckdb.DuckDBPyRelation,
    debug_options: Optional[DebugOptions] = None,
) -> Optional[duckdb.DuckDBPyRelation]:
    """Run a SQL pipeline and return the matched-rows relation.

    Returns ``None`` when the pipeline produces no output (e.g. explain mode).
    Returns the final pipeline relation otherwise — the caller is responsible
    for writing matched rows into the results table (typically via
    ``MatchingStage.run()``).
    """
    pipeline = create_sql_pipeline(
        con,
        [
            InputBinding("fuzzy_addresses", df_unmatched),
            InputBinding("canonical_addresses", df_canonical),
        ],
        pipeline_stages,
        pipeline_name=f"Stage: {stage_name}",
        pipeline_description=f"Matching stage: {stage_name}",
    )

    if debug_options is not None:
        pipeline.show_plan()

    return pipeline.run(options=debug_options)
