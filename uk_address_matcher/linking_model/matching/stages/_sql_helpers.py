from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Sequence

from uk_address_matcher.sql_pipeline.runner import InputBinding, create_sql_pipeline

if TYPE_CHECKING:
    import duckdb

    from uk_address_matcher.sql_pipeline.runner import DebugOptions
    from uk_address_matcher.sql_pipeline.steps import StageLike


def run_sql_pipeline(
    con: duckdb.DuckDBPyConnection,
    pipeline_stages: Sequence[StageLike],
    stage_name: str,
    df_unmatched: duckdb.DuckDBPyRelation,
    df_canonical: duckdb.DuckDBPyRelation,
    debug_options: Optional[DebugOptions] = None,
    explain: bool = False,
) -> Optional[duckdb.DuckDBPyRelation]:
    """Run a SQL pipeline stage with standard matching inputs."""
    pipeline = create_sql_pipeline(
        con,
        [
            InputBinding("messy_addresses", df_unmatched),
            InputBinding("canonical_addresses", df_canonical),
        ],
        list(pipeline_stages),
        pipeline_name=f"Deterministic Match Stage: {stage_name}",
        pipeline_description=f"Deterministic matching stage: {stage_name}",
    )

    if debug_options is not None or explain:
        pipeline.show_plan()

    return pipeline.run(options=debug_options, explain=explain)
