from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from uk_address_matcher.linking_model.exact_matching import run_deterministic_match_pass

if TYPE_CHECKING:
    import duckdb

    from uk_address_matcher.sql_pipeline.runner import DebugOptions


def run_deterministic_pipeline(
    *,
    con: duckdb.DuckDBPyConnection,
    df_to_match: duckdb.DuckDBPyRelation,
    df_canonical: duckdb.DuckDBPyRelation,
    enabled_stage_names: Optional[list[str]] = None,
    pipeline_name: str,
    debug_options: Optional[DebugOptions] = None,
    explain: bool = False,
) -> duckdb.DuckDBPyRelation:
    """Run deterministic matching pipeline using run_deterministic_match_pass."""
    if enabled_stage_names:
        print(f"Running with additional enabled stages: {enabled_stage_names}")

    relation = run_deterministic_match_pass(
        con,
        df_to_match,
        df_canonical,
        enabled_stage_names=enabled_stage_names,
        debug_options=debug_options,
        explain=explain,
    )
    show_relation(
        f"Final matches from deterministic pipeline: {pipeline_name}", relation
    )
    return relation


def show_relation(
    title: str,
    relation: duckdb.DuckDBPyRelation,
    *,
    limit: Optional[int] = None,
) -> None:
    """Display a DuckDB relation with optional row limit."""
    print(f"\n=== {title} ===")
    relation_to_show = relation.limit(limit) if limit is not None else relation
    relation_to_show.show(max_width=20000)
