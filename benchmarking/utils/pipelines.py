from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from uk_address_matcher.linking_model.matching import (
    ExactMatchStage,
    StageName,
    TrigramStage,
    run_matching,
)

if TYPE_CHECKING:
    import duckdb

    from uk_address_matcher.sql_pipeline.runner import DebugOptions


def run_deterministic_pipeline(
    *,
    con: duckdb.DuckDBPyConnection,
    df_to_match: duckdb.DuckDBPyRelation,
    df_canonical: duckdb.DuckDBPyRelation,
    enabled_stage_names: Optional[list[StageName | str]] = None,
    pipeline_name: str,
    debug_options: Optional[DebugOptions] = None,
) -> duckdb.DuckDBPyRelation:
    """Run deterministic matching pipeline using the unified run_matching API."""
    if enabled_stage_names:
        print(f"Running with additional enabled stages: {enabled_stage_names}")

    stages = [ExactMatchStage()]
    if enabled_stage_names:
        for stage in enabled_stage_names:
            name = stage if isinstance(stage, StageName) else StageName(stage)
            if name is StageName.UNIQUE_TRIGRAM:
                stages.append(TrigramStage())
            elif name is StageName.EXACT_MATCHES:
                continue
            else:
                raise ValueError(f"Unsupported deterministic stage: {name.value}.")

    relation = run_matching(
        con=con,
        df_messy_clean=df_to_match,
        df_canonical_clean=df_canonical,
        stages=stages,
        debug_options=debug_options,
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
