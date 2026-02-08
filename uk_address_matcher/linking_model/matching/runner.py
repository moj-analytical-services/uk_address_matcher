from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Iterable, Optional

from uk_address_matcher.linking_model.matching.registry import (
    StageName,
    StageInput,
    _STAGE_REGISTRY,
    _normalise_stage_list,
    _stage_name_for_instance,
)
from uk_address_matcher.linking_model.matching.stages.base_stage import MatchingStage
from uk_address_matcher.sql_pipeline.helpers import _uid
from uk_address_matcher.sql_pipeline.validation import ColumnSpec, validate_tables

if TYPE_CHECKING:
    import duckdb

    from uk_address_matcher.sql_pipeline.runner import DebugOptions

logger = logging.getLogger("uk_address_matcher")


def _get_unmatched(
    con: duckdb.DuckDBPyConnection,
    df_messy_clean: duckdb.DuckDBPyRelation,
    results_table: str,
) -> duckdb.DuckDBPyRelation:
    """Join unmatched result rows back to full cleaned messy data for features."""
    return con.sql(f"""
        SELECT f.*
        FROM ({df_messy_clean.sql_query()}) AS f
        INNER JOIN {results_table} AS r
            ON f.ukam_address_id = r.ukam_address_id
        WHERE r.resolved_canonical_id IS NULL
    """)


def _build_final_output(
    con: duckdb.DuckDBPyConnection,
    df_canonical_clean: duckdb.DuckDBPyRelation,
    results_table: str,
) -> duckdb.DuckDBPyRelation:
    """Join __ukam_results with canonical data for enriched output."""

    # Dynamically detect any extra columns stages added (e.g. match_weight)
    results_columns = [
        c
        for c in con.table(results_table).columns
        if c
        not in (
            "ukam_address_id",
            "unique_id",
            "resolved_canonical_id",
            "canonical_ukam_address_id",
            "match_reason",
        )
    ]
    extra_cols_select = "".join(f", r.{c}" for c in results_columns)

    sql = f"""
        SELECT
            r.unique_id,
            r.resolved_canonical_id,
            r.match_reason,
            r.canonical_ukam_address_id,
            r.ukam_address_id
            {extra_cols_select},
            c.original_address_concat AS original_address_concat_canonical,
            c.postcode AS postcode_canonical
        FROM {results_table} AS r
        LEFT JOIN ({df_canonical_clean.sql_query()}) AS c
            ON r.canonical_ukam_address_id = c.ukam_address_id
    """

    return con.sql(sql)


def run_matching(
    con: duckdb.DuckDBPyConnection,
    df_messy_clean: duckdb.DuckDBPyRelation,
    df_canonical_clean: duckdb.DuckDBPyRelation,
    *,
    stages: Optional[Iterable[StageInput]] = None,
    debug_options: Optional[DebugOptions] = None,
) -> duckdb.DuckDBPyRelation:
    """Run all matching stages sequentially and return unified results.

    Parameters
    ----------
    con :
        Active DuckDB connection.
    df_messy_clean :
        Cleaned messy addresses (output of prepare_data_for_matching).
    df_canonical_clean :
        Cleaned canonical addresses (output of prepare_data_for_matching).
    stages :
        Ordered list of stage names or stage instances to run. Defaults to
        [EXACT_MATCHES, SPLINK]. EXACT_MATCHES is always
        included even if omitted.
    debug_options :
        Debug options passed to stages that support them.
    Returns
    -------
    duckdb.DuckDBPyRelation
        Relation with columns: unique_id, resolved_canonical_id,
        match_reason, canonical_ukam_address_id, ukam_address_id,
        plus any stage-added columns (e.g. match_weight, distinguishability),
        plus original_address_concat_canonical and postcode_canonical.
    """

    validate_tables(
        relations={
            "messy_addresses": df_messy_clean,
            "canonical_addresses": df_canonical_clean,
        },
        required=[
            ColumnSpec("unique_id"),
            ColumnSpec("original_address_concat"),
            ColumnSpec("postcode"),
            ColumnSpec("ukam_address_id"),
        ],
    )

    # Resolve stage list
    ordered_stages = _normalise_stage_list(stages)

    # Create __ukam_results table upfront with all messy rows
    uid = _uid()
    results_table = f"__ukam_results_{uid}"
    con.execute(f"DROP TABLE IF EXISTS {results_table}")

    # Include ukam_label if present in messy data (used for accuracy evaluation)
    has_ukam_label = "ukam_label" in df_messy_clean.columns
    ukam_label_col = ", ukam_label" if has_ukam_label else ""

    con.execute(f"""
        CREATE TABLE {results_table} AS
        SELECT
            ukam_address_id,
            unique_id
            {ukam_label_col},
            NULL::VARCHAR AS resolved_canonical_id,
            NULL::VARCHAR AS canonical_ukam_address_id,
            NULL::VARCHAR AS match_reason
        FROM ({df_messy_clean.sql_query()})
    """)

    # Run each stage sequentially
    for stage_item in ordered_stages:
        if isinstance(stage_item, MatchingStage):
            stage = stage_item
            stage_name = _stage_name_for_instance(stage_item)
        else:
            stage_name = stage_item
            stage = _STAGE_REGISTRY[stage_name]

        df_unmatched = _get_unmatched(con, df_messy_clean, results_table)

        unmatched_count = df_unmatched.count("*").fetchone()[0]
        if unmatched_count == 0:
            logger.info(
                "All records matched; skipping stage '%s' and remaining stages.",
                stage_name.value,
            )
            break

        logger.info(
            "Running stage '%s' with %d unmatched records...",
            stage_name.value,
            unmatched_count,
        )

        stage.run(
            con,
            stage_name.value,
            results_table,
            df_unmatched,
            df_canonical_clean,
            debug_options,
        )

        # Log how many were matched by this stage
        remaining = con.execute(
            f"SELECT COUNT(*) FROM {results_table} WHERE resolved_canonical_id IS NULL"
        ).fetchone()[0]
        matched_this_stage = unmatched_count - remaining
        logger.info(
            "Stage '%s' matched %d records (%d remaining).",
            stage_name.value,
            matched_this_stage,
            remaining,
        )

    # Build final enriched output
    return _build_final_output(con, df_canonical_clean, results_table)
