from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Optional

from uk_address_matcher._typing import StageDiagnostics
from uk_address_matcher.linking_model.matching.stages.base_stage import MatchingStage
from uk_address_matcher.linking_model.matching.utils import (
    format_elapsed,
    relation_sql,
    safe_divide,
)
from uk_address_matcher.sql_pipeline.helpers import _uid
from uk_address_matcher.sql_pipeline.match_reasons import MatchReason
from uk_address_matcher.sql_pipeline.validation import ColumnSpec, validate_tables

if TYPE_CHECKING:
    import duckdb

    from uk_address_matcher.sql_pipeline.runner import DebugOptions

logger = logging.getLogger("uk_address_matcher")


def _duckdb_column_type(
    con: duckdb.DuckDBPyConnection,
    relation: duckdb.DuckDBPyRelation,
    column_name: str,
    fallback_type: str,
) -> str:
    rows = con.execute(
        f"DESCRIBE SELECT {column_name} FROM ({relation.sql_query()})"
    ).fetchall()
    if not rows:
        return fallback_type
    return str(rows[0][1])


def _create_results_table(
    con: duckdb.DuckDBPyConnection,
    df_messy_clean: duckdb.DuckDBPyRelation,
    df_canonical_clean: duckdb.DuckDBPyRelation,
    results_table: str,
) -> None:
    has_ukam_label = "ukam_label" in df_messy_clean.columns
    ukam_label_projection = ", messy.ukam_label" if has_ukam_label else ""

    resolved_canonical_type = _duckdb_column_type(
        con=con,
        relation=df_canonical_clean,
        column_name="unique_id",
        fallback_type="VARCHAR",
    )
    canonical_ukam_type = _duckdb_column_type(
        con=con,
        relation=df_canonical_clean,
        column_name="ukam_address_id",
        fallback_type="BIGINT",
    )

    enum_values = str(MatchReason.enum_values())

    con.execute(f'DROP TABLE IF EXISTS "{results_table}"')
    con.execute(
        f"""
        CREATE TABLE \"{results_table}\" AS
        SELECT
            messy.ukam_address_id,
            messy.unique_id
            {ukam_label_projection},
            NULL::{resolved_canonical_type} AS resolved_canonical_id,
            NULL::{canonical_ukam_type} AS canonical_ukam_address_id,
            NULL::ENUM {enum_values} AS match_reason
        FROM {relation_sql(df_messy_clean)} AS messy
        """
    )


def _get_unmatched(
    con: duckdb.DuckDBPyConnection,
    df_messy_clean: duckdb.DuckDBPyRelation,
    results_table: str,
) -> duckdb.DuckDBPyRelation:
    return con.sql(
        f"""
        SELECT messy.*
        FROM {relation_sql(df_messy_clean)} AS messy
        INNER JOIN \"{results_table}\" AS results
            ON results.ukam_address_id = messy.ukam_address_id
        WHERE results.resolved_canonical_id IS NULL
        """
    )


def _build_final_output(
    con: duckdb.DuckDBPyConnection,
    df_messy_clean: duckdb.DuckDBPyRelation,
    df_canonical_clean: duckdb.DuckDBPyRelation,
    results_table: str,
) -> duckdb.DuckDBPyRelation:
    results_columns = con.table(f'"{results_table}"').columns

    excluded = {
        "ukam_address_id",
        "unique_id",
        "ukam_label",
        "resolved_canonical_id",
        "canonical_ukam_address_id",
        "match_reason",
    }
    additional_columns = [column for column in results_columns if column not in excluded]
    additional_projection = "".join(
        f",\n            results.{column}" for column in additional_columns
    )

    canonical_projection = [
        "canonical.clean_full_address AS clean_full_address_canonical",
        "canonical.postcode AS postcode_canonical",
    ]
    if "original_address_concat" in df_canonical_clean.columns:
        canonical_projection.append(
            "canonical.original_address_concat AS original_address_concat_canonical"
        )
    canonical_projection_sql = ",\n            ".join(canonical_projection)

    return con.sql(
        f"""
        SELECT
            messy.unique_id,
            results.resolved_canonical_id,
            messy.* EXCLUDE(unique_id),
            results.canonical_ukam_address_id,
            results.match_reason
            {additional_projection}
            ,
            {canonical_projection_sql}
        FROM {relation_sql(df_messy_clean)} AS messy
        INNER JOIN \"{results_table}\" AS results
            ON results.ukam_address_id = messy.ukam_address_id
        LEFT JOIN {relation_sql(df_canonical_clean)} AS canonical
            ON canonical.ukam_address_id = results.canonical_ukam_address_id
        """
    )


def _run_matching(
    con: duckdb.DuckDBPyConnection,
    df_messy_clean: duckdb.DuckDBPyRelation,
    df_canonical_clean: duckdb.DuckDBPyRelation,
    *,
    stages: list[MatchingStage],
    debug_options: Optional[DebugOptions] = None,
    explain: bool = False,
) -> tuple[Optional[duckdb.DuckDBPyRelation], StageDiagnostics]:
    """Run matching stages sequentially and return unified results.

    Each stage receives only the still-unmatched messy records. Matches found
    by earlier stages are never revisited.

    Args:
        con: DuckDB connection.
        df_messy_clean: Cleaned messy addresses to match.
        df_canonical_clean: Cleaned canonical addresses to match against.
        stages: Ordered list of ``MatchingStage`` instances to execute.
        debug_options: Optional debug/trace settings.
        explain: If ``True``, run stages in explain-only mode.

    Returns:
        A DuckDB relation containing all messy records with match results
        joined to canonical address details, or ``None`` when ``explain=True``.
        Also returns stage diagnostics for executed stages.
    """
    validate_tables(
        relations={"messy_addresses": df_messy_clean},
        required=[
            ColumnSpec("unique_id"),
            ColumnSpec("original_address_concat"),
            ColumnSpec("postcode"),
            ColumnSpec("ukam_address_id"),
        ],
    )
    validate_tables(
        relations={"canonical_addresses": df_canonical_clean},
        required=[
            ColumnSpec("unique_id"),
            ColumnSpec("clean_full_address"),
            ColumnSpec("postcode"),
            ColumnSpec("ukam_address_id"),
        ],
    )

    run_id = _uid()
    results_table = f"__ukam_results_{run_id}"
    _create_results_table(
        con=con,
        df_messy_clean=df_messy_clean,
        df_canonical_clean=df_canonical_clean,
        results_table=results_table,
    )

    total_input_rows = df_messy_clean.count("*").fetchone()[0]
    stage_diagnostics: StageDiagnostics = []

    for stage in stages:
        stage_name = _stage_name_for_instance(stage)

        unmatched_count = con.execute(
            f'SELECT COUNT(*) FROM "{results_table}" WHERE resolved_canonical_id IS NULL'
        ).fetchone()[0]

        if unmatched_count == 0:
            logger.info(
                "All records matched; skipping stage '%s' and remaining stages.",
                stage_name,
            )
            break

        logger.info(
            "Running stage '%s' with %d unmatched records...",
            stage_name,
            unmatched_count,
        )

        df_unmatched = _get_unmatched(con, df_messy_clean, results_table)

        started_at = time.perf_counter()
        stage.run(
            con=con,
            stage_name=stage_name,
            results_table=results_table,
            df_unmatched=df_unmatched,
            df_canonical=df_canonical_clean,
            debug_options=debug_options,
            explain=explain,
        )
        elapsed_seconds = time.perf_counter() - started_at
        logger.debug(
            "Stage '%s' completed in %s.",
            stage_name,
            format_elapsed(elapsed_seconds),
        )

        if explain:
            continue

        remaining = con.execute(
            f'SELECT COUNT(*) FROM "{results_table}" WHERE resolved_canonical_id IS NULL'
        ).fetchone()[0]
        matched_this_stage = unmatched_count - remaining
        stage_diagnostics.append(
            {
                "stage": stage_name,
                "unmatched_before": int(unmatched_count),
                "matched_this_stage": int(matched_this_stage),
                "remaining_after": int(remaining),
                "matched_pct_of_unmatched": round(
                    safe_divide(
                        int(matched_this_stage),
                        int(unmatched_count),
                    ),
                    6,
                ),
                "matched_pct_of_input": round(
                    safe_divide(
                        int(matched_this_stage),
                        int(total_input_rows),
                    ),
                    6,
                ),
                "elapsed_seconds": round(float(elapsed_seconds), 6),
            }
        )
        logger.info(
            "Stage '%s' matched %d records (%d remaining).",
            stage_name,
            matched_this_stage,
            remaining,
        )

    if explain:
        con.execute(f'DROP TABLE IF EXISTS "{results_table}"')
        return None, stage_diagnostics

    result = _build_final_output(
        con=con,
        df_messy_clean=df_messy_clean,
        df_canonical_clean=df_canonical_clean,
        results_table=results_table,
    )

    final_table = f"__ukam_final_matches_{run_id}"
    con.execute(f'DROP TABLE IF EXISTS "{final_table}"')
    result.to_table(f'"{final_table}"')
    final_result = con.table(f'"{final_table}"')

    con.execute(f'DROP TABLE IF EXISTS "{results_table}"')

    return final_result, stage_diagnostics


def _stage_name_for_instance(stage: MatchingStage) -> str:
    """Derive a human-readable stage name from a stage instance."""
    from uk_address_matcher.linking_model.matching.stages import (
        ExactMatchStage,
        PeeledAddressStage,
        UniqueTrigramStage,
    )
    from uk_address_matcher.linking_model.matching.stages.splink import SplinkStage

    _names = {
        ExactMatchStage: "exact_matches",
        UniqueTrigramStage: "unique_trigram",
        PeeledAddressStage: "peeled_address",
        SplinkStage: "splink",
    }
    return _names.get(type(stage), stage.__class__.__name__.lower())
