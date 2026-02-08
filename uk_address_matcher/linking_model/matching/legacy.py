"""Legacy API — backward-compatible wrappers from before the unified stage runner.

These functions are deprecated and will be removed in a future version.
Use ``run_matching()`` instead.
"""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Iterable, Optional

from uk_address_matcher.linking_model.matching.registry import (
    StageName,
    StageInput,
    _ALWAYS_ON,
    _STAGE_REGISTRY,
    _normalise_enabled_stages,
)
from uk_address_matcher.sql_pipeline.helpers import _uid
from uk_address_matcher.sql_pipeline.validation import ColumnSpec, validate_tables

if TYPE_CHECKING:
    import duckdb

    from uk_address_matcher.sql_pipeline.runner import DebugOptions


def _finalise_results(
    df_addresses_to_match: duckdb.DuckDBPyRelation,
    matches_union: duckdb.DuckDBPyRelation,
) -> duckdb.DuckDBPyRelation:
    """Join matches back to original fuzzy table to produce final annotated output.

    Handles precedence if multiple stages matched the same ID (first stage wins).
    """
    # Prepare match results with renamed columns to avoid conflicts
    matched_records = matches_union.select("""
        ukam_address_id,
        resolved_canonical_id,
        canonical_ukam_address_id,
        match_reason
    """)

    # Join matches back to original fuzzy addresses
    fuzzy_with_matches = df_addresses_to_match.join(
        matched_records,
        "ukam_address_id",
        how="left",
    )

    # Reorder our columns to enhance readability
    return fuzzy_with_matches.select("""
        unique_id,
        resolved_canonical_id,
        * EXCLUDE (unique_id, resolved_canonical_id, canonical_ukam_address_id, match_reason),
        canonical_ukam_address_id,
        match_reason
    """)


def _run_stage(
    con: duckdb.DuckDBPyConnection,
    stage_name: StageName,
    df_fuzzy_unmatched: duckdb.DuckDBPyRelation,
    df_addresses_to_search_within: duckdb.DuckDBPyRelation,
    debug_options: Optional[DebugOptions] = None,
    explain: bool = False,
) -> Optional[duckdb.DuckDBPyRelation]:
    """Execute a single matching stage and return results as a relation.

    .. deprecated::
        Use ``run_matching()`` instead.
    """
    uid = _uid()
    results_table = f"__ukam_legacy_stage_{uid}"
    con.execute(f"DROP TABLE IF EXISTS {results_table}")

    # Create a results table with all fuzzy rows
    con.execute(f"""
        CREATE TABLE {results_table} AS
        SELECT
            ukam_address_id,
            unique_id,
            NULL::VARCHAR AS resolved_canonical_id,
            NULL::VARCHAR AS canonical_ukam_address_id,
            NULL::VARCHAR AS match_reason
        FROM ({df_fuzzy_unmatched.sql_query()})
    """)

    # Run the stage via its run() method
    stage = _STAGE_REGISTRY[stage_name]
    stage.run(
        con, stage_name.value, results_table,
        df_fuzzy_unmatched, df_addresses_to_search_within, debug_options,
    )

    # Return matched rows as a relation
    result = con.sql(f"""
        SELECT
            ukam_address_id,
            canonical_ukam_address_id,
            resolved_canonical_id,
            match_reason
        FROM {results_table}
        WHERE resolved_canonical_id IS NOT NULL
    """)

    # Materialise before cleanup
    final_table = f"__ukam_legacy_result_{uid}"
    con.execute(f"DROP TABLE IF EXISTS {final_table}")
    result.create(final_table)
    final = con.table(final_table)

    con.execute(f"DROP TABLE IF EXISTS {results_table}")

    return final


def _get_unmatched_subset(
    con: duckdb.DuckDBPyConnection,
    df_addresses_to_match: duckdb.DuckDBPyRelation,
    matches_table_name: str,
    has_matches: bool,
) -> duckdb.DuckDBPyRelation:
    """Filter to records not yet matched using anti-join against materialised table."""
    if not has_matches:
        return df_addresses_to_match
    return con.sql(f"""
        SELECT f.*
        FROM ({df_addresses_to_match.sql_query()}) AS f
        WHERE f.ukam_address_id NOT IN (
            SELECT ukam_address_id FROM {matches_table_name}
        )
    """)


def run_deterministic_match_pass(
    con: duckdb.DuckDBPyConnection,
    df_addresses_to_match: duckdb.DuckDBPyRelation,
    df_addresses_to_search_within: duckdb.DuckDBPyRelation,
    *,
    enabled_stage_names: Optional[Iterable[StageInput]] = None,
    debug_options: Optional[DebugOptions] = None,
    explain: bool = False,
) -> duckdb.DuckDBPyRelation:
    """Run the deterministic matching pipeline with the configured exact stages.

    .. deprecated::
        Use ``run_matching()`` instead.

    Parameters
    ----------
    con:
        Active DuckDB connection.
    df_addresses_to_match:
        Relation holding the fuzzy records we want to resolve.
    df_addresses_to_search_within:
        Relation providing the canonical search space.
    enabled_stage_names:
        Optional iterable of stage names to enable. exact_matches is always enabled.
        Use available_deterministic_stages() to discover available stages.
    debug_options:
        Optional ``DebugOptions`` to forward to the pipeline runner.
    explain:
        If True, show the execution plan for each stage without running.

    Returns
    -------
    duckdb.DuckDBPyRelation
        Relation containing all fuzzy input rows annotated with any matches.
    """
    warnings.warn(
        "run_deterministic_match_pass() is deprecated. Use run_matching() instead.",
        DeprecationWarning,
        stacklevel=2,
    )

    validate_tables(
        relations={
            "fuzzy_addresses": df_addresses_to_match,
            "canonical_addresses": df_addresses_to_search_within,
        },
        required=[
            ColumnSpec("unique_id"),
            ColumnSpec("original_address_concat"),
            ColumnSpec("postcode"),
            ColumnSpec("ukam_address_id"),
        ],
    )

    # Build ordered stage list: always-on first, then optional user-specified
    ordered: list[StageName] = list(_ALWAYS_ON)
    for stage in _normalise_enabled_stages(enabled_stage_names):
        if stage not in ordered:
            ordered.append(stage)

    # Use a materialised table for accumulated matches to avoid lazy relation chains
    uid = _uid()
    matches_table = f"__ukam_exact_matches_{uid}"
    has_matches = False

    for stage_index, stage_name in enumerate(ordered):
        df_fuzzy_unmatched = _get_unmatched_subset(
            con, df_addresses_to_match, matches_table, has_matches
        )

        # Early exit if nothing left to match
        if df_fuzzy_unmatched.count("*").fetchone()[0] == 0:
            break

        stage_result = _run_stage(
            con,
            stage_name,
            df_fuzzy_unmatched,
            df_addresses_to_search_within,
            debug_options,
            explain,
        )

        if stage_result is None:
            continue

        if explain:
            continue

        # Materialise results into the accumulator table
        if stage_index == 0:
            con.execute(f"DROP TABLE IF EXISTS {matches_table}")
            stage_result.create(matches_table)
        else:
            stage_result.insert_into(matches_table)
        has_matches = True

    if explain:
        return None

    if not has_matches:
        return df_addresses_to_match

    # Materialise the final result before cleaning up the temporary table
    result = _finalise_results(df_addresses_to_match, con.table(matches_table))
    result.to_table(f"__ukam_final_matches_{uid}")
    final_result = con.table(f"__ukam_final_matches_{uid}")

    # Clean up temporary tables
    con.execute(f"DROP TABLE IF EXISTS {matches_table}")

    return final_result
