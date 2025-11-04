from __future__ import annotations

from typing import TYPE_CHECKING, Iterable, Optional

from uk_address_matcher.linking_model.exact_matching.matching_stages import (
    _STAGE_REGISTRY,
    _finalise_results,
    _normalise_optional_stage_names,
)
from uk_address_matcher.sql_pipeline.runner import InputBinding, create_sql_pipeline
from uk_address_matcher.sql_pipeline.validation import ColumnSpec, validate_tables

if TYPE_CHECKING:
    import duckdb

    from uk_address_matcher.sql_pipeline.runner import DebugOptions

EXPECTED_COLUMNS = [
    ColumnSpec("unique_id"),
    ColumnSpec("original_address_concat"),
    ColumnSpec("postcode"),
    ColumnSpec("ukam_address_id"),
]


def run_deterministic_match_pass(
    con: duckdb.DuckDBPyConnection,
    df_addresses_to_match: duckdb.DuckDBPyRelation,
    df_addresses_to_search_within: duckdb.DuckDBPyRelation,
    *,
    enabled_stage_names: Optional[Iterable[str]] = None,
    debug_options: Optional[DebugOptions] = None,
    explain: bool = False,
) -> duckdb.DuckDBPyRelation:
    """Run the deterministic matching pipeline with the configured exact stages.

    Strategy:
    1. For each stage, filter fuzzy to only unmatched IDs (anti-join on accumulated matches)
    2. Run matching stage on filtered subset
    3. Extract matched records (match_reason != 'UNMATCHED') with narrow projection
    4. Accumulate all matched relations
    5. Union all matches and join back to original fuzzy table

    Parameters
    ----------
    con:
        Active DuckDB connection.
    df_addresses_to_match:
        Relation holding the fuzzy records we want to resolve.
    df_addresses_to_search_within:
        Relation providing the canonical search space.
    enabled_stage_names:
        Optional iterable of stage names (excluding `annotate_exact_matches`)
        to execute after the initial exact pass.
    debug_options:
        Optional `DebugOptions` to forward to the pipeline runner.
    explain:
        If True, show the execution plan for each stage without running.

    Returns
    -------
    duckdb.DuckDBPyRelation
        Relation containing all fuzzy input rows annotated with any matches discovered
        by the configured deterministic stages.
    """
    validate_tables(
        relations={
            "fuzzy_addresses": df_addresses_to_match,
            "canonical_addresses": df_addresses_to_search_within,
        },
        required=EXPECTED_COLUMNS,
    )

    optional_stage_names = _normalise_optional_stage_names(enabled_stage_names)

    # Track all matched records (narrow projection)
    matches_union: Optional[duckdb.DuckDBPyRelation] = None

    for stage_name in ["annotate_exact_matches", *optional_stage_names]:
        config = _STAGE_REGISTRY[stage_name]

        # Filter out already-matched IDs
        if matches_union is not None:
            df_fuzzy_unmatched = df_addresses_to_match.join(
                matches_union.select("ukam_address_id"), "ukam_address_id", how="anti"
            )
        else:
            # First iteration - use full input
            df_fuzzy_unmatched = df_addresses_to_match

        # Early exit if nothing left to match
        if df_fuzzy_unmatched.count("*").fetchone()[0] == 0:
            break

        # Run matching stage
        pipeline = create_sql_pipeline(
            con,
            [
                InputBinding(config.fuzzy_input_name, df_fuzzy_unmatched),
                InputBinding("canonical_addresses", df_addresses_to_search_within),
            ],
            config.to_stages(),
            pipeline_name=f"Deterministic Exact Match Stage: {stage_name}",
            pipeline_description=(f"Deterministic exact matching stage: {stage_name}"),
        )

        if debug_options is not None or explain:
            pipeline.show_plan()

        stage_result = pipeline.run(options=debug_options, explain=explain)

        if explain:
            continue

        # Extract narrow projection of matched records only
        # Accumulate matches incrementally
        if matches_union is None:
            matches_union = stage_result
        else:
            matches_union = matches_union.union(stage_result)

    if explain:
        return None

    # Final assembly: join back to original fuzzy table
    if matches_union is None:
        return df_addresses_to_match

    # Final join back to original fuzzy table
    return _finalise_results(
        df_addresses_to_match=df_addresses_to_match, matches_union=matches_union
    )


__all__ = ["run_deterministic_match_pass"]
