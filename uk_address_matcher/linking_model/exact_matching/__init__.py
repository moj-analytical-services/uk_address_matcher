from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from uk_address_matcher.linking_model.exact_matching.exact_matching_model import (
    _annotate_exact_matches,
    _filter_unmatched_exact_matches,
    _resolve_with_trie,
)
from uk_address_matcher.sql_pipeline.runner import InputBinding, create_sql_pipeline
from uk_address_matcher.sql_pipeline.validation import ColumnSpec, validate_table

if TYPE_CHECKING:
    import duckdb

    from uk_address_matcher.sql_pipeline.runner import DebugOptions


# TODO(ThomasHepworth): move this upstream to cleaning once we have agreed a
# standard schema for input addresses.
CANONICAL_TABLE_REQUIRED_SCHEMA: list[ColumnSpec] = [ColumnSpec("unique_id", "BIGINT")]


def run_deterministic_match_pass(
    con: duckdb.DuckDBPyConnection,
    df_addresses_to_match: duckdb.DuckDBPyRelation,
    df_addresses_to_search_within: duckdb.DuckDBPyRelation,
    *,
    debug_options: Optional[DebugOptions] = None,
) -> duckdb.DuckDBPyRelation:
    """
    Run the exact matching pipeline stages to annotate fuzzy addresses with exact matches
    from the canonical dataset, and then resolve remaining unmatched fuzzy addresses using
    a trie-based approach.

    Args:
        con: DuckDB connection to use for running the pipeline.
        df_fuzzy: Relation containing the fuzzy addresses to be matched.
        df_canonical: Relation containing the canonical addresses to match against.
        match_using_trie: Whether to run the trie resolution stage after exact matching.
            Defaults to True. Trie matching will capture additional matches but may
            also introduce some false positives and is more computationally intensive.

    Returns:
        Relation with fuzzy addresses annotated with exact matches and trie-resolved matches.
    """

    input_bindings = [
        InputBinding("fuzzy_addresses", df_addresses_to_match),
        InputBinding("canonical_addresses", df_addresses_to_search_within),
    ]

    validate_table(
        df_addresses_to_search_within,
        required=CANONICAL_TABLE_REQUIRED_SCHEMA,
        label="Canonical addresses",
    )

    two_phase_pipeline = create_sql_pipeline(
        con,
        input_bindings,
        [_annotate_exact_matches, _filter_unmatched_exact_matches, _resolve_with_trie],
        pipeline_name="Exact + Trie",
        pipeline_description="Exact matches followed by trie resolution",
    )
    if debug_options is not None:
        if debug_options.debug_mode:
            two_phase_pipeline.show_plan()
    exact_match_results = two_phase_pipeline.run(options=debug_options)

    return exact_match_results


__all__ = ["run_deterministic_match_pass"]
