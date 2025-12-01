from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Callable, Iterable, Literal, Optional, Union

from uk_address_matcher.linking_model.exact_matching.annotate_exact_matches import (
    _annotate_exact_matches,
)
from uk_address_matcher.linking_model.exact_matching.input_filters import (
    _restrict_canonical_to_fuzzy_postcodes,
)
from uk_address_matcher.linking_model.exact_matching.resolve_with_trigrams import (
    _resolve_with_trigrams,
)
from uk_address_matcher.sql_pipeline.helpers import _uid
from uk_address_matcher.sql_pipeline.runner import InputBinding, create_sql_pipeline
from uk_address_matcher.sql_pipeline.validation import ColumnSpec, validate_tables

if TYPE_CHECKING:
    import duckdb

    from uk_address_matcher.sql_pipeline.runner import DebugOptions
    from uk_address_matcher.sql_pipeline.steps import Stage


FuzzyInputName = Literal["fuzzy_addresses", "unmatched_records"]
StageFactory = Callable[[FuzzyInputName], "Stage"]


class StageName(str, Enum):
    """Available exact matching stages."""

    EXACT_MATCHES = "exact_matches"
    UNIQUE_TRIGRAM = "unique_trigram"


@dataclass(frozen=True)
class ExactMatchStageConfig:
    """A basic stage configuration for exact matching pipeline stages.

    Parameters
    ----------
    factory:
        The primary factory function that produces the stage. Accepts a `fuzzy_input_name`
        parameter to specify which input table the stage should read from.
    pre_filter_canonical:
        The strategy to determine what canonical addresses are considered for
        matching. See `PostcodeStrategy` for details.
    fuzzy_input_name:
        The placeholder name for the fuzzy input table. Defaults to "fuzzy_addresses".
        Should be set to "unmatched_records" for stages that run after filtering.
    """

    factory: StageFactory
    pre_filter_canonical: Optional[Stage] = None

    def to_stages(self) -> list[Stage]:
        """Build the stage queue for this exact matching stage."""
        stages: list[Stage] = []
        if self.pre_filter_canonical is not None:
            stages.append(self.pre_filter_canonical)
        stages.append(self.factory)
        return stages


_STAGE_REGISTRY: dict[StageName, ExactMatchStageConfig] = {
    StageName.EXACT_MATCHES: ExactMatchStageConfig(
        factory=_annotate_exact_matches,
        pre_filter_canonical=_restrict_canonical_to_fuzzy_postcodes("exact"),
    ),
    StageName.UNIQUE_TRIGRAM: ExactMatchStageConfig(
        factory=_resolve_with_trigrams(
            ngram_size=3,
            min_unique_hits=1,
            include_conflicts=False,
            include_trigram_text=True,
        ),
        pre_filter_canonical=_restrict_canonical_to_fuzzy_postcodes("exact"),
    ),
}

_ALWAYS_ON: tuple[StageName, ...] = (StageName.EXACT_MATCHES,)


def available_deterministic_stages() -> list[StageName]:
    """Get a list of available deterministic matching stage names that can be enabled.

    Returns stages that can be enabled via enabled_stage_names.
    EXACT_MATCHES is always on and excluded from this list.
    """
    return [s for s in _STAGE_REGISTRY if s not in _ALWAYS_ON]


StageInput = Union[StageName, str]


def _normalise_enabled_stages(
    enabled: Optional[Iterable[StageInput]],
) -> list[StageName]:
    """Validate optional stage configuration while preserving order."""
    if enabled is None:
        return []

    out: list[StageName] = []
    seen: set[StageName] = set()

    for item in enabled:
        try:
            name = item if isinstance(item, StageName) else StageName(item)
        except ValueError as e:
            allowed = ", ".join(s.value for s in available_deterministic_stages())
            raise ValueError(
                f"Unknown exact matching stage: {item!r}. Available stages: {allowed}"
            ) from e

        if name in _ALWAYS_ON:
            raise ValueError(
                f"{name.value} is always enabled and should not be provided."
            )

        if name in seen:
            raise ValueError(f"Duplicate exact matching stage specified: {name.value}")

        seen.add(name)
        out.append(name)

    return out


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


def _get_unmatched_subset(
    con: duckdb.DuckDBPyConnection,
    df_addresses_to_match: duckdb.DuckDBPyRelation,
    matches_table_name: str,
    has_matches: bool,
) -> duckdb.DuckDBPyRelation:
    """Filter to records not yet matched using anti-join against materialised table."""
    if not has_matches:
        return df_addresses_to_match
    # Use SQL anti-join against the materialised matches table for efficiency
    return con.sql(f"""
        SELECT f.*
        FROM ({df_addresses_to_match.sql_query()}) AS f
        WHERE f.ukam_address_id NOT IN (
            SELECT ukam_address_id FROM {matches_table_name}
        )
    """)


def _run_stage(
    con: duckdb.DuckDBPyConnection,
    stage_name: StageName,
    df_fuzzy_unmatched: duckdb.DuckDBPyRelation,
    df_addresses_to_search_within: duckdb.DuckDBPyRelation,
    debug_options: Optional[DebugOptions] = None,
    explain: bool = False,
) -> Optional[duckdb.DuckDBPyRelation]:
    """Execute a single matching stage and return results."""

    config = _STAGE_REGISTRY[stage_name]

    pipeline = create_sql_pipeline(
        con,
        [
            InputBinding("fuzzy_addresses", df_fuzzy_unmatched),
            InputBinding("canonical_addresses", df_addresses_to_search_within),
        ],
        config.to_stages(),
        pipeline_name=f"Deterministic Exact Match Stage: {stage_name.value}",
        pipeline_description=f"Deterministic exact matching stage: {stage_name.value}",
    )

    if debug_options is not None or explain:
        pipeline.show_plan()

    return pipeline.run(options=debug_options, explain=explain)


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
        Optional iterable of stage names to enable. Pass as Iterable[StageName] (preferred)
        or Iterable[str] for backward compatibility. exact_matches is always enabled.
        Use available_deterministic_stages() to discover available stages that can be enabled.
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
    # which cause exponential slowdown as stages are added
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

        # Stages can return any number of columns, but MUST include:
        # - ukam_address_id
        # - canonical_ukam_address_id
        # - resolved_canonical_id
        # - match_reason
        stage_result = _run_stage(
            con,
            stage_name,
            df_fuzzy_unmatched,
            df_addresses_to_search_within,
            debug_options,
            explain,
        ).select(
            "ukam_address_id, canonical_ukam_address_id, resolved_canonical_id, match_reason"
        )

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
    # This is necessary because DuckDB uses lazy evaluation
    result = _finalise_results(df_addresses_to_match, con.table(matches_table))
    result.to_table(f"__ukam_final_matches_{uid}")
    final_result = con.table(f"__ukam_final_matches_{uid}")

    # Clean up temporary tables
    con.execute(f"DROP TABLE IF EXISTS {matches_table}")

    return final_result
