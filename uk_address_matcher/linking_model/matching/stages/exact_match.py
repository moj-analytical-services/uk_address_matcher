from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, Optional

from uk_address_matcher.linking_model.matching.input_filters import (
    _restrict_canonical_to_messy_postcodes,
)
from uk_address_matcher.linking_model.matching.stages.base_stage import MatchingStage
from uk_address_matcher.sql_pipeline.match_reasons import MatchReason
from uk_address_matcher.sql_pipeline.steps import CTEStep, pipeline_stage

if TYPE_CHECKING:
    import duckdb

    from uk_address_matcher.sql_pipeline.runner import DebugOptions

MessyInputName = Literal["__ukam__tmp_messy_addresses", "unmatched_records"]


def _remove_flat_keyword_sql(column_reference: str) -> str:
    """Return SQL that strips standalone FLAT and re-normalises spaces."""

    return f"""
        TRIM(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    ' ' || {column_reference} || ' ',
                    '\\bFLAT\\b',
                    ' ',
                    'g'
                ),
                '\\s+',
                ' ',
                'g'
            )
        )
    """


def _flat_field_compatibility_sql() -> str:
    """Return SQL requiring flat fields to be non-contradictory.

    We allow one-sided nulls and only reject explicit conflicts.
    """

    return """
       (
           messy.flat_number IS NULL
           OR canon.flat_number IS NULL
           OR messy.flat_number = canon.flat_number
       )
       AND (
           messy.flat_letter IS NULL
           OR canon.flat_letter IS NULL
           OR messy.flat_letter = canon.flat_letter
       )
       AND (
           messy.flat_positional IS NULL
           OR canon.flat_positional IS NULL
           OR messy.flat_positional = canon.flat_positional
       )
    """


def _flat_retraction_unit_evidence_sql(alias: str) -> str:
    """Return SQL for independent sub-unit evidence using parsed columns.

    This intentionally relies on structured features rather than regex over
    ``clean_full_address``.
    """

    return f"""
        (
            {alias}.flat_number IS NOT NULL
            OR {alias}.flat_letter IS NOT NULL
            OR {alias}.flat_positional IS NOT NULL
            OR COALESCE({alias}.has_business_unit, FALSE)
            OR {alias}.business_unit_id IS NOT NULL
            OR COALESCE(array_length({alias}.numeric_tokens), 0) >= 2
        )
    """


@dataclass(frozen=True, repr=False)
class ExactMatchStage(MatchingStage):
    """Deterministic exact matching on ``clean_full_address`` and ``postcode``.

    This is usually the first stage in a pipeline. It accepts the easy,
    unambiguous cases before any probabilistic matching is attempted.

    Phase 1 matches on exact ``clean_full_address + postcode``.  When multiple
    canonical records share the same address text the first is returned
    (deterministic ORDER BY ``canonical_unique_id``).

    Phase 2 retries after removing the standalone ``FLAT`` keyword and only
    emits a match when there is exactly one canonical candidate for the messy
    record, parsed flat fields are consistent, and there is independent parsed
    unit evidence on either side.

    Set ``enable_flat_retraction=False`` to run strict phase 1 only.

    """

    enable_flat_retraction: bool = True

    def find_matches(
        self,
        con: duckdb.DuckDBPyConnection,
        stage_name: str,
        df_unmatched: duckdb.DuckDBPyRelation,
        df_canonical: duckdb.DuckDBPyRelation,
        debug_options: Optional[DebugOptions] = None,
        explain: bool = False,
    ) -> Optional[duckdb.DuckDBPyRelation]:
        from uk_address_matcher.linking_model.matching.stages._sql_helpers import (
            run_sql_pipeline,
        )

        return run_sql_pipeline(
            con=con,
            pipeline_stages=[
                _restrict_canonical_to_messy_postcodes("exact"),
                _exact_matches(
                    "__ukam__tmp_messy_addresses",
                    enable_phase_2_flat_retraction=self.enable_flat_retraction,
                ),
            ],
            stage_name=stage_name,
            df_unmatched=df_unmatched,
            df_canonical=df_canonical,
            debug_options=debug_options,
            explain=explain,
        )


@pipeline_stage(
    name="exact_matches",
    description="Match using exact hash-join on clean_full_address + postcode",
    tags=["phase_1", "matching"],
    depends_on=["restrict_canonical_to_messy_postcodes"],
)
def _exact_matches(
    messy_input_name: MessyInputName = "__ukam__tmp_messy_addresses",
    *,
    enable_phase_2_flat_retraction: bool = True,
) -> list[CTEStep]:
    """Find deterministic matches using a two-phase exact strategy.

    Parameters
    ----------
    messy_input_name:
        The placeholder name for the messy input table. Defaults to
        "__ukam__tmp_messy_addresses" for the initial pass. Can be set
        to "unmatched_records" when running after filtering.
    enable_phase_2_flat_retraction:
        If ``True``, run phase 2 and emit
        ``exact_flat_retraction: match after removing FLAT keyword`` when it
        yields an unambiguous candidate. If ``False``, skip phase 2 entirely
        and emit only strict phase 1 exact matches.
    """
    messy_without_flat = _remove_flat_keyword_sql("messy.clean_full_address")
    canon_without_flat = _remove_flat_keyword_sql("canon.clean_full_address")
    flat_compatibility_condition = _flat_field_compatibility_sql()
    unit_evidence_condition = f"""
        (
            {_flat_retraction_unit_evidence_sql("messy")}
            OR {_flat_retraction_unit_evidence_sql("canon")}
        )
    """
    exact_match_condition = """
        messy.clean_full_address = canon.clean_full_address
        AND messy.postcode = canon.postcode
    """
    keyword_stripped_match_condition = f"""
        {messy_without_flat} = {canon_without_flat}
        AND messy.postcode = canon.postcode
    """

    exact_value = MatchReason.EXACT.value
    exact_flat_retraction_value = MatchReason.EXACT_FLAT_RETRACTION.value
    enum_values = str(MatchReason.enum_values())

    exact_phase_1_matches_sql = f"""
        SELECT
            candidates.ukam_address_id AS ukam_address_id,
            MIN(candidates.canonical_ukam_address_id) AS canonical_ukam_address_id,
            MIN(candidates.resolved_canonical_id) AS resolved_canonical_id,
            '{exact_value}'::ENUM {enum_values} AS match_reason
        FROM {{exact_phase_1_selected_candidates}} AS candidates
        GROUP BY candidates.ukam_address_id
    """

    exact_phase_1_candidate_pool_sql = f"""
        SELECT
            messy.ukam_address_id AS ukam_address_id,
            canon.ukam_address_id AS canonical_ukam_address_id,
            canon.canonical_unique_id AS resolved_canonical_id,
            ROW_NUMBER() OVER (
                PARTITION BY messy.ukam_address_id
                ORDER BY canon.canonical_unique_id
            ) AS candidate_rank
        FROM {{{messy_input_name}}} AS messy
        INNER JOIN {{canonical_addresses_restricted}} AS canon
            ON {exact_match_condition}
    """

    exact_phase_1_selected_candidates_sql = """
        SELECT
            ukam_address_id,
            canonical_ukam_address_id,
            resolved_canonical_id
        FROM {exact_phase_1_candidate_pool}
        WHERE candidate_rank = 1
    """

    phase_2_enabled_condition = "TRUE" if enable_phase_2_flat_retraction else "FALSE"

    exact_phase_2_candidate_pool_sql = f"""
        SELECT
            messy.ukam_address_id AS ukam_address_id,
            canon.ukam_address_id AS canonical_ukam_address_id,
            canon.canonical_unique_id AS resolved_canonical_id
        FROM {{{messy_input_name}}} AS messy
        INNER JOIN {{canonical_addresses_restricted}} AS canon
            ON messy.postcode = canon.postcode
        WHERE NOT EXISTS (
            SELECT 1
            FROM {{exact_phase_1_matches}} AS phase_1
            WHERE phase_1.ukam_address_id = messy.ukam_address_id
        )
        AND {phase_2_enabled_condition}
        AND {keyword_stripped_match_condition}
        AND (
            {messy_without_flat} <> messy.clean_full_address
            OR {canon_without_flat} <> canon.clean_full_address
        )
        AND {flat_compatibility_condition}
        AND {unit_evidence_condition}
    """

    exact_phase_2_unique_messy_ids_sql = """
        SELECT
            ukam_address_id
        FROM {exact_phase_2_candidate_pool}
        GROUP BY ukam_address_id
        HAVING COUNT(DISTINCT resolved_canonical_id) = 1
    """

    exact_phase_2_selected_candidates_sql = """
        SELECT
            candidates.ukam_address_id,
            candidates.canonical_ukam_address_id,
            candidates.resolved_canonical_id
        FROM {exact_phase_2_candidate_pool} AS candidates
        INNER JOIN {exact_phase_2_unique_messy_ids} AS unique_messy
            ON candidates.ukam_address_id = unique_messy.ukam_address_id
    """

    exact_phase_2_matches_sql = f"""
        SELECT
            candidates.ukam_address_id AS ukam_address_id,
            MIN(candidates.canonical_ukam_address_id) AS canonical_ukam_address_id,
            MIN(candidates.resolved_canonical_id) AS resolved_canonical_id,
            '{exact_flat_retraction_value}'::ENUM {enum_values} AS match_reason
        FROM {{exact_phase_2_selected_candidates}} AS candidates
        GROUP BY candidates.ukam_address_id
    """

    exact_matches_sql = """
        SELECT * FROM {exact_phase_1_matches}
        UNION ALL
        SELECT * FROM {exact_phase_2_matches}
    """

    return [
        CTEStep("exact_phase_1_candidate_pool", exact_phase_1_candidate_pool_sql),
        CTEStep(
            "exact_phase_1_selected_candidates",
            exact_phase_1_selected_candidates_sql,
        ),
        CTEStep("exact_phase_1_matches", exact_phase_1_matches_sql),
        CTEStep("exact_phase_2_candidate_pool", exact_phase_2_candidate_pool_sql),
        CTEStep("exact_phase_2_unique_messy_ids", exact_phase_2_unique_messy_ids_sql),
        CTEStep(
            "exact_phase_2_selected_candidates",
            exact_phase_2_selected_candidates_sql,
        ),
        CTEStep("exact_phase_2_matches", exact_phase_2_matches_sql),
        CTEStep("exact_matches", exact_matches_sql),
    ]
