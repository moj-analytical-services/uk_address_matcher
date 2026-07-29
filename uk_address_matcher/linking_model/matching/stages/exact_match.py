from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, Optional

from uk_address_matcher.linking_model.matching.input_filters import (
    _numeric_tokens_from_scalar_columns_sql,
    _restrict_canonical_to_messy_postcodes,
)
from uk_address_matcher.linking_model.matching.stages.base_stage import MatchingStage
from uk_address_matcher.sql_pipeline.match_reasons import MatchReason
from uk_address_matcher.sql_pipeline.steps import CTEStep, pipeline_stage

if TYPE_CHECKING:
    import duckdb

    from uk_address_matcher.sql_pipeline.runner import DebugOptions

MessyInputName = Literal["__ukam__tmp_messy_addresses", "unmatched_records"]


def _flat_field_compatibility_sql() -> str:
    """Return SQL requiring flat fields to be non-contradictory."""

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
       AND (
           messy.sub_premise_location IS NULL
           OR canon.sub_premise_location IS NULL
           OR messy.sub_premise_location = canon.sub_premise_location
       )
    """


def _flat_retraction_unit_evidence_sql(alias: str) -> str:
    """Return SQL for independent sub-unit evidence using parsed columns."""

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
    """Deterministic exact matching on `clean_full_address` and `postcode`.

    This is usually the first stage in a pipeline. It accepts the easy,
    unambiguous cases before any probabilistic matching is attempted.

    A match is emitted when the cleaned messy address and the cleaned canonical
    address are identical and the postcode is also identical. A cleaned address
    match on its own is not enough: differing postcodes will not match.

    Example:
        ``"10 Demo Road Townton"`` matches
        ``"10 Demo Road, Townton"`` only when cleaning normalises punctuation
        and whitespace and both records have the same postcode.

    This stage applies three deterministic phases in priority order:
    1. exact ``clean_full_address + postcode``
    2. exact after removing all whitespace
    3. exact after removing standalone ``FLAT`` *and* all whitespace, gated by
       parsed-unit heuristics and flat-field compatibility checks

    Set ``enable_flat_retraction=False`` to skip phase 3.
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
                    enable_flat_retraction=self.enable_flat_retraction,
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
    enable_flat_retraction: bool = True,
) -> list[CTEStep]:
    """Find deterministic matches using exact, no-whitespace and FLAT phases.

    Phases 1 and 2 are selected by ranked preference. Phase 3 applies only to
    records still unmatched after phases 1 and 2.

    Parameters
    ----------
    messy_input_name:
        The placeholder name for the messy input table. Defaults to
        "__ukam__tmp_messy_addresses" for the initial pass. Can be set
        to "unmatched_records" when running after filtering.
    enable_flat_retraction:
        If ``True``, run phase 3 and emit
        ``exact_flat_retraction: match after removing FLAT keyword`` when it
        yields an unambiguous candidate. If ``False``, skip phase 3.
    """
    exact_value = MatchReason.EXACT.value
    exact_no_whitespace_value = MatchReason.EXACT_NO_WHITESPACE.value
    exact_flat_retraction_value = MatchReason.EXACT_FLAT_RETRACTION.value
    enum_values = str(MatchReason.enum_values())

    # Both messy_keys and canon_keys compute every derived key (plus the flat
    # metadata phase 3 needs) in a single pass so we never rescan the source
    # relation. Phase 3 reads from the same CTEs as phases 1 & 2.
    #
    # Key derivation strategy (one regex pass per row when FLAT is absent):
    #   clean_full_address_no_ws         : strip whitespace only
    #   clean_full_address_no_flat_no_ws : strip FLAT + whitespace, via an
    #       alternation regex gated by a cheap contains() check. When no
    #       "FLAT" token is present the column degrades to the no-ws value
    #       with no extra regex work.
    _flat_metadata_messy_sql = (
        ",\n                messy.flat_number,"
        "\n                messy.flat_letter,"
        "\n                messy.flat_positional,"
        "\n                messy.sub_premise_location,"
        "\n                messy.has_business_unit,"
        "\n                messy.business_unit_id,"
        f"\n                {_numeric_tokens_from_scalar_columns_sql('messy')} "
        "AS numeric_tokens"
        if enable_flat_retraction
        else ""
    )
    _flat_metadata_canon_sql = (
        ",\n                canon.flat_number,"
        "\n                canon.flat_letter,"
        "\n                canon.flat_positional,"
        "\n                canon.sub_premise_location,"
        "\n                canon.has_business_unit,"
        "\n                canon.business_unit_id,"
        "\n                canon.numeric_tokens"
        if enable_flat_retraction
        else ""
    )
    _flat_extra_cols_sql = (
        """,
            base.flat_number,
            base.flat_letter,
            base.flat_positional,
            base.sub_premise_location,
            base.has_business_unit,
            base.business_unit_id,
            base.numeric_tokens"""
        if enable_flat_retraction
        else ""
    )
    _flat_key_sql = (
        r""",
            CASE
                WHEN contains(base.clean_full_address, 'FLAT')
                THEN regexp_replace(
                    base.clean_full_address, '\bFLAT\b|\s+', '', 'g'
                )
                ELSE base.clean_full_address_no_ws
            END AS clean_full_address_no_flat_no_ws"""
        if enable_flat_retraction
        else ""
    )

    messy_keys_sql = rf"""
        SELECT
            base.ukam_address_id,
            base.postcode,
            base.clean_full_address,
            base.clean_full_address_no_ws{_flat_key_sql}{_flat_extra_cols_sql}
        FROM (
            SELECT
                messy.ukam_address_id,
                messy.postcode,
                messy.clean_full_address,
                regexp_replace(messy.clean_full_address, '\s+', '', 'g')
                    AS clean_full_address_no_ws{_flat_metadata_messy_sql}
            FROM {{{messy_input_name}}} AS messy
        ) AS base
    """

    canon_keys_sql = rf"""
        SELECT
            base.canonical_ukam_address_id,
            base.canonical_unique_id,
            base.postcode,
            base.clean_full_address,
            base.clean_full_address_no_ws{_flat_key_sql}{_flat_extra_cols_sql}
        FROM (
            SELECT
                canon.ukam_address_id AS canonical_ukam_address_id,
                canon.canonical_unique_id,
                canon.postcode,
                canon.clean_full_address,
                regexp_replace(canon.clean_full_address, '\s+', '', 'g')
                    AS clean_full_address_no_ws{_flat_metadata_canon_sql}
            FROM {{canonical_addresses_restricted}} AS canon
        ) AS base
    """

    exact_candidates_sql = f"""
        SELECT
            messy.ukam_address_id AS ukam_address_id,
            canon.canonical_ukam_address_id,
            canon.canonical_unique_id AS resolved_canonical_id,
            '{exact_value}'::ENUM {enum_values} AS match_reason,
            1 AS match_priority
        FROM {{messy_keys}} AS messy
        INNER JOIN {{canon_keys}} AS canon
            ON messy.clean_full_address = canon.clean_full_address
            AND messy.postcode = canon.postcode
    """

    no_ws_candidates_sql = f"""
        SELECT
            messy.ukam_address_id AS ukam_address_id,
            canon.canonical_ukam_address_id,
            canon.canonical_unique_id AS resolved_canonical_id,
            '{exact_no_whitespace_value}'::ENUM {enum_values} AS match_reason,
            2 AS match_priority
        FROM {{messy_keys}} AS messy
        INNER JOIN {{canon_keys}} AS canon
            ON messy.postcode = canon.postcode
            AND messy.clean_full_address_no_ws = canon.clean_full_address_no_ws
            AND messy.clean_full_address <> canon.clean_full_address
            AND messy.clean_full_address_no_ws <> ''
    """

    all_ranked_candidates_sql = """
        SELECT * FROM {exact_candidates}
        UNION ALL
        SELECT * FROM {no_ws_candidates}
    """

    ranked_pre_flat_candidates_sql = """
        SELECT
            candidates.ukam_address_id,
            candidates.canonical_ukam_address_id,
            candidates.resolved_canonical_id,
            candidates.match_reason,
            ROW_NUMBER() OVER (
                PARTITION BY candidates.ukam_address_id
                ORDER BY
                    candidates.match_priority,
                    candidates.canonical_ukam_address_id
            ) AS rn
        FROM {all_ranked_candidates} AS candidates
    """

    pre_flat_matches_sql = """
        SELECT
            ukam_address_id,
            canonical_ukam_address_id,
            resolved_canonical_id,
            match_reason
        FROM {ranked_pre_flat_candidates}
        WHERE rn = 1
    """

    steps = [
        CTEStep("messy_keys", messy_keys_sql),
        CTEStep("canon_keys", canon_keys_sql),
        CTEStep("exact_candidates", exact_candidates_sql),
        CTEStep("no_ws_candidates", no_ws_candidates_sql),
        CTEStep("all_ranked_candidates", all_ranked_candidates_sql),
        CTEStep("ranked_pre_flat_candidates", ranked_pre_flat_candidates_sql),
        CTEStep("pre_flat_matches", pre_flat_matches_sql),
    ]

    if enable_flat_retraction:
        flat_compatibility_condition = _flat_field_compatibility_sql()

        # Phase 3 reuses the already-projected messy_keys / canon_keys CTEs —
        # no second scan of the source relations, no duplicate regex work.
        # Residual messy rows are the ones that didn't match in phases 1-2.
        messy_flat_keys_sql = """
            SELECT messy.*
            FROM {messy_keys} AS messy
            LEFT JOIN {pre_flat_matches} AS matched
                ON matched.ukam_address_id = messy.ukam_address_id
            WHERE matched.ukam_address_id IS NULL
        """

        # Canonical is already postcode-restricted to the full messy set by
        # canonical_addresses_restricted; narrowing further to residual
        # postcodes shrinks the phase-3 dedupe GROUP BY.
        residual_postcodes_sql = """
            SELECT DISTINCT postcode
            FROM {messy_flat_keys}
        """

        canon_flat_keys_sql = """
            SELECT canon.*
            FROM {canon_keys} AS canon
            SEMI JOIN {residual_postcodes} AS rp
                ON rp.postcode = canon.postcode
        """

        canon_flat_unique_sql = """
            SELECT
                canon.postcode,
                canon.clean_full_address_no_flat_no_ws,
                MIN(canon.canonical_ukam_address_id) AS canonical_ukam_address_id,
                MIN(canon.canonical_unique_id) AS resolved_canonical_id,
                MIN(canon.flat_number) AS flat_number,
                MIN(canon.flat_letter) AS flat_letter,
                MIN(canon.flat_positional) AS flat_positional,
                MIN(canon.sub_premise_location) AS sub_premise_location,
                BOOL_OR(COALESCE(canon.has_business_unit, FALSE)) AS has_business_unit,
                MIN(canon.business_unit_id) AS business_unit_id,
                BOOL_OR(
                    canon.clean_full_address_no_flat_no_ws
                        <> canon.clean_full_address_no_ws
                ) AS canonical_flat_keyword_removed,
                BOOL_OR(
                    canon.flat_number IS NOT NULL
                    OR canon.flat_letter IS NOT NULL
                    OR canon.flat_positional IS NOT NULL
                    OR COALESCE(canon.has_business_unit, FALSE)
                    OR canon.business_unit_id IS NOT NULL
                    OR COALESCE(array_length(canon.numeric_tokens), 0) >= 2
                ) AS canonical_has_unit_evidence
            FROM {canon_flat_keys} AS canon
            WHERE canon.clean_full_address_no_flat_no_ws <> ''
            GROUP BY canon.postcode, canon.clean_full_address_no_flat_no_ws
            HAVING COUNT(DISTINCT canon.canonical_ukam_address_id) = 1
        """

        flat_retraction_matches_sql = f"""
            SELECT
                messy.ukam_address_id,
                canon.canonical_ukam_address_id,
                canon.resolved_canonical_id,
                '{exact_flat_retraction_value}'::ENUM {enum_values} AS match_reason
            FROM {{messy_flat_keys}} AS messy
            INNER JOIN {{canon_flat_unique}} AS canon
                ON messy.postcode = canon.postcode
                AND messy.clean_full_address_no_flat_no_ws
                    = canon.clean_full_address_no_flat_no_ws
            WHERE messy.clean_full_address_no_flat_no_ws <> ''
            AND (
                messy.clean_full_address_no_flat_no_ws
                    <> messy.clean_full_address_no_ws
                OR COALESCE(canon.canonical_flat_keyword_removed, FALSE)
            )
            AND {flat_compatibility_condition}
            AND (
                {_flat_retraction_unit_evidence_sql("messy")}
                OR COALESCE(canon.canonical_has_unit_evidence, FALSE)
            )
        """

        steps.extend(
            [
                CTEStep("messy_flat_keys", messy_flat_keys_sql),
                CTEStep("residual_postcodes", residual_postcodes_sql),
                CTEStep("canon_flat_keys", canon_flat_keys_sql),
                CTEStep("canon_flat_unique", canon_flat_unique_sql),
                CTEStep("flat_retraction_matches", flat_retraction_matches_sql),
            ]
        )

        exact_matches_sql = """
            SELECT * FROM {pre_flat_matches}
            UNION ALL
            SELECT * FROM {flat_retraction_matches}
        """
    else:
        exact_matches_sql = "SELECT * FROM {pre_flat_matches}"

    steps.append(CTEStep("exact_matches", exact_matches_sql))

    return steps
