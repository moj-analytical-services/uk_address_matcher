from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from uk_address_matcher.linking_model.matching.stages.base_stage import MatchingStage
from uk_address_matcher.sql_pipeline.match_reasons import MatchReason
from uk_address_matcher.sql_pipeline.steps import CTEStep, pipeline_stage

if TYPE_CHECKING:
    import duckdb

    from uk_address_matcher.sql_pipeline.runner import DebugOptions


_PREMISE_GAP_TOKENS = (
    "APARTMENT",
    "BLOCK",
    "BUILDING",
    "COTTAGE",
    "FLAT",
    "HOUSE",
    "MAISONETTE",
    "OFFICE",
    "STEADING",
    "STUDIO",
    "SUITE",
    "UNIT",
    "WAREHOUSE",
    "WORKSHOP",
)


@dataclass(frozen=True, repr=False)
class DistinguishingTokenStage(MatchingStage):
    """Match an address using its local prefix and two ordered evidence tokens.

    Canonical addresses must be prepared with
    ``derive_distinguishing_wrt_adjacent_records=True``. The distinguishing
    prefix must occur at the start of the messy address, followed by the next
    two canonical tokens in order. Up to two intervening tokens are allowed,
    provided they contain no digits and are not premise types such as ``FLAT``,
    ``HOUSE``, or ``UNIT``. Postcodes must be identical and the evidence must
    identify exactly one canonical ID.

    Use this optional deterministic stage before ``SplinkStage``.
    """

    def find_matches(
        self,
        con: duckdb.DuckDBPyConnection,
        stage_name: str,
        df_unmatched: duckdb.DuckDBPyRelation,
        df_canonical: duckdb.DuckDBPyRelation,
        debug_options: Optional[DebugOptions] = None,
        explain: bool = False,
    ) -> Optional[duckdb.DuckDBPyRelation]:
        if "distinguishing_adj_start_tokens" not in df_canonical.columns:
            raise ValueError(
                "DistinguishingTokenStage requires canonical addresses prepared "
                "with derive_distinguishing_wrt_adjacent_records=True."
            )

        from uk_address_matcher.linking_model.matching.stages._sql_helpers import (
            run_sql_pipeline,
        )

        return run_sql_pipeline(
            con=con,
            pipeline_stages=[_distinguishing_token_matches()],
            stage_name=stage_name,
            df_unmatched=df_unmatched,
            df_canonical=df_canonical,
            debug_options=debug_options,
            explain=explain,
        )


def _safe_gap_token(token_sql: str) -> str:
    premise_tokens = ", ".join(f"'{token}'" for token in _PREMISE_GAP_TOKENS)
    return (
        f"(NOT regexp_matches({token_sql}, '[0-9]') "
        f"AND {token_sql} NOT IN ({premise_tokens}))"
    )


def _ordered_evidence_condition() -> str:
    prefix_length = "canonical.prefix_length"
    evidence_1 = f"split_part(canonical.clean_full_address, ' ', {prefix_length} + 1)"
    evidence_2 = f"split_part(canonical.clean_full_address, ' ', {prefix_length} + 2)"
    offsets = {
        offset: (f"split_part(messy.clean_full_address, ' ', {prefix_length} + {offset})")
        for offset in range(1, 5)
    }
    offset_1, offset_2, offset_3, offset_4 = offsets.values()

    return f"""
        (
            (
                {offset_1} = {evidence_1}
                AND (
                    {offset_2} = {evidence_2}
                    OR (
                        {_safe_gap_token(offset_2)}
                        AND (
                            {offset_3} = {evidence_2}
                            OR (
                                {_safe_gap_token(offset_3)}
                                AND {offset_4} = {evidence_2}
                            )
                        )
                    )
                )
            )
            OR (
                {_safe_gap_token(offset_1)}
                AND (
                    (
                        {offset_2} = {evidence_1}
                        AND (
                            {offset_3} = {evidence_2}
                            OR (
                                {_safe_gap_token(offset_3)}
                                AND {offset_4} = {evidence_2}
                            )
                        )
                    )
                    OR (
                        {_safe_gap_token(offset_2)}
                        AND {offset_3} = {evidence_1}
                        AND {offset_4} = {evidence_2}
                    )
                )
            )
        )
    """


@pipeline_stage(
    name="distinguishing_token_matching",
    description=(
        "Resolve addresses using a locally distinguishing prefix and two ordered "
        "tokens separated by at most two safe gaps."
    ),
    tags=["phase_1", "matching"],
)
def _distinguishing_token_matches() -> list[CTEStep]:
    match_reason = MatchReason.DISTINGUISHING_TOKEN.value
    enum_values = str(MatchReason.enum_values())
    evidence_condition = _ordered_evidence_condition()

    canonical_signatures_sql = """
        SELECT
            canonical.ukam_address_id AS canonical_ukam_address_id,
            canonical.unique_id AS resolved_canonical_id,
            canonical.clean_full_address,
            canonical.postcode,
            canonical.distinguishing_adj_start_tokens,
            len(canonical.distinguishing_adj_start_tokens) AS prefix_length
        FROM {__ukam__tmp_canonical_addresses} AS canonical
        INNER JOIN (
            SELECT DISTINCT postcode
            FROM {__ukam__tmp_messy_addresses}
            WHERE postcode IS NOT NULL
        ) AS messy_postcodes
            ON canonical.postcode = messy_postcodes.postcode
        WHERE canonical.unique_id IS NOT NULL
          AND len(canonical.distinguishing_adj_start_tokens) > 0
          AND len(string_split(canonical.clean_full_address, ' '))
              >= len(canonical.distinguishing_adj_start_tokens) + 2
    """
    candidates_sql = f"""
        SELECT
            messy.ukam_address_id,
            canonical.canonical_ukam_address_id,
            canonical.resolved_canonical_id
        FROM {{__ukam__tmp_messy_addresses}} AS messy
        INNER JOIN {{canonical_distinguishing_signatures}} AS canonical
            ON messy.postcode = canonical.postcode
        WHERE starts_with(
            messy.clean_full_address,
            array_to_string(canonical.distinguishing_adj_start_tokens, ' ') || ' '
        )
          AND {evidence_condition}
    """
    distinct_ids_sql = """
        SELECT
            ukam_address_id,
            resolved_canonical_id,
            min(canonical_ukam_address_id) AS canonical_ukam_address_id
        FROM {distinguishing_token_candidates}
        GROUP BY ukam_address_id, resolved_canonical_id
    """
    unambiguous_matches_sql = f"""
        SELECT
            ukam_address_id,
            canonical_ukam_address_id,
            resolved_canonical_id,
            '{match_reason}'::ENUM {enum_values} AS match_reason
        FROM {{distinct_distinguishing_token_ids}}
        QUALIFY count(*) OVER (PARTITION BY ukam_address_id) = 1
    """

    return [
        CTEStep("canonical_distinguishing_signatures", canonical_signatures_sql),
        CTEStep("distinguishing_token_candidates", candidates_sql),
        CTEStep("distinct_distinguishing_token_ids", distinct_ids_sql),
        CTEStep("unambiguous_distinguishing_token_matches", unambiguous_matches_sql),
    ]
