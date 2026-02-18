from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from uk_address_matcher.linking_model.matching.input_filters import (
    _restrict_canonical_to_messy_postcodes,
)
from uk_address_matcher.linking_model.matching.stages.base_stage import MatchingStage
from uk_address_matcher.sql_pipeline.match_reasons import MatchReason
from uk_address_matcher.sql_pipeline.steps import CTEStep, pipeline_stage

if TYPE_CHECKING:
    import duckdb

    from uk_address_matcher.sql_pipeline.runner import DebugOptions


@dataclass(frozen=True, repr=False)
class PeeledAddressStage(MatchingStage):
    """Match records after peeling common UK locality suffix tokens."""

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
                _peeled_address_matches,
            ],
            stage_name=stage_name,
            df_unmatched=df_unmatched,
            df_canonical=df_canonical,
            debug_options=debug_options,
            explain=explain,
        )


@pipeline_stage(
    name="peeled_address_matching",
    description=(
        "Find matches by comparing addresses after peeling common UK end tokens "
        "(cities, counties, boroughs) and performing exact match on the peeled addresses."
    ),
    tags=["phase_1", "matching"],
    depends_on=["restrict_canonical_to_messy_postcodes"],
)
def _peeled_address_matches() -> list[CTEStep]:
    """Find matches using peeled addresses (after removing common UK end tokens).

    Peeling refers to the iterative removal of common UK locality tokens from
    the end of addresses. These include cities (LONDON, MANCHESTER), counties
    (HERTFORDSHIRE, KENT), London boroughs (HACKNEY, LAMBETH), and regions
    (GREATER LONDON, WEST MIDLANDS).

    Example transformations:
        - "100 TEST STREET LONDON" -> "100 TEST STREET"
        - "25 HIGH ROAD HACKNEY LONDON" -> "25 HIGH ROAD"
        - "10 MAIN AVENUE MANCHESTER GREATER MANCHESTER" -> "10 MAIN AVENUE"

    This stage uses pre-computed columns from the cleaning pipeline:
        - clean_full_address: cleaned address string used to derive tokens inline
        - peeled_tokens_list: VARCHAR[] of tokens that were peeled from the end

    Matching rules:
        1. Postcodes must be identical
        2. Peeled addresses (address_tokens minus peeled words) must be identical
        3. At least one side must have peeled something (to avoid duplicating
           exact match results)
    """
    # Use the dedicated enum value for peeled-address matches so that any SQL
    # cast to the MatchReason ENUM remains valid.
    match_reason_value = MatchReason.PEELED_ADDRESS.value
    enum_values = str(MatchReason.enum_values())

    messy_peeled_sql = """
        SELECT
            ukam_address_id,
            postcode,
            clean_full_address,
            string_split(clean_full_address, ' ') AS address_tokens,
            peeled_tokens_list,
            COALESCE(
                (SELECT SUM(len(string_split(token, ' ')))
                 FROM unnest(peeled_tokens_list) AS t(token)),
                0
            )::INTEGER AS peeled_word_count,
            CASE
                WHEN peeled_tokens_list IS NULL OR len(peeled_tokens_list) = 0
                THEN array_to_string(address_tokens, ' ')
                ELSE array_to_string(
                    list_slice(
                        address_tokens,
                        1,
                        len(address_tokens) - COALESCE(
                            (SELECT SUM(len(string_split(token, ' ')))
                             FROM unnest(peeled_tokens_list) AS t(token)),
                            0
                        )::INTEGER
                    ),
                    ' '
                )
            END AS peeled_address
        FROM {messy_addresses}
    """

    canonical_peeled_sql = """
        SELECT
            ukam_address_id AS canonical_ukam_address_id,
            canonical_unique_id,
            postcode,
            clean_full_address AS canonical_clean_full_address,
            string_split(clean_full_address, ' ') AS canonical_address_tokens,
            peeled_tokens_list AS canonical_peeled_tokens_list,
            COALESCE(
                (SELECT SUM(len(string_split(token, ' ')))
                 FROM unnest(peeled_tokens_list) AS t(token)),
                0
            )::INTEGER AS canonical_peeled_word_count,
            CASE
                WHEN peeled_tokens_list IS NULL OR len(peeled_tokens_list) = 0
                THEN array_to_string(canonical_address_tokens, ' ')
                ELSE array_to_string(
                    list_slice(
                        canonical_address_tokens,
                        1,
                        len(canonical_address_tokens) - COALESCE(
                            (SELECT SUM(len(string_split(token, ' ')))
                             FROM unnest(peeled_tokens_list) AS t(token)),
                            0
                        )::INTEGER
                    ),
                    ' '
                )
            END AS peeled_address
        FROM {canonical_addresses_restricted}
    """

    candidates_sql = """
        SELECT
            messy.ukam_address_id AS messy_ukam_address_id,
            messy.clean_full_address AS messy_clean_full_address,
            messy.peeled_address AS messy_peeled_address,
            messy.peeled_tokens_list AS messy_peeled_tokens,
            messy.peeled_word_count AS messy_peeled_word_count,
            canon.canonical_ukam_address_id,
            canon.canonical_unique_id,
            canon.canonical_clean_full_address,
            canon.peeled_address AS canonical_peeled_address,
            canon.canonical_peeled_tokens_list AS canonical_peeled_tokens,
            canon.canonical_peeled_word_count
        FROM {messy_peeled} AS messy
        INNER JOIN {canonical_peeled} AS canon
            ON messy.postcode = canon.postcode
            AND messy.peeled_address = canon.peeled_address
        WHERE
            messy.peeled_word_count > 0
            OR canon.canonical_peeled_word_count > 0
    """

    annotated_sql = f"""
        SELECT
            messy_ukam_address_id AS ukam_address_id,
            canonical_ukam_address_id,
            canonical_unique_id AS resolved_canonical_id,
            '{match_reason_value}'::ENUM {enum_values} AS match_reason
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY messy_ukam_address_id
                    ORDER BY canonical_ukam_address_id
                ) AS rn
            FROM {{peeled_address_candidates}}
        )
        WHERE rn = 1
    """

    return [
        CTEStep("messy_peeled", messy_peeled_sql),
        CTEStep("canonical_peeled", canonical_peeled_sql),
        CTEStep("peeled_address_candidates", candidates_sql),
        CTEStep("peeled_address_matches", annotated_sql),
    ]
