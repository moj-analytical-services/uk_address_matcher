from __future__ import annotations

import json
import re
from dataclasses import dataclass
from functools import lru_cache
from importlib.resources import files
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

    This stage generates peeled tokens on-the-fly so it no longer relies on
    upstream cleaning stages to populate `peeled_tokens_list`.

    Matching rules:
        1. Postcodes must be identical
        2. Peeled addresses (address_tokens minus peeled words) must be identical
        3. At least one side must have peeled something (to avoid duplicating
           exact match results)
    """
    match_reason_value = MatchReason.PEELED_ADDRESS.value
    enum_values = str(MatchReason.enum_values())

    messy_peeled_sql = _build_regex_peel_sql(
        source_placeholder="messy_addresses",
        id_column="ukam_address_id",
        canonical=False,
    )

    canonical_peeled_sql = _build_regex_peel_sql(
        source_placeholder="canonical_addresses_restricted",
        id_column="ukam_address_id",
        canonical=True,
    )

    candidates_sql = """
        SELECT
            messy.ukam_address_id AS messy_ukam_address_id,
            messy.clean_full_address AS messy_clean_full_address,
            messy.peeled_address AS messy_peeled_address,
            messy.did_peel AS messy_did_peel,
            canon.canonical_ukam_address_id,
            canon.canonical_unique_id,
            canon.canonical_clean_full_address,
            canon.peeled_address AS canonical_peeled_address,
            canon.did_peel AS canonical_did_peel
        FROM {messy_peeled} AS messy
        INNER JOIN {canonical_peeled} AS canon
            ON messy.postcode = canon.postcode
            AND messy.peeled_address = canon.peeled_address
        WHERE
            messy.did_peel
            OR canon.did_peel
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


def _normalise_end_token(token: str) -> str:
    return " ".join(token.strip().upper().split())


@lru_cache(maxsize=1)
def _load_end_tokens_for_regex() -> tuple[str, ...]:
    data_path = files("uk_address_matcher.data").joinpath("common_uk_end_tokens.json")
    data = json.loads(data_path.read_text(encoding="utf-8"))

    aliases = data.get("aliases", {}) or {}
    candidates = (
        list(data.get("single_tokens", []) or [])
        + list(data.get("multi_tokens", []) or [])
        + list(aliases.keys())
        + list(aliases.values())
    )

    seen: set[str] = set()
    ordered: list[str] = []
    for value in candidates:
        if not isinstance(value, str):
            continue
        token = _normalise_end_token(value)
        if not token or token in seen:
            continue
        seen.add(token)
        ordered.append(token)

    ordered.sort(key=lambda token: (-len(token.split()), -len(token), token))
    return tuple(ordered)


@lru_cache(maxsize=1)
def _build_suffix_peel_regex_sql_literal() -> str:
    tokens = _load_end_tokens_for_regex()
    escaped = "|".join(re.escape(token).replace(r"\ ", " ") for token in tokens)
    pattern = rf"(?:^|\s+)(?:{escaped})(?:\s+(?:{escaped}))*\s*$"
    return pattern.replace("'", "''")


def _build_regex_peel_sql(
    *,
    source_placeholder: str,
    id_column: str,
    canonical: bool,
) -> str:
    pattern_sql = _build_suffix_peel_regex_sql_literal()

    if canonical:
        return f"""
            WITH normalised AS (
                SELECT
                    {id_column} AS canonical_ukam_address_id,
                    canonical_unique_id,
                    postcode,
                    regexp_replace(
                        upper(trim(clean_full_address)),
                        '\\s+',
                        ' ',
                        'g'
                    ) AS canonical_clean_full_address
                FROM {{{source_placeholder}}}
            ),
            peeled AS (
                SELECT
                    canonical_ukam_address_id,
                    canonical_unique_id,
                    postcode,
                    canonical_clean_full_address,
                    trim(
                        regexp_replace(
                            canonical_clean_full_address,
                            '{pattern_sql}',
                            ''
                        )
                    ) AS peeled_address
                FROM normalised
            )
            SELECT
                canonical_ukam_address_id,
                canonical_unique_id,
                postcode,
                canonical_clean_full_address,
                peeled_address,
                peeled_address <> canonical_clean_full_address AS did_peel
            FROM peeled
        """

    return f"""
        WITH normalised AS (
            SELECT
                {id_column} AS ukam_address_id,
                postcode,
                regexp_replace(
                    upper(trim(clean_full_address)),
                    '\\s+',
                    ' ',
                    'g'
                ) AS clean_full_address
            FROM {{{source_placeholder}}}
        ),
        peeled AS (
            SELECT
                ukam_address_id,
                postcode,
                clean_full_address,
                trim(
                    regexp_replace(
                        clean_full_address,
                        '{pattern_sql}',
                        ''
                    )
                ) AS peeled_address
            FROM normalised
        )
        SELECT
            ukam_address_id,
            postcode,
            clean_full_address,
            peeled_address,
            peeled_address <> clean_full_address AS did_peel
        FROM peeled
    """
