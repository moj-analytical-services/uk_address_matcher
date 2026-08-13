from __future__ import annotations

import json
import re
from dataclasses import dataclass
from functools import lru_cache
from importlib.resources import files
from typing import TYPE_CHECKING, Optional

from uk_address_matcher.linking_model.matching.input_filters import (
    _restrict_canonical_to_messy_postcodes,
    _validate_inward_postcode_levenshtein,
)
from uk_address_matcher.linking_model.matching.stages.base_stage import MatchingStage
from uk_address_matcher.sql_pipeline.match_reasons import MatchReason
from uk_address_matcher.sql_pipeline.steps import CTEStep, pipeline_stage

if TYPE_CHECKING:
    import duckdb

    from uk_address_matcher.sql_pipeline.runner import DebugOptions


@dataclass(frozen=True, repr=False)
class PeeledAddressStage(MatchingStage):
    """Deterministic matching after peeling common UK locality suffixes.

    This stage removes trailing locality words such as borough, county, or city
    names, then performs an exact match on the peeled address plus postcode.
    It is useful when one side includes extra suffixes such as ``"Hackney
    London"`` and the other does not. A second, optional pass supports the
    configured inward-postcode edit distance when outward postcodes match.

    Use this before ``SplinkStage`` so these high-precision cases are resolved
    without needing probabilistic thresholds.

    Set ``inward_postcode_levenshtein`` to a positive integer to enable the
    optional inward postcode phase; the default ``0`` skips it.

    Example:
        ``"100 Test Street Hackney London"`` can match
        ``"100 Test Street"`` when both share the same postcode. The optional
        partial-postcode pass also permits the configured inward edit distance.
    """

    enable_whitespace_punctuation_stripping: bool = True
    inward_postcode_levenshtein: int = 0

    def __post_init__(self) -> None:
        _validate_inward_postcode_levenshtein(self.inward_postcode_levenshtein)

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
                _restrict_canonical_to_messy_postcodes(
                    "outward" if self.inward_postcode_levenshtein > 0 else "exact"
                ),
                _peeled_address_matches(
                    enable_whitespace_punctuation_stripping=(
                        self.enable_whitespace_punctuation_stripping
                    ),
                    inward_postcode_levenshtein=self.inward_postcode_levenshtein,
                ),
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
        "(cities, counties, boroughs), with an optional whitespace/punctuation "
        "stripping fallback on the peeled shell and partial-postcode fallback."
    ),
    tags=["phase_1", "matching"],
    depends_on=["restrict_canonical_to_messy_postcodes"],
)
def _peeled_address_matches(
    *,
    enable_whitespace_punctuation_stripping: bool = True,
    inward_postcode_levenshtein: int = 0,
) -> list[CTEStep]:
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
        1. Exact postcodes are considered first.
          2. Optionally, unmatched rows can use an exact outward postcode and an
              inward postcode Levenshtein distance within the configured cap.
        3. Peeled addresses (address_tokens minus peeled words) must be identical.
        4. At least one side must have peeled something (to avoid duplicating
           exact match results).
    """
    match_reason_value = MatchReason.PEELED_ADDRESS.value
    stripped_match_reason_value = MatchReason.PEELED_ADDRESS_STRIPPED.value
    partial_postcode_match_reason_value = (
        MatchReason.PEELED_ADDRESS_PARTIAL_POSTCODE.value
    )
    enum_values = str(MatchReason.enum_values())

    messy_peeled_sql = _build_regex_peel_sql(
        source_placeholder="__ukam__tmp_messy_addresses",
        id_column="ukam_address_id",
        canonical=False,
    )

    canonical_peeled_sql = _build_regex_peel_sql(
        source_placeholder="canonical_addresses_restricted",
        id_column="ukam_address_id",
        canonical=True,
    )

    if inward_postcode_levenshtein > 0:
        single_pass_messy_sql = f"""
            SELECT
                messy.*,
                messy.peeled_address AS match_key,
                1 AS key_priority
            FROM {{messy_peeled}} AS messy
            UNION ALL
            SELECT
                messy.*,
                {_compacted_address_sql("messy.peeled_address")} AS match_key,
                2 AS key_priority
            FROM {{messy_peeled}} AS messy
            WHERE {_compacted_address_sql("messy.peeled_address")} <> ''
            AND {_compacted_address_sql("messy.peeled_address")}
                <> messy.peeled_address
        """
        single_pass_canonical_sql = f"""
            SELECT
                canon.*,
                canon.peeled_address AS match_key,
                1 AS key_priority
            FROM {{canonical_peeled}} AS canon
            UNION ALL
            SELECT
                canon.*,
                {_compacted_address_sql("canon.peeled_address")} AS match_key,
                2 AS key_priority
            FROM {{canonical_peeled}} AS canon
            WHERE {_compacted_address_sql("canon.peeled_address")} <> ''
            AND {_compacted_address_sql("canon.peeled_address")}
                <> canon.peeled_address
        """
        single_pass_candidates_sql = f"""
            SELECT
                messy.ukam_address_id,
                canon.canonical_ukam_address_id,
                canon.canonical_unique_id AS resolved_canonical_id,
                CASE
                    WHEN messy.postcode <> canon.postcode
                    THEN '{partial_postcode_match_reason_value}'::ENUM {enum_values}
                    WHEN messy.key_priority = 1 AND canon.key_priority = 1
                    THEN '{match_reason_value}'::ENUM {enum_values}
                    ELSE '{stripped_match_reason_value}'::ENUM {enum_values}
                END AS match_reason,
                CASE WHEN messy.postcode = canon.postcode THEN 0 ELSE 1 END
                    AS postcode_priority,
                messy.key_priority + canon.key_priority AS key_priority
            FROM {{single_pass_messy}} AS messy
            INNER JOIN {{single_pass_canonical}} AS canon
                ON split_part(messy.postcode, ' ', 1)
                    = split_part(canon.postcode, ' ', 1)
                AND messy.match_key = canon.match_key
            WHERE messy.postcode IS NOT NULL
            AND canon.postcode IS NOT NULL
            AND split_part(messy.postcode, ' ', 2) <> ''
            AND split_part(canon.postcode, ' ', 2) <> ''
            AND (messy.did_peel OR canon.did_peel)
            AND (
                messy.postcode = canon.postcode
                OR levenshtein(
                    split_part(messy.postcode, ' ', 2),
                    split_part(canon.postcode, ' ', 2)
                ) <= {inward_postcode_levenshtein}
            )
        """
        single_pass_ranked_sql = """
            SELECT
                candidates.ukam_address_id,
                candidates.canonical_ukam_address_id,
                candidates.resolved_canonical_id,
                candidates.match_reason,
                ROW_NUMBER() OVER (
                    PARTITION BY candidates.ukam_address_id
                    ORDER BY
                        candidates.postcode_priority,
                        candidates.key_priority,
                        candidates.canonical_ukam_address_id
                ) AS rn
            FROM {single_pass_candidates} AS candidates
        """
        single_pass_matches_sql = """
            SELECT
                ukam_address_id,
                canonical_ukam_address_id,
                resolved_canonical_id,
                match_reason
            FROM {single_pass_ranked}
            WHERE rn = 1
        """
        return [
            CTEStep("messy_peeled", messy_peeled_sql),
            CTEStep("canonical_peeled", canonical_peeled_sql),
            CTEStep("single_pass_messy", single_pass_messy_sql),
            CTEStep("single_pass_canonical", single_pass_canonical_sql),
            CTEStep("single_pass_candidates", single_pass_candidates_sql),
            CTEStep("single_pass_ranked", single_pass_ranked_sql),
            CTEStep("peeled_address_matches", single_pass_matches_sql),
        ]

    peeled_candidates_sql = f"""
        SELECT
            messy.ukam_address_id AS ukam_address_id,
            canon.canonical_ukam_address_id,
            canon.canonical_unique_id AS resolved_canonical_id,
            '{match_reason_value}'::ENUM {enum_values} AS match_reason,
            1 AS match_priority
        FROM {{messy_peeled}} AS messy
        INNER JOIN {{canonical_peeled}} AS canon
            ON messy.postcode = canon.postcode
            AND messy.peeled_address = canon.peeled_address
        WHERE messy.did_peel OR canon.did_peel
    """

    ranked_peeled_candidates_sql = """
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
        FROM {peeled_address_candidates} AS candidates
    """

    pre_stripped_matches_sql = """
        SELECT
            ukam_address_id,
            canonical_ukam_address_id,
            resolved_canonical_id,
            match_reason
        FROM {ranked_peeled_candidates}
        WHERE rn = 1
    """

    steps = [
        CTEStep("messy_peeled", messy_peeled_sql),
        CTEStep("canonical_peeled", canonical_peeled_sql),
        CTEStep("peeled_address_candidates", peeled_candidates_sql),
        CTEStep("ranked_peeled_candidates", ranked_peeled_candidates_sql),
        CTEStep("pre_stripped_matches", pre_stripped_matches_sql),
    ]

    if enable_whitespace_punctuation_stripping:
        messy_residual_sql = """
            SELECT messy.*
            FROM {messy_peeled} AS messy
            LEFT JOIN {pre_stripped_matches} AS matched
                ON matched.ukam_address_id = messy.ukam_address_id
            WHERE matched.ukam_address_id IS NULL
        """

        residual_postcodes_sql = """
            SELECT DISTINCT postcode
            FROM {messy_residual}
        """

        canonical_residual_sql = """
            SELECT canon.*
            FROM {canonical_peeled} AS canon
            SEMI JOIN {residual_postcodes} AS rp
                ON rp.postcode = canon.postcode
        """

        stripped_messy_sql = f"""
            SELECT
                messy.ukam_address_id,
                messy.postcode,
                messy.peeled_address,
                messy.did_peel,
                {_compacted_address_sql("messy.peeled_address")}
                    AS compact_peeled_address
            FROM {{messy_residual}} AS messy
        """

        stripped_canonical_sql = f"""
            SELECT
                canon.canonical_ukam_address_id,
                canon.canonical_unique_id,
                canon.postcode,
                canon.peeled_address,
                canon.did_peel,
                {_compacted_address_sql("canon.peeled_address")}
                    AS compact_peeled_address
            FROM {{canonical_residual}} AS canon
        """

        stripped_candidates_sql = f"""
            SELECT
                messy.ukam_address_id,
                canon.canonical_ukam_address_id,
                canon.canonical_unique_id AS resolved_canonical_id,
                '{stripped_match_reason_value}'::ENUM {enum_values} AS match_reason
            FROM {{stripped_messy}} AS messy
            INNER JOIN {{stripped_canonical}} AS canon
                ON messy.postcode = canon.postcode
                AND messy.compact_peeled_address = canon.compact_peeled_address
            WHERE messy.compact_peeled_address <> ''
            AND (messy.did_peel OR canon.did_peel)
            AND (
                messy.compact_peeled_address <> messy.peeled_address
                OR canon.compact_peeled_address <> canon.peeled_address
            )
        """

        steps.extend(
            [
                CTEStep("messy_residual", messy_residual_sql),
                CTEStep("residual_postcodes", residual_postcodes_sql),
                CTEStep("canonical_residual", canonical_residual_sql),
                CTEStep("stripped_messy", stripped_messy_sql),
                CTEStep("stripped_canonical", stripped_canonical_sql),
                CTEStep("stripped_candidates", stripped_candidates_sql),
            ]
        )

        exact_postcode_matches_sql = """
            SELECT * FROM {pre_stripped_matches}
            UNION ALL
            SELECT * FROM {stripped_candidates}
        """
    else:
        exact_postcode_matches_sql = "SELECT * FROM {pre_stripped_matches}"

    steps.append(CTEStep("exact_postcode_matches", exact_postcode_matches_sql))

    if inward_postcode_levenshtein > 0:
        partial_messy_residual_sql = """
            SELECT messy.*
            FROM {messy_peeled} AS messy
            LEFT JOIN {exact_postcode_matches} AS matched
                ON matched.ukam_address_id = messy.ukam_address_id
            WHERE matched.ukam_address_id IS NULL
        """

        partial_canonical_restricted_sql = """
            SELECT
                canon.clean_full_address,
                canon.postcode,
                canon.unique_id AS canonical_unique_id,
                canon.ukam_address_id AS ukam_address_id
            FROM {__ukam__tmp_canonical_addresses} AS canon
            SEMI JOIN (
                SELECT DISTINCT
                    split_part(postcode, ' ', 1) AS postcode_outward
                FROM {partial_messy_residual}
                WHERE postcode IS NOT NULL
                AND split_part(postcode, ' ', 1) <> ''
            ) AS messy
                ON split_part(canon.postcode, ' ', 1) = messy.postcode_outward
            WHERE canon.unique_id IS NOT NULL
        """

        partial_canonical_peeled_sql = _build_regex_peel_sql(
            source_placeholder="partial_canonical_addresses_restricted",
            id_column="ukam_address_id",
            canonical=True,
        )

        partial_candidates_sql = f"""
            SELECT
                messy.ukam_address_id,
                canon.canonical_ukam_address_id,
                canon.canonical_unique_id AS resolved_canonical_id,
                '{partial_postcode_match_reason_value}'::ENUM {enum_values}
                    AS match_reason
            FROM {{partial_messy_residual}} AS messy
            INNER JOIN {{partial_canonical_peeled}} AS canon
                ON split_part(messy.postcode, ' ', 1)
                    = split_part(canon.postcode, ' ', 1)
                AND levenshtein(
                    split_part(messy.postcode, ' ', 2),
                    split_part(canon.postcode, ' ', 2)
                ) <= {inward_postcode_levenshtein}
                AND messy.peeled_address = canon.peeled_address
            WHERE messy.postcode <> canon.postcode
            AND split_part(messy.postcode, ' ', 2) <> ''
            AND split_part(canon.postcode, ' ', 2) <> ''
            AND (messy.did_peel OR canon.did_peel)
        """

        ranked_partial_candidates_sql = """
            SELECT
                candidates.ukam_address_id,
                candidates.canonical_ukam_address_id,
                candidates.resolved_canonical_id,
                candidates.match_reason,
                ROW_NUMBER() OVER (
                    PARTITION BY candidates.ukam_address_id
                    ORDER BY candidates.canonical_ukam_address_id
                ) AS rn
            FROM {partial_candidates} AS candidates
        """

        partial_matches_sql = """
            SELECT
                ukam_address_id,
                canonical_ukam_address_id,
                resolved_canonical_id,
                match_reason
            FROM {ranked_partial_candidates}
            WHERE rn = 1
        """

        steps.extend(
            [
                CTEStep("partial_messy_residual", partial_messy_residual_sql),
                CTEStep(
                    "partial_canonical_addresses_restricted",
                    partial_canonical_restricted_sql,
                ),
                CTEStep("partial_canonical_peeled", partial_canonical_peeled_sql),
                CTEStep("partial_candidates", partial_candidates_sql),
                CTEStep(
                    "ranked_partial_candidates",
                    ranked_partial_candidates_sql,
                ),
                CTEStep("partial_matches", partial_matches_sql),
            ]
        )

        final_matches_sql = """
            SELECT * FROM {exact_postcode_matches}
            UNION ALL
            SELECT * FROM {partial_matches}
        """
    else:
        final_matches_sql = "SELECT * FROM {exact_postcode_matches}"

    steps.append(CTEStep("peeled_address_matches", final_matches_sql))
    return steps


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


def _compacted_address_sql(expression: str) -> str:
    return rf"regexp_replace({expression}, '[^A-Z0-9]+', '', 'g')"


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
