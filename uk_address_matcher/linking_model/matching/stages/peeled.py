from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from uk_address_matcher.linking_model.matching.input_filters import (
    _restrict_canonical_to_messy_postcodes,
)
from uk_address_matcher.linking_model.matching.stages.base_stage import MatchingStage
from uk_address_matcher.sql_pipeline.helpers import package_resource_read_sql
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
    token_lookup_sql = _load_peeling_lookup_sql()

    messy_steps, messy_final = _build_peel_ctes(
        prefix="messy",
        source_placeholder="messy_addresses",
    )

    canonical_steps, canonical_final = _build_peel_ctes(
        prefix="canonical",
        source_placeholder="canonical_addresses_restricted",
    )

    messy_peeled_sql = f"""
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
        FROM {{{messy_final}}}
    """

    canonical_peeled_sql = f"""
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
        FROM {{{canonical_final}}}
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
        CTEStep("uk_end_tokens_lookup", token_lookup_sql),
        *messy_steps,
        *canonical_steps,
        CTEStep("messy_peeled", messy_peeled_sql),
        CTEStep("canonical_peeled", canonical_peeled_sql),
        CTEStep("peeled_address_candidates", candidates_sql),
        CTEStep("peeled_address_matches", annotated_sql),
    ]


PEEL_ITERATIONS = 5


def _load_peeling_lookup_sql() -> str:
    read_end_tokens_sql = package_resource_read_sql(
        "uk_address_matcher.data", "common_uk_end_tokens.json"
    )
    return f"""
        WITH json_data AS (
            {read_end_tokens_sql}
        ),
        single_tokens AS (
            SELECT UPPER(TRIM(unnest(single_tokens))) AS pattern, 1 AS token_count
            FROM json_data
        ),
        multi_tokens AS (
            SELECT
                UPPER(TRIM(unnest(multi_tokens))) AS pattern,
                length(pattern) - length(replace(pattern, ' ', '')) + 1 AS token_count
            FROM json_data
        ),
        all_patterns AS (
            SELECT DISTINCT pattern, token_count
            FROM (
                SELECT * FROM single_tokens
                UNION ALL
                SELECT * FROM multi_tokens
            )
        ),
        exact_keys AS (
            SELECT pattern, pattern AS lookup_key, token_count, 0 AS edit_dist
            FROM all_patterns
        ),
        deletion_keys AS (
            SELECT
                pattern,
                substr(pattern, 1, i - 1) || substr(pattern, i + 1) AS lookup_key,
                token_count,
                1 AS edit_dist
            FROM all_patterns, generate_series(1, length(pattern)) AS t(i)
            WHERE token_count = 1
                        AND length(pattern) >= 5
                            AND {{fuzzy_enabled}}
        ),
        transposition_keys AS (
            SELECT
                pattern,
                substr(pattern, 1, i - 1) ||
                substr(pattern, i + 1, 1) ||
                substr(pattern, i, 1) ||
                substr(pattern, i + 2) AS lookup_key,
                token_count,
                1 AS edit_dist
            FROM all_patterns, generate_series(1, length(pattern) - 1) AS t(i)
            WHERE token_count = 1
                        AND length(pattern) >= 4
                            AND {{fuzzy_enabled}}
        ),
        all_keys AS (
            SELECT * FROM exact_keys
            UNION ALL
            SELECT * FROM deletion_keys
                WHERE lookup_key NOT IN (SELECT pattern FROM all_patterns)
            UNION ALL
            SELECT * FROM transposition_keys
                WHERE lookup_key NOT IN (SELECT pattern FROM all_patterns)
        )
        SELECT DISTINCT ON (lookup_key)
            pattern,
            lookup_key,
            token_count
        FROM all_keys
        ORDER BY lookup_key, edit_dist
    """.format(fuzzy_enabled="TRUE")


def _build_peel_ctes(prefix: str, source_placeholder: str) -> tuple[list[CTEStep], str]:
    steps: list[CTEStep] = []
    tokenised_name = f"{prefix}_tokenised"
    steps.append(CTEStep(tokenised_name, _tokenise_sql(source_placeholder)))
    prev = tokenised_name
    for i in range(PEEL_ITERATIONS):
        step_name = f"{prefix}_peel_{i}"
        steps.append(CTEStep(step_name, _make_peel_iteration_sql(prev)))
        prev = step_name
    final_name = f"{prefix}_with_peeled"
    steps.append(CTEStep(final_name, _final_peel_sql(prev)))
    return steps, final_name


def _tokenise_sql(source_placeholder: str) -> str:
    return f"""
        SELECT
            *,
            string_split(clean_full_address, ' ') AS __tokens,
            CAST([] AS VARCHAR[]) AS __peeled,
            TRUE AS __can_still_peel
        FROM {{{source_placeholder}}}
    """


def _make_peel_iteration_sql(prev_cte: str) -> str:
    return f"""
        WITH __with_ends AS (
            SELECT
                *,
                len(__tokens) AS __n,
                 CASE WHEN __can_still_peel AND len(__tokens) >= 1
                     THEN __tokens[len(__tokens)]
                     ELSE NULL END AS end1,
                 CASE WHEN __can_still_peel AND len(__tokens) >= 2
                     THEN array_to_string(
                         list_slice(__tokens, len(__tokens) - 1, len(__tokens)),
                         ' '
                     )
                     ELSE NULL END AS end2,
                 CASE WHEN __can_still_peel AND len(__tokens) >= 3
                     THEN array_to_string(
                         list_slice(__tokens, len(__tokens) - 2, len(__tokens)),
                         ' '
                     )
                     ELSE NULL END AS end3
            FROM {{{prev_cte}}}
        ),
        __matched AS (
            SELECT
                e.*,
                l3.pattern AS match3,
                l2.pattern AS match2,
                l1.pattern AS match1
            FROM __with_ends e
            LEFT JOIN {{uk_end_tokens_lookup}} l3
                ON e.__can_still_peel AND l3.token_count = 3 AND l3.lookup_key = e.end3
            LEFT JOIN {{uk_end_tokens_lookup}} l2
                ON e.__can_still_peel AND l2.token_count = 2 AND l2.lookup_key = e.end2
            LEFT JOIN {{uk_end_tokens_lookup}} l1
                ON e.__can_still_peel AND l1.token_count = 1 AND l1.lookup_key = e.end1
        )
        SELECT
            * EXCLUDE (
                __n,
                end1,
                end2,
                end3,
                match1,
                match2,
                match3,
                __tokens,
                __peeled,
                __can_still_peel
            ),
            CASE
                WHEN match3 IS NOT NULL THEN list_slice(__tokens, 1, __n - 3)
                WHEN match2 IS NOT NULL THEN list_slice(__tokens, 1, __n - 2)
                WHEN match1 IS NOT NULL THEN list_slice(__tokens, 1, __n - 1)
                ELSE __tokens
            END AS __tokens,
            CASE
                WHEN match3 IS NOT NULL THEN list_prepend(match3, __peeled)
                WHEN match2 IS NOT NULL THEN list_prepend(match2, __peeled)
                WHEN match1 IS NOT NULL THEN list_prepend(match1, __peeled)
                ELSE __peeled
            END AS __peeled,
            CASE
                WHEN match3 IS NOT NULL THEN __n > 3
                WHEN match2 IS NOT NULL THEN __n > 2
                WHEN match1 IS NOT NULL THEN __n > 1
                ELSE FALSE
            END AS __can_still_peel
        FROM __matched
    """


def _final_peel_sql(prev_cte: str) -> str:
    return f"""
        SELECT
            * EXCLUDE (__tokens, __peeled, __can_still_peel),
            __peeled AS peeled_tokens_list
        FROM {{{prev_cte}}}
    """
