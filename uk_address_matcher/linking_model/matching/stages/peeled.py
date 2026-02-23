from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, Optional

from uk_address_matcher.linking_model.matching.input_filters import (
    _restrict_canonical_to_messy_postcodes,
)
from uk_address_matcher.linking_model.matching.stages.base_stage import MatchingStage
from uk_address_matcher.sql_pipeline.helpers import package_resource_read_sql
from uk_address_matcher.sql_pipeline.match_reasons import MatchReason
from uk_address_matcher.sql_pipeline.steps import CTEStep, Stage, pipeline_stage

if TYPE_CHECKING:
    import duckdb

    from uk_address_matcher.sql_pipeline.runner import DebugOptions


DEFAULT_MAX_PEELED_WORDS = 6
DEFAULT_MAX_MATCH_TOKEN_COUNT = 6
MAX_PEELED_WORDS = DEFAULT_MAX_PEELED_WORDS
MAX_MATCH_TOKEN_COUNT = DEFAULT_MAX_MATCH_TOKEN_COUNT
PEEL_ITERATIONS = MAX_PEELED_WORDS

PeeledStrategy = Literal[
    "strategy_1_join",
    "strategy_2_map",
    "strategy_3_multiword_gate",
    "strategy_4_suffix_window",
]

_EXCLUDED_COUNTRY_PATTERNS = {
    "UK",
    "ENGLAND",
    "WALES",
    "SCOTLAND",
    "UNITED KINGDOM",
    "GREAT BRITAIN",
    "BRITAIN",
    "NORTHERN IRELAND",
}


@dataclass(frozen=True, repr=False)
class PeeledAddressStage(MatchingStage):
    """Match records after peeling common UK locality suffix tokens."""

    strategy: PeeledStrategy = "strategy_2_map"
    max_match_token_count: int = DEFAULT_MAX_MATCH_TOKEN_COUNT
    max_peeled_words: int = DEFAULT_MAX_PEELED_WORDS

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

        peeled_stage = _make_peeled_address_matches_stage(
            strategy=self.strategy,
            max_match_token_count=self.max_match_token_count,
            max_peeled_words=self.max_peeled_words,
        )

        return run_sql_pipeline(
            con=con,
            pipeline_stages=[
                _restrict_canonical_to_messy_postcodes("exact"),
                peeled_stage,
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
    return _build_peeled_address_match_steps(
        strategy="strategy_2_map",
        max_match_token_count=DEFAULT_MAX_MATCH_TOKEN_COUNT,
        max_peeled_words=DEFAULT_MAX_PEELED_WORDS,
    )


def _make_peeled_address_matches_stage(
    strategy: PeeledStrategy,
    max_match_token_count: int,
    max_peeled_words: int,
) -> Stage:
    @pipeline_stage(
        name="peeled_address_matching",
        description=(
            "Find matches by comparing addresses after peeling common UK end tokens "
            "(cities, counties, boroughs) and performing exact match on the peeled addresses."
        ),
        tags=["phase_1", "matching"],
        depends_on=["restrict_canonical_to_messy_postcodes"],
    )
    def _configured_stage() -> list[CTEStep]:
        return _build_peeled_address_match_steps(
            strategy=strategy,
            max_match_token_count=max_match_token_count,
            max_peeled_words=max_peeled_words,
        )

    return _configured_stage()


def _build_peeled_address_match_steps(
    strategy: PeeledStrategy,
    max_match_token_count: int,
    max_peeled_words: int,
) -> list[CTEStep]:
    """Find matches using peeled addresses after removing UK locality suffix tokens."""
    match_reason_value = MatchReason.PEELED_ADDRESS.value
    enum_values = str(MatchReason.enum_values())

    token_lookup_sql = _load_peeling_lookup_sql(
        max_match_token_count=max_match_token_count,
        max_peeled_words=max_peeled_words,
    )

    steps: list[CTEStep] = [CTEStep("uk_end_tokens_lookup", token_lookup_sql)]

    if strategy == "strategy_2_map":
        steps.append(CTEStep("uk_end_tokens_map", _build_lookup_map_sql()))

    if strategy in {"strategy_3_multiword_gate", "strategy_4_suffix_window"}:
        steps.append(
            CTEStep("uk_end_tokens_multi_last_words", _build_multi_last_words_sql())
        )

    messy_steps, messy_final = _build_peel_ctes(
        prefix="messy",
        source_placeholder="messy_addresses",
        strategy=strategy,
        max_match_token_count=max_match_token_count,
        max_peeled_words=max_peeled_words,
    )

    canonical_steps, canonical_final = _build_peel_ctes(
        prefix="canonical",
        source_placeholder="canonical_addresses_restricted",
        strategy=strategy,
        max_match_token_count=max_match_token_count,
        max_peeled_words=max_peeled_words,
    )

    canonical_keyed_all_sql = f"""
        SELECT
            postcode,
            peeled_address,
            MIN(ukam_address_id) AS canonical_ukam_address_id,
            arg_min(canonical_unique_id, ukam_address_id) AS resolved_canonical_id
        FROM {{{canonical_final}}}
        GROUP BY postcode, peeled_address
    """

    canonical_keyed_peeled_sql = f"""
        SELECT
            postcode,
            peeled_address,
            MIN(ukam_address_id) AS canonical_ukam_address_id,
            arg_min(canonical_unique_id, ukam_address_id) AS resolved_canonical_id
        FROM {{{canonical_final}}}
        WHERE did_peel
        GROUP BY postcode, peeled_address
    """

    messy_peeled_candidates_sql = f"""
        SELECT
            messy.ukam_address_id,
            canon.canonical_ukam_address_id,
            canon.resolved_canonical_id,
            '{match_reason_value}'::ENUM {enum_values} AS match_reason
        FROM {{{messy_final}}} AS messy
        INNER JOIN {{canonical_keyed_all}} AS canon
            ON messy.did_peel
            AND messy.postcode = canon.postcode
            AND messy.peeled_address = canon.peeled_address
    """

    messy_unpeeled_candidates_sql = f"""
        SELECT
            messy.ukam_address_id,
            canon.canonical_ukam_address_id,
            canon.resolved_canonical_id,
            '{match_reason_value}'::ENUM {enum_values} AS match_reason
        FROM {{{messy_final}}} AS messy
        INNER JOIN {{canonical_keyed_peeled}} AS canon
            ON NOT messy.did_peel
            AND messy.postcode = canon.postcode
            AND messy.peeled_address = canon.peeled_address
    """

    matches_sql = """
        SELECT * FROM {messy_peeled_candidates}
        UNION ALL
        SELECT * FROM {messy_unpeeled_candidates}
    """

    return [
        *steps,
        *messy_steps,
        *canonical_steps,
        CTEStep("canonical_keyed_all", canonical_keyed_all_sql),
        CTEStep("canonical_keyed_peeled", canonical_keyed_peeled_sql),
        CTEStep("messy_peeled_candidates", messy_peeled_candidates_sql),
        CTEStep("messy_unpeeled_candidates", messy_unpeeled_candidates_sql),
        CTEStep("peeled_address_matches", matches_sql),
    ]


def _load_peeling_lookup_sql(
    max_match_token_count: int = DEFAULT_MAX_MATCH_TOKEN_COUNT,
    max_peeled_words: int = DEFAULT_MAX_PEELED_WORDS,
) -> str:
    read_end_tokens_sql = package_resource_read_sql(
        "uk_address_matcher.data", "common_uk_end_tokens.json"
    )
    excluded_patterns = ", ".join(
        f"'{pattern}'" for pattern in sorted(_EXCLUDED_COUNTRY_PATTERNS)
    )
    return f"""
        WITH json_data AS (
            {read_end_tokens_sql}
        ),
        raw_patterns AS (
            SELECT unnest(single_tokens) AS raw_pattern FROM json_data
            UNION ALL
            SELECT unnest(multi_tokens) AS raw_pattern FROM json_data
        ),
        normalised AS (
            SELECT
                regexp_replace(
                    REPLACE(REPLACE(UPPER(TRIM(raw_pattern)), '&', ' AND '), '-', ' '),
                    '\\s+',
                    ' ',
                    'g'
                ) AS pattern
            FROM raw_patterns
            WHERE raw_pattern IS NOT NULL
        ),
        filtered AS (
            SELECT
                pattern,
                len(string_split(pattern, ' '))::INTEGER AS token_count,
                reverse(split_part(reverse(pattern), ' ', 1)) AS last_word
            FROM normalised
            WHERE pattern <> ''
                AND pattern NOT IN ({excluded_patterns})
        )
        SELECT DISTINCT
            pattern,
            pattern AS lookup_key,
            token_count,
            last_word
        FROM filtered
        WHERE token_count BETWEEN 1 AND {max_match_token_count}
            AND token_count <= {max_peeled_words}
    """


def _build_lookup_map_sql() -> str:
    return """
        SELECT map(list(lookup_key), list(token_count)) AS token_map
        FROM {uk_end_tokens_lookup}
    """


def _build_multi_last_words_sql() -> str:
    return """
        SELECT DISTINCT last_word
        FROM {uk_end_tokens_lookup}
        WHERE token_count > 1
    """


def _build_peel_ctes(
    prefix: str,
    source_placeholder: str,
    strategy: PeeledStrategy = "strategy_1_join",
    max_match_token_count: int = DEFAULT_MAX_MATCH_TOKEN_COUNT,
    max_peeled_words: int = DEFAULT_MAX_PEELED_WORDS,
) -> tuple[list[CTEStep], str]:
    steps: list[CTEStep] = []
    candidates_name = f"{prefix}_candidates"
    steps.append(
        CTEStep(
            candidates_name,
            _candidate_gate_sql(
                source_placeholder=source_placeholder,
                strategy=strategy,
            ),
        )
    )
    tokenised_name = f"{prefix}_tokenised"
    steps.append(
        CTEStep(
            tokenised_name,
            _tokenise_sql(
                source_placeholder=candidates_name,
                strategy=strategy,
                max_match_token_count=max_match_token_count,
                max_peeled_words=max_peeled_words,
            ),
        )
    )

    prev = tokenised_name
    for i in range(max_peeled_words):
        step_name = f"{prefix}_peel_{i}"
        steps.append(
            CTEStep(
                step_name,
                _make_peel_iteration_sql(
                    prev_cte=prev,
                    strategy=strategy,
                    max_match_token_count=max_match_token_count,
                    max_peeled_words=max_peeled_words,
                ),
            )
        )
        prev = step_name

    final_name = f"{prefix}_with_peeled"
    steps.append(
        CTEStep(
            final_name,
            _final_peel_sql(
                prev_cte=prev,
                strategy=strategy,
            ),
        )
    )
    return steps, final_name


def _candidate_gate_sql(source_placeholder: str, strategy: PeeledStrategy) -> str:
    multiword_join = ""
    multiword_flag = "FALSE AS __is_multiword_candidate"

    if strategy in {"strategy_3_multiword_gate", "strategy_4_suffix_window"}:
        multiword_join = """
            LEFT JOIN {uk_end_tokens_multi_last_words} AS mw
                ON wl.__last_word = mw.last_word
        """
        multiword_flag = "mw.last_word IS NOT NULL AS __is_multiword_candidate"

    return f"""
        WITH with_last_word AS (
            SELECT
                *,
                reverse(split_part(reverse(trim(clean_full_address)), ' ', 1)) AS __last_word
            FROM {{{source_placeholder}}}
        )
        SELECT
            wl.*,
            __last_word IN (SELECT DISTINCT last_word FROM {{uk_end_tokens_lookup}}) AS __is_candidate,
            {multiword_flag}
        FROM with_last_word AS wl
        {multiword_join}
    """


def _tokenise_sql(
    source_placeholder: str,
    strategy: PeeledStrategy,
    max_match_token_count: int,
    max_peeled_words: int,
) -> str:
    if strategy == "strategy_4_suffix_window":
        suffix_window = max_peeled_words + max_match_token_count - 1
        return f"""
            WITH tokenised AS (
                SELECT
                    *,
                    CASE
                        WHEN __is_candidate THEN regexp_extract(
                            clean_full_address,
                            '(?:\\S+\\s+){{0,{suffix_window - 1}}}\\S+$',
                            0
                        )
                        ELSE NULL
                    END AS __suffix_window
                FROM {{{source_placeholder}}}
            )
            SELECT
                * EXCLUDE (__suffix_window),
                CASE
                    WHEN __suffix_window IS NULL THEN NULL
                    ELSE string_split(__suffix_window, ' ')
                END AS __tokens,
                CASE
                    WHEN __suffix_window IS NULL THEN 0
                    ELSE len(string_split(__suffix_window, ' '))
                END::INTEGER AS __n_tokens,
                0::INTEGER AS __peeled_words,
                __is_candidate AS __can_still_peel
            FROM tokenised
        """

    return f"""
        WITH tokenised AS (
            SELECT
                *,
                CASE
                    WHEN __is_candidate THEN string_split(clean_full_address, ' ')
                    ELSE NULL
                END AS __tokens
            FROM {{{source_placeholder}}}
        )
        SELECT
            * EXCLUDE (__tokens),
            __tokens,
            CASE
                WHEN __tokens IS NULL THEN 0
                ELSE len(__tokens)
            END::INTEGER AS __n_tokens,
            0::INTEGER AS __peeled_words,
            __is_candidate AS __can_still_peel
        FROM tokenised
    """


def _make_peel_iteration_sql(
    prev_cte: str,
    strategy: PeeledStrategy,
    max_match_token_count: int,
    max_peeled_words: int,
) -> str:
    if strategy == "strategy_2_map":
        return _make_peel_iteration_sql_strategy_2(
            prev_cte=prev_cte,
            max_match_token_count=max_match_token_count,
            max_peeled_words=max_peeled_words,
        )

    return _make_peel_iteration_sql_join_based(
        prev_cte=prev_cte,
        strategy=strategy,
        max_match_token_count=max_match_token_count,
        max_peeled_words=max_peeled_words,
    )


def _make_peel_iteration_sql_join_based(
    prev_cte: str,
    strategy: PeeledStrategy,
    max_match_token_count: int,
    max_peeled_words: int,
) -> str:
    match_width = min(max_match_token_count, max_peeled_words)
    use_multiword_gate = strategy == "strategy_3_multiword_gate"

    end_fields: list[str] = []
    for width in range(1, match_width + 1):
        if width == 1:
            phrase_expr = "__tokens[__idx]"
        else:
            parts = [f"__tokens[__idx-{offset}]" for offset in range(width - 1, -1, -1)]
            phrase_expr = " || ' ' || ".join(parts)

        gate_multiword = ""
        if use_multiword_gate and width > 1:
            gate_multiword = "AND __allow_multiword "

        end_fields.append(
            f"""CASE
                    WHEN __can_still_peel
                        {gate_multiword}
                        AND __idx >= {width}
                        AND __peeled_words + {width} <= {max_peeled_words}
                    THEN {phrase_expr}
                    ELSE NULL
                END AS end{width}"""
        )

    join_clauses = []
    for width in range(match_width, 0, -1):
        gate_multiword = ""
        if use_multiword_gate and width > 1:
            gate_multiword = "AND e.__allow_multiword "
        join_clauses.append(
            f"""LEFT JOIN {{uk_end_tokens_lookup}} l{width}
                ON e.__can_still_peel
                {gate_multiword}
                AND l{width}.token_count = {width}
                AND l{width}.lookup_key = e.end{width}"""
        )

    coalesce_patterns = ", ".join(
        [f"l{width}.pattern" for width in range(match_width, 0, -1)]
    )

    matched_len_case = "\n".join(
        [
            f"WHEN l{width}.pattern IS NOT NULL THEN {width}"
            for width in range(match_width, 0, -1)
        ]
    )

    exclude_cols = [
        "__idx",
        "__matched_pattern",
        "__matched_len",
        "__peeled_words",
        "__can_still_peel",
        "__allow_multiword",
    ] + [f"end{width}" for width in range(1, match_width + 1)]

    exclude_cols_sql = ",\n                ".join(exclude_cols)

    return f"""
        WITH __with_idx AS (
            SELECT
                *,
                __n_tokens - __peeled_words AS __idx,
                {"__tokens[__n_tokens - __peeled_words] IN (SELECT last_word FROM {uk_end_tokens_multi_last_words}) AS __allow_multiword" if use_multiword_gate else "FALSE AS __allow_multiword"}
            FROM {{{prev_cte}}}
        ),
        __with_ends AS (
            SELECT
                *,
                {", ".join(end_fields)}
            FROM __with_idx
        ),
        __matched AS (
            SELECT
                e.*,
                COALESCE({coalesce_patterns}) AS __matched_pattern,
                CASE
                    {matched_len_case}
                    ELSE 0
                END AS __matched_len
            FROM __with_ends e
            {" ".join(join_clauses)}
        )
        SELECT
            * EXCLUDE ({exclude_cols_sql}),
            __peeled_words + __matched_len AS __peeled_words,
            CASE
                WHEN __matched_len > 0
                    AND (__n_tokens - (__peeled_words + __matched_len)) > 0
                    AND (__peeled_words + __matched_len) < {max_peeled_words}
                THEN TRUE
                ELSE FALSE
            END AS __can_still_peel
        FROM __matched
    """


def _make_peel_iteration_sql_strategy_2(
    prev_cte: str,
    max_match_token_count: int,
    max_peeled_words: int,
) -> str:
    match_width = min(max_match_token_count, max_peeled_words)

    end_fields: list[str] = []
    value_fields: list[str] = []

    for width in range(1, match_width + 1):
        if width == 1:
            phrase_expr = "__tokens[__idx]"
        else:
            parts = [f"__tokens[__idx-{offset}]" for offset in range(width - 1, -1, -1)]
            phrase_expr = " || ' ' || ".join(parts)

        end_fields.append(
            f"""CASE
                    WHEN __can_still_peel
                        AND __idx >= {width}
                        AND __peeled_words + {width} <= {max_peeled_words}
                    THEN {phrase_expr}
                    ELSE NULL
                END AS end{width}"""
        )
        value_fields.append(
            f"COALESCE(CAST(map_extract_value(m.token_map, e.end{width}) AS INTEGER), 0) AS v{width}"
        )

    matched_len_case = "\n".join(
        [f"WHEN v{width} = {width} THEN {width}" for width in range(match_width, 0, -1)]
    )

    exclude_cols = (
        [
            "__idx",
            "__matched_len",
            "__peeled_words",
            "__can_still_peel",
        ]
        + [f"end{width}" for width in range(1, match_width + 1)]
        + [f"v{width}" for width in range(1, match_width + 1)]
    )

    exclude_cols_sql = ",\n                ".join(exclude_cols)

    return f"""
        WITH __with_idx AS (
            SELECT
                *,
                __n_tokens - __peeled_words AS __idx
            FROM {{{prev_cte}}}
        ),
        __with_ends AS (
            SELECT
                *,
                {", ".join(end_fields)}
            FROM __with_idx
        ),
        __with_values AS (
            SELECT
                e.*,
                {", ".join(value_fields)},
                CASE
                    {matched_len_case}
                    ELSE 0
                END AS __matched_len
            FROM __with_ends AS e
            CROSS JOIN {{uk_end_tokens_map}} AS m
        )
        SELECT
            * EXCLUDE ({exclude_cols_sql}),
            __peeled_words + __matched_len AS __peeled_words,
            CASE
                WHEN __matched_len > 0
                AND (
                    __n_tokens - (__peeled_words + __matched_len)
                ) > 0
                AND (__peeled_words + __matched_len) < {max_peeled_words}
                THEN TRUE
                ELSE FALSE
            END AS __can_still_peel
        FROM __with_values
    """


def _final_peel_sql(prev_cte: str, strategy: PeeledStrategy) -> str:
    if strategy == "strategy_4_suffix_window":
        return f"""
            WITH with_suffix AS (
                SELECT
                    *,
                    CASE
                        WHEN __peeled_words = 0 THEN NULL
                        ELSE array_to_string(
                            list_slice(__tokens, __n_tokens - __peeled_words + 1, __n_tokens),
                            ' '
                        )
                    END AS __peeled_suffix
                FROM {{{prev_cte}}}
            )
            SELECT
                * EXCLUDE (
                    __tokens,
                    __n_tokens,
                    __peeled_words,
                    __can_still_peel,
                    __last_word,
                    __is_candidate,
                    __is_multiword_candidate,
                    __peeled_suffix
                ),
                __peeled_words AS peeled_word_count,
                __peeled_words > 0 AS did_peel,
                CASE
                    WHEN __peeled_words = 0 THEN clean_full_address
                    WHEN __peeled_suffix IS NULL THEN clean_full_address
                    ELSE LEFT(
                        clean_full_address,
                        GREATEST(0, LENGTH(clean_full_address) - LENGTH(__peeled_suffix) - 1)
                    )
                END AS peeled_address
            FROM with_suffix
        """

    return f"""
        SELECT
            * EXCLUDE (
                __tokens,
                __n_tokens,
                __peeled_words,
                __can_still_peel,
                __last_word,
                __is_candidate,
                __is_multiword_candidate
            ),
            __peeled_words AS peeled_word_count,
            __peeled_words > 0 AS did_peel,
            CASE
                WHEN __peeled_words = 0 THEN clean_full_address
                ELSE array_to_string(
                    list_slice(__tokens, 1, __n_tokens - __peeled_words),
                    ' '
                )
            END AS peeled_address
        FROM {{{prev_cte}}}
    """
