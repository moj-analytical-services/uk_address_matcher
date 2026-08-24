from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from uk_address_matcher.cleaning.steps.inverted_index_strategies import (
    InvertedIndexPortfolio as InvertedIndexBuildPortfolio,
    range_context_ctes,
)
from uk_address_matcher.sql_pipeline.steps import CTEStep, pipeline_stage


@dataclass(frozen=True)
class SignatureKeyGenerator:
    """Name the SQL expression that produces one or more signature keys."""

    name: str
    keys_sql_expr: str


@dataclass(frozen=True)
class PhysicalIndexStrategy:
    """Defines a persisted canonical inverted-index family."""

    name: str
    key_generator: SignatureKeyGenerator
    maximum_posting_size: int

    def __post_init__(self) -> None:
        if self.maximum_posting_size < 1:
            raise ValueError("maximum_posting_size must be positive")

    @property
    def keys_sql_expr(self) -> str:
        """Expose physical key SQL for read-only reporting compatibility."""
        return self.key_generator.keys_sql_expr


@dataclass(frozen=True)
class InvertedIndexLookupStrategy:
    """Defines transient source keys and their canonical target index."""

    name: str
    source_key_generator: SignatureKeyGenerator
    target_index: PhysicalIndexStrategy
    maximum_posting_size_override: int | None = None
    contributes_signature_evidence: bool = True
    transformation_cost: int = 0
    lookup_precedence: int = 0

    def __post_init__(self) -> None:
        if (
            self.maximum_posting_size_override is not None
            and self.maximum_posting_size_override < 1
        ):
            raise ValueError("maximum_posting_size_override must be positive")
        if self.transformation_cost < 0:
            raise ValueError("transformation_cost cannot be negative")
        if self.lookup_precedence < 0:
            raise ValueError("lookup_precedence cannot be negative")

    @property
    def maximum_posting_size(self) -> int:
        return (
            self.maximum_posting_size_override
            if self.maximum_posting_size_override is not None
            else self.target_index.maximum_posting_size
        )


@dataclass(frozen=True)
class InvertedIndexPortfolio:
    """Names the physical indexes and transient lookups enabled together."""

    name: str
    physical_indexes: tuple[PhysicalIndexStrategy, ...]
    lookup_strategies: tuple[InvertedIndexLookupStrategy, ...]


ADJACENT_TRIGRAM_KEYS = SignatureKeyGenerator(
    name="adjacent_trigram_keys",
    keys_sql_expr="""\
CASE
    WHEN len(__tokens) >= 3 THEN
        list_transform(
            generate_series(1, len(__tokens) - 2),
            __i -> __tokens[__i]
                   || ' ' || __tokens[__i + 1]
                   || ' ' || __tokens[__i + 2]
        )
    ELSE []::VARCHAR[]
END""",
)

ADJACENT_BIGRAM_KEYS = SignatureKeyGenerator(
    name="adjacent_bigram_keys",
    keys_sql_expr="""\
CASE
    WHEN len(__tokens) >= 2 THEN
        list_transform(
            generate_series(1, len(__tokens) - 1),
            __i -> __tokens[__i] || ' ' || __tokens[__i + 1]
        )
    ELSE []::VARCHAR[]
END""",
)
SKIP1_TRIGRAM_KEYS = SignatureKeyGenerator(
    name="skip1_trigram_keys",
    keys_sql_expr="""\
CASE
    WHEN len(__tokens) >= 4 THEN list_concat(
        list_transform(
            generate_series(1, len(__tokens) - 3),
            __i -> __tokens[__i] || ' ' || __tokens[__i + 1] || ' ' || __tokens[__i + 3]
        ),
        list_transform(
            generate_series(1, len(__tokens) - 3),
            __i -> __tokens[__i] || ' ' || __tokens[__i + 2] || ' ' || __tokens[__i + 3]
        )
    )
    ELSE []::VARCHAR[]
END""",
)
SKIP1_BIGRAM_KEYS = SignatureKeyGenerator(
    name="skip1_bigram_keys",
    keys_sql_expr="""\
CASE
    WHEN len(__tokens) >= 3 THEN
        list_transform(
            generate_series(1, len(__tokens) - 2),
            __i -> __tokens[__i] || ' ' || __tokens[__i + 2]
        )
    ELSE []::VARCHAR[]
END""",
)
SKIP2_BIGRAM_KEYS = SignatureKeyGenerator(
    name="skip2_bigram_keys",
    keys_sql_expr="""\
CASE
    WHEN len(__tokens) >= 4 THEN
        list_transform(
            generate_series(1, len(__tokens) - 3),
            __i -> __tokens[__i] || ' ' || __tokens[__i + 3]
        )
    ELSE []::VARCHAR[]
END""",
)
SKIP2_TRIGRAM_KEYS = SignatureKeyGenerator(
    name="skip2_trigram_keys",
    keys_sql_expr="""\
CASE
    WHEN len(__tokens) >= 5 THEN list_concat(
        list_transform(
            generate_series(1, len(__tokens) - 4),
            __i -> __tokens[__i] || ' ' || __tokens[__i + 1] || ' ' || __tokens[__i + 4]
        ),
        list_transform(
            generate_series(1, len(__tokens) - 4),
            __i -> __tokens[__i] || ' ' || __tokens[__i + 2] || ' ' || __tokens[__i + 4]
        ),
        list_transform(
            generate_series(1, len(__tokens) - 4),
            __i -> __tokens[__i] || ' ' || __tokens[__i + 3] || ' ' || __tokens[__i + 4]
        )
    )
    ELSE []::VARCHAR[]
END""",
)

TRIGRAM_INDEX = PhysicalIndexStrategy(
    name="trigram",
    key_generator=ADJACENT_TRIGRAM_KEYS,
    maximum_posting_size=InvertedIndexBuildPortfolio().posting_caps()["trigram"],
)
BIGRAM_INDEX = PhysicalIndexStrategy(
    name="bigram",
    key_generator=ADJACENT_BIGRAM_KEYS,
    maximum_posting_size=InvertedIndexBuildPortfolio().posting_caps()["bigram"],
)
TRIGRAM_LOOKUP = InvertedIndexLookupStrategy(
    name="trigram",
    source_key_generator=ADJACENT_TRIGRAM_KEYS,
    target_index=TRIGRAM_INDEX,
)
BIGRAM_LOOKUP = InvertedIndexLookupStrategy(
    name="bigram",
    source_key_generator=ADJACENT_BIGRAM_KEYS,
    target_index=BIGRAM_INDEX,
)
SKIP1_TRIGRAM_LOOKUP = InvertedIndexLookupStrategy(
    name="skip1_trigram",
    source_key_generator=SKIP1_TRIGRAM_KEYS,
    target_index=TRIGRAM_INDEX,
    maximum_posting_size_override=5,
    contributes_signature_evidence=False,
    transformation_cost=1,
    lookup_precedence=1,
)
SKIP1_BIGRAM_LOOKUP = InvertedIndexLookupStrategy(
    name="skip1_bigram",
    source_key_generator=SKIP1_BIGRAM_KEYS,
    target_index=BIGRAM_INDEX,
    maximum_posting_size_override=5,
    contributes_signature_evidence=False,
    transformation_cost=1,
    lookup_precedence=1,
)
SKIP2_BIGRAM_LOOKUP = InvertedIndexLookupStrategy(
    name="skip2_bigram",
    source_key_generator=SKIP2_BIGRAM_KEYS,
    target_index=BIGRAM_INDEX,
    maximum_posting_size_override=5,
    contributes_signature_evidence=False,
    transformation_cost=2,
    lookup_precedence=2,
)
SKIP2_TRIGRAM_LOOKUP = InvertedIndexLookupStrategy(
    name="skip2_trigram",
    source_key_generator=SKIP2_TRIGRAM_KEYS,
    target_index=TRIGRAM_INDEX,
    maximum_posting_size_override=5,
    contributes_signature_evidence=False,
    transformation_cost=2,
    lookup_precedence=2,
)
BASE_INDEX_PORTFOLIO = InvertedIndexPortfolio(
    name="base",
    physical_indexes=(BIGRAM_INDEX, TRIGRAM_INDEX),
    lookup_strategies=(BIGRAM_LOOKUP, TRIGRAM_LOOKUP),
)
DEFAULT_INDEXING_STRATEGIES = list(BASE_INDEX_PORTFOLIO.physical_indexes)
DEFAULT_INVERTED_INDEX_LOOKUP_STRATEGIES = list(BASE_INDEX_PORTFOLIO.lookup_strategies)
MESSY_INVERTED_INDEX_LOOKUP_STRATEGIES = [
    *DEFAULT_INVERTED_INDEX_LOOKUP_STRATEGIES,
    SKIP1_BIGRAM_LOOKUP,
    SKIP2_BIGRAM_LOOKUP,
    SKIP1_TRIGRAM_LOOKUP,
    SKIP2_TRIGRAM_LOOKUP,
]


def _derive_keys_for_strategy(
    strategy: PhysicalIndexStrategy,
    *,
    num_of_chunks: int | None = None,
    chunk_index: int | None = None,
):
    """Create a stage deriving canonical keys for one physical strategy."""
    chunked = num_of_chunks is not None and chunk_index is not None
    chunk_label = f" (chunk {chunk_index + 1}/{num_of_chunks})" if chunked else ""

    @pipeline_stage(
        name=f"derive_keys_{strategy.name}",
        description=f"Generate {strategy.name} canonical keys{chunk_label}",
        tags="inverted_index",
    )
    def _stage():
        keys_expr = strategy.key_generator.keys_sql_expr
        filtered_expr = keys_expr
        if chunked:
            filtered_expr = (
                "list_filter("
                f"{keys_expr}, "
                f"__k -> (abs(hash(__k)) % {num_of_chunks}) = {chunk_index}"
                ")"
            )
        return f"""
        SELECT
            unique_id,
            {filtered_expr} AS __index_keys
        FROM (
            SELECT *, regexp_split_to_array(trim(clean_full_address), '\\s+') AS __tokens
            FROM {{input}}
        ) AS tokenised
        """

    return _stage


def _build_inverted_index_from_keys(strategy: PhysicalIndexStrategy):
    """Create a stage that applies the physical strategy's authoritative cap."""
    maximum_posting_size = strategy.maximum_posting_size

    @pipeline_stage(
        name=f"build_inverted_index_{strategy.name}",
        description=(
            f"Aggregate {strategy.name} keys into inverted index "
            f"(max {maximum_posting_size} unique_ids per key)"
        ),
        tags="inverted_index",
    )
    def _stage():
        return f"""
        WITH unnested_keys AS (
            SELECT unique_id, unnest(__index_keys) AS key
            FROM {{input}}
        ),
        grouped AS (
            SELECT
                key,
                list(DISTINCT unique_id ORDER BY unique_id) AS unique_ids,
                COUNT(DISTINCT unique_id) AS count_unique_ids
            FROM unnested_keys
            GROUP BY key
        )
        SELECT
            key,
            unique_ids,
            '{strategy.name}' AS index_strategy
        FROM grouped
        WHERE count_unique_ids BETWEEN 1 AND {maximum_posting_size}
        """

    return _stage


def _lookup_keys_in_inverted_index(
    strategies: Sequence[InvertedIndexLookupStrategy] | None = None,
    *,
    canonical_range_slots_table: str | None = None,
):
    """Create the transient source-key lookup stage."""
    lookup_strategies = tuple(strategies or DEFAULT_INVERTED_INDEX_LOOKUP_STRATEGIES)
    if not all(
        isinstance(strategy, InvertedIndexLookupStrategy)
        for strategy in lookup_strategies
    ):
        raise TypeError("strategies must contain InvertedIndexLookupStrategy values")

    @pipeline_stage(
        name="lookup_keys_in_inverted_index",
        description=(
            "Look up transient source keys to populate candidates and signature evidence"
        ),
        tags="inverted_index",
    )
    def _stage():
        base_sql = """
        SELECT *, regexp_split_to_array(trim(clean_full_address), '\\s+') AS __tokens
        FROM {input}
        """
        union_parts = []
        for strategy in lookup_strategies:
            evidence_mode = (
                "scored" if strategy.contributes_signature_evidence else "candidate_only"
            )
            contributes_evidence = str(strategy.contributes_signature_evidence).upper()
            union_parts.append(
                f"SELECT unique_id AS __messy_uid, "
                f"unnest({strategy.source_key_generator.keys_sql_expr}) AS __key, "
                f"'{strategy.name}' AS __source_strategy, "
                f"'{strategy.target_index.name}' AS __lookup_strategy, "
                f"'{evidence_mode}' AS __evidence_mode, "
                f"{contributes_evidence} AS __contributes_signature_evidence, "
                f"{strategy.maximum_posting_size} AS __maximum_posting_size, "
                f"{strategy.transformation_cost} AS __transformation_cost, "
                f"{strategy.lookup_precedence} AS __lookup_precedence, "
                "NULL::UINTEGER AS __probe_value, "
                "NULL::VARCHAR AS __probe_context, "
                "NULL::UTINYINT AS __probe_role, "
                "FALSE AS __requires_range_verification "
                "FROM {base}"
            )
        unnested_keys_sql = " UNION ALL ".join(union_parts)

        if canonical_range_slots_table is not None:
            portfolio = InvertedIndexBuildPortfolio()
            ranges = portfolio.numeric_ranges
            range_context_sql = range_context_ctes(
                source_name="{base}",
                portfolio=portfolio,
            )
            unnested_keys_sql = f"""
            WITH ordinary_requested AS (
                {unnested_keys_sql}
            ),
            scalar_positions AS (
                SELECT
                    base.unique_id,
                    base.__tokens,
                    token,
                    token_position,
                    row_number() OVER (
                        PARTITION BY base.unique_id
                        ORDER BY token_position
                    ) AS scalar_ordinal
                FROM {{base}} AS base
                CROSS JOIN UNNEST(base.__tokens)
                    WITH ORDINALITY AS tokens(token, token_position)
                WHERE base.numeric_range_count = 0
                  AND len(base.numeric_scalar_tokens) > 0
                  AND regexp_full_match(token, '\\d{{1,5}}')
            ),
            scalar_values AS (
                SELECT
                    scalar_positions.*,
                    list_extract(base.numeric_scalar_tokens, scalar_ordinal)
                        AS probe_value,
                    list_extract(base.numeric_scalar_suffixes, scalar_ordinal)
                        AS probe_suffix,
                    list_extract(base.numeric_scalar_roles, scalar_ordinal)
                        AS probe_role
                FROM scalar_positions
                                INNER JOIN {{base}} AS base
                                    ON base.unique_id = scalar_positions.unique_id
                                WHERE list_extract(
                                    base.numeric_scalar_tokens, scalar_ordinal
                                ) = try_cast(token AS UINTEGER)
                  AND COALESCE(
                    list_extract(base.numeric_scalar_suffixes, scalar_ordinal), ''
                  ) = ''
            ),
            masked_scalars AS (
                SELECT
                    scalar_values.*,
                    list_concat(
                        CASE WHEN token_position > 1
                            THEN list_slice(__tokens, 1, token_position - 1)
                            ELSE []::VARCHAR[]
                        END,
                        ['{ranges.mask_token}']::VARCHAR[],
                        CASE WHEN token_position < len(__tokens)
                            THEN list_slice(__tokens, token_position + 1, len(__tokens))
                            ELSE []::VARCHAR[]
                        END
                    ) AS masked_tokens
                FROM scalar_values
            ),
            scalar_contexts AS (
                SELECT *, array_to_string(
                    list_slice(masked_tokens, token_position - 1, token_position), ' '
                ) AS context
                FROM masked_scalars WHERE token_position > 1
                UNION ALL
                SELECT *, array_to_string(
                    list_slice(masked_tokens, token_position, token_position + 1), ' '
                ) AS context
                FROM masked_scalars WHERE token_position < len(masked_tokens)
                UNION ALL
                SELECT *, array_to_string(
                    list_slice(masked_tokens, token_position - 2, token_position), ' '
                ) AS context
                FROM masked_scalars WHERE token_position > 2
                UNION ALL
                SELECT *, array_to_string(
                    list_slice(masked_tokens, token_position - 1, token_position + 1), ' '
                ) AS context
                FROM masked_scalars
                WHERE token_position > 1 AND token_position < len(masked_tokens)
                UNION ALL
                SELECT *, array_to_string(
                    list_slice(masked_tokens, token_position, token_position + 2), ' '
                ) AS context
                FROM masked_scalars
                WHERE token_position + 2 <= len(masked_tokens)
            ),
            scalar_range_requested AS (
                SELECT
                    unique_id AS __messy_uid,
                    'NRB16|' || context || '|B='
                        || floor(probe_value / {ranges.bucket_width})::BIGINT::VARCHAR
                            AS __key,
                    '{ranges.context_strategy_name}' AS __source_strategy,
                    '{ranges.context_strategy_name}' AS __lookup_strategy,
                    'candidate_only' AS __evidence_mode,
                    FALSE AS __contributes_signature_evidence,
                    {ranges.context_posting_cap} AS __maximum_posting_size,
                    0 AS __transformation_cost,
                    0 AS __lookup_precedence,
                    probe_value AS __probe_value,
                    context AS __probe_context,
                    probe_role AS __probe_role,
                    TRUE AS __requires_range_verification
                FROM scalar_contexts
            ),
            {range_context_sql},
            exact_range_requested AS (
                SELECT
                    unique_id AS __messy_uid,
                    'NRX1|' || context || '|R=' || range_start::VARCHAR
                        || ':' || range_end::VARCHAR AS __key,
                    '{ranges.exact_strategy_name}' AS __source_strategy,
                    '{ranges.exact_strategy_name}' AS __lookup_strategy,
                    'candidate_only' AS __evidence_mode,
                    FALSE AS __contributes_signature_evidence,
                    {ranges.exact_posting_cap} AS __maximum_posting_size,
                    0 AS __transformation_cost,
                    0 AS __lookup_precedence,
                    range_start AS __probe_value,
                    context AS __probe_context,
                    range_role AS __probe_role,
                    TRUE AS __requires_range_verification
                FROM range_contexts
            )
            SELECT * FROM ordinary_requested
            UNION ALL
            SELECT * FROM scalar_range_requested
            UNION ALL
            SELECT * FROM exact_range_requested
            """

        requested_keys_sql = """
        SELECT *
        FROM {unnested_keys}
        WHERE __key IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY __messy_uid, __lookup_strategy, __key
            ORDER BY __lookup_precedence, __transformation_cost, __source_strategy
        ) = 1
        """
        matched_sql = """
        SELECT
            requested.__messy_uid,
            requested.__source_strategy,
            requested.__lookup_strategy,
            requested.__key,
            len(index.unique_ids) AS __posting_list_size,
            index.unique_ids,
            requested.__contributes_signature_evidence,
            requested.__evidence_mode,
            requested.__maximum_posting_size,
            requested.__transformation_cost,
            requested.__probe_value,
            requested.__probe_context,
            requested.__probe_role,
            requested.__requires_range_verification
        FROM {requested_keys} AS requested
        INNER JOIN __ukam_inverted_index AS index
            ON requested.__key = index.key
           AND requested.__lookup_strategy = index.index_strategy
           AND len(index.unique_ids) <= requested.__maximum_posting_size
        """
        provenance_sql = """
        SELECT
            __messy_uid,
            __source_strategy,
            __lookup_strategy,
            __key,
            __posting_list_size,
            candidate_id,
            __contributes_signature_evidence,
            __evidence_mode,
            __maximum_posting_size,
            __transformation_cost,
            __probe_value,
            __probe_context,
            __probe_role,
            __requires_range_verification
        FROM {matched}, unnest(unique_ids) AS candidates(candidate_id)
        """
        resolved_candidate_provenance_sql = "SELECT * FROM {candidate_provenance}"
        if canonical_range_slots_table is not None:
            resolved_candidate_provenance_sql = f"""
            SELECT * FROM {{candidate_provenance}}
            WHERE NOT __requires_range_verification
            UNION ALL
            SELECT candidate.*
            FROM {{candidate_provenance}} AS candidate
            INNER JOIN {canonical_range_slots_table} AS canonical_range
                ON candidate.candidate_id = canonical_range.unique_id
               AND candidate.__probe_context = canonical_range.context
            WHERE candidate.__requires_range_verification
              AND candidate.__probe_value BETWEEN canonical_range.range_start
                  AND canonical_range.range_end
              AND (
                  candidate.__probe_role = canonical_range.range_role
                  OR candidate.__probe_role = 0
                  OR canonical_range.range_role = 0
              )
            """
        deduplicated_sql = """
        SELECT
            __messy_uid,
            list(DISTINCT candidate_id ORDER BY candidate_id) AS exploding_unique_ids
        FROM {resolved_candidate_provenance}
        WHERE candidate_id IS NOT NULL
        GROUP BY __messy_uid
        """
        cand_scores_sql = """
        SELECT
            __messy_uid,
            CAST(candidate_id AS VARCHAR) AS __cand_id,
            SUM(
                log2(
                    (SELECT n FROM __ukam_index_meta)::DOUBLE
                    / __posting_list_size
                )
            ) AS __score,
            SUM(
                CASE WHEN __posting_list_size = 1 THEN 1 ELSE 0 END
            ) AS __unique_hits
        FROM {resolved_candidate_provenance}
        WHERE __contributes_signature_evidence
          AND __posting_list_size > 0
        GROUP BY __messy_uid, CAST(candidate_id AS VARCHAR)
        """
        score_map_sql = """
        SELECT
            __messy_uid,
            map(
                list(__cand_id ORDER BY __cand_id),
                list(__score ORDER BY __cand_id)
            ) AS signature_score_map,
            map(
                list(__cand_id ORDER BY __cand_id),
                list(__unique_hits ORDER BY __cand_id)
            ) AS signature_unique_hits_map
        FROM {cand_scores}
        GROUP BY __messy_uid
        """
        final_sql = """
        SELECT
            base.* EXCLUDE (__tokens),
            COALESCE(candidates.exploding_unique_ids, []) AS exploding_unique_ids,
            COALESCE(
                scores.signature_score_map,
                MAP([]::VARCHAR[], []::DOUBLE[])
            ) AS signature_score_map,
            COALESCE(
                scores.signature_unique_hits_map,
                MAP([]::VARCHAR[], []::BIGINT[])
            ) AS signature_unique_hits_map
        FROM {base} AS base
        LEFT JOIN {deduplicated} AS candidates
          ON base.unique_id = candidates.__messy_uid
        LEFT JOIN {score_map} AS scores
          ON base.unique_id = scores.__messy_uid
        """
        steps = [CTEStep("base", base_sql)]
        steps.extend(
            [
                CTEStep("unnested_keys", unnested_keys_sql),
                CTEStep("requested_keys", requested_keys_sql),
                CTEStep("matched", matched_sql),
                CTEStep("candidate_provenance", provenance_sql),
                CTEStep(
                    "resolved_candidate_provenance",
                    resolved_candidate_provenance_sql,
                ),
                CTEStep("deduplicated", deduplicated_sql),
                CTEStep("cand_scores", cand_scores_sql),
                CTEStep("score_map", score_map_sql),
                CTEStep("final", final_sql),
            ]
        )
        return steps

    return _stage


@pipeline_stage(
    name="set_exploding_unique_ids_to_self",
    description="Set canonical candidate IDs to each record's own unique ID",
    tags="inverted_index",
)
def _set_exploding_unique_ids_to_self():
    return """
    SELECT
        *,
        [unique_id] AS exploding_unique_ids,
        MAP([]::VARCHAR[], []::DOUBLE[]) AS signature_score_map,
        MAP([]::VARCHAR[], []::BIGINT[]) AS signature_unique_hits_map
    FROM {input}
    """
