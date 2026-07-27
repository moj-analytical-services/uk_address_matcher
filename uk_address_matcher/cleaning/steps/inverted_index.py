from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, replace
from enum import StrEnum

from uk_address_matcher.cleaning.steps.signature_patterns import ordered_pattern_keys_sql
from uk_address_matcher.sql_pipeline.steps import CTEStep, pipeline_stage

BASE_POSTING_CAP = 20
TRANSFORMED_TRIGRAM_POSTING_CAP = 5
POSTCODE_FREE_FALLBACK_PREDICATE = """\
NOT COALESCE(
    regexp_full_match(
        postcode,
        '([A-Z]{1,2}\\d[A-Z\\d]?|GIR) \\d[A-Z]{2}'
    ),
    FALSE
)"""


class SignatureEvidenceMode(StrEnum):
    """Control whether lookup matches contribute to the legacy signature score."""

    SCORED = "scored"
    CANDIDATE_ONLY = "candidate_only"


class LookupActivation(StrEnum):
    """Control which messy records generate transient keys for a lookup.

    This gates messy-side lookup execution only; it does not affect which
    canonical keys are built or persisted in a physical index.
    """

    ALWAYS = "always"
    POSTCODE_FREE_FALLBACK = "postcode_free_fallback"
    NO_BASE_TRIGRAM_CANDIDATES = "no_base_trigram_candidates"


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
    evidence_mode: SignatureEvidenceMode = SignatureEvidenceMode.SCORED
    activation: LookupActivation = LookupActivation.ALWAYS
    transformation_cost: int = 0
    deduplication_precedence: int | None = None

    def __post_init__(self) -> None:
        if (
            self.maximum_posting_size_override is not None
            and self.maximum_posting_size_override < 1
        ):
            raise ValueError("maximum_posting_size_override must be positive")
        if self.transformation_cost < 0:
            raise ValueError("transformation_cost cannot be negative")
        if (
            self.deduplication_precedence is not None
            and self.deduplication_precedence < 0
        ):
            raise ValueError("deduplication_precedence cannot be negative")

    @property
    def maximum_posting_size(self) -> int:
        return (
            self.maximum_posting_size_override
            if self.maximum_posting_size_override is not None
            else self.target_index.maximum_posting_size
        )

    @property
    def lookup_precedence(self) -> int:
        """Return the route ordering used when strategies emit the same key."""
        return (
            self.deduplication_precedence
            if self.deduplication_precedence is not None
            else self.transformation_cost
        )


@dataclass(frozen=True)
class InvertedIndexPortfolio:
    """Names the physical indexes and transient lookups enabled together."""

    name: str
    physical_indexes: tuple[PhysicalIndexStrategy, ...]
    lookup_strategies: tuple[InvertedIndexLookupStrategy, ...]


def with_posting_cap(strategy: PhysicalIndexStrategy, cap: int) -> PhysicalIndexStrategy:
    """Return an explicit experimental variant with a different posting cap."""
    return replace(strategy, maximum_posting_size=cap)


def lookup_activation_predicate(activation: LookupActivation) -> str:
    """Return the source-row predicate for a lookup activation mode."""
    if activation is LookupActivation.ALWAYS:
        return "TRUE"
    if activation is LookupActivation.POSTCODE_FREE_FALLBACK:
        return POSTCODE_FREE_FALLBACK_PREDICATE
    if activation is LookupActivation.NO_BASE_TRIGRAM_CANDIDATES:
        return """NOT EXISTS (
            SELECT 1
            FROM {base_trigram_candidate_sources} AS base_candidates
            WHERE base_candidates.__messy_uid = unique_id
        )"""
    raise ValueError(f"Unsupported lookup activation: {activation}")


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

SOURCE_GAP1_TRIGRAM_KEYS = SignatureKeyGenerator(
    name="source_gap1_trigram_keys",
    keys_sql_expr=ordered_pattern_keys_sql(token_arity=3, gap_class="gap1"),
)
SOURCE_GAP2_TRIGRAM_KEYS = SignatureKeyGenerator(
    name="source_gap2_trigram_keys",
    keys_sql_expr=ordered_pattern_keys_sql(token_arity=3, gap_class="gap2"),
)


TRIGRAM_INDEX = PhysicalIndexStrategy(
    name="trigram",
    key_generator=ADJACENT_TRIGRAM_KEYS,
    maximum_posting_size=BASE_POSTING_CAP,
)
BIGRAM_INDEX = PhysicalIndexStrategy(
    name="bigram",
    key_generator=ADJACENT_BIGRAM_KEYS,
    maximum_posting_size=BASE_POSTING_CAP,
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
SOURCE_GAP1_TRIGRAM_LOOKUP = InvertedIndexLookupStrategy(
    name="source_gap1_trigram",
    source_key_generator=SOURCE_GAP1_TRIGRAM_KEYS,
    target_index=TRIGRAM_INDEX,
    maximum_posting_size_override=TRANSFORMED_TRIGRAM_POSTING_CAP,
    evidence_mode=SignatureEvidenceMode.CANDIDATE_ONLY,
    transformation_cost=1,
    deduplication_precedence=1,
)
SOURCE_GAP2_TRIGRAM_LOOKUP = InvertedIndexLookupStrategy(
    name="source_gap2_trigram",
    source_key_generator=SOURCE_GAP2_TRIGRAM_KEYS,
    target_index=TRIGRAM_INDEX,
    maximum_posting_size_override=TRANSFORMED_TRIGRAM_POSTING_CAP,
    evidence_mode=SignatureEvidenceMode.CANDIDATE_ONLY,
    transformation_cost=2,
    deduplication_precedence=2,
)
BASE_INDEX_PORTFOLIO = InvertedIndexPortfolio(
    name="base",
    physical_indexes=(BIGRAM_INDEX, TRIGRAM_INDEX),
    lookup_strategies=(BIGRAM_LOOKUP, TRIGRAM_LOOKUP),
)
WAVE2_SOURCE_GAPS_PORTFOLIO = InvertedIndexPortfolio(
    name="wave2_source_gaps",
    physical_indexes=BASE_INDEX_PORTFOLIO.physical_indexes,
    lookup_strategies=(
        *BASE_INDEX_PORTFOLIO.lookup_strategies,
        SOURCE_GAP1_TRIGRAM_LOOKUP,
        SOURCE_GAP2_TRIGRAM_LOOKUP,
    ),
)
DEFAULT_INDEXING_STRATEGIES = list(BASE_INDEX_PORTFOLIO.physical_indexes)
DEFAULT_INVERTED_INDEX_LOOKUP_STRATEGIES = list(BASE_INDEX_PORTFOLIO.lookup_strategies)

# Transitional aliases retain existing imports and persisted strategy names.
TRIGRAM_STRATEGY = TRIGRAM_INDEX
BIGRAM_STRATEGY = BIGRAM_INDEX


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
            is_scored = strategy.evidence_mode is SignatureEvidenceMode.SCORED
            activation = lookup_activation_predicate(strategy.activation)
            union_parts.append(
                f"SELECT unique_id AS __messy_uid, "
                f"unnest({strategy.source_key_generator.keys_sql_expr}) AS __key, "
                f"'{strategy.name}' AS __source_strategy, "
                f"'{strategy.target_index.name}' AS __lookup_strategy, "
                f"'{strategy.evidence_mode}' AS __evidence_mode, "
                f"{str(is_scored).upper()} AS __contributes_signature_evidence, "
                f"{strategy.maximum_posting_size} AS __maximum_posting_size, "
                f"{strategy.transformation_cost} AS __transformation_cost "
                f", {strategy.lookup_precedence} AS __lookup_precedence "
                f"FROM {{base}} WHERE {activation}"
            )
        unnested_keys_sql = " UNION ALL ".join(union_parts)

        requested_keys_sql = """
        SELECT *
        FROM {unnested_keys}
        WHERE __key IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY __messy_uid, __lookup_strategy, __key
            ORDER BY __lookup_precedence, __transformation_cost, __source_strategy
        ) = 1
        """
        base_trigram_candidate_sources_sql = """
        SELECT DISTINCT base.unique_id AS __messy_uid
        FROM {{base}} AS base
        CROSS JOIN UNNEST({trigram_keys}) AS keys(__key)
        INNER JOIN __ukam_inverted_index AS index
            ON keys.__key = index.key
           AND index.index_strategy = '{trigram_index_name}'
           AND len(index.unique_ids) <= {trigram_posting_cap}
        """.format(
            trigram_keys=ADJACENT_TRIGRAM_KEYS.keys_sql_expr,
            trigram_index_name=TRIGRAM_INDEX.name,
            trigram_posting_cap=TRIGRAM_LOOKUP.maximum_posting_size,
        )
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
            requested.__transformation_cost
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
            __transformation_cost
        FROM {matched}, unnest(unique_ids) AS candidates(candidate_id)
        """
        deduplicated_sql = """
        SELECT
            __messy_uid,
            list(DISTINCT candidate_id ORDER BY candidate_id) AS exploding_unique_ids
        FROM {candidate_provenance}
        WHERE candidate_id IS NOT NULL
        GROUP BY __messy_uid
        """
        distinct_keys_sql = """
        SELECT DISTINCT
            __messy_uid,
            __key,
            __lookup_strategy,
            __maximum_posting_size
        FROM {requested_keys}
        WHERE __contributes_signature_evidence
        """
        key_scores_sql = """
        SELECT
            requested.__messy_uid,
            index.unique_ids AS __cand_ids,
            len(index.unique_ids) AS __posting_size,
            log2(
                (SELECT n FROM __ukam_index_meta)::DOUBLE
                / len(index.unique_ids)
            ) AS __key_idf
        FROM {distinct_keys} AS requested
        INNER JOIN __ukam_inverted_index AS index
            ON requested.__key = index.key
           AND requested.__lookup_strategy = index.index_strategy
           AND len(index.unique_ids) <= requested.__maximum_posting_size
        WHERE index.unique_ids IS NOT NULL
          AND len(index.unique_ids) > 0
        """
        cand_scores_sql = """
        SELECT
            __messy_uid,
            CAST(candidate_id AS VARCHAR) AS __cand_id,
            SUM(__key_idf) AS __score,
            SUM(CASE WHEN __posting_size = 1 THEN 1 ELSE 0 END) AS __unique_hits
        FROM {key_scores}, unnest(__cand_ids) AS candidates(candidate_id)
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
        if any(
            strategy.activation is LookupActivation.NO_BASE_TRIGRAM_CANDIDATES
            for strategy in lookup_strategies
        ):
            steps.append(
                CTEStep(
                    "base_trigram_candidate_sources",
                    base_trigram_candidate_sources_sql,
                )
            )
        steps.extend(
            [
                CTEStep("unnested_keys", unnested_keys_sql),
                CTEStep("requested_keys", requested_keys_sql),
                CTEStep("matched", matched_sql),
                CTEStep("candidate_provenance", provenance_sql),
                CTEStep("deduplicated", deduplicated_sql),
                CTEStep("distinct_keys", distinct_keys_sql),
                CTEStep("key_scores", key_scores_sql),
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
