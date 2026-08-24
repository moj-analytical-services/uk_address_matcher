from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any



@dataclass(frozen=True)
class NumericRangeIndexConfig:
    bucket_width: int = 16
    maximum_range_width: int = 25
    context_posting_cap: int = 30
    exact_posting_cap: int = 100
    mask_token: str = "__R__"
    context_strategy_name: str = "numeric_range_context_bucket16"
    exact_strategy_name: str = "numeric_range_exact"

    def to_manifest_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class AdjacentNgramIndexConfig:
    name: str
    ngram_size: int
    maximum_posting_size: int = 20

    def to_manifest_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class InvertedIndexPortfolio:
    name: str = "default"
    schema_version: int = 2
    ordinary_indexes: tuple[AdjacentNgramIndexConfig, ...] = field(
        default_factory=lambda: (
            AdjacentNgramIndexConfig(name="bigram", ngram_size=2),
            AdjacentNgramIndexConfig(name="trigram", ngram_size=3),
        )
    )
    numeric_ranges: NumericRangeIndexConfig = field(
        default_factory=NumericRangeIndexConfig
    )

    def to_manifest_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "schema_version": self.schema_version,
            "ordinary_indexes": [
                strategy.to_manifest_dict() for strategy in self.ordinary_indexes
            ],
            "numeric_ranges": self.numeric_ranges.to_manifest_dict(),
        }

    def posting_caps(self) -> dict[str, int]:
        return {
            **{
                strategy.name: strategy.maximum_posting_size
                for strategy in self.ordinary_indexes
            },
            self.numeric_ranges.context_strategy_name: (
                self.numeric_ranges.context_posting_cap
            ),
            self.numeric_ranges.exact_strategy_name: (
                self.numeric_ranges.exact_posting_cap
            ),
        }


def range_context_ctes(*, source_name: str, portfolio: InvertedIndexPortfolio) -> str:
    """Return CTEs producing eligible range contexts from tokenised input."""
    ranges = portfolio.numeric_ranges
    return f"""
range_slots AS MATERIALIZED (
    SELECT
        {source_name}.unique_id,
        {source_name}.__tokens,
        ranges.ordinality AS range_ordinal,
        range_value.lower AS range_start,
        range_value.upper AS range_end,
        range_value.role AS range_role,
        range_value.flags AS range_flags,
        range_value.lower_suffix AS start_suffix,
        range_value.upper_suffix AS end_suffix,
        list_position({source_name}.__tokens, range_value.raw) AS token_position
    FROM {source_name}
    CROSS JOIN UNNEST({source_name}.numeric_range_attributes)
        WITH ORDINALITY AS ranges(range_value, ordinality)
    WHERE range_value.lower IS NOT NULL
      AND range_value.upper IS NOT NULL
      AND range_value.lower < range_value.upper
      AND range_value.upper - range_value.lower <= {ranges.maximum_range_width}
      AND (range_value.flags & 29) = 0
      AND range_value.role <> 3
      AND COALESCE(range_value.lower_suffix, '') = ''
      AND COALESCE(range_value.upper_suffix, '') = ''
      AND list_position({source_name}.__tokens, range_value.raw) IS NOT NULL
),
range_contexts AS MATERIALIZED (
    SELECT
        unique_id,
        range_ordinal,
        range_start,
        range_end,
        range_role,
        range_flags,
        token_position,
        __tokens[token_position - 1] || ' ' || '{ranges.mask_token}' AS context
    FROM range_slots
    WHERE token_position > 1

    UNION ALL

    SELECT
        unique_id,
        range_ordinal,
        range_start,
        range_end,
        range_role,
        range_flags,
        token_position,
        '{ranges.mask_token}' || ' ' || __tokens[token_position + 1] AS context
    FROM range_slots
    WHERE token_position < len(__tokens)

    UNION ALL

    SELECT
        unique_id,
        range_ordinal,
        range_start,
        range_end,
        range_role,
        range_flags,
        token_position,
        __tokens[token_position - 2] || ' ' || __tokens[token_position - 1]
            || ' ' || '{ranges.mask_token}' AS context
    FROM range_slots
    WHERE token_position > 2

    UNION ALL

    SELECT
        unique_id,
        range_ordinal,
        range_start,
        range_end,
        range_role,
        range_flags,
        token_position,
        __tokens[token_position - 1] || ' ' || '{ranges.mask_token}'
            || ' ' || __tokens[token_position + 1] AS context
    FROM range_slots
    WHERE token_position > 1 AND token_position < len(__tokens)

    UNION ALL

    SELECT
        unique_id,
        range_ordinal,
        range_start,
        range_end,
        range_role,
        range_flags,
        token_position,
        '{ranges.mask_token}' || ' ' || __tokens[token_position + 1]
            || ' ' || __tokens[token_position + 2] AS context
    FROM range_slots
    WHERE token_position + 2 <= len(__tokens)
)
"""


def build_index_chunk_sql(
    *,
    source_table: str,
    chunk_index: int,
    number_of_chunks: int,
    range_source_table: str | None = None,
    include_ordinary: bool = True,
    include_ranges: bool = True,
) -> str:
    """Build every persisted key family for one key-hash chunk."""
    portfolio = InvertedIndexPortfolio()
    if not include_ordinary and not include_ranges:
        raise ValueError("At least one key family must be included")
    ordinary_ctes: list[str] = []
    ordinary_selects: list[str] = []
    for strategy in portfolio.ordinary_indexes if include_ordinary else ():
        cte_name = f"{strategy.name}_occurrences"
        upper_bound = strategy.ngram_size - 1
        key_parts = "\n            || ' ' || ".join(
            f"source.__tokens[__position + {offset}]"
            for offset in range(strategy.ngram_size)
        )
        ordinary_ctes.append(
            f"""{cte_name} AS (
    SELECT
        source.unique_id,
        generated.key,
        '{strategy.name}'::VARCHAR AS index_strategy
    FROM source
    CROSS JOIN UNNEST(
        CASE
            WHEN len(source.__tokens) >= {strategy.ngram_size}
                THEN list_filter(
                    list_transform(
                        generate_series(1, len(source.__tokens) - {upper_bound}),
                        __position -> {key_parts}
                    ),
                    __key -> (
                        abs(hash(__key)) % {number_of_chunks}
                    ) = {chunk_index}
                )
            ELSE []::VARCHAR[]
        END
    ) AS generated(key)
)"""
        )
        ordinary_selects.append(
            f"SELECT unique_id, key::VARCHAR AS key, "
            f"index_strategy::VARCHAR AS index_strategy FROM {cte_name}"
        )

    ranges = portfolio.numeric_ranges
    caps = portfolio.posting_caps()
    cap_case = "\n      ".join(f"WHEN '{name}' THEN {cap}" for name, cap in caps.items())
    with_clauses: list[str] = []
    if include_ordinary or range_source_table is None:
        source_projection = """
            unique_id,
            clean_full_address,
            regexp_split_to_array(trim(clean_full_address), '\\s+') AS __tokens
        """
        if include_ranges:
            source_projection += ",\n            numeric_range_attributes"
        with_clauses.append(f"""source AS MATERIALIZED (
    SELECT {source_projection}
    FROM {source_table}
)""")
    with_clauses.extend(ordinary_ctes)

    occurrence_selects = list(ordinary_selects)
    if include_ranges:
        range_source_sql = (
            "SELECT unique_id, __tokens, numeric_range_attributes\n"
            f"    FROM {range_source_table}"
            if range_source_table is not None
            else (
                "SELECT unique_id, __tokens, numeric_range_attributes\n"
                "    FROM source\n"
                "    WHERE numeric_range_attributes IS NOT NULL"
            )
        )
        with_clauses.append(f"""range_source AS MATERIALIZED (
    {range_source_sql}
)""")
        with_clauses.append(
            range_context_ctes(
                source_name="range_source",
                portfolio=portfolio,
            ).strip()
        )
        with_clauses.append(f"""range_bucket_occurrences AS (
    SELECT
        range_contexts.unique_id,
        'NRB16|' || range_contexts.context || '|B=' || bucket_id::VARCHAR AS key,
        '{ranges.context_strategy_name}'::VARCHAR AS index_strategy
    FROM range_contexts
    CROSS JOIN LATERAL UNNEST(
        list_filter(
            generate_series(
                floor(range_start / {ranges.bucket_width})::BIGINT,
                floor(range_end / {ranges.bucket_width})::BIGINT
            ),
            bucket_id -> (
                abs(hash(
                    'NRB16|' || range_contexts.context || '|B='
                    || bucket_id::VARCHAR
                )) % {number_of_chunks}
            ) = {chunk_index}
        )
    ) AS buckets(bucket_id)
)""")
        with_clauses.append(f"""exact_range_occurrences AS (
    SELECT
        range_contexts.unique_id,
        'NRX1|' || range_contexts.context || '|R='
            || range_contexts.range_start::VARCHAR || ':'
            || range_contexts.range_end::VARCHAR AS key,
        '{ranges.exact_strategy_name}'::VARCHAR AS index_strategy
    FROM range_contexts
    WHERE (
        abs(hash(
            'NRX1|' || range_contexts.context || '|R='
            || range_contexts.range_start::VARCHAR || ':'
            || range_contexts.range_end::VARCHAR
        )) % {number_of_chunks}
    ) = {chunk_index}
 )""")
        occurrence_selects.extend(
            [
                "SELECT unique_id, key::VARCHAR AS key, index_strategy::VARCHAR "
                "AS index_strategy FROM range_bucket_occurrences",
                "SELECT unique_id, key::VARCHAR AS key, index_strategy::VARCHAR "
                "AS index_strategy FROM exact_range_occurrences",
            ]
        )

    all_selects = "\n    UNION ALL\n\n    ".join(occurrence_selects)
    with_clauses.extend(
        [
            f"""all_occurrences AS (
    {all_selects}
            )""",
            """chunk_occurrences AS (
    SELECT unique_id, key, index_strategy
    FROM all_occurrences
    WHERE key IS NOT NULL
)""",
            """grouped AS (
    SELECT
        key,
        index_strategy,
        list(DISTINCT unique_id ORDER BY unique_id) AS unique_ids,
        count(DISTINCT unique_id) AS posting_size
    FROM chunk_occurrences
    GROUP BY key, index_strategy
 )""",
        ]
    )
    return f"""
WITH {",\n".join(with_clauses)}
SELECT key, unique_ids, index_strategy
FROM grouped
WHERE posting_size >= 1
  AND posting_size <= CASE index_strategy
      {cap_case}
      ELSE 0
  END
"""


def canonical_range_slots_sql(
    *,
    source_table: str,
    range_attributes_sql: str = "numeric_range_attributes",
    source_filter_sql: str | None = None,
) -> str:
    """Return the narrow transient range relation used for containment checks."""
    portfolio = InvertedIndexPortfolio()
    source_filter = f"WHERE {source_filter_sql}" if source_filter_sql else ""
    return f"""
WITH source AS (
    SELECT unique_id, regexp_split_to_array(trim(clean_full_address), '\\s+') AS __tokens,
        {range_attributes_sql} AS numeric_range_attributes
    FROM {source_table}
    {source_filter}
),
{range_context_ctes(source_name="source", portfolio=portfolio)}
SELECT unique_id, range_ordinal, context, range_start, range_end, range_role
FROM range_contexts
"""
