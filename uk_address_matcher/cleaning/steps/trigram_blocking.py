from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from uk_address_matcher.sql_pipeline.steps import CTEStep, pipeline_stage


@dataclass(frozen=True)
class BlockingIndexStrategy:
    """A strategy that emits zero or more index values per address row."""

    name: str
    description: str
    sql_list_expression: str


def _consecutive_ngram_list_expression(n: int) -> str:
    if n < 1:
        raise ValueError("n-gram size must be at least 1")

    return f"""
    CASE
        WHEN len(address_tokens) >= {n} THEN
            list_transform(
                generate_series(1, len(address_tokens) - {n - 1}),
                i -> array_to_string(list_slice(address_tokens, i, i + {n - 1}), ' ')
            )
        ELSE []
    END
    """.strip()


TRIGRAM_STRATEGY = BlockingIndexStrategy(
    name="trigram",
    description="Consecutive 3-token sequences from address_tokens",
    sql_list_expression=_consecutive_ngram_list_expression(3),
)

BIGRAM_STRATEGY = BlockingIndexStrategy(
    name="bigram",
    description="Consecutive 2-token sequences from address_tokens",
    sql_list_expression=_consecutive_ngram_list_expression(2),
)

NUMERIC_PLUS_FIRST_NON_NUMERIC_STRATEGY = BlockingIndexStrategy(
    name="numeric_plus_first_non_numeric",
    description="All numeric tokens plus first non-numeric token after numeric run",
    sql_list_expression=r"""
    CASE
        WHEN len(numeric_tokens) >= 1
            AND len(
                list_filter(
                    address_tokens,
                    (tok, i) -> i > len(numeric_tokens)
                        AND NOT regexp_matches(tok, '^\d+[A-Z]?$')
                )
            ) >= 1
        THEN
            [
                array_to_string(
                    list_concat(
                        numeric_tokens,
                        [
                            list_filter(
                                address_tokens,
                                (tok, i) -> i > len(numeric_tokens)
                                    AND NOT regexp_matches(tok, '^\d+[A-Z]?$')
                            )[1]
                        ]
                    ),
                    ' '
                )
            ]
        ELSE []
    END
    """.strip(),
)

DEFAULT_BLOCKING_INDEX_STRATEGIES: tuple[BlockingIndexStrategy, ...] = (
    TRIGRAM_STRATEGY,
    BIGRAM_STRATEGY,
    NUMERIC_PLUS_FIRST_NON_NUMERIC_STRATEGY,
)


def _derive_index_values_from_strategies(
    strategies: Sequence[BlockingIndexStrategy] = DEFAULT_BLOCKING_INDEX_STRATEGIES,
):
    """Create a stage that derives index values from a list of strategies."""
    strategies = tuple(strategies)
    if not strategies:
        raise ValueError("At least one blocking index strategy must be supplied")

    strategy_names = ", ".join(strategy.name for strategy in strategies)
    strategy_sql = ",\n                        ".join(
        strategy.sql_list_expression for strategy in strategies
    )

    @pipeline_stage(
        name="derive_index_values_from_address_tokens",
        description=f"Generate blocking index values from strategies: {strategy_names}",
        tags="trigram_blocking",
    )
    def _stage():
        sql = f"""
        SELECT
            *,
            list_sort(
                list_distinct(
                    list_filter(
                        flatten([
                            {strategy_sql}
                        ]),
                        x -> x IS NOT NULL AND len(trim(x)) > 0
                    )
                )
            ) AS index_values
        FROM {{input}}
        """
        return sql

    return _stage


@pipeline_stage(
    name="lookup_index_values_in_inverted_index",
    description="Look up derived index values in inverted index to populate exploding_unique_ids",
    tags="trigram_blocking",
)
def _lookup_index_values_in_inverted_index():
    """Look up generic index values in the registered __ukam_inverted_index table."""
    base_sql = """
    SELECT * FROM {input}
    """

    unnested_index_values_sql = """
    SELECT
        unique_id AS __messy_uid,
        unnest(index_values) AS __index_value
    FROM {base}
    """

    matched_sql = """
    SELECT
        __messy_uid,
        flatten(list(ii.unique_ids)) AS __matched
    FROM {unnested_index_values} ut
    LEFT JOIN __ukam_inverted_index ii ON ut.__index_value = ii.index_value
    WHERE ii.unique_ids IS NOT NULL
    GROUP BY __messy_uid
    """

    deduplicated_sql = """
    SELECT
        __messy_uid,
        list(DISTINCT x ORDER BY x) AS exploding_unique_ids
    FROM {matched}, unnest(__matched) AS t(x)
    WHERE x IS NOT NULL
    GROUP BY __messy_uid
    """

    final_sql = """
    SELECT
        base.* EXCLUDE (index_values),
        COALESCE(d.exploding_unique_ids, []) AS exploding_unique_ids
    FROM {base} AS base
    LEFT JOIN {deduplicated} AS d ON base.unique_id = d.__messy_uid
    """

    steps = [
        CTEStep("base", base_sql),
        CTEStep("unnested_index_values", unnested_index_values_sql),
        CTEStep("matched", matched_sql),
        CTEStep("deduplicated", deduplicated_sql),
        CTEStep("final", final_sql),
    ]

    return steps


@pipeline_stage(
    name="set_exploding_unique_ids_to_self",
    description="Set exploding_unique_ids to [unique_id] for canonical data (no inverted index lookup)",
    tags="trigram_blocking",
)
def _set_exploding_unique_ids_to_self():
    """Set exploding_unique_ids to contain just the record's own unique_id.

    Used for canonical data that doesn't need trigram blocking lookup.
    """
    sql = """
    SELECT
        *,
        [unique_id] AS exploding_unique_ids
    FROM {input}
    """
    return sql


def _build_inverted_index_from_index_values(max_unique_ids_per_index_value: int = 20):
    """Build an inverted index from records with index_values column."""

    @pipeline_stage(
        name="build_inverted_index_from_index_values",
        description=(
            "Aggregate index values into inverted index "
            f"(max {max_unique_ids_per_index_value} unique_ids per value)"
        ),
        tags="trigram_blocking",
    )
    def _stage():
        sql = f"""
        WITH unnested_index_values AS (
            SELECT
                unique_id,
                unnest(index_values) AS index_value
            FROM {{input}}
        ),
        grouped AS (
            SELECT
                index_value,
                list(DISTINCT unique_id ORDER BY unique_id) AS unique_ids,
                COUNT(DISTINCT unique_id) AS count_unique_ids
            FROM unnested_index_values
            GROUP BY index_value
        )
        SELECT
            index_value,
            unique_ids
        FROM grouped
        WHERE count_unique_ids >= 1
          AND count_unique_ids <= {max_unique_ids_per_index_value}
        """
        return sql

    return _stage
