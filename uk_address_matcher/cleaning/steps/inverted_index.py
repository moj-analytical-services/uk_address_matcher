from __future__ import annotations

from typing import NamedTuple

from uk_address_matcher.sql_pipeline.steps import CTEStep, pipeline_stage


class IndexingStrategy(NamedTuple):
    """Defines an inverted index strategy mapping clean_full_address to keys.

    Each strategy provides a SQL expression that evaluates to a ``VARCHAR[]``
    array of index keys. The expression can assume both
    ``clean_full_address`` and ``__tokens`` are available.

    Attributes:
        name: Short identifier for the strategy (e.g. ``"trigram"``).
        keys_sql_expr: DuckDB SQL expression referencing ``clean_full_address``
            that evaluates to ``VARCHAR[]``.
    """

    name: str
    keys_sql_expr: str


# ---------------------------------------------------------------------------
# Built-in strategies
# ---------------------------------------------------------------------------

TRIGRAM_STRATEGY = IndexingStrategy(
    name="trigram",
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

BIGRAM_STRATEGY = IndexingStrategy(
    name="bigram",
    keys_sql_expr="""\
CASE
    WHEN len(__tokens) >= 2 THEN
        list_transform(
            generate_series(1, len(__tokens) - 1),
            __i -> __tokens[__i]
                   || ' ' || __tokens[__i + 1]
        )
    ELSE []::VARCHAR[]
END""",
)

DEFAULT_INDEXING_STRATEGIES: list[IndexingStrategy] = [
    TRIGRAM_STRATEGY,
    BIGRAM_STRATEGY,
]


# ---------------------------------------------------------------------------
# Pipeline stages — index building
# ---------------------------------------------------------------------------


def _derive_keys_for_strategy(
    strategy: IndexingStrategy,
    *,
    num_of_chunks: int | None = None,
    chunk_index: int | None = None,
):
    """Factory for a pipeline stage that derives index keys for one strategy.

    When ``num_of_chunks`` and ``chunk_index`` are provided, the generated SQL
    applies ``list_filter`` so only keys whose hash maps to the given chunk are
    retained.  This allows the inverted index to be built in memory-bounded
    chunks.

    Args:
        strategy: The indexing strategy to use.
        num_of_chunks: Total number of chunks.
        chunk_index: Zero-based index of the current chunk.
    """
    chunked = num_of_chunks is not None and chunk_index is not None
    chunk_label = f" (chunk {chunk_index + 1}/{num_of_chunks})" if chunked else ""

    @pipeline_stage(
        name=f"derive_keys_{strategy.name}",
        description=(
            f"Generate {strategy.name} keys from clean_full_address" + chunk_label
        ),
        tags="inverted_index",
    )
    def _stage():
        keys_expr = strategy.keys_sql_expr
        if chunked:
            sql = f"""
            WITH tokenised AS (
                SELECT
                    unique_id,
                    clean_full_address,
                    string_split(clean_full_address, ' ') AS __tokens
                FROM {{input}}
            )
            SELECT
                unique_id,
                list_filter(
                    {keys_expr},
                    __k -> (abs(hash(__k)) % {num_of_chunks}) = {chunk_index}
                ) AS __index_keys
            FROM tokenised
            """
        else:
            sql = f"""
            WITH tokenised AS (
                SELECT
                    unique_id,
                    clean_full_address,
                    string_split(clean_full_address, ' ') AS __tokens
                FROM {{input}}
            )
            SELECT
                unique_id,
                {keys_expr} AS __index_keys
            FROM tokenised
            """
        return sql

    return _stage


def _build_inverted_index_from_keys(
    strategy: IndexingStrategy,
    max_unique_ids_per_key: int = 20,
):
    """Build inverted index rows from the output of ``_derive_keys_for_strategy``.

    Args:
        strategy: The indexing strategy (used to populate ``index_strategy``).
        max_unique_ids_per_key: Maximum number of unique_ids a key can
            reference before being filtered out.
    """

    @pipeline_stage(
        name=f"build_inverted_index_{strategy.name}",
        description=(
            f"Aggregate {strategy.name} keys into inverted index "
            f"(max {max_unique_ids_per_key} unique_ids per key)"
        ),
        tags="inverted_index",
    )
    def _stage():
        sql = f"""
        WITH unnested_keys AS (
            SELECT
                unique_id,
                unnest(__index_keys) AS key
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
        WHERE count_unique_ids >= 1
          AND count_unique_ids <= {max_unique_ids_per_key}
        """
        return sql

    return _stage


# ---------------------------------------------------------------------------
# Pipeline stages — lookup (used when matching messy data)
# ---------------------------------------------------------------------------


def _lookup_keys_in_inverted_index(strategies=None):
    """Factory for a pipeline stage that looks up index keys in the inverted index.

    For each messy address the stage computes keys using every strategy,
    looks them up in ``__ukam_inverted_index`` (key-only join), and
    populates ``exploding_unique_ids`` with the deduplicated matches.

    Args:
        strategies: List of :class:`IndexingStrategy` instances.  Defaults
            to :data:`DEFAULT_INDEXING_STRATEGIES`.
    """
    if strategies is None:
        strategies = DEFAULT_INDEXING_STRATEGIES

    @pipeline_stage(
        name="lookup_keys_in_inverted_index",
        description=(
            "Look up index keys in pre-registered inverted index "
            "to populate exploding_unique_ids and signature_score_map"
        ),
        tags="inverted_index",
    )
    def _stage():
        base_sql = """
        SELECT
            *,
            string_split(clean_full_address, ' ') AS __tokens
        FROM {input}
        """

        # Build UNION ALL of unnested keys from every strategy
        union_parts = []
        for strategy in strategies:
            union_parts.append(
                f"SELECT unique_id AS __messy_uid, "
                f"unnest({strategy.keys_sql_expr}) AS __key "
                f"FROM {{base}}"
            )
        unnested_keys_sql = " UNION ALL ".join(union_parts)

        # Join to inverted index and aggregate matches
        matched_sql = """
        SELECT
            __messy_uid,
            flatten(list(ii.unique_ids)) AS __matched
        FROM {unnested_keys} ut
        LEFT JOIN __ukam_inverted_index ii ON ut.__key = ii.key
        WHERE ii.unique_ids IS NOT NULL
        GROUP BY __messy_uid
        """

        # Deduplicate the matched unique_ids
        deduplicated_sql = """
        SELECT
            __messy_uid,
            list(DISTINCT x ORDER BY x) AS exploding_unique_ids
        FROM {matched}, unnest(__matched) AS t(x)
        WHERE x IS NOT NULL
        GROUP BY __messy_uid
        """

        # Signature evidence scoring.
        # For each messy record, weight every shared index key by its IDF
        # (log2(N / posting_list_size)) and accumulate the IDF onto each
        # candidate canonical id that the key points at.  The result is a
        # MAP<canonical_id (VARCHAR) -> summed IDF> the Splink comparison reads
        # via list_extract(map_extract(signature_score_map_r, unique_id_l), 1).
        # Keys are de-duplicated per record first so a repeated bigram/trigram
        # only contributes its IDF once.
        distinct_keys_sql = """
        SELECT DISTINCT __messy_uid, __key
        FROM {unnested_keys}
        WHERE __key IS NOT NULL
        """

        key_scores_sql = """
        SELECT
            dk.__messy_uid,
            ii.unique_ids AS __cand_ids,
            len(ii.unique_ids) AS __posting_size,
            log2(
                (SELECT n FROM __ukam_index_meta)::DOUBLE
                / len(ii.unique_ids)
            ) AS __key_idf
        FROM {distinct_keys} AS dk
        JOIN __ukam_inverted_index AS ii ON dk.__key = ii.key
        WHERE ii.unique_ids IS NOT NULL
          AND len(ii.unique_ids) > 0
        """

        cand_scores_sql = """
        SELECT
            __messy_uid,
            CAST(cid AS VARCHAR) AS __cand_id,
            SUM(__key_idf) AS __score,
            SUM(CASE WHEN __posting_size = 1 THEN 1 ELSE 0 END) AS __unique_hits
        FROM {key_scores}, unnest(__cand_ids) AS t(cid)
        GROUP BY __messy_uid, CAST(cid AS VARCHAR)
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

        # Join back to base and add the blocking + scoring columns
        final_sql = """
        SELECT
            base.* EXCLUDE (__tokens),
            COALESCE(d.exploding_unique_ids, []) AS exploding_unique_ids,
            COALESCE(
                s.signature_score_map,
                MAP([]::VARCHAR[], []::DOUBLE[])
            ) AS signature_score_map,
            COALESCE(
                s.signature_unique_hits_map,
                MAP([]::VARCHAR[], []::BIGINT[])
            ) AS signature_unique_hits_map
        FROM {base} AS base
        LEFT JOIN {deduplicated} AS d ON base.unique_id = d.__messy_uid
        LEFT JOIN {score_map} AS s ON base.unique_id = s.__messy_uid
        """

        steps = [
            CTEStep("base", base_sql),
            CTEStep("unnested_keys", unnested_keys_sql),
            CTEStep("matched", matched_sql),
            CTEStep("deduplicated", deduplicated_sql),
            CTEStep("distinct_keys", distinct_keys_sql),
            CTEStep("key_scores", key_scores_sql),
            CTEStep("cand_scores", cand_scores_sql),
            CTEStep("score_map", score_map_sql),
            CTEStep("final", final_sql),
        ]

        return steps

    return _stage


# ---------------------------------------------------------------------------
# Canonical self-blocking (no inverted index)
# ---------------------------------------------------------------------------


@pipeline_stage(
    name="set_exploding_unique_ids_to_self",
    description=(
        "Set exploding_unique_ids to [unique_id] for canonical data "
        "(no inverted index lookup)"
    ),
    tags="inverted_index",
)
def _set_exploding_unique_ids_to_self():
    """Set exploding_unique_ids to contain just the record's own unique_id.

    Used for canonical data that doesn't need inverted index lookup.
    """
    sql = """
    SELECT
        *,
        [unique_id] AS exploding_unique_ids,
        MAP([]::VARCHAR[], []::DOUBLE[]) AS signature_score_map,
        MAP([]::VARCHAR[], []::BIGINT[]) AS signature_unique_hits_map
    FROM {input}
    """
    return sql
