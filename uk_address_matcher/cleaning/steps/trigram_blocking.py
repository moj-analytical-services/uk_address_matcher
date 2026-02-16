from __future__ import annotations

from uk_address_matcher.sql_pipeline.steps import CTEStep, pipeline_stage


@pipeline_stage(
    name="derive_trigrams_from_address_tokens",
    description="Generate trigrams (consecutive 3-token sequences) from clean_full_address",
    tags="trigram_blocking",
)
def _derive_trigrams_from_address_tokens():
    """Generate trigrams from clean_full_address tokens.

    Creates a 'trigrams' column containing an array of space-delimited trigrams.
    For addresses with fewer than 3 tokens, returns an empty array.
    """
    sql = """
    SELECT
        base.* EXCLUDE (tokenised),
        CASE
            WHEN len(tokenised) >= 3 THEN
                list_transform(
                    generate_series(1, len(tokenised) - 2),
                    i -> tokenised[i] || ' ' || tokenised[i+1] || ' ' || tokenised[i+2]
                )
            ELSE []
        END AS trigrams
    FROM (
        SELECT
            *,
            string_split(clean_full_address, ' ') AS tokenised
        FROM {input}
    ) AS base
    """
    return sql


@pipeline_stage(
    name="lookup_trigrams_in_inverted_index",
    description="Look up trigrams in pre-registered inverted index to populate exploding_unique_ids",
    tags="trigram_blocking",
)
def _lookup_trigrams_in_inverted_index():
    """Look up trigrams in the registered __ukam_inverted_index table.

    The inverted index must be pre-registered before running this stage.
    Populates the exploding_unique_ids column with matching unique_ids from the index.
    """
    base_sql = """
    SELECT * FROM {input}
    """

    # Unnest trigrams for lookup
    unnested_trigrams_sql = """
    SELECT
        unique_id AS __messy_uid,
        unnest(trigrams) AS __trigram
    FROM {base}
    """

    # Join to inverted index and aggregate matches
    matched_sql = """
    SELECT
        __messy_uid,
        flatten(list(ii.unique_ids)) AS __matched
    FROM {unnested_trigrams} ut
    LEFT JOIN __ukam_inverted_index ii ON ut.__trigram = ii.trigram
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

    # Join back to base and add exploding_unique_ids column
    final_sql = """
    SELECT
        base.* EXCLUDE (trigrams),
        COALESCE(d.exploding_unique_ids, []) AS exploding_unique_ids
    FROM {base} AS base
    LEFT JOIN {deduplicated} AS d ON base.unique_id = d.__messy_uid
    """

    steps = [
        CTEStep("base", base_sql),
        CTEStep("unnested_trigrams", unnested_trigrams_sql),
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


def _build_inverted_index_from_trigrams(max_unique_ids_per_trigram: int = 20):
    """Build inverted index from records with trigrams column.

    This is a factory function that returns a pipeline stage with the
    max_unique_ids_per_trigram parameter baked in.

    Args:
        max_unique_ids_per_trigram: Maximum number of unique_ids a trigram can
            reference before being filtered out. Default 20.
    """

    @pipeline_stage(
        name="build_inverted_index_from_trigrams",
        description=f"Aggregate trigrams into inverted index (max {max_unique_ids_per_trigram} unique_ids per trigram)",
        tags="trigram_blocking",
    )
    def _stage():
        sql = f"""
        WITH unnested_trigrams AS (
            SELECT
                unique_id,
                unnest(trigrams) AS trigram
            FROM {{input}}
        ),
        grouped AS (
            SELECT
                trigram,
                list(DISTINCT unique_id ORDER BY unique_id) AS unique_ids,
                COUNT(DISTINCT unique_id) AS count_unique_ids
            FROM unnested_trigrams
            GROUP BY trigram
        )
        SELECT
            trigram,
            unique_ids
        FROM grouped
        WHERE count_unique_ids >= 1
          AND count_unique_ids <= {max_unique_ids_per_trigram}
        """
        return sql

    return _stage
