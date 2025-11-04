"""Pipeline stage for trigram-based resolution."""

from __future__ import annotations

from uk_address_matcher.sql_pipeline.match_reasons import MatchReason
from uk_address_matcher.sql_pipeline.steps import CTEStep, pipeline_stage


def _ngram_expression(tokens_column: str, ngram_size: int) -> str:
    if ngram_size <= 0:
        raise ValueError("n must be greater than zero.")
    return f"""
        list_transform(
            range(1, length({tokens_column}) - {ngram_size} + 2),  -- stop is exclusive
            i -> {tokens_column}[i : i + {ngram_size} - 1]
        )
    """.strip()


def _trigram_hash_expression(alias: str = "tri") -> str:
    return f"hash(array_to_string({alias}, ' '))"


@pipeline_stage(
    name="resolve_with_trigrams",
    description="Resolve unmatched records using unique trigram matches per postcode",
    tags=["phase_1", "trigram", "exact_matching"],
    depends_on=["filter_unmatched_matches", "distinct_unmatched_postcodes"],
    stage_output="fuzzy_addresses",
)
def _resolve_with_trigrams(
    ngram_size: int = 3,
    min_unique_hits: int = 1,
    include_conflicts: bool = False,
    include_trigram_text: bool = False,
) -> list[CTEStep]:
    trigram_value = MatchReason.TRIGRAM.value
    enum_values = str(MatchReason.enum_values())

    trigram_text_projection = (
        ", array_to_string(tri, ' ') AS trigram_text" if include_trigram_text else ""
    )
    candidate_text_projection = ", fuzzy.trigram_text" if include_trigram_text else ""
    supporting_text_projection = (
        ", LIST(DISTINCT links.trigram_text) AS supporting_trigram_texts"
        if include_trigram_text
        else ""
    )
    conflicts_text_projection = (
        ", LIST(DISTINCT links.trigram_text) AS conflicting_trigram_texts"
        if include_trigram_text
        else ""
    )
    supporting_text_select = (
        ", supporting_trigram_texts" if include_trigram_text else ""
    )
    combined_text_select = (
        ", matches.supporting_trigram_texts" if include_trigram_text else ""
    )

    canonical_candidates_sql = """
        SELECT
            c.ukam_address_id AS canonical_ukam_address_id,
            c.unique_id AS canonical_unique_id,
            c.postcode,
            c.address_tokens
        FROM {canonical_addresses} AS c
        JOIN {distinct_unmatched_postcodes} AS postcodes
          ON c.postcode = postcodes.postcode
        WHERE c.unique_id IS NOT NULL
    """

    canonical_trigrams_sql = f"""
        SELECT
            canon.canonical_ukam_address_id,
            canon.canonical_unique_id,
            canon.postcode,
            {_ngram_expression("canon.address_tokens", ngram_size)} AS ngrams
        FROM {{canonical_trigram_candidates}} AS canon
        WHERE length(canon.address_tokens) >= {ngram_size}
    """

    canonical_trigrams_exploded_sql = f"""
        SELECT DISTINCT
            trigram.canonical_ukam_address_id,
            trigram.canonical_unique_id,
            trigram.postcode,
            {_trigram_hash_expression()} AS trigram_hash
        FROM {{canonical_trigrams}} AS trigram,
        UNNEST(trigram.ngrams) AS u(tri)
        WHERE tri IS NOT NULL
    """

    unique_trigram_index_sql = """
        SELECT
            postcode,
            trigram_hash,
            MIN(canonical_ukam_address_id) AS canonical_ukam_address_id,
            MIN(canonical_unique_id) AS canonical_unique_id
        FROM {canonical_trigrams_exploded}
        GROUP BY postcode, trigram_hash
        HAVING COUNT(DISTINCT canonical_ukam_address_id) = 1
    """

    fuzzy_trigrams_sql = f"""
        SELECT
            f.ukam_address_id AS fuzzy_ukam_address_id,
            f.postcode,
            {_ngram_expression("f.address_tokens", ngram_size)} AS ngrams
    FROM {{unmatched_records}} AS f
        WHERE length(f.address_tokens) >= {ngram_size}
    """

    fuzzy_trigrams_exploded_sql = f"""
        SELECT DISTINCT
            fuzzy_trigrams.fuzzy_ukam_address_id,
            fuzzy_trigrams.postcode,
            {_trigram_hash_expression()} AS trigram_hash
            {trigram_text_projection}
        FROM {{fuzzy_trigrams}} AS fuzzy_trigrams,
        UNNEST(fuzzy_trigrams.ngrams) AS u(tri)
        WHERE tri IS NOT NULL
    """

    trigram_candidate_links_sql = f"""
        SELECT
            fuzzy.fuzzy_ukam_address_id,
            fuzzy.postcode,
            unique_index.canonical_ukam_address_id,
            unique_index.canonical_unique_id,
            fuzzy.trigram_hash
            {candidate_text_projection}
        FROM {{fuzzy_trigrams_exploded}} AS fuzzy
        JOIN {{unique_trigram_index}} AS unique_index
          USING (postcode, trigram_hash)
    """

    trigram_one_to_one_links_sql = f"""
        SELECT
            links.fuzzy_ukam_address_id,
            MIN(links.canonical_ukam_address_id) AS canonical_ukam_address_id,
            MIN(links.canonical_unique_id) AS canonical_unique_id,
            links.postcode,
            COUNT(*) AS trigram_hit_count,
            LIST(DISTINCT links.trigram_hash) AS supporting_trigram_hashes
            {supporting_text_projection}
        FROM {{trigram_candidate_links}} AS links
        GROUP BY links.fuzzy_ukam_address_id, links.postcode
        HAVING COUNT(DISTINCT links.canonical_ukam_address_id) = 1
           AND COUNT(*) >= {min_unique_hits}
    """

    trigram_matches_sql = f"""
        SELECT
            fuzzy_ukam_address_id,
            canonical_ukam_address_id,
            canonical_unique_id,
            trigram_hit_count,
            supporting_trigram_hashes
            {supporting_text_select}
        FROM {{trigram_one_to_one_links}}
    """

    steps: list[CTEStep] = [
        CTEStep("canonical_trigram_candidates", canonical_candidates_sql),
        CTEStep("canonical_trigrams", canonical_trigrams_sql),
        CTEStep("canonical_trigrams_exploded", canonical_trigrams_exploded_sql),
        CTEStep("unique_trigram_index", unique_trigram_index_sql),
        CTEStep("fuzzy_trigrams", fuzzy_trigrams_sql),
        CTEStep("fuzzy_trigrams_exploded", fuzzy_trigrams_exploded_sql),
        CTEStep("trigram_candidate_links", trigram_candidate_links_sql),
        CTEStep("trigram_one_to_one_links", trigram_one_to_one_links_sql),
        CTEStep("trigram_matches", trigram_matches_sql),
    ]

    if include_conflicts:
        trigram_conflicts_sql = f"""
            SELECT
                links.fuzzy_ukam_address_id,
                links.postcode,
                COUNT(DISTINCT links.canonical_ukam_address_id) AS candidate_canonical_count,
                LIST(DISTINCT links.canonical_ukam_address_id) AS candidate_canonical_ukam_address_ids,
                LIST(DISTINCT links.trigram_hash) AS conflicting_trigram_hashes
                {conflicts_text_projection}
            FROM {{trigram_candidate_links}} AS links
            GROUP BY links.fuzzy_ukam_address_id, links.postcode
            HAVING COUNT(DISTINCT links.canonical_ukam_address_id) > 1
        """
        steps.append(CTEStep("trigram_conflicts", trigram_conflicts_sql))

    combined_results_sql = f"""
        SELECT
            base.unique_id,
            COALESCE(
                matches.canonical_unique_id,
                base.resolved_canonical_id
            ) AS resolved_canonical_id,
            base.* EXCLUDE (unique_id, resolved_canonical_id, match_reason),
            {combined_text_select}
            CASE
                WHEN matches.canonical_unique_id IS NOT NULL THEN '{trigram_value}'::ENUM {enum_values}
                ELSE base.match_reason
            END AS match_reason
        FROM {{fuzzy_addresses}} AS base
        LEFT JOIN {{trigram_matches}} AS matches
          ON base.ukam_address_id = matches.fuzzy_ukam_address_id
    """

    steps.append(CTEStep("fuzzy_with_trigram_matches", combined_results_sql))
    return steps


__all__ = ["_resolve_with_trigrams"]
