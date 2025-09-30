from typing import TYPE_CHECKING

from uk_address_matcher.sql_pipeline.steps import CTEStep, pipeline_stage

if TYPE_CHECKING:
    pass


@pipeline_stage(
    name="annotate_exact_matches",
    description=(
        "Annotate fuzzy addresses with exact hash-join matches on "
        "original_address_concat + postcode"
    ),
    tags=["phase_1", "exact_matching"],
)
def _annotate_exact_matches() -> list[CTEStep]:
    annotated_sql = """
        SELECT
            f.*,
            c.unique_id AS exact_match_canonical_id,
            TRY_CAST(c.unique_id AS BIGINT) AS exact_match_canonical_id_bigint,
            (c.unique_id IS NOT NULL) AS exact_match,
            CASE
                WHEN c.unique_id IS NOT NULL THEN 'exact'
                ELSE 'unmatched'
            END AS match_method,
            (c.unique_id IS NOT NULL) AS has_match
        FROM {input} AS f
        LEFT JOIN {canonical_addresses} AS c
          ON f.original_address_concat = c.original_address_concat
         AND f.postcode = c.postcode
    """
    return annotated_sql


@pipeline_stage(
    name="resolve_with_trie",
    description="Build tries for unmatched canonical addresses and resolve remaining fuzzy rows",
    tags=["phase_1", "trie", "exact_matching"],
    depends_on="annotate_exact_matches",
)
def _resolve_with_trie() -> list[CTEStep]:
    unmatched_fuzzy_sql = """
        SELECT
            f.*
        FROM {input} AS f
        WHERE NOT f.exact_match
    """

    matched_canonical_ids_sql = """
        SELECT DISTINCT exact_match_canonical_id AS canonical_unique_id
        FROM {input}
        WHERE exact_match_canonical_id IS NOT NULL
    """

    filtered_canonical_sql = """
        SELECT
            TRY_CAST(c.unique_id AS BIGINT) AS canonical_unique_id_bigint,
            c.unique_id AS canonical_unique_id,
            c.postcode,
            LEFT(c.postcode, LENGTH(c.postcode) - 1) AS postcode_group,
            c.address_tokens
        FROM {canonical_addresses} AS c
        WHERE TRY_CAST(c.unique_id AS BIGINT) IS NOT NULL
          AND c.unique_id NOT IN (
              SELECT canonical_unique_id FROM {canonical_ids_from_exact_matches}
          )
          AND c.postcode IN (
              SELECT DISTINCT postcode
              FROM {fuzzy_without_exact_matches}
          )
    """

    tries_sql = """
        SELECT
            postcode_group,
            build_suffix_trie(canonical_unique_id_bigint, address_tokens) AS trie
        FROM {canonical_candidates_for_trie}
        GROUP BY postcode_group
    """

    trie_matches_sql = """
        SELECT
            f.unique_id AS fuzzy_unique_id,
            find_address(f.address_tokens, t.trie) AS trie_match_unique_id
        FROM {fuzzy_without_exact_matches} AS f
        JOIN {postcode_group_tries} AS t
          ON LEFT(f.postcode, LENGTH(f.postcode) - 1) = t.postcode_group
    """

    combined_results_sql = """
        SELECT
            f.* EXCLUDE (match_method, has_match),
            m.trie_match_unique_id AS trie_match_unique_id_bigint,
            CAST(m.trie_match_unique_id AS VARCHAR) AS trie_match_unique_id,
            COALESCE(
                f.exact_match_canonical_id_bigint,
                m.trie_match_unique_id
            ) AS resolved_canonical_unique_id_bigint,
            COALESCE(
                f.exact_match_canonical_id,
                CAST(m.trie_match_unique_id AS VARCHAR)
            ) AS resolved_canonical_unique_id,
            CASE
                WHEN f.exact_match THEN 'exact'
                WHEN m.trie_match_unique_id IS NOT NULL THEN 'trie_match'
                ELSE 'unmatched'
            END AS match_method,
            (f.has_match OR m.trie_match_unique_id IS NOT NULL) AS has_match
        FROM {input} AS f
        LEFT JOIN {trie_match_candidates} AS m
          ON f.unique_id = m.fuzzy_unique_id
    """

    return [
        CTEStep("fuzzy_without_exact_matches", unmatched_fuzzy_sql),
        CTEStep("canonical_ids_from_exact_matches", matched_canonical_ids_sql),
        CTEStep("canonical_candidates_for_trie", filtered_canonical_sql),
        CTEStep("postcode_group_tries", tries_sql),
        CTEStep("trie_match_candidates", trie_matches_sql),
        CTEStep("fuzzy_with_resolved_matches", combined_results_sql),
    ]
