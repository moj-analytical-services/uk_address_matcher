from uk_address_matcher.sql_pipeline.match_reasons import MatchReason
from uk_address_matcher.sql_pipeline.steps import CTEStep, pipeline_stage


@pipeline_stage(
    name="annotate_exact_matches",
    description=(
        "Annotate fuzzy addresses with exact hash-join matches on "
        "original_address_concat + postcode"
    ),
    tags=["phase_1", "exact_matching"],
    # Overwrite the input to be the annotated output
    stage_output="fuzzy_addresses",
)
def _annotate_exact_matches() -> list[CTEStep]:
    # TODO(ThomasHepworth): Review whether we want to deduplicate canonical addresses here
    # (we probably do, to avoid multiple matches to the same fuzzy address)

    # Input tables should be named: `fuzzy_addresses` and `canonical_addresses`

    exact_value = MatchReason.EXACT.value
    enum_values = str(MatchReason.enum_values())
    unique_canonical_addresses_sql = """
        SELECT
            canon.original_address_concat,
            canon.postcode,
            canon.unique_id
        FROM {canonical_addresses} AS canon
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY canon.original_address_concat, canon.postcode
            ORDER BY canon.unique_id
        ) = 1
    """
    annotated_sql = f"""
        SELECT
            fuzzy.unique_id AS unique_id,
            canon.unique_id AS resolved_canonical_id,
            fuzzy.* EXCLUDE (match_reason, unique_id, resolved_canonical_id),
            CASE
                WHEN canon.unique_id IS NOT NULL THEN '{exact_value}'::ENUM {enum_values}
                ELSE fuzzy.match_reason
            END AS match_reason
        FROM {{fuzzy_addresses}} AS fuzzy
        LEFT JOIN {{canonical_exact_matches}} AS canon
          ON fuzzy.original_address_concat = canon.original_address_concat
         AND fuzzy.postcode = canon.postcode
    """
    return [
        CTEStep("canonical_exact_matches", unique_canonical_addresses_sql),
        CTEStep("annotated_exact_matches", annotated_sql),
    ]


@pipeline_stage(
    name="filter_unmatched_matches",
    description="Filter records that haven't been matched yet",
    tags=["phase_1", "exact_matching"],
    stage_output="unmatched_records",
)
def _filter_unmatched_exact_matches() -> list[CTEStep]:
    unmatched_value = MatchReason.UNMATCHED.value
    return f"""
        SELECT
            f.*
        FROM {{input}} AS f
        WHERE f.match_reason = '{unmatched_value}'
    """


# Depends on filter_unmatched_matches, which creates the 'unmatched_records' CTE
@pipeline_stage(
    name="resolve_with_trie",
    description="Build tries for unmatched canonical addresses and resolve remaining fuzzy rows",
    tags=["phase_1", "trie", "exact_matching"],
    # Recommended to be run after `annotate_exact_matches`
    depends_on=["filter_unmatched_matches"],
)
def _resolve_with_trie() -> list[CTEStep]:
    filtered_canonical_sql = """
        SELECT
            c.unique_id AS canonical_unique_id,
            c.postcode,
            LEFT(c.postcode, LENGTH(c.postcode) - 1) AS postcode_group,
            c.address_tokens
        FROM {canonical_addresses} AS c
        WHERE c.unique_id IS NOT NULL
          AND c.postcode IN (
              SELECT DISTINCT postcode
              FROM {unmatched_records}
          )
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY c.original_address_concat, c.postcode
            ORDER BY c.unique_id
        ) = 1
    """

    tries_sql = """
        SELECT
            postcode_group,
            build_suffix_trie(canonical_unique_id, address_tokens) AS trie
        FROM {canonical_candidates_for_trie}
        GROUP BY postcode_group
    """

    raw_trie_matches_sql = """
        SELECT
            fuzzy.unique_id,
            find_address(fuzzy.address_tokens, tries.trie) AS trie_match_canonical_id
        FROM {input} AS fuzzy
        JOIN {postcode_group_tries} AS tries
          ON LEFT(fuzzy.postcode, LENGTH(fuzzy.postcode) - 1) = tries.postcode_group
    """

    trie_matches_sql = """
        SELECT
            candidates.unique_id,
            candidates.trie_match_canonical_id
        FROM {raw_trie_matches} AS candidates
        WHERE candidates.trie_match_canonical_id IS NOT NULL
    """

    trie_value = MatchReason.TRIE.value
    enum_values = str(MatchReason.enum_values())
    combined_results_sql = f"""
        SELECT
            a.unique_id,
            COALESCE(
                m.trie_match_canonical_id,
                a.resolved_canonical_id
            ) AS resolved_canonical_id,
            a.* EXCLUDE (unique_id, resolved_canonical_id, match_reason),
            CASE
                WHEN m.trie_match_canonical_id IS NOT NULL THEN '{trie_value}'::ENUM {enum_values}
                ELSE a.match_reason
            END AS match_reason
        FROM {{fuzzy_addresses}} AS a
        LEFT JOIN {{trie_match_candidates}} AS m
          ON a.unique_id = m.unique_id
    """

    return [
        CTEStep("canonical_candidates_for_trie", filtered_canonical_sql),
        CTEStep("postcode_group_tries", tries_sql),
        CTEStep("raw_trie_matches", raw_trie_matches_sql),
        CTEStep("trie_match_candidates", trie_matches_sql),
        CTEStep("fuzzy_with_resolved_matches", combined_results_sql),
    ]
