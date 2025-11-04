from __future__ import annotations

from typing import Literal

from uk_address_matcher.sql_pipeline.match_reasons import MatchReason
from uk_address_matcher.sql_pipeline.steps import CTEStep, pipeline_stage

FuzzyInputName = Literal["fuzzy_addresses", "unmatched_records"]


@pipeline_stage(
    name="resolve_with_trie",
    description="Build tries for unmatched canonical addresses and resolve remaining fuzzy rows",
    tags=["phase_1", "trie", "exact_matching"],
)
def _resolve_with_trie(
    fuzzy_input_name: FuzzyInputName = "fuzzy_addresses",
) -> list[CTEStep]:
    """Resolve fuzzy addresses using trie-based suffix matching.

    Parameters
    ----------
    fuzzy_input_name:
        The placeholder name for the fuzzy input table. Defaults to "fuzzy_addresses" for
        the initial pass. Should be set to "unmatched_records" when running after filtering.
    """
    # Build tries grouped by postcode_group from the pre-filtered canonical addresses
    tries_sql = """
        SELECT
            postcode_group,
            build_suffix_trie(ukam_address_id, address_tokens) AS trie
        FROM {canonical_addresses_restricted}
        GROUP BY postcode_group
    """

    # Match fuzzy addresses to tries by joining on postcode_group
    raw_trie_matches_sql = f"""
        SELECT
            fuzzy.ukam_address_id AS fuzzy_ukam_address_id,
            find_address(fuzzy.address_tokens, tries.trie) AS canonical_ukam_address_id
        FROM {{{fuzzy_input_name}}} AS fuzzy
        JOIN {{postcode_group_tries}} AS tries
          ON LEFT(fuzzy.postcode, LENGTH(fuzzy.postcode) - 1) = tries.postcode_group
    """

    # Join back to canonical to get the canonical_unique_id and create final output
    trie_value = MatchReason.TRIE.value
    enum_values = str(MatchReason.enum_values())
    trie_matches_sql = f"""
        SELECT
            candidates.fuzzy_ukam_address_id AS ukam_address_id,
            candidates.canonical_ukam_address_id AS canonical_ukam_address_id,
            canon.canonical_unique_id AS resolved_canonical_id,
            '{trie_value}'::ENUM {enum_values} AS match_reason
        FROM {{raw_trie_matches}} AS candidates
        JOIN {{canonical_addresses_restricted}} AS canon
          ON candidates.canonical_ukam_address_id = canon.ukam_address_id
        WHERE candidates.canonical_ukam_address_id IS NOT NULL
    """

    return [
        CTEStep("postcode_group_tries", tries_sql),
        CTEStep("raw_trie_matches", raw_trie_matches_sql),
        CTEStep("trie_matches", trie_matches_sql),
    ]


__all__ = ["_resolve_with_trie"]
