from __future__ import annotations

from uk_address_matcher.sql_pipeline.match_reasons import MatchReason
from uk_address_matcher.sql_pipeline.steps import CTEStep, pipeline_stage


@pipeline_stage(
    name="peeled_address_matching",
    description=(
        "Find matches by comparing addresses after peeling common UK end tokens "
        "(cities, counties, boroughs) and performing exact match on the peeled addresses."
    ),
    tags=["phase_1", "exact_matching"],
    depends_on=["restrict_canonical_to_fuzzy_postcodes"],
)
def _peeled_address_matches() -> list[CTEStep]:
    """Find matches using peeled addresses (after removing common UK end tokens).

    "Peeling" refers to the iterative removal of common UK locality tokens from
    the end of addresses. These include cities (LONDON, MANCHESTER), counties
    (HERTFORDSHIRE, KENT), London boroughs (HACKNEY, LAMBETH), and regions
    (GREATER LONDON, WEST MIDLANDS).

    Example transformations:
        - "100 TEST STREET LONDON" -> "100 TEST STREET"
        - "25 HIGH ROAD HACKNEY LONDON" -> "25 HIGH ROAD"
        - "10 MAIN AVENUE MANCHESTER GREATER MANCHESTER" -> "10 MAIN AVENUE"

    This stage uses pre-computed columns from the cleaning pipeline:
        - address_tokens: VARCHAR[] of the full address tokens
        - peeled_tokens_list: VARCHAR[] of tokens that were peeled from the end

    Note on peeled_tokens_list format:
        Multi-token patterns like "GREATER LONDON" or "TUNBRIDGE WELLS" are stored
        as single entries containing spaces. For example:
            ['HACKNEY', 'LONDON', 'GREATER LONDON']
        This means we need to count words within each entry, not just list length.

    Matching rules:
        1. Postcodes must be identical
        2. Peeled addresses (address_tokens minus peeled words) must be identical
        3. At least one side must have peeled something (to avoid duplicating
           exact match results)
    """
    match_reason_value = MatchReason.PEELED_ADDRESS.value
    enum_values = str(MatchReason.enum_values())

    # Step 1: Compute peeled address for fuzzy addresses
    # We tokenise each peeled entry, flatten to get total word count, then slice
    fuzzy_peeled_sql = """
        SELECT
            ukam_address_id,
            postcode,
            clean_full_address,
            address_tokens,
            peeled_tokens_list,
            -- Count total words in peeled_tokens_list (handling multi-word entries)
            COALESCE(
                (SELECT SUM(len(string_split(token, ' ')))
                 FROM unnest(peeled_tokens_list) AS t(token)),
                0
            )::INTEGER AS peeled_word_count,
            -- Slice address_tokens to remove peeled words from the end
            CASE
                WHEN peeled_tokens_list IS NULL OR len(peeled_tokens_list) = 0
                THEN array_to_string(address_tokens, ' ')
                ELSE array_to_string(
                    list_slice(
                        address_tokens,
                        1,
                        len(address_tokens) - COALESCE(
                            (SELECT SUM(len(string_split(token, ' ')))
                             FROM unnest(peeled_tokens_list) AS t(token)),
                            0
                        )::INTEGER
                    ),
                    ' '
                )
            END AS peeled_address
        FROM {fuzzy_addresses}
    """

    # Step 2: Compute peeled address for canonical addresses
    canonical_peeled_sql = """
        SELECT
            ukam_address_id AS canonical_ukam_address_id,
            canonical_unique_id,
            postcode,
            clean_full_address AS canonical_clean_full_address,
            address_tokens AS canonical_address_tokens,
            peeled_tokens_list AS canonical_peeled_tokens_list,
            -- Same word count logic for canonical
            COALESCE(
                (SELECT SUM(len(string_split(token, ' ')))
                 FROM unnest(peeled_tokens_list) AS t(token)),
                0
            )::INTEGER AS canonical_peeled_word_count,
            -- Same slicing logic for canonical
            CASE
                WHEN peeled_tokens_list IS NULL OR len(peeled_tokens_list) = 0
                THEN array_to_string(address_tokens, ' ')
                ELSE array_to_string(
                    list_slice(
                        address_tokens,
                        1,
                        len(address_tokens) - COALESCE(
                            (SELECT SUM(len(string_split(token, ' ')))
                             FROM unnest(peeled_tokens_list) AS t(token)),
                            0
                        )::INTEGER
                    ),
                    ' '
                )
            END AS peeled_address
        FROM {canonical_addresses_restricted}
    """

    # Step 3: Join on postcode + peeled address (exact match)
    candidates_sql = """
        SELECT
            fuzzy.ukam_address_id AS fuzzy_ukam_address_id,
            fuzzy.clean_full_address AS fuzzy_clean_full_address,
            fuzzy.peeled_address AS fuzzy_peeled_address,
            fuzzy.peeled_tokens_list AS fuzzy_peeled_tokens,
            fuzzy.peeled_word_count AS fuzzy_peeled_word_count,
            canon.canonical_ukam_address_id,
            canon.canonical_unique_id,
            canon.canonical_clean_full_address,
            canon.peeled_address AS canonical_peeled_address,
            canon.canonical_peeled_tokens_list AS canonical_peeled_tokens,
            canon.canonical_peeled_word_count
        FROM {fuzzy_peeled} AS fuzzy
        INNER JOIN {canonical_peeled} AS canon
            ON fuzzy.postcode = canon.postcode
            AND fuzzy.peeled_address = canon.peeled_address
        WHERE
            -- Require at least one side to have peeled something
            -- (otherwise this duplicates exact match results)
            fuzzy.peeled_word_count > 0
            OR canon.canonical_peeled_word_count > 0
    """

    # Step 4: Annotate matches with match reason, dedupe by fuzzy_ukam_address_id
    annotated_sql = f"""
        SELECT
            fuzzy_ukam_address_id AS ukam_address_id,
            canonical_ukam_address_id,
            canonical_unique_id AS resolved_canonical_id,
            '{match_reason_value}'::ENUM {enum_values} AS match_reason
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY fuzzy_ukam_address_id
                    ORDER BY canonical_ukam_address_id
                ) AS rn
            FROM {{peeled_address_candidates}}
        )
        WHERE rn = 1
    """

    return [
        CTEStep("fuzzy_peeled", fuzzy_peeled_sql),
        CTEStep("canonical_peeled", canonical_peeled_sql),
        CTEStep("peeled_address_candidates", candidates_sql),
        CTEStep("peeled_address_matches", annotated_sql),
    ]
