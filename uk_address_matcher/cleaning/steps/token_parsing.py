from __future__ import annotations

from uk_address_matcher.cleaning.steps.regexes import (
    construct_nested_call,
    remove_multiple_spaces,
    trim,
)
from uk_address_matcher.sql_pipeline.steps import CTEStep, pipeline_stage


@pipeline_stage(
    name="separate_distinguishing_start_tokens_from_with_respect_to_adjacent_recrods",
    description="Identify common suffixes between addresses and separate them into unique and common token parts",
    tags=["token_analysis", "address_comparison"],
)
def _separate_distinguishing_start_tokens_from_with_respect_to_adjacent_records():
    """
    Identifies common suffixes between addresses and separates them into unique and common parts.
    This function analyzes each address in relation to its neighbors (previous and next addresses
    when sorted by unique_id) to find common suffix patterns. It then splits each address into:
    - unique_tokens: The tokens that are unique to this address (typically the beginning part)
    - common_tokens: The tokens that are shared with neighboring addresses (typically the end part)
    Args:
        ddb_pyrel (DuckDBPyRelation): The input relation
        con (DuckDBPyConnection): The DuckDB connection
    Returns:
        DuckDBPyRelation: The modified table with unique_tokens and common_tokens fields
    """
    # We will only ever have FLAT in the code by this point, as APARTMENT and UNIT
    # have already been removed in earlier cleaning steps
    tokens_sql = """
    SELECT
        ['FLAT'] AS __tokens_to_remove,
        list_filter(
            regexp_split_to_array(clean_full_address, '\\s+'),
            x -> NOT list_contains(__tokens_to_remove, x)
        ) AS __tokens,
        row_number() OVER (ORDER BY reverse(clean_full_address)) AS row_order,
        *
    FROM {input}
    """

    neighbors_sql = """
    SELECT
        lag(__tokens) OVER (ORDER BY row_order) AS __prev_tokens,
        lead(__tokens) OVER (ORDER BY row_order) AS __next_tokens,
        *
    FROM {tokens}
    """

    suffix_lengths_sql = """
    SELECT
        len(__tokens) AS __token_count,
        CASE WHEN __prev_tokens IS NOT NULL THEN
            (
                SELECT max(i)
                FROM range(0, least(len(__tokens), len(__prev_tokens))) AS t(i)
                WHERE list_slice(list_reverse(__tokens), 1, i + 1) =
                    list_slice(list_reverse(__prev_tokens), 1, i + 1)
            )
        ELSE 0 END AS prev_common_suffix,
        CASE WHEN __next_tokens IS NOT NULL THEN
            (
                SELECT max(i)
                FROM range(0, least(len(__tokens), len(__next_tokens))) AS t(i)
                WHERE list_slice(list_reverse(__tokens), 1, i + 1) =
                    list_slice(list_reverse(__next_tokens), 1, i + 1)
            )
        ELSE 0 END AS next_common_suffix,
        *
    FROM {with_neighbors}
    """

    unique_parts_sql = """
    SELECT
        *,
        greatest(prev_common_suffix, next_common_suffix) AS max_common_suffix,
        list_filter(
            __tokens,
            (token, i) -> i < __token_count - greatest(prev_common_suffix, next_common_suffix)
        ) AS unique_tokens,
        list_filter(
            __tokens,
            (token, i) -> i >= __token_count - greatest(prev_common_suffix, next_common_suffix)
        ) AS common_tokens
    FROM {with_suffix_lengths}
    """

    final_sql = """
    SELECT
        * EXCLUDE (
            __tokens,
            __prev_tokens,
            __next_tokens,
            __token_count,
            __tokens_to_remove,
            max_common_suffix,
            next_common_suffix,
            prev_common_suffix,
            row_order,
            common_tokens,
            unique_tokens
        ),
        COALESCE(unique_tokens, ARRAY[]) AS distinguishing_adj_start_tokens,
        COALESCE(common_tokens, ARRAY[]) AS common_adj_start_tokens
    FROM {with_unique_parts}
    """

    steps = [
        CTEStep("tokens", tokens_sql),
        CTEStep("with_neighbors", neighbors_sql),
        CTEStep("with_suffix_lengths", suffix_lengths_sql),
        CTEStep("with_unique_parts", unique_parts_sql),
        CTEStep("final", final_sql),
    ]

    return steps


@pipeline_stage(
    name="parse_out_flat_position_and_letter",
    description="Extract flat positions and letters from address strings into separate columns",
    tags=["token_extraction", "flat_parsing"],
)
def _parse_out_flat_position_and_letter():
    """
    Robustly extracts flat positions, letters, and numbers from address strings.

    Strategy:
      - Detect a 'flat signal' (FLAT, floor position, digit+letter like 15B)
      - When number+letter pattern exists (11A, 15B), the LETTER is the flat determinant
      - Only extract flat_number from explicit FLAT markers (e.g., FLAT 12) or multi-number heuristics
      - Treat '2 69 GIPSY HILL' as flat_number=2 (two-number start heuristic)
    """

    # Floor positions: BASEMENT and GARDEN are standalone, others paired with FLOOR/GROUND
    standalone_floors = ["BASEMENT", "GARDEN"]
    floor_with_suffix = [
        "LOWER",
        "UPPER",
        "GROUND",
        "FIRST",
        "SECOND",
        "THIRD",
        "FOURTH",
        "FIFTH",
        "SIXTH",
        "SEVENTH",
        "EIGHTH",
        "NINTH",
        "TOP",
    ]
    # Build regex: standalone floors OR (prefix + FLOOR) OR (prefix + GROUND for LOWER/UPPER)
    floor_positions = (
        r"\b("
        + "|".join(
            standalone_floors
            + [f"{f} FLOOR" for f in floor_with_suffix]
            + [f"{f} GROUND" for f in ["LOWER", "UPPER"]]
        )
        + r")\b"
    )

    # Core token patterns (RE2-compatible; avoid lookbehind)
    num_letter_anywhere = r"\b(\d{1,4})([A-Za-z])\b"  # e.g., 15B (anywhere)
    leading_num_letter = (
        r"^\s*(\d{1,4})([A-Za-z])\b"  # e.g., 11A ... (number=grp1, letter=grp2)
    )
    # Match all numbers (standalone digits, not part of ranges like 120-122)
    count_numbers = r"\b(\d{1,5})\b"

    flat_num_after_flat = r"\bFLAT\s+(\d{1,4})\b"  # FLAT 12
    flat_letter_after_num_after_flat = (
        r"\bFLAT\s+\d{1,4}\s*([A-Za-z])\b"  # FLAT 12A / FLAT 12 A
    )
    flat_letter_after_flat = r"\bFLAT\s+([A-Za-z])\b"  # FLAT A

    # Scottish style "FLAT 3/2" → use the right-hand number as the unit/flat number
    scottish_flat = r"\bFLAT\s+(\d+)\s*/\s*(\d+)\b"

    final_base_sql = f"""
    SELECT
        i.*,

        -- 1) Positional/floor signal
        NULLIF(regexp_extract(i.clean_full_address, '{floor_positions}', 1), '') AS flat_positional,

        -- 2) flat_letter (priority: FLAT 12A → A, FLAT A → A, 11A start → A, 15B anywhere → B)
        COALESCE(
            NULLIF(regexp_extract(i.clean_full_address, '{flat_letter_after_num_after_flat}', 1), ''),
            NULLIF(regexp_extract(i.clean_full_address, '{flat_letter_after_flat}', 1), ''),
            NULLIF(regexp_extract(i.clean_full_address, '{leading_num_letter}', 2), ''),
            NULLIF(regexp_extract(i.clean_full_address, '{num_letter_anywhere}', 2), '')
        ) AS flat_letter,

        -- 3) flat_number (priority explained inline)
        -- Accept flat_number if we have:
        -- A) Explicit FLAT + number AND 2+ total numbers, OR
        -- B) Multiple numbers AND no number+letter pattern
        CASE
            WHEN (
                regexp_extract(i.clean_full_address, '{flat_num_after_flat}', 1) IS NOT NULL
                AND COALESCE(length(regexp_extract_all(i.clean_full_address, '{count_numbers}')), 0) >= 2
            ) OR (
                COALESCE(length(regexp_extract_all(i.clean_full_address, '{count_numbers}')), 0) >= 2
                AND regexp_extract(i.clean_full_address, '{leading_num_letter}', 1) IS NULL
                AND regexp_extract(i.clean_full_address, '{num_letter_anywhere}', 1) IS NULL
            ) THEN COALESCE(
                -- FLAT 3/2 → 2
                NULLIF(regexp_extract(i.original_address_concat, '{scottish_flat}', 2), ''),

                -- FLAT 12 → 12: take the number next to FLAT first
                NULLIF(regexp_extract(i.clean_full_address, '{flat_num_after_flat}', 1), ''),

                -- Two-number start heuristic: "2 69 GIPSY HILL" → 2 (only if there is a second number)
                CASE
                    WHEN NULLIF(regexp_extract(i.clean_full_address, '^\\s*(\\d{{1,4}})\\b', 1), '') IS NOT NULL
                     AND NULLIF(regexp_extract(i.clean_full_address, '^\\s*\\d{{1,4}}\\D+.*?\\b(\\d{{1,4}})\\b', 1), '') IS NOT NULL
                    THEN regexp_extract(i.clean_full_address, '^\\s*(\\d{{1,4}})\\b', 1)
                END
            )
            ELSE NULL
        END AS flat_number

    FROM {{input}} i
    """

    # Final step: boolean indicator (split out so we can refer to computed aliases)
    # Also check for the word FLAT itself as a flat signal
    final_sql = r"""
    SELECT
        *,
        (
            flat_letter IS NOT NULL
            OR flat_number IS NOT NULL
            OR flat_positional IS NOT NULL
            OR regexp_matches(clean_full_address, '\bFLAT\b')
        ) AS has_flat_indicator
    FROM {final_base}
    """

    steps = [
        CTEStep("final_base", final_base_sql),
        CTEStep("final", final_sql),
    ]
    return steps


@pipeline_stage(
    name="parse_out_numbers",
    description="Extract and process numeric tokens from addresses, handling ranges and alphanumeric patterns",
    tags="token_extraction",
)
def _parse_out_numbers():
    """
    Extracts and processes numeric tokens from address strings, ensuring the max length
    of the number+letter is 6 with no more than 1 letter which can be at the start or end.
    It also captures ranges like '1-2', '12-17', '98-102' as a single 'number', and
    matches patterns like '20A', 'A20', '20', and '20-21'.

    Special case: If flat_letter is a number, the first number found will be ignored
    as it's likely a duplicate of the flat number.

    Args:
        table_name (str): The name of the table to process.
        con (DuckDBPyConnection): The DuckDB connection.

    Returns:
        DuckDBPyRelation: The modified table with processed fields.
    """
    regex_pattern = (
        r"\b"  # Word boundary
        # Prioritize matching number ranges first
        r"(\d{1,5}-\d{1,5}|[A-Za-z]?\d{1,5}[A-Za-z]?)"
        r"\b"  # Word boundary
    )
    sql = f"""
    SELECT
        *,
        regexp_replace(clean_full_address, '{regex_pattern}', '', 'g') AS address_without_numbers,
        CASE
            WHEN flat_letter IS NOT NULL AND flat_letter ~ '^\\d+$' THEN
            regexp_extract_all(clean_full_address, '{regex_pattern}')[2:]
            ELSE
                regexp_extract_all(clean_full_address, '{regex_pattern}')
        END AS numeric_tokens
    FROM {{input}}
    """
    return sql


@pipeline_stage(
    name="clean_address_string_second_pass",
    description="Apply final cleaning to address without numbers: remove multiple spaces and trim",
    tags="cleaning",
)
def _clean_address_string_second_pass():
    fn_call = construct_nested_call(
        "address_without_numbers",
        [remove_multiple_spaces, trim],
    )
    sql = f"""
    select
        * exclude (address_without_numbers),
        {fn_call} as address_without_numbers
    from {{input}}
    """
    return sql


GENERALISED_TOKEN_ALIASES_CASE_STATEMENT = """
    CASE
        WHEN token in ('FIRST', 'SECOND', 'THIRD', 'TOP') THEN ['UPPERFLOOR', 'LEVEL']
        WHEN token in ('GARDEN', 'GROUND') THEN ['GROUNDFLOOR', 'LEVEL']
        WHEN token in ('BASEMENT') THEN ['LEVEL']
        ELSE [TOKEN]
    END

"""


@pipeline_stage(
    name="generalised_token_aliases",
    description="Map specific tokens to more general categories for better matching heuristics",
    tags="token_transformation",
)
def _generalised_token_aliases():
    """
    Maps specific tokens to more general categories to create a generalised representation
    of the unique tokens in an address.

    The idea is to guide matches away from implausible matches and towards
    possible matches

    The real tokens always take precidence over genearlised

    For example sometimes a 2nd floor flat will match to top floor.  Whilst 'top floor'
    is often ambiguous (is the 2nd floor the top floor), we know that
    'top floor' cannot match to 'ground' or 'basement'

    This function applies the following mappings:

    [FIRST, SECOND, THIRD, TOP] -> [UPPERFLOOR, LEVEL]

    [GARDEN, GROUND] -> [GROUNDFLOOR, LEVEL]


    This function applies the following mappings:
    - Single letters (A-E) -> UNIT_NUM_LET
    - Single digits (1-5) -> UNIT_NUM_LET
    - Floor indicators (FIRST, SECOND, THIRD) -> LEVEL
    - Position indicators (TOP, FIRST, SECOND, THIRD) -> TOP
    The following tokens are filtered out completely:
    - FLAT, APARTMENT, UNIT
    Args:
        ddb_pyrel (DuckDBPyRelation): The input relation with unique_tokens field
        con (DuckDBPyConnection): The DuckDB connection
    Returns:
        DuckDBPyRelation: The modified table with generalised_unique_tokens field
    """
    sql = f"""
    SELECT
        *,
        flatten(
            list_transform(distinguishing_adj_start_tokens, token ->
               {GENERALISED_TOKEN_ALIASES_CASE_STATEMENT}
            )
        ) AS distinguishing_adj_token_aliases
    FROM {{input}}
    """
    return sql
