from __future__ import annotations

from uk_address_matcher.cleaning.steps.regexes import (
    construct_nested_call,
    remove_multiple_spaces,
    trim,
)
from uk_address_matcher.sql_pipeline.steps import CTEStep, pipeline_stage


@pipeline_stage(
    name="separate_distinguishing_start_tokens_from_with_respect_to_adjacent_recrods",
    description=(
        "Identify common suffixes between addresses and separate them "
        "into unique and common token parts"
    ),
    tags=["token_analysis", "address_comparison"],
)
def _separate_distinguishing_start_tokens_from_with_respect_to_adjacent_records():
    """
    Identifies common suffixes between addresses and separates them
    into unique and common parts.

    This function analyses each address in relation to its neighbours
    (previous and next addresses when sorted by unique_id) to find
    common suffix patterns. It then splits each address into:

        - unique_tokens: tokens unique to this address,
            typically the beginning part.
        - common_tokens: tokens shared with neighbouring addresses,
            typically the end part.

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
            (token, i) ->
                i < __token_count - greatest(prev_common_suffix, next_common_suffix)
        ) AS unique_tokens,
        list_filter(
            __tokens,
            (token, i) ->
                i >= __token_count - greatest(prev_common_suffix, next_common_suffix)
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
    description="Extract ordered flat-related tokens from address strings",
    tags=["token_extraction", "flat_parsing"],
)
def _parse_out_flat_position_and_letter():
    """
    Extract an ordered list of floor words, flat letters, and numeric fragments.

    This intentionally replaces the older split between ``flat_positional``,
    ``flat_letter``, ``flat_number``, ``has_flat_indicator``, and
    ``flat_identity`` with one simpler field:

    ``numberic_letter_and_positional``

    The list preserves the address order and keeps all values as strings so
    leading zeroes remain intact, e.g. ``7.07`` -> ``['7', '07']``.
    """

    floor_tokens = [
        "BASEMENT",
        "GARDEN",
        "REAR",
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
    relevant_token_pattern = (
        r"\b("
        r"FLAT\s+\d+\s*/\s*\d+|"
        r"FLAT\s+\d+\s+[A-Za-z]|"
        r"FLAT\s+\d+[A-Za-z]|"
        r"FLAT\s+[A-Za-z]|"
        r"BLOCK\s+[A-Za-z]|" + "|".join(floor_tokens) + r"|"
        r"\d{1,5}[./-]\d{1,5}|"
        r"[A-Za-z]\d{1,5}|"
        r"\d{1,5}[A-Za-z]?"
        r")\b"
    )

    sql = f"""
    SELECT
        * EXCLUDE (numberic_letter_and_positional_raw),
        CASE
            WHEN length(numberic_letter_and_positional_raw) = 0 THEN NULL
            ELSE numberic_letter_and_positional_raw
        END AS numberic_letter_and_positional
    FROM (
        SELECT
            i.*,
            flatten(
                list_transform(
                    regexp_extract_all(
                        i.clean_full_address,
                        '{relevant_token_pattern}'
                    ),
                    token -> CASE
                        WHEN regexp_matches(token, '^FLAT\\s+\\d+\\s*/\\s*\\d+$')
                            THEN regexp_extract_all(token, '\\d+')
                        WHEN regexp_matches(token, '^FLAT\\s+\\d+\\s+[A-Za-z]$')
                            THEN [
                                regexp_extract(token, '(\\d+)', 1),
                                UPPER(regexp_extract(token, '([A-Za-z])$', 1))
                            ]
                        WHEN regexp_matches(token, '^FLAT\\s+\\d+[A-Za-z]$')
                            THEN [
                                regexp_extract(token, '(\\d+)', 1),
                                UPPER(regexp_extract(token, '([A-Za-z])$', 1))
                            ]
                        WHEN regexp_matches(token, '^(FLAT|BLOCK)\\s+[A-Za-z]$')
                            THEN [UPPER(regexp_extract(token, '([A-Za-z])$', 1))]
                        WHEN regexp_matches(token, '^\\d+[./-]\\d+$')
                            THEN regexp_extract_all(token, '\\d+')
                        WHEN regexp_matches(token, '^[A-Za-z]\\d+$')
                            THEN [
                                UPPER(regexp_extract(token, '^([A-Za-z])', 1)),
                                regexp_extract(token, '(\\d+)$', 1)
                            ]
                        WHEN regexp_matches(token, '^\\d+[A-Za-z]$')
                            THEN [
                                regexp_extract(token, '^(\\d+)', 1),
                                UPPER(regexp_extract(token, '([A-Za-z])$', 1))
                            ]
                        ELSE [UPPER(token)]
                    END
                )
            ) AS numberic_letter_and_positional_raw
        FROM {{input}} i
    )
    """
    return sql


@pipeline_stage(
    name="parse_out_business_unit",
    description=(
        "Extract business unit identifiers (UNIT, SUITE, OFFICE, etc.) from addresses"
    ),
    tags=["token_extraction", "business_parsing"],
)
def _parse_out_business_unit():
    """
    Extracts business unit identifiers from address strings.

    Business addresses often have unit identifiers that distinguish different
    tenants within the same building, e.g.:
      - "UNIT C 32 PARKHALL BUSINESS CENTRE"
      - "UNIT F 32 PARKHALL BUSINESS CENTRE"

    These are distinct from residential flat indicators as they typically appear
    in commercial/industrial contexts. Common patterns:
      - UNIT A, UNIT 5, UNIT 5A, UNITS 1-3
      - SUITE 100, SUITE A
      - OFFICE 5, OFFICE A
      - WORKSHOP 3, WORKSHOP A
      - WAREHOUSE A, WAREHOUSE 5

    We capture:
      - business_unit_type: The type keyword (UNIT, SUITE, OFFICE, etc.)
      - business_unit_id: The identifier (letter, number, or alphanumeric)
      - has_business_unit: Boolean indicator
    """
    # Business unit keywords - these indicate commercial/industrial premises
    # Note: UNIT is normalised FROM residential APARTMENT in earlier cleaning,
    # but raw UNIT in business contexts (UNIT C, UNIT 5) remains
    business_keywords = ["UNIT", "SUITE", "OFFICE", "WORKSHOP", "WAREHOUSE", "STUDIO"]

    # Build pattern: (UNIT|SUITE|...) followed by identifier
    # Identifier can be: letter (A-Z), number (1-999), or alphanumeric (5A, A5)
    # Also handle plural forms like "UNITS 1-3" or "UNITS A AND B"
    keywords_pattern = "|".join(business_keywords)

    # Pattern for singular: UNIT A, UNIT 5, UNIT 5A, UNIT A5
    singular_pattern = (
        rf"\b({keywords_pattern})S?\s+([A-Za-z]?\d{{1,4}}[A-Za-z]?|[A-Za-z])\b"
    )

    sql = f"""
    SELECT
        i.*,

        -- Extract the business unit type (UNIT, SUITE, OFFICE, etc.)
        NULLIF(
            UPPER(regexp_extract(i.clean_full_address, '{singular_pattern}', 1)),
            ''
        ) AS business_unit_type,

        -- Extract the business unit identifier (A, 5, 5A, etc.)
        NULLIF(
            UPPER(regexp_extract(i.clean_full_address, '{singular_pattern}', 2)),
            ''
        ) AS business_unit_id,

        -- Boolean indicator for having a business unit
        regexp_matches(
            i.clean_full_address,
            '\\b({keywords_pattern})S?\\s+([A-Za-z]?\\d{{1,4}}[A-Za-z]?|[A-Za-z])\\b'
        ) AS has_business_unit

    FROM {{input}} i
    """
    return sql


@pipeline_stage(
    name="parse_out_numbers",
    description=(
        "Extract and process numeric tokens from addresses, "
        "handling ranges and alphanumeric patterns"
    ),
    tags="token_extraction",
)
def _parse_out_numbers():
    """
    Extracts and processes numeric tokens from address strings, ensuring the max length
    of the number+letter is 6 with no more than 1 letter which can be at the start or end.
    It also captures ranges like '1-2', '12-17', '98-102' as a single 'number', and
    matches patterns like '20A', 'A20', '20', and '20-21'.

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
        regexp_replace(
            clean_full_address,
            '{regex_pattern}',
            '',
            'g'
        ) AS address_without_numbers,
        regexp_extract_all(clean_full_address, '{regex_pattern}') AS numeric_tokens
    FROM {{input}}
    """
    return sql


@pipeline_stage(
    name="clean_address_string_second_pass",
    description=(
        "Apply final cleaning to address without numbers: remove multiple spaces and trim"
    ),
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
    description=(
        "Map specific tokens to more general categories for better matching heuristics"
    ),
    tags="token_transformation",
)
def _generalised_token_aliases():
    """
    Maps specific tokens to more general categories to create a generalised representation
    of the unique tokens in an address.

    The idea is to guide matches away from implausible matches and towards
    possible matches

    The real tokens always take precedence over generalised tokens.

    For example sometimes a 2nd floor flat will match to top floor.  Whilst 'top floor'
    is often ambiguous (is the 2nd floor the top floor), we know that
    'top floor' cannot match to 'ground' or 'basement'

    This stage expands each token in `distinguishing_adj_start_tokens`
    into a small list of
    aliases, then flattens the result into `distinguishing_adj_token_aliases`.

    Mappings applied:
    - FIRST, SECOND, THIRD, TOP -> UPPERFLOOR, LEVEL
    - GARDEN, GROUND -> GROUNDFLOOR, LEVEL
    - BASEMENT -> LEVEL
    - Everything else is kept as-is (the original token is retained).
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
