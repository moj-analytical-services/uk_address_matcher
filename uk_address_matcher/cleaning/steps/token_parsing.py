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
    description=(
        "Extract flat positions and letters from address strings into separate columns"
    ),
    tags=["token_extraction", "flat_parsing"],
)
def _parse_out_flat_position_and_letter():
    """
    Robustly extracts flat positions, letters, and numbers from address strings.

    Strategy:
      - Detect a 'flat signal' (FLAT, floor position, digit+letter like 15B)
      - When number+letter pattern exists (11A, 15B), the LETTER is the flat determinant
      - Only extract flat_number from explicit FLAT markers (e.g., FLAT 12)
      - Ambiguous patterns like '2 69 GIPSY HILL' do NOT populate flat_number
    """

    # Floor positions: BASEMENT, GARDEN, and BLOCK are standalone;
    # others are paired with FLOOR/GROUND.
    # BLOCK indicates a flat block (e.g., "BLOCK B STANNARD HALL")
    standalone_floors = ["BASEMENT", "GARDEN", "BLOCK"]
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
        "TENTH",
        "TOP",
    ]
    # Build regex: standalone floors OR (prefix + FLOOR) OR
    # (prefix + GROUND for LOWER/UPPER)
    # Also handle multi-floor patterns like "GROUND FIRST SECOND AND THIRD FLOORS"
    # or comma-separated "FIRST, SECOND AND THIRD FLOORS"
    # Pattern handles: WORD, or WORD (space) or AND, followed by final floor + FLOORS
    multi_floor_pattern = (
        r"(?:(?:GROUND|FIRST|SECOND|THIRD|FOURTH|FIFTH|SIXTH|"
        r"SEVENTH|EIGHTH|NINTH|TENTH|TOP),? ?|AND )*"
        r"(?:GROUND|FIRST|SECOND|THIRD|FOURTH|FIFTH|SIXTH|"
        r"SEVENTH|EIGHTH|NINTH|TENTH|TOP) FLOORS"
    )
    floor_positions = (
        r"\b("
        + "|".join(
            standalone_floors
            + [f"{f} FLOOR" for f in floor_with_suffix]
            + [f"{f} GROUND" for f in ["LOWER", "UPPER"]]
        )
        + r"|"
        + multi_floor_pattern
        + r")\b"
    )
    leading_bare_floor_position = (
        r"^\s*(GROUND|FIRST|SECOND|THIRD|FOURTH|FIFTH|SIXTH|"
        r"SEVENTH|EIGHTH|NINTH|TENTH|TOP)\s+\d"
    )
    # Core token patterns (RE2-compatible; avoid lookbehind)
    num_letter_anywhere = r"\b(\d{1,4})([A-Za-z])\b"  # e.g., 15B (anywhere)
    leading_num_letter = (
        r"^\s*(\d{1,4})([A-Za-z])\b"  # e.g., 11A ... (number=grp1, letter=grp2)
    )
    # Match all numbers (standalone digits, not part of ranges like 120-122)
    count_numbers = r"\b(\d{1,5})\b"

    flat_num_after_flat = (
        r"\bFLAT\s+(\d{1,4})(?:\s|[A-Za-z/])"  # FLAT 12 / FLAT 12A / FLAT 12/2
    )
    flat_letter_after_num_after_flat = (
        r"\bFLAT\s+\d{1,4}\s*([A-Za-z])\b"  # FLAT 12A / FLAT 12 A
    )
    flat_letter_after_flat = r"\bFLAT\s+([A-Za-z])\b"  # FLAT A
    block_letter = r"\bBLOCK\s+([A-Za-z])\b"  # BLOCK A / BLOCK B
    explicit_sub_premise_signal = r"\b(FLAT|MAISONETTE|ROOM)\b"
    business_unit_signal = r"^\s*(UNIT|SUITE|OFFICE|WORKSHOP|WAREHOUSE|STUDIO)S?\b"

    # Scottish style "FLAT 3/2" → use the right-hand number as the unit/flat number
    scottish_flat = r"\bFLAT\s+(\d+)\s*/\s*(\d+)\b"

    final_base_sql = f"""
    SELECT
        i.*,

        -- 1) Positional/floor signal from the address string itself.
        CASE
            WHEN NULLIF(
                regexp_extract(i.clean_full_address, '{floor_positions}', 1),
                ''
            ) = 'LOWER GROUND'
                THEN 'GROUND FLOOR'
            WHEN NULLIF(
                regexp_extract(i.clean_full_address, '{floor_positions}', 1),
                ''
            ) = 'LOWER FLOOR'
                THEN 'LOWER FLOOR'
            WHEN NULLIF(
                regexp_extract(
                    i.clean_full_address,
                    '{leading_bare_floor_position}',
                    1
                ),
                ''
            ) IS NOT NULL
                THEN CONCAT(
                    NULLIF(
                        regexp_extract(
                            i.clean_full_address,
                            '{leading_bare_floor_position}',
                            1
                        ),
                        ''
                    ),
                    ' FLOOR'
                )
            ELSE NULLIF(
                regexp_extract(i.clean_full_address, '{floor_positions}', 1),
                ''
            )
        END AS flat_positional,

        regexp_matches(
            i.clean_full_address,
            '{explicit_sub_premise_signal}'
        ) AS has_explicit_sub_premise_signal,

        regexp_matches(
            i.clean_full_address,
            '{business_unit_signal}'
        ) AS starts_with_business_unit_signal,

        -- 2) flat_letter (priority:
        -- FLAT 12A → A, FLAT A → A, BLOCK A → A,
        -- 11A start → A, 15B anywhere → B)
        COALESCE(
            NULLIF(
                regexp_extract(
                    i.clean_full_address,
                    '{flat_letter_after_num_after_flat}',
                    1
                ),
                ''
            ),
            NULLIF(
                regexp_extract(i.clean_full_address, '{flat_letter_after_flat}', 1),
                ''
            ),
            NULLIF(
                regexp_extract(i.clean_full_address, '{block_letter}', 1),
                ''
            ),
            CASE
                WHEN (
                    regexp_matches(i.clean_full_address, '{explicit_sub_premise_signal}')
                    OR NULLIF(
                        regexp_extract(i.clean_full_address, '{floor_positions}', 1),
                        ''
                    ) IS NOT NULL
                    OR NULLIF(
                        regexp_extract(
                            i.clean_full_address,
                            '{leading_bare_floor_position}',
                            1
                        ),
                        ''
                    ) IS NOT NULL
                )
                AND NOT regexp_matches(i.clean_full_address, '{business_unit_signal}')
                THEN NULLIF(
                    regexp_extract(i.clean_full_address, '{leading_num_letter}', 2),
                    ''
                )
            END,
            CASE
                WHEN (
                    regexp_matches(i.clean_full_address, '{explicit_sub_premise_signal}')
                    OR NULLIF(
                        regexp_extract(i.clean_full_address, '{floor_positions}', 1),
                        ''
                    ) IS NOT NULL
                    OR NULLIF(
                        regexp_extract(
                            i.clean_full_address,
                            '{leading_bare_floor_position}',
                            1
                        ),
                        ''
                    ) IS NOT NULL
                )
                AND NOT regexp_matches(i.clean_full_address, '{business_unit_signal}')
                THEN NULLIF(
                    regexp_extract(i.clean_full_address, '{num_letter_anywhere}', 2),
                    ''
                )
            END
        ) AS flat_letter,

        -- 3) flat_number (priority explained inline)
        -- Only extract flat_number when there's an EXPLICIT FLAT indicator.
        -- Ambiguous cases like "2 69 GIPSY HILL" should NOT populate flat_number
        -- since "2" might be a building number, not a flat.
        -- Note: DuckDB regexp_extract returns '' not NULL for no match, so
        -- we use NULLIF(..., '') to normalise non-matches.
        CASE
            -- Explicit "FLAT X" - extract if (multiple numbers) OR (BLOCK pattern)
            -- OR (letter follows the number, e.g., "FLAT 12A")
            -- OR (Scottish style FLAT X/Y pattern)
            WHEN NULLIF(
                regexp_extract(i.clean_full_address, '{flat_num_after_flat}', 1),
                ''
            ) IS NOT NULL
                 AND (
                     COALESCE(
                        length(
                            regexp_extract_all(i.clean_full_address, '{count_numbers}')
                        ),
                        0
                    ) >= 2
                     OR NULLIF(
                        regexp_extract(i.clean_full_address, '{block_letter}', 1),
                        ''
                    ) IS NOT NULL
                     OR NULLIF(
                        regexp_extract(
                           i.clean_full_address,
                           '{flat_letter_after_num_after_flat}',
                           1
                       ),
                       ''
                   ) IS NOT NULL
                     OR NULLIF(
                        regexp_extract(i.original_address_concat, '{scottish_flat}', 1),
                        ''
                    ) IS NOT NULL
                 )
            THEN COALESCE(
                -- FLAT 3/2 → 2 (Scottish style)
                NULLIF(
                    regexp_extract(i.original_address_concat, '{scottish_flat}', 2),
                    ''
                ),
                -- FLAT 12 → 12
                NULLIF(
                    regexp_extract(i.clean_full_address, '{flat_num_after_flat}', 1),
                    ''
                )
            )
            ELSE NULL
        END AS flat_number

    FROM {{input}} i
    """

    # Final step: boolean indicator and composite flat identity
    # (split out so we can refer to computed aliases)
    # Also check for the word FLAT itself as a flat signal
    final_sql = r"""
    SELECT
        * EXCLUDE (has_explicit_sub_premise_signal, starts_with_business_unit_signal),
        (
            flat_letter IS NOT NULL
            OR flat_number IS NOT NULL
            OR flat_positional IS NOT NULL
            OR regexp_matches(clean_full_address, '\bFLAT\b')
        ) AS has_flat_indicator,
        CASE
            WHEN flat_number IS NOT NULL OR flat_letter IS NOT NULL
                 OR flat_positional IS NOT NULL
            THEN CONCAT_WS('_',
                     COALESCE(flat_number, ''),
                     COALESCE(flat_letter, ''),
                     COALESCE(flat_positional, ''))
            ELSE NULL
        END AS flat_identity
    FROM {final_base}
    """

    steps = [
        CTEStep("final_base", final_base_sql),
        CTEStep("final", final_sql),
    ]
    return steps


@pipeline_stage(
    name="parse_out_sub_premise_location",
    description=(
        "Extract sub-premise side/location descriptors from the first half of the address"
    ),
    tags=["token_extraction", "flat_parsing"],
)
def _parse_out_sub_premise_location():
    """Extract sub-premise side/location labels such as LEFT and FRONT.

    This stage keeps sub-premise location evidence separate from
    `flat_identity` so it can be compared independently downstream. The signal
    is intentionally broader than flats alone: maisonettes and other
    sub-premise occupancies can carry the same FRONT/REAR/LEFT/RIGHT cues.
    Only the first half of the cleaned address is scanned to reduce false
    positives from place names such as commercial centres later in the string.
    """

    tokens_sql = r"""
    SELECT
        regexp_split_to_array(
            i.clean_full_address, '\s+'
        ) AS sub_premise_location_tokens,
        i.*
    FROM {input} i
    """

    prefix_sql = r"""
    SELECT
        i.*,
        array_to_string(
            list_slice(
                sub_premise_location_tokens,
                1,
                LEAST(
                    len(sub_premise_location_tokens),
                    GREATEST(
                        6,
                        CAST(
                            CEIL(len(sub_premise_location_tokens) / 2.0)
                            AS BIGINT
                        )
                    )
                )
            ),
            ' '
        ) AS sub_premise_location_prefix
    FROM {tokenised} i
    """

    final_sql = r"""
    SELECT
        * EXCLUDE (
            sub_premise_location_tokens,
            sub_premise_location_prefix
        ),
        CASE
            WHEN NOT (
                flat_positional IS NOT NULL
                OR flat_letter IS NOT NULL
                OR flat_number IS NOT NULL
                OR regexp_matches(
                    clean_full_address,
                    '\b(FLAT|MAISONETTE|APARTMENT|PENTHOUSE|ROOM|UNIT|STUDIO)\b'
                )
            ) THEN NULL
            WHEN regexp_matches(
                sub_premise_location_prefix, '\bRIGHT HAND SIDE\b'
            )
                OR regexp_matches(
                    sub_premise_location_prefix, '\bRIGHT SIDE\b'
                )
                OR regexp_matches(sub_premise_location_prefix, '\bRIGHT\b')
                THEN 'RIGHT'
            WHEN regexp_matches(
                sub_premise_location_prefix, '\bLEFT HAND SIDE\b'
            )
                OR regexp_matches(sub_premise_location_prefix, '\bLEFT SIDE\b')
                OR regexp_matches(sub_premise_location_prefix, '\bLEFT\b')
                THEN 'LEFT'
            WHEN regexp_matches(sub_premise_location_prefix, '\bCENTRE\b')
                OR regexp_matches(sub_premise_location_prefix, '\bCENTER\b')
                THEN 'CENTRE'
            WHEN regexp_matches(sub_premise_location_prefix, '\bFRONT\b')
                THEN 'FRONT'
            WHEN regexp_matches(sub_premise_location_prefix, '\bREAR OF\b')
                OR regexp_matches(sub_premise_location_prefix, '\bREAR\b')
                THEN 'REAR'
            ELSE NULL
        END AS sub_premise_location
    FROM {with_prefix}
    """

    return [
        CTEStep("tokenised", tokens_sql),
        CTEStep("with_prefix", prefix_sql),
        CTEStep("final", final_sql),
    ]


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
    # Business unit keywords - these indicate commercial/industrial premises.
    # UNIT and STUDIO can also appear in residential shells, so those need
    # extra gating against nearby residential evidence.
    business_keywords = ["UNIT", "SUITE", "OFFICE", "WORKSHOP", "WAREHOUSE", "STUDIO"]

    # Build pattern: (UNIT|SUITE|...) followed by identifier
    # Identifier can be: letter (A-Z), number, or alphanumeric (5A, A5).
    # We allow up to 5 digits because real commercial studio/unit identifiers
    # can exceed 4 digits (e.g. STUDIO 10001 WORKS HOUSE ...).
    # Also handle plural forms like "UNITS 1-3" or "UNITS A AND B"
    keywords_pattern = "|".join(business_keywords)

    # Pattern for singular: UNIT A, UNIT 5, UNIT 5A, UNIT A5
    singular_pattern = (
        rf"\b({keywords_pattern})S?\s+([A-Za-z]?\d{{1,5}}[A-Za-z]?|[A-Za-z])\b"
    )
    residential_shell_pattern = r"\b(FLAT|MAISONETTE|APARTMENT|PENTHOUSE|ROOM)\b"
    residential_context_pattern = r"\b(LEFT|RIGHT|FRONT|REAR|FLOOR|BASEMENT|GARDEN)\b"
    studio_residential_pattern = r"^\s*STUDIO\s+[A-Za-z]?\d{1,5}[A-Za-z]?\s+\d"

    sql = f"""
    WITH business_unit_raw AS (
    SELECT
        i.*,

        NULLIF(
            UPPER(regexp_extract(i.clean_full_address, '{singular_pattern}', 1)),
            ''
        ) AS __business_unit_type,

        NULLIF(
            UPPER(regexp_extract(i.clean_full_address, '{singular_pattern}', 2)),
            ''
        ) AS __business_unit_id,

        (
            regexp_matches(i.clean_full_address, '{residential_shell_pattern}')
            OR regexp_matches(i.clean_full_address, '{residential_context_pattern}')
        ) AS __has_residential_context,

        regexp_matches(
            i.clean_full_address,
            '{studio_residential_pattern}'
        ) AS __looks_like_residential_studio,

        -- Extract the business unit type (UNIT, SUITE, OFFICE, etc.)
        CASE
            WHEN __business_unit_type IN ('UNIT', 'STUDIO')
                 AND (
                    __has_residential_context
                    OR (
                        __business_unit_type = 'STUDIO'
                        AND __looks_like_residential_studio
                    )
                 )
                THEN NULL
            ELSE __business_unit_type
        END AS business_unit_type,

        -- Extract the business unit identifier (A, 5, 5A, etc.)
        CASE
            WHEN __business_unit_type IN ('UNIT', 'STUDIO')
                 AND (
                    __has_residential_context
                    OR (
                        __business_unit_type = 'STUDIO'
                        AND __looks_like_residential_studio
                    )
                 )
                THEN NULL
            ELSE __business_unit_id
        END AS business_unit_id,

        -- Boolean indicator for having a business unit
        CASE
            WHEN __business_unit_type IN ('UNIT', 'STUDIO')
                 AND (
                    __has_residential_context
                    OR (
                        __business_unit_type = 'STUDIO'
                        AND __looks_like_residential_studio
                    )
                 )
                THEN FALSE
            ELSE regexp_matches(
                i.clean_full_address,
                '\\b({keywords_pattern})S?\\s+([A-Za-z]?\\d{{1,5}}[A-Za-z]?|[A-Za-z])\\b'
            )
        END AS has_business_unit

    FROM {{input}} i
    )
    SELECT * EXCLUDE (
        __business_unit_type,
        __business_unit_id,
        __has_residential_context,
        __looks_like_residential_studio
    )
    FROM business_unit_raw
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
        regexp_replace(
            clean_full_address,
            '{regex_pattern}',
            '',
            'g'
        ) AS address_without_numbers,
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
