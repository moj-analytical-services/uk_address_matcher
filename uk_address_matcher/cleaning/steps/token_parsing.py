from __future__ import annotations

from uk_address_matcher.cleaning.steps.regexes import (
    construct_nested_call,
    remove_multiple_spaces,
    trim,
)
from uk_address_matcher.sql_pipeline.steps import CTEStep, pipeline_stage


def _premise_identifier_pattern(max_digits: int = 4) -> str:
    return rf"[A-Za-z]?\d{{1,{max_digits}}}[A-Za-z]?|[A-Za-z]"


def _premise_role_segment_pattern(role: str) -> str:
    identifier = rf"\b(?:{_premise_identifier_pattern(5)})\b"
    marker = f"{role}S?"
    return (
        rf"\b{marker}\s+(?:PART\s+)?{identifier}"
        rf"(?:\s*(?:AND|,|&)\s*(?:PART\s+)?(?:{marker}\s+)?{identifier})*"
    )


def _address_for_parsing(address_expression: str) -> str:
    exclusion_pattern = r"'(^|[ (])EXCLUDING +([^ )]+)', '\1EXCLUDING_\2', 'g'"
    return f"regexp_replace({address_expression}, {exclusion_pattern})"


def _scope_uncertainty_pattern(*patterns: str) -> str:
    exclusions = (
        r"\b(?:EXCLUDE|EXCLUDING|EXCLUDED|EXCEPT|EXCEPTING|EXCEPTED|NOT\s+INCLUDING)\b"
    )
    return "|".join((*patterns, exclusions))


@pipeline_stage(name="derive_local_key_tokens")
def _derive_local_key_tokens(*, numericless_only: bool = True):
    """Extract lexical keys without changing original token positions."""
    eligible = "clean_full_address IS NOT NULL"
    if numericless_only:
        eligible += " AND len(numeric_tokens) = 0"
    return [
        CTEStep(
            "local_tokens",
            """
            SELECT input.*,
                coalesce(string_split(clean_full_address, ' '), []::VARCHAR[])
                    AS __local_tokens
            FROM {input} input
            """,
        ),
        CTEStep(
            "lexical_flags",
            """
            SELECT input.*, list_transform(__local_tokens, token ->
                length(token) >= 2 AND NOT regexp_matches(token, '[0-9]'))
                AS __local_lexical
            FROM {local_tokens} input
            """,
        ),
        CTEStep(
            "final",
            f"""
            SELECT * EXCLUDE (__local_tokens, __local_lexical),
                CASE WHEN {eligible} THEN list_concat(
                    list_transform(
                        list_filter(range(1, len(__local_tokens) + 1), position ->
                            __local_lexical[position]
                            AND __local_tokens[position] NOT IN (
                                'THE','OF','TO','AT','NEAR','VIA','FROM','AND','WITH'
                            )
                        ),
                        position -> struct_pack(
                            pos := position::INTEGER, kind := 'word',
                            key := __local_tokens[position]
                        )
                    ),
                    list_transform(
                        list_filter(range(1, len(__local_tokens)), position ->
                            __local_lexical[position] AND __local_lexical[position + 1]
                            AND (__local_tokens[position] NOT IN (
                                    'THE','OF','TO','AT','NEAR','VIA','FROM','AND','WITH'
                                )
                                OR __local_tokens[position + 1]
                                    NOT IN (
                                        'THE','OF','TO','AT','NEAR','VIA','FROM','AND','WITH'
                                    ))
                        ),
                        position -> struct_pack(
                            pos := position::INTEGER, kind := 'phrase',
                            key := __local_tokens[position] || ' '
                                || __local_tokens[position + 1]
                        )
                    )
                ) ELSE []::STRUCT(pos INTEGER, kind VARCHAR, key VARCHAR)[] END
                    AS local_key_tokens
            FROM {{lexical_flags}}
            """,
        ),
    ]


_DISTINGUISHING_MARKER_VALUES = (
    "ANNEXE",
    "WORKSHOP",
    "DEPOT",
    "FARM",
    "BUSINESS",
    "CENTRE",
    "CENTER",
    "BUILDING",
    "STUDIO",
    "WAREHOUSE",
    "OFFICE",
    "UNIT",
    "UNITS",
    "SUITE",
    "SUITES",
    "ROOM",
    "FLOOR",
    "FLOORS",
    "SHOP",
    "KIOSK",
    "PLOT",
    "STALL",
    "GARAGE",
    "YARD",
    "BAY",
    "PARKING",
    "CAR",
    "PARK",
    "SPACE",
    "LOCK",
    "LOCKUP",
    "CONTAINER",
    "FLAT",
)


@pipeline_stage(
    name="separate_distinguishing_start_tokens_from_with_respect_to_adjacent_records",
    description=(
        "Identify common suffixes between addresses and separate them "
        "into unique and common token parts"
    ),
    tags=["token_analysis", "address_comparison"],
)
def _separate_distinguishing_start_tokens_from_with_respect_to_adjacent_records(
    *,
    include_input_columns: bool = True,
    carry_neighbour_tokens: bool = True,
    use_precomputed_tokens: bool = False,
):
    """Split each address around its longest suffix shared by a local neighbour."""
    token_expression = (
        "clean_full_address_tokens::VARCHAR[]"
        if use_precomputed_tokens
        else "string_split(clean_full_address, ' ')::VARCHAR[]"
    )
    tokenised_addresses_sql = f"""
    SELECT
        ukam_address_id,
        unique_id,
        clean_full_address,
        {token_expression} AS __tokens
    FROM {{input}} AS input_address
    """

    neighbour_value = "__tokens" if carry_neighbour_tokens else "clean_full_address"
    neighbour_suffix = "tokens" if carry_neighbour_tokens else "address"
    neighbouring_addresses_sql = """
    SELECT
        ukam_address_id,
        unique_id,
        __tokens,
        lag(unique_id, 1) OVER address_order AS __lag_1_unique_id,
        lag({neighbour_value}, 1) OVER address_order AS __lag_1_{neighbour_suffix},
        lag(unique_id, 2) OVER address_order AS __lag_2_unique_id,
        lag({neighbour_value}, 2) OVER address_order AS __lag_2_{neighbour_suffix},
        lag(unique_id, 3) OVER address_order AS __lag_3_unique_id,
        lag({neighbour_value}, 3) OVER address_order AS __lag_3_{neighbour_suffix},
        lead(unique_id, 1) OVER address_order AS __lead_1_unique_id,
        lead({neighbour_value}, 1) OVER address_order AS __lead_1_{neighbour_suffix},
        lead(unique_id, 2) OVER address_order AS __lead_2_unique_id,
        lead({neighbour_value}, 2) OVER address_order AS __lead_2_{neighbour_suffix},
        lead(unique_id, 3) OVER address_order AS __lead_3_unique_id,
        lead({neighbour_value}, 3) OVER address_order AS __lead_3_{neighbour_suffix}
    FROM {tokenised_addresses} AS tokenised
    WINDOW address_order AS (
        ORDER BY
            reverse(clean_full_address),
            CAST(unique_id AS VARCHAR),
            ukam_address_id
    )
    """.replace("{neighbour_value}", neighbour_value).replace(
        "{neighbour_suffix}", neighbour_suffix
    )

    nearest_distinct_neighbours_sql = """
    SELECT
        *,
        CASE
            WHEN __lag_1_unique_id IS NOT NULL
             AND __lag_1_unique_id <> unique_id THEN __lag_1_{neighbour_suffix}
            WHEN __lag_2_unique_id IS NOT NULL
             AND __lag_2_unique_id <> unique_id THEN __lag_2_{neighbour_suffix}
            WHEN __lag_3_unique_id IS NOT NULL
             AND __lag_3_unique_id <> unique_id THEN __lag_3_{neighbour_suffix}
            ELSE NULL
        END AS __left_neighbour_{neighbour_suffix},
        CASE
            WHEN __lead_1_unique_id IS NOT NULL
             AND __lead_1_unique_id <> unique_id THEN __lead_1_{neighbour_suffix}
            WHEN __lead_2_unique_id IS NOT NULL
             AND __lead_2_unique_id <> unique_id THEN __lead_2_{neighbour_suffix}
            WHEN __lead_3_unique_id IS NOT NULL
             AND __lead_3_unique_id <> unique_id THEN __lead_3_{neighbour_suffix}
            ELSE NULL
        END AS __right_neighbour_{neighbour_suffix}
    FROM {neighbouring_addresses} AS neighbours
    """.replace("{neighbour_suffix}", neighbour_suffix)

    left_neighbour_value = f"__left_neighbour_{neighbour_suffix}"
    right_neighbour_value = f"__right_neighbour_{neighbour_suffix}"
    left_neighbour_tokens = (
        left_neighbour_value
        if carry_neighbour_tokens
        else f"string_split({left_neighbour_value}, ' ')::VARCHAR[]"
    )
    right_neighbour_tokens = (
        right_neighbour_value
        if carry_neighbour_tokens
        else f"string_split({right_neighbour_value}, ' ')::VARCHAR[]"
    )

    suffix_lengths_sql = (
        """
    SELECT
        ukam_address_id,
        __tokens,
        CASE
            WHEN {left_neighbour_value} IS NULL THEN 0
            ELSE COALESCE(
                list_position(
                    list_transform(
                        list_zip(
                            list_reverse(__tokens),
                            list_reverse({left_neighbour_tokens}),
                            true
                        ),
                        token_pair -> token_pair[1] != token_pair[2]
                    ),
                    true
                ) - 1,
                least(len(__tokens), len({left_neighbour_tokens}))
            )
        END AS __left_common_suffix_length,
        CASE
            WHEN {right_neighbour_value} IS NULL THEN 0
            ELSE COALESCE(
                list_position(
                    list_transform(
                        list_zip(
                            list_reverse(__tokens),
                            list_reverse({right_neighbour_tokens}),
                            true
                        ),
                        token_pair -> token_pair[1] != token_pair[2]
                    ),
                    true
                ) - 1,
                least(len(__tokens), len({right_neighbour_tokens}))
            )
        END AS __right_common_suffix_length
    FROM {nearest_distinct_neighbours} AS neighbours
    """.replace("{left_neighbour_value}", left_neighbour_value)
        .replace("{right_neighbour_value}", right_neighbour_value)
        .replace("{left_neighbour_tokens}", left_neighbour_tokens)
        .replace("{right_neighbour_tokens}", right_neighbour_tokens)
    )

    maximum_suffix_lengths_sql = """
    SELECT
        suffix_lengths.ukam_address_id,
        suffix_lengths.__tokens,
        greatest(
            __left_common_suffix_length,
            __right_common_suffix_length
        ) AS __max_common_suffix_length
    FROM {suffix_lengths} AS suffix_lengths
    """

    output_columns_sql = (
        "input_address.*," if include_input_columns else "maximums.ukam_address_id,"
    )
    output_source_sql = (
        "FROM {input} AS input_address\n"
        "LEFT JOIN {maximum_suffix_lengths} AS maximums\n"
        "  ON input_address.ukam_address_id = maximums.ukam_address_id"
        if include_input_columns
        else "FROM {maximum_suffix_lengths} AS maximums"
    )
    final_sql = """
    SELECT
        {output_columns_sql}
        CASE
            WHEN COALESCE(maximums.__max_common_suffix_length, 0) > 0
                THEN COALESCE(
                    list_slice(
                        maximums.__tokens,
                        1,
                        len(maximums.__tokens)
                            - maximums.__max_common_suffix_length
                    ),
                    []::VARCHAR[]
                )
            ELSE []::VARCHAR[]
        END::VARCHAR[] AS distinguishing_adj_start_tokens,
        CASE
            WHEN COALESCE(maximums.__max_common_suffix_length, 0) > 0
                THEN COALESCE(
                    list_slice(
                        maximums.__tokens,
                        len(maximums.__tokens)
                            - maximums.__max_common_suffix_length + 1,
                        len(maximums.__tokens)
                    ),
                    []::VARCHAR[]
                )
            ELSE COALESCE(maximums.__tokens, []::VARCHAR[])
        END::VARCHAR[] AS common_adj_start_tokens
    {output_source_sql}
    """.replace(
        "{output_columns_sql}",
        output_columns_sql,
    ).replace(
        "{output_source_sql}",
        output_source_sql,
    )

    steps = [
        CTEStep("tokenised_addresses", tokenised_addresses_sql),
        CTEStep("neighbouring_addresses", neighbouring_addresses_sql),
        CTEStep(
            "nearest_distinct_neighbours",
            nearest_distinct_neighbours_sql,
        ),
        CTEStep("suffix_lengths", suffix_lengths_sql),
        CTEStep("maximum_suffix_lengths", maximum_suffix_lengths_sql),
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
    - Ambiguous patterns like '7 42 FICTIONAL ROAD' do NOT populate flat_number
    """

    # Floor positions: BASEMENT, GARDEN, and BLOCK are standalone;
    # others are paired with FLOOR/GROUND.
    # BLOCK indicates a flat block (e.g., "BLOCK B STANNARD HALL")
    standalone_floors = ["BASEMENT", "GARDEN", "BLOCK"]
    floor_with_suffix = (
        "LOWER UPPER GROUND FIRST SECOND THIRD FOURTH FIFTH SIXTH "
        "SEVENTH EIGHTH NINTH TENTH TOP"
    ).split()
    # Build regex: standalone floors OR (prefix + FLOOR) OR
    # (prefix + GROUND for LOWER/UPPER)
    # Also handle multi-floor patterns like "GROUND FIRST SECOND AND THIRD FLOORS"
    # or comma-separated "FIRST, SECOND AND THIRD FLOORS"
    # Pattern handles: WORD, or WORD (space) or AND, followed by final floor + FLOORS
    numbered_floor_pattern = "|".join(floor_with_suffix[2:])
    multi_floor_pattern = (
        rf"(?:(?:{numbered_floor_pattern}),? ?|AND )*"
        rf"(?:{numbered_floor_pattern}) FLOORS"
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
    floor_first_address_pattern = (
        r"^\s*(?:" + floor_positions + "|" + "|".join(floor_with_suffix) + r")\s+FLAT\b"
    )
    leading_bare_floor_position = rf"^\s*({numbered_floor_pattern})\s+\d"
    # Core token patterns (RE2-compatible; avoid lookbehind)
    num_letter_anywhere = r"\b(\d{1,4})([A-Za-z])\b"  # e.g., 15B (anywhere)
    leading_num_letter = (
        r"^\s*(\d{1,4})([A-Za-z])\b"  # e.g., 11A ... (number=grp1, letter=grp2)
    )
    # Match all numbers (standalone digits, not part of ranges like 120-122)
    count_numbers = r"\b(\d{1,5})\b"

    flat_num_after_flat = (
        r"\bFLAT\s+(\d{1,4})(?:\s|[A-Za-z/]|$)"  # FLAT 12 / FLAT 12A / FLAT 12/2
    )
    flat_letter_after_num_after_flat = (
        r"\bFLAT\s+\d{1,4}\s*([A-Za-z])\b"  # FLAT 12A / FLAT 12 A
    )
    flat_letter_after_flat = r"\bFLAT\s+([A-Za-z])\b"  # FLAT A
    block_letter = r"\bBLOCK\s+([A-Za-z])\b"  # BLOCK A / BLOCK B

    # Scottish style "FLAT 3/2" → use the right-hand number as the unit/flat number
    scottish_flat = r"\bFLAT\s+(\d+)\s*/\s*(\d+)\b"
    address_for_parsing = _address_for_parsing("input.clean_full_address")
    flat_number_delimiter_pattern = r"\bFLAT\s*[,;]\s*\d"

    final_base_sql = f"""
    SELECT
        i.* EXCLUDE (clean_full_address, __original_clean_full_address),
        i.__original_clean_full_address AS clean_full_address,

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
            NULLIF(
                regexp_extract(i.clean_full_address, '{leading_num_letter}', 2),
                ''
            ),
            NULLIF(
                regexp_extract(i.clean_full_address, '{num_letter_anywhere}', 2),
                ''
            )
        ) AS flat_letter,

        -- 3) flat_number (priority explained inline)
        -- Only extract flat_number when there's an EXPLICIT FLAT indicator.
        -- Ambiguous cases like "7 42 FICTIONAL ROAD" should NOT populate flat_number
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
                     OR (
                         COALESCE(
                             length(
                                 regexp_extract_all(
                                     i.clean_full_address,
                                     '{count_numbers}'
                                 )
                             ),
                             0
                         ) = 1
                         AND NOT regexp_matches(
                             i.clean_full_address,
                             '{floor_first_address_pattern}'
                         )
                         AND NOT regexp_matches(
                             upper(coalesce(i.original_address_concat, '')),
                             '{flat_number_delimiter_pattern}'
                         )
                     )
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
        END AS flat_number,
        regexp_matches(i.clean_full_address, '\\bFLAT\\b') AS __has_explicit_flat
    FROM (
        SELECT
            input.* EXCLUDE (clean_full_address),
            input.clean_full_address AS __original_clean_full_address,
            {address_for_parsing} AS clean_full_address
        FROM {{input}} input
    ) i
    """

    # Final step: boolean indicator and composite flat identity
    # (split out so we can refer to computed aliases)
    # Also check for the word FLAT itself as a flat signal
    final_sql = r"""
    SELECT
        * EXCLUDE (__has_explicit_flat),
        (
            flat_letter IS NOT NULL
            OR flat_number IS NOT NULL
            OR flat_positional IS NOT NULL
            OR __has_explicit_flat
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
        CTEStep("flat_identity", final_sql),
        CTEStep("final", _floor_key_set_sql(floor_with_suffix)),
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

    tokens_for_parsing = _address_for_parsing(
        "array_to_string(i.clean_full_address_tokens, ' ')"
    )
    prefix_sql = f"""
    SELECT
        i.* EXCLUDE (__tokens_for_parsing),
        list_slice(
                __tokens_for_parsing,
                1,
                LEAST(
                    len(__tokens_for_parsing),
                    GREATEST(
                        6,
                        CAST(CEIL(len(__tokens_for_parsing) / 2.0) AS BIGINT)
                    )
                )
            ) AS sub_premise_location_prefix_tokens
    FROM (
        SELECT
            i.*,
            string_split({tokens_for_parsing}, ' ')::VARCHAR[]
                AS __tokens_for_parsing
        FROM {{input}} i
    ) i
    """

    final_sql = r"""
    SELECT
        * EXCLUDE (
            sub_premise_location_prefix_tokens
        ),
        CASE
            WHEN NOT (
                flat_positional IS NOT NULL
                OR flat_letter IS NOT NULL
                OR flat_number IS NOT NULL
                OR list_contains(clean_full_address_tokens, 'FLAT')
                OR list_contains(clean_full_address_tokens, 'MAISONETTE')
            ) THEN NULL
            WHEN list_contains(sub_premise_location_prefix_tokens, 'RIGHT')
                THEN 'RIGHT'
            WHEN list_contains(sub_premise_location_prefix_tokens, 'LEFT')
                THEN 'LEFT'
            WHEN list_contains(sub_premise_location_prefix_tokens, 'CENTRE')
                OR list_contains(sub_premise_location_prefix_tokens, 'CENTER')
                THEN 'CENTRE'
            WHEN list_contains(sub_premise_location_prefix_tokens, 'FRONT')
                THEN 'FRONT'
            WHEN list_contains(sub_premise_location_prefix_tokens, 'REAR')
                THEN 'REAR'
            ELSE NULL
        END AS sub_premise_location
    FROM {with_prefix}
    """

    return [CTEStep("with_prefix", prefix_sql), CTEStep("final", final_sql)]


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
    roles = "UNIT SUITE ROOM OFFICE BLOCK BUILDING WORKSHOP WAREHOUSE STUDIO".split()
    business_keywords = [
        role for role in roles if role not in ("ROOM", "BLOCK", "BUILDING")
    ]

    # Build pattern: (UNIT|SUITE|...) followed by identifier
    # Identifier can be: letter (A-Z), number (1-999), or alphanumeric (5A, A5)
    # Also handle plural forms like "UNITS 1-3" or "UNITS A AND B"
    keywords_pattern = "|".join(business_keywords)

    # Pattern for singular: UNIT A, UNIT 5, UNIT 5A, UNIT A5
    singular_pattern = rf"\b({keywords_pattern})S?\s+({_premise_identifier_pattern()})\b"
    address_for_parsing = _address_for_parsing(
        "array_to_string(source.__premise_segments, ' ')"
    )

    sql = f"""
    SELECT
        i.* EXCLUDE (__business_unit_match),
        NULLIF(
            UPPER(i.__business_unit_match.business_unit_type),
            ''
        ) AS business_unit_type,
        NULLIF(
            UPPER(i.__business_unit_match.business_unit_id),
            ''
        ) AS business_unit_id,
        i.__business_unit_match.business_unit_type != '' AS has_business_unit
    FROM (
        SELECT
            source.*,
            regexp_extract(
                {address_for_parsing},
                '{singular_pattern}',
                ['business_unit_type', 'business_unit_id']
            ) AS __business_unit_match
        FROM {{role_segments}} AS source
    ) AS i
    """
    segment_pattern = (
        r"(?:\bEXCLUDING\s+)?(?:"
        + "|".join(_premise_role_segment_pattern(role) for role in roles)
        + ")"
    )
    return [
        CTEStep(
            "role_segments",
            f"""
            SELECT input.*, regexp_extract_all(
                upper(coalesce(input.clean_full_address, '')),
                '{segment_pattern}', 0
            ) AS __premise_segments
            FROM {{input}} AS input
            """,
        ),
        CTEStep("business_unit", sql),
        CTEStep("final", _premise_key_set_sql(roles)),
    ]


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

    Road references such as 'A96T', 'C110S', and 'A 952' are excluded unless
    explicitly labelled as a flat, unit, or other premise identifier.

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
        r"(\d{1,5}[A-Za-z]?-\d{1,5}[A-Za-z]?|[A-Za-z]?\d{1,5}[A-Za-z]?)"
        r"\b"  # Word boundary
    )
    road_code_pattern = (
        r"\b((?:FLAT|APARTMENT|UNIT|SUITE|OFFICE|ROOM|BUILDING|BLOCK|PLOT|GARAGE|BAY|"
        r"SHOP|FLOOR|LEVEL|WORKSHOP|WAREHOUSE|STUDIO|CONTAINER|PLATFORM|MAST|YARD|"
        r"PARKING SPACE)\s+[ABCU]\s*\d+[A-Z]?)\b"
        r"|\b[ABCU]\s*\d+[A-Z]?\b"
    )
    sql = f"""
    SELECT
        * EXCLUDE (__numeric_token_address),
        regexp_replace(
            clean_full_address,
            '{regex_pattern}',
            '',
            'g'
        ) AS address_without_numbers,
        CASE
            WHEN flat_letter IS NOT NULL AND flat_letter ~ '^\\d+$' THEN
            regexp_extract_all(__numeric_token_address, '{regex_pattern}')[2:]
            ELSE
                regexp_extract_all(__numeric_token_address, '{regex_pattern}')
        END AS numeric_tokens
    FROM (
        SELECT *, regexp_replace(
            clean_full_address, '{road_code_pattern}', '\\1', 'g'
        ) AS __numeric_token_address
        FROM {{input}}
    ) AS numbers_input
    """
    return sql


@pipeline_stage(
    name="derive_missingness_aware_sub_premise_features",
    description=(
        "Derive role and identifier candidates without promoting unknown markers"
    ),
    tags=["token_extraction", "sub_premise_parsing"],
)
def _derive_missingness_aware_sub_premise_features():
    marker_pattern = rf"^\s*([A-Z]+)\s+(?:{_premise_identifier_pattern(5)})\b"
    return f"""
    SELECT
        i.*,
        NULLIF(
            regexp_extract(i.clean_full_address, '{marker_pattern}', 1),
            ''
        ) AS sub_premise_marker_token,
        CASE
            WHEN i.has_flat_indicator
                OR i.flat_number IS NOT NULL
                OR i.flat_letter IS NOT NULL
                OR i.flat_positional IS NOT NULL
                THEN 'FLAT'
            WHEN i.has_business_unit OR i.business_unit_id IS NOT NULL
                THEN 'BUSINESS_UNIT'
            ELSE NULL
        END AS sub_premise_role,
        CASE
            WHEN i.has_flat_indicator
                AND i.flat_number IS NOT NULL
                AND i.flat_letter IS NOT NULL
                THEN CONCAT(i.flat_number, i.flat_letter)
            WHEN i.has_flat_indicator
                THEN COALESCE(
                    NULLIF(i.flat_number, ''),
                    NULLIF(i.flat_letter, '')
                )
            WHEN i.has_business_unit OR i.business_unit_id IS NOT NULL
                THEN NULLIF(i.business_unit_id, '')
            WHEN i.numeric_tokens IS NOT NULL
                AND len(i.numeric_tokens) >= 1
                THEN list_extract(i.numeric_tokens, 1)
            ELSE NULL
        END AS sub_premise_identifier
    FROM {{input}} AS i
    """


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


@pipeline_stage(
    name="clean_address_string_second_pass_and_tokenise",
    description=("Clean address_without_numbers and derive its token array in one stage"),
    tags=["cleaning", "tokenisation"],
)
def _clean_address_string_second_pass_and_tokenise():
    fn_call = construct_nested_call(
        "address_without_numbers",
        [remove_multiple_spaces, trim],
    )
    return [
        CTEStep(
            "cleaned",
            f"""
            SELECT
                * EXCLUDE (address_without_numbers),
                {fn_call} AS address_without_numbers
            FROM {{input}}
            """,
        ),
        CTEStep(
            "final",
            r"""
            SELECT
                *,
                string_split(address_without_numbers, ' ')
                    AS __address_without_numbers_tokenised
            FROM {cleaned}
            """,
        ),
    ]


def _premise_key_set_sql(roles: list[str]) -> str:
    identifier_pattern = rf"\b(?:{_premise_identifier_pattern(5)})\b"
    role_pattern = "|".join(f"{role}S?" for role in roles)
    map_entries = []
    for role in roles:
        marker = f"{role}S?"
        marker_removal_pattern = rf"\b(?:{marker}|PART)\b"
        map_entries.append(
            f"""struct_pack(key := '{role}', value :=
            list_sort(list_distinct(flatten(list_transform(
                list_filter(__premise_segments, segment -> regexp_matches(
                    segment, '^(?:EXCLUDING\\s+)?{marker}\\b'
                )),
                segment -> regexp_extract_all(
                    regexp_replace(segment, '{marker_removal_pattern}', ' ', 'g'),
                    '{identifier_pattern}',
                    0
                )
            )))))"""
        )

    map_entry_sql = ",\n".join(map_entries)
    role_range_pattern = (
        rf"\b(?:{role_pattern})\s+{identifier_pattern}"
        rf"\s*(?:-|TO|THROUGH|UP\s+TO)\s*{identifier_pattern}\b"
    )
    partial_role_pattern = rf"\bPART\s+(?:{role_pattern})\b"
    uncertain_pattern = _scope_uncertainty_pattern(
        role_range_pattern, partial_role_pattern
    )
    return f"""
    SELECT
        * EXCLUDE (__premise_segments, __premise_parse_address),
        map_from_entries(list_filter(
            [{map_entry_sql}], entry -> len(struct_extract(entry, 'value')) > 0
        )) AS premise_keys,
        len(map_keys(premise_keys)) > 0
            AND regexp_matches(__premise_parse_address, '{uncertain_pattern}')
            AS premise_scope_uncertain
    FROM (
        SELECT *, upper(coalesce(clean_full_address, '')) AS __premise_parse_address
        FROM {{business_unit}}
    ) AS input
    """


def _floor_key_set_sql(floor_names: list[str]) -> str:
    floor_token_pattern = (
        "(?:" + "|".join((*floor_names, r"\d{1,2}(?:ST|ND|RD|TH)")) + ")"
    )
    floor_list_pattern = (
        rf"\b{floor_token_pattern}(?:\s*(?:AND|,|&)\s*"
        rf"{floor_token_pattern})*\s+FLOORS?\b"
    )
    floor_range_pattern = (
        rf"\b{floor_token_pattern}\s*(?:-|TO|THROUGH)\s*"
        rf"{floor_token_pattern}\s+FLOORS?\b"
    )
    ordinal_cases = []
    for number, name in enumerate(floor_names[3:-1], start=1):
        suffix = {1: "ST", 2: "ND", 3: "RD"}.get(number, "TH")
        ordinal_cases.append(f"WHEN '{number}{suffix}' THEN '{name}'")
    floor_normalisation = (
        "CASE floor_name " + " ".join(ordinal_cases) + " ELSE floor_name END"
    )
    uncertain_pattern = _scope_uncertainty_pattern(
        floor_range_pattern, r"\bTOP\s+FLOORS?\b"
    )
    return f"""
    SELECT
        * EXCLUDE (__floor_parse_address),
        list_sort(list_distinct(list_transform(
            flatten(list_transform(
                regexp_extract_all(__floor_parse_address, '{floor_list_pattern}', 0),
                segment -> regexp_extract_all(segment, '{floor_token_pattern}', 0)
            )),
            floor_name -> {floor_normalisation}
        ))) AS floor_keys,
        len(floor_keys) > 0
            AND regexp_matches(__floor_parse_address, '{uncertain_pattern}')
            AS floor_scope_uncertain
    FROM (
        SELECT *, upper(coalesce(clean_full_address, '')) AS __floor_parse_address
        FROM {{flat_identity}}
    ) AS input
    """


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


def _address_structure_premise_sql() -> str:
    address_structure_premise_patterns = [
        r"CAR\s+PARK\s+SPACE",
        r"PARKING\s+SPACE",
        r"CAR\s+PARK",
        r"LOCK\s+UP",
        "LOCKUP",
        "SHOP",
        "KIOSK",
        "PLOT",
        "STALL",
        "GARAGE",
        "YARD",
        "BAY",
    ]
    premise_pattern = "|".join(address_structure_premise_patterns)
    identifier_pattern = _premise_identifier_pattern()
    address_for_parsing = _address_for_parsing("input.clean_full_address")
    return f"""
    SELECT
        source.* EXCLUDE (__address_structure_premise_match),
        NULLIF(
            regexp_replace(
                UPPER(
                    source.__address_structure_premise_match.address_structure_premise_type
                ),
                '\\s+',
                ' ',
                'g'
            ),
            ''
        ) AS address_structure_premise_type,
        NULLIF(
            UPPER(source.__address_structure_premise_match.address_structure_premise_id),
            ''
        ) AS address_structure_premise_id,
        source.__address_structure_premise_match.address_structure_premise_type != ''
            AS has_address_structure_premise,
        NULLIF(
            regexp_replace(
                UPPER(
                    source.__address_structure_premise_match.address_structure_premise_type
                ),
                '\\s+',
                ' ',
                'g'
            ),
            ''
        ) AS commercial_premise_type,
        NULLIF(
            UPPER(source.__address_structure_premise_match.address_structure_premise_id),
            ''
        ) AS commercial_premise_id,
        source.__address_structure_premise_match.address_structure_premise_type != ''
            AS has_commercial_premise
    FROM (
        SELECT
            input.*,
            regexp_extract(
                {address_for_parsing},
                '\\b({premise_pattern})\\b(?:\\s+({identifier_pattern})\\b)?',
                ['address_structure_premise_type', 'address_structure_premise_id']
            ) AS __address_structure_premise_match
        FROM {{input}} AS input
    ) AS source
    """


@pipeline_stage(
    name="parse_out_address_structure_premise",
    description="Extract address-structure premise types and identifiers from addresses",
    tags=["token_extraction", "address_structure_parsing"],
)
def _parse_out_address_structure_premise():
    return _address_structure_premise_sql()


@pipeline_stage(
    name="parse_out_commercial_premise",
    description="Compatibility alias for address-structure premise parsing",
    tags=["token_extraction", "business_parsing"],
)
def _parse_out_commercial_premise():
    return _address_structure_premise_sql()


@pipeline_stage(
    name="derive_distinguishing_token_components",
    description="Split canonical distinguishing prefixes into lexical residuals",
    tags=["token_analysis", "address_structure_parsing"],
)
def _derive_distinguishing_token_components():
    marker_values = ", ".join(f"'{value}'" for value in _DISTINGUISHING_MARKER_VALUES)
    return f"""
    WITH marked AS (
        SELECT
            input.*,
            list_filter(
                range(1, len(input.distinguishing_adj_start_tokens) + 1),
                position -> list_extract(
                    input.distinguishing_adj_start_tokens, position
                ) IN ({marker_values})
                OR (
                    position > 1
                    AND list_extract(
                        input.distinguishing_adj_start_tokens, position - 1
                    ) IN ({marker_values})
                    AND regexp_matches(
                        list_extract(
                            input.distinguishing_adj_start_tokens, position
                        ),
                        '^[A-Z]?[0-9]{{1,4}}[A-Z]?$|^[A-Z]$'
                    )
                )
            ) AS __structural_positions
        FROM {{input}} AS input
    )
    SELECT
        marked.* EXCLUDE (__structural_positions),
        list_transform(
            __structural_positions,
            position -> list_extract(distinguishing_adj_start_tokens, position)
        ) AS distinguishing_structural_tokens,
        list_transform(
            distinguishing_adj_start_tokens,
            (token, position) -> struct_pack(
                token := token,
                is_lexical := NOT list_contains(__structural_positions, position)
            )
        ) AS distinguishing_token_parts,
        list_filter(
            distinguishing_adj_start_tokens,
            (token, position) -> NOT list_contains(__structural_positions, position)
        ) AS distinguishing_lexical_tokens
    FROM marked
    """
