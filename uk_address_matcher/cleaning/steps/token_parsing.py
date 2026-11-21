from __future__ import annotations

from uk_address_matcher.cleaning.steps.regexes import (
    construct_nested_call,
    remove_multiple_spaces,
    trim,
)
from uk_address_matcher.sql_pipeline.steps import CTEStep, pipeline_stage


def _premise_identifier_pattern(max_digits: int = 4) -> str:
    return rf"[A-Za-z]?\d{{1,{max_digits}}}[A-Za-z]?|[A-Za-z]"


def _premise_role_segment_pattern(role: str, range_separator: str) -> str:
    endpoint = rf"\b(?:{_premise_identifier_pattern(5)})\b"
    identifier = rf"{endpoint}(?:\s*{range_separator}\s*{endpoint})?"
    marker = f"{role}S?"
    return (
        rf"\b{marker}\s+(?:PART\s+)?{identifier}"
        rf"(?:\s*(?:AND|,|&)\s*(?:PART\s+)?(?:{marker}\s+)?{identifier})*"
    )


def _address_for_parsing(address_expression: str) -> str:
    exclusion_pattern = r"'(^|[ (])EXCLUDING +([^ )]+)', '\1EXCLUDING_\2', 'g'"
    return (
        f"CASE WHEN strpos({address_expression}, 'EXCLUDING ') > 0 "
        f"THEN regexp_replace({address_expression}, {exclusion_pattern}) "
        f"ELSE {address_expression} END"
    )


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
            SELECT input.*, list_transform(__local_tokens, lambda token:
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
                        list_filter(range(1, len(__local_tokens) + 1), lambda position:
                            __local_lexical[position]
                            AND __local_tokens[position] NOT IN (
                                'THE','OF','TO','AT','NEAR','VIA','FROM','AND','WITH'
                            )
                        ),
                        lambda position: struct_pack(
                            pos := position::INTEGER, kind := 'word',
                            key := __local_tokens[position]
                        )
                    ),
                    list_transform(
                        list_filter(range(1, len(__local_tokens)), lambda position:
                            __local_lexical[position] AND __local_lexical[position + 1]
                            AND (__local_tokens[position] NOT IN (
                                    'THE','OF','TO','AT','NEAR','VIA','FROM','AND','WITH'
                                )
                                OR __local_tokens[position + 1]
                                    NOT IN (
                                        'THE','OF','TO','AT','NEAR','VIA','FROM','AND','WITH'
                                    ))
                        ),
                        lambda position: struct_pack(
                            pos := position::INTEGER, kind := 'phrase',
                            key := __local_tokens[position] || ' '
                                || __local_tokens[position + 1]
                        )
                    ),
                    list_transform(
                        list_filter(range(1, greatest(len(__local_tokens) - 1, 1)),
                            lambda position:
                                __local_lexical[position]
                                AND __local_tokens[position + 1] IN ('Y', 'YR')
                                AND __local_lexical[position + 2]
                        ),
                        lambda position: struct_pack(
                            pos := position::INTEGER, kind := 'phrase',
                            key := __local_tokens[position] || ' '
                                || __local_tokens[position + 1] || ' '
                                || __local_tokens[position + 2]
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
    "FFLAT",
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
                        lambda token_pair: token_pair[1] != token_pair[2]
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
                        lambda token_pair: token_pair[1] != token_pair[2]
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
    name="parse_out_sub_premise",
    description="Extract explicit sub-premise identity separately from floor evidence",
    tags=["token_extraction", "sub_premise_parsing"],
)
def _parse_out_sub_premise():
    """Derive sub-premise fields; map lower ground to basement and attic to top floor."""

    # Floor positions: BASEMENT, GARDEN, and BLOCK are standalone;
    # others are paired with FLOOR/GROUND.
    # BLOCK indicates a flat block (e.g., "BLOCK B STANNARD HALL")
    standalone_floors = ["BASEMENT", "GARDEN", "BLOCK"]
    floor_with_suffix = (
        "LOWER UPPER GROUND FIRST SECOND THIRD FOURTH FIFTH SIXTH "
        "SEVENTH EIGHTH NINTH TENTH TOP"
    ).split()
    # Build regex: standalone floors OR (prefix + FLOOR) OR
    # lower/upper ground and attic variants
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
            + [r"LOWER GROUND(?: FLOOR)?", "UPPER GROUND", r"ATTIC(?: FLOOR)?"]
        )
        + r"|"
        + multi_floor_pattern
        + r")\b"
    )
    flat_marker = r"F?FLAT"
    explicit_flat_pattern = rf"\b{flat_marker}\b"
    floor_first_address_pattern = (
        r"^\s*(?:"
        + floor_positions
        + "|"
        + "|".join(floor_with_suffix)
        + rf")\s+(?:{flat_marker}|APARTMENT|MAISONETTE|PENTHOUSE)\b"
    )
    leading_bare_floor_position = rf"^\s*({numbered_floor_pattern})\s+\d"
    leading_sub_premise_floor = (
        rf"^\s*({'|'.join(floor_with_suffix[3:])})\s+"
        rf"(?:BLOCK\s+[A-Z]\s+)?(?:{flat_marker}|APARTMENT|MAISONETTE|PENTHOUSE)"
        r"\s+\d{1,5}[A-Z]?\b"
    )
    # Core token patterns (RE2-compatible; avoid lookbehind)
    num_letter_anywhere = r"\b(\d{1,4})([A-Za-z])\b"  # e.g., 15B (anywhere)
    leading_num_letter = (
        r"^\s*(\d{1,4})([A-Za-z])\b"  # e.g., 11A ... (number=grp1, letter=grp2)
    )
    # Match all numbers (standalone digits, not part of ranges like 120-122)
    count_numbers = r"\b(\d{1,5})\b"

    block_letter = r"\bBLOCK\s+([A-Za-z])\b"  # BLOCK A / BLOCK B
    scottish_flat = rf"{explicit_flat_pattern}\s+(\d+)\s*/\s*(\d+)\b"
    address_for_parsing = _address_for_parsing("input.clean_full_address")
    flat_number_delimiter_pattern = rf"{explicit_flat_pattern}\s*[,;]\s*\d"
    business_descriptors = "'UNIT', 'SUITE', 'OFFICE', 'WORKSHOP', 'WAREHOUSE', 'STUDIO'"
    descriptors = (
        "F?FLAT|APARTMENT|MAISONETTE|PENTHOUSE|UNIT|SUITE|OFFICE|"
        "WORKSHOP|WAREHOUSE|STUDIO"
    )
    identifier = r"[A-Z]?\d{1,5}[A-Z]?(?:\s*/\s*\d{1,5})?|[A-Z]"
    prefix_words = "|".join(
        floor_with_suffix
        + standalone_floors
        + [
            "ATTIC",
            "MEZZANINE",
            "FLOORS?",
            "FRONT",
            "REAR",
            "LEFT",
            "RIGHT",
            "CENTRE",
            "CENTER",
            "AND",
        ]
    )
    prefix = rf"^\s*(?:(?:{prefix_words})[, ]+)*"
    building_number = r"[0-9]{1,5}[A-Z]?(?:-[0-9]{1,5}[A-Z]?)?"
    unit_identifier = (
        rf"(?:NORTH|SOUTH|EAST|WEST|[NSEW])\s+[0-9]{{1,4}}[A-Z]?"
        rf"|[0-9]{{1,4}}\s+[A-Z]|{identifier}"
    )
    qualified_unit = (
        rf"^(UNITS?)\s+(?:(?:{prefix_words}|OF)\s+)*"
        rf"({unit_identifier})\b(\s+(?:AT\s+)?{building_number}(?:\s.*|$))"
    )
    first_descriptor = (
        rf"regexp_extract({address_for_parsing}, '\b(?:{descriptors})S?\s+.*', 0)"
    )
    split_identifier = rf"^\s*([0-9]{{1,4}}[A-Z]?)\s+AT\s+{building_number}(?:\s|$)"
    explicit_sql = rf"""
    SELECT input.*,
        {address_for_parsing} AS __sub_premise_address,
        CASE WHEN regexp_matches({first_descriptor}, '{qualified_unit}')
            THEN regexp_extract({first_descriptor}, '{qualified_unit}',
                ['descriptor', 'identifier', 'tail'])
            ELSE regexp_extract(
            CASE WHEN regexp_matches(upper(input.original_address_concat),
                    '{scottish_flat}')
                THEN regexp_extract(
                    {_address_for_parsing("upper(input.original_address_concat)")},
                    '\bF?FLAT\s+.*', 0)
                WHEN regexp_matches({address_for_parsing},
                    '{prefix}(?:{descriptors})S?\s+(?:{identifier})\b')
                    THEN {address_for_parsing}
                ELSE regexp_extract({address_for_parsing},
                    '\b(?:F?FLAT|UNIT|SUITE|OFFICE|WORKSHOP|WAREHOUSE)S?\s+.*', 0)
            END,
            '{prefix}({descriptors})S?\s+({identifier})\b(.*)',
            ['descriptor', 'identifier', 'tail']
        ) END AS __sub_premise_match
    FROM {{input}} input
    """
    identifier_sql = rf"""
    SELECT * EXCLUDE (__sub_premise_match),
        nullif(regexp_extract(__sub_premise_address, '{leading_sub_premise_floor}', 1),
            '') AS __leading_sub_premise_floor,
        nullif(replace(regexp_replace(__sub_premise_match.descriptor, 'S$', ''),
            'FFLAT', 'FLAT'), '') AS __descriptor,
        CASE
            WHEN __sub_premise_match.descriptor IN ('UNIT', 'UNITS')
                AND regexp_matches(__sub_premise_match.identifier,
                    '^(NORTH|SOUTH|EAST|WEST)\s')
                THEN substr(__sub_premise_match.identifier, 1, 1)
                    || regexp_extract(__sub_premise_match.identifier,
                        '([0-9]{{1,4}}[A-Z]?)$', 1)
            WHEN __sub_premise_match.descriptor IN ('UNIT', 'UNITS')
                AND regexp_matches(__sub_premise_match.identifier, '\s')
                THEN replace(__sub_premise_match.identifier, ' ', '')
            WHEN regexp_full_match(__sub_premise_match.identifier, '[A-Z]')
                AND regexp_matches(__sub_premise_match.tail, '{split_identifier}')
                THEN __sub_premise_match.identifier
                    || regexp_extract(__sub_premise_match.tail, '{split_identifier}', 1)
            WHEN __sub_premise_match.descriptor IN ('FLAT', 'FFLAT')
                AND regexp_full_match(__sub_premise_match.identifier, '[0-9]+')
                AND regexp_matches(__sub_premise_match.tail, '^\s+[A-Z](\s|$)')
                THEN __sub_premise_match.identifier
                    || regexp_extract(__sub_premise_match.tail, '^\s+([A-Z])\b', 1)
            ELSE nullif(regexp_replace(__sub_premise_match.identifier,
                '^.*\/\s*', ''), '')
        END AS __identifier,
        CASE
            WHEN regexp_full_match(__sub_premise_match.identifier, '[A-Z]')
                AND regexp_matches(__sub_premise_match.tail, '{split_identifier}')
                THEN regexp_replace(__sub_premise_match.tail,
                    '^\s*[0-9]{{1,4}}[A-Z]?\s+', '')
            WHEN __sub_premise_match.descriptor IN ('FLAT', 'FFLAT')
                AND regexp_full_match(__sub_premise_match.identifier, '[0-9]+')
                AND regexp_matches(__sub_premise_match.tail, '^\s+[A-Z](\s|$)')
                THEN regexp_replace(__sub_premise_match.tail, '^\s+[A-Z]\s*', '')
            ELSE __sub_premise_match.tail
        END AS __sub_premise_tail,
        nullif(regexp_extract(__sub_premise_address,
            '{explicit_flat_pattern}\s+(\d{{1,4}})(?:\s|[A-Za-z/]|$)', 1), '')
            AS __flat_number_candidate,
        CASE WHEN __descriptor = 'FLAT' THEN coalesce(
            nullif(regexp_extract(__sub_premise_match.identifier, '([A-Z])$', 1), ''),
            nullif(regexp_extract(__identifier, '([A-Z])$', 1), '')
        ) END AS __flat_letter_candidate
    FROM {{explicit}}
    """
    final_base_sql = f"""
    SELECT
        i.*,
        CASE
            WHEN NULLIF(
                regexp_extract(i.__sub_premise_address, '{floor_positions}', 1),
                ''
            ) IN ('LOWER GROUND', 'LOWER GROUND FLOOR')
                THEN 'BASEMENT'
            WHEN regexp_extract(
                i.__sub_premise_address, '{floor_positions}', 1
            ) IN ('ATTIC', 'ATTIC FLOOR')
                THEN 'TOP FLOOR'
            WHEN NULLIF(
                regexp_extract(i.__sub_premise_address, '{floor_positions}', 1),
                ''
            ) = 'LOWER FLOOR'
                THEN 'LOWER FLOOR'
            WHEN NULLIF(
                regexp_extract(
                    i.__sub_premise_address,
                    '{leading_bare_floor_position}',
                    1
                ),
                ''
            ) IS NOT NULL
                THEN CONCAT(
                    NULLIF(
                        regexp_extract(
                            i.__sub_premise_address,
                            '{leading_bare_floor_position}',
                            1
                        ),
                        ''
                    ),
                    ' FLOOR'
                )
            WHEN __leading_sub_premise_floor IS NOT NULL
                THEN __leading_sub_premise_floor || ' FLOOR'
            ELSE NULLIF(
                regexp_extract(i.__sub_premise_address, '{floor_positions}', 1),
                ''
            )
        END AS flat_positional,

        coalesce(
                __flat_letter_candidate,
                nullif(regexp_extract(__sub_premise_address, '{block_letter}', 1), ''),
                nullif(regexp_extract(__sub_premise_address,
                    '{leading_num_letter}', 2), ''),
                nullif(regexp_extract(__sub_premise_address,
                    '{num_letter_anywhere}', 2), '')
        ) AS flat_letter,
        CASE
            WHEN __flat_number_candidate IS NOT NULL
                AND (
                    len(regexp_extract_all(__sub_premise_address, '{count_numbers}')) >= 2
                    OR regexp_matches(__sub_premise_address, '{block_letter}')
                    OR regexp_matches(__sub_premise_address,
                        '{explicit_flat_pattern}\\s+\\d{{1,4}}\\s*[A-Za-z]\\b')
                    OR regexp_matches(upper(original_address_concat), '{scottish_flat}')
                    OR (
                        NOT regexp_matches(__sub_premise_address,
                            '{floor_first_address_pattern}')
                        AND NOT regexp_matches(upper(original_address_concat),
                            '{flat_number_delimiter_pattern}')
                    )
                )
                THEN coalesce(
                    nullif(regexp_extract(upper(original_address_concat),
                        '{scottish_flat}', 2), ''), __flat_number_candidate)
        END AS flat_number,
        CASE WHEN __descriptor IN ({business_descriptors})
            THEN __descriptor END AS business_unit_type,
        CASE WHEN __descriptor IN ({business_descriptors})
            THEN __identifier END AS business_unit_id,
        coalesce(__descriptor IN ({business_descriptors}), false) AS has_business_unit,
        coalesce(__descriptor,
            nullif(regexp_extract(__sub_premise_address,
                '^\\s*([A-Z]+)\\s+(?:[A-Z]?[0-9]{{1,5}}[A-Z]?|[A-Z])\\b', 1), '')
        ) AS sub_premise_marker_token,
        CASE WHEN NOT regexp_matches(__sub_premise_tail,
                '\\b(?:{descriptors})\\s+(?:{identifier})(?:\\s|$)')
            AND len(regexp_extract_all(__sub_premise_tail,
                '\\b{building_number}\\b')) = 1
            THEN regexp_extract(__sub_premise_tail, '\\b({building_number})\\b', 1)
        END AS sub_premise_building_number,
        __identifier AS sub_premise_explicit_identifier,
        list_distinct(regexp_extract_all(
            regexp_extract(clean_full_address, '^[^0-9]*', 0),
            'BASEMENT|GROUND|FIRST|SECOND|THIRD|FOURTH|FIFTH', 0
        )) AS flat_floor_prefixes
    FROM {{identifier}} i
    """

    # Final step: boolean indicator and composite flat identity
    # (split out so we can refer to computed aliases)
    # Also check for the word FLAT itself as a flat signal
    final_sql = f"""
    SELECT
        * EXCLUDE (__descriptor, __identifier, __sub_premise_tail,
            __sub_premise_address, __flat_number_candidate, __flat_letter_candidate),
        (flat_letter IS NOT NULL OR flat_number IS NOT NULL
                OR flat_positional IS NOT NULL
                OR regexp_matches(__sub_premise_address, '{explicit_flat_pattern}'))
            AS has_flat_indicator,
        CASE
            WHEN flat_number IS NOT NULL OR flat_letter IS NOT NULL
                OR flat_positional IS NOT NULL
            THEN CONCAT_WS('_',
                     COALESCE(flat_number, ''),
                     COALESCE(flat_letter, ''),
                     COALESCE(flat_positional, ''))
            ELSE NULL
        END AS flat_identity,
        CASE
            WHEN __descriptor IN ('UNIT', 'STUDIO') THEN 'BUSINESS_UNIT'
            WHEN __descriptor IN ('APARTMENT', 'MAISONETTE', 'PENTHOUSE') THEN 'FLAT'
            WHEN has_flat_indicator THEN 'FLAT'
            WHEN has_business_unit THEN 'BUSINESS_UNIT'
        END AS sub_premise_role,
        CASE
            WHEN __descriptor IN ('UNIT', 'STUDIO', 'APARTMENT', 'MAISONETTE',
                'PENTHOUSE') THEN __identifier
            WHEN has_flat_indicator THEN
                nullif(concat(coalesce(flat_number, ''), coalesce(flat_letter, '')), '')
            WHEN has_business_unit THEN business_unit_id
        END AS sub_premise_identifier
    FROM {{final_base}}
    """

    roles = "UNIT SUITE ROOM OFFICE BLOCK BUILDING WORKSHOP WAREHOUSE STUDIO".split()
    range_separator = r"(?:-|TO|THROUGH|UP\s+TO)"
    segment_pattern = (
        r"(?:\bEXCLUDING\s+)?(?:"
        + "|".join(_premise_role_segment_pattern(role, range_separator) for role in roles)
        + ")"
    )
    steps = [
        CTEStep("explicit", explicit_sql),
        CTEStep("identifier", identifier_sql),
        CTEStep("final_base", final_base_sql),
        CTEStep("flat_identity", final_sql),
        CTEStep("floor_keys", _floor_key_set_sql(floor_with_suffix)),
        CTEStep(
            "role_segments",
            f"""
            SELECT input.*, regexp_extract_all(
                upper(coalesce(input.clean_full_address, '')),
                '{segment_pattern}', 0
            ) AS __premise_segments
            FROM {{floor_keys}} AS input
            """,
        ),
        CTEStep("business_unit", "SELECT * FROM {role_segments}"),
        CTEStep("final", _premise_key_set_sql(roles, range_separator)),
    ]
    return steps


@pipeline_stage(
    name="parse_out_sub_premise_location",
    description=(
        "Extract sub-premise side/location descriptors before the street address"
    ),
    tags=["token_extraction", "flat_parsing"],
)
def _parse_out_sub_premise_location():
    """Extract sub-premise side/location labels such as LEFT and FRONT.

    This stage keeps sub-premise location evidence separate from
    `flat_identity` so it can be compared independently downstream. The signal
    is intentionally broader than flats alone: maisonettes and other
    sub-premise occupancies can carry the same FRONT/REAR/LEFT/RIGHT cues.
    A premise number usually marks the end of the sub-premise description.
    For unnumbered addresses, use a short prefix to limit place-name matches.
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
            COALESCE(
                list_first(list_filter(
                    range(1, len(__tokens_for_parsing) + 1),
                    lambda idx: regexp_full_match(
                        __tokens_for_parsing[idx], '[0-9]+[A-Z]?(-[0-9]+[A-Z]?)?'
                    ) AND NOT (
                        idx = 2 AND __tokens_for_parsing[1] IN (
                            'FLAT', 'FFLAT', 'MAISONETTE', 'APARTMENT', 'PENTHOUSE',
                            'UNIT', 'UNITS', 'SUITE', 'OFFICE', 'WORKSHOP',
                            'WAREHOUSE', 'STUDIO'
                        )
                    )
                )) - 1,
                GREATEST(6, CAST(CEIL(len(__tokens_for_parsing) / 2.0) AS BIGINT))
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
                has_business_unit
                OR flat_positional IS NOT NULL
                OR flat_letter IS NOT NULL
                OR flat_number IS NOT NULL
                OR list_contains(clean_full_address_tokens, 'FLAT')
                OR list_contains(clean_full_address_tokens, 'FFLAT')
                OR list_contains(clean_full_address_tokens, 'MAISONETTE')
            ) THEN NULL
            WHEN list_contains(sub_premise_location_prefix_tokens, 'RIGHT')
                THEN 'RIGHT'
            WHEN list_contains(sub_premise_location_prefix_tokens, 'LEFT')
                THEN 'LEFT'
            WHEN list_contains(sub_premise_location_prefix_tokens, 'CENTRE')
                OR list_contains(sub_premise_location_prefix_tokens, 'CENTER')
                OR regexp_matches(
                    array_to_string(sub_premise_location_prefix_tokens, ' '),
                    '\b((FLOORS?|F?FLAT|MAISONETTE) MIDDLE|MIDDLE (F?FLAT|MAISONETTE))\b'
                )
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
    range_separator = r"(?:-|TO|THROUGH|UP\s+TO)"
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
        + "|".join(_premise_role_segment_pattern(role, range_separator) for role in roles)
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
        CTEStep("final", _premise_key_set_sql(roles, range_separator)),
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
        r"\b((?:F?FLAT|APARTMENT|UNIT|SUITE|OFFICE|ROOM|BUILDING|BLOCK|PLOT|GARAGE|BAY|"
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
    return """
    SELECT
        * EXCLUDE (sub_premise_identifier),
        coalesce(sub_premise_identifier,
            CASE WHEN sub_premise_role IS NULL THEN numeric_tokens[1] END
        ) AS sub_premise_identifier
    FROM {input}
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


def _premise_key_set_sql(roles: list[str], range_separator: str) -> str:
    identifier_pattern = rf"\b(?:{_premise_identifier_pattern(5)})\b"
    numeric_range_pattern = rf"\b\d{{1,5}}\s*{range_separator}\s*\d{{1,5}}\b"
    role_pattern = "|".join(f"{role}S?" for role in roles)
    map_entries = []
    for role in roles:
        marker = f"{role}S?"
        marker_removal_pattern = rf"\b(?:{marker}|PART)\b"
        map_entries.append(
            f"""struct_pack(key := '{role}', value :=
            list_sort(list_distinct(flatten(list_transform(
                list_filter(__premise_segments, lambda segment: regexp_matches(
                    segment, '^(?:EXCLUDING\\s+)?{marker}\\b'
                )),
                lambda segment: list_concat(
                    regexp_extract_all(
                        regexp_replace(segment, '{marker_removal_pattern}', ' ', 'g'),
                        '{identifier_pattern}', 0
                    ),
                    flatten(list_transform(
                        regexp_extract_all(segment, '{numeric_range_pattern}', 0),
                        lambda numeric_range: list_transform(
                            range(
                                regexp_extract(numeric_range, '^\\d+', 0)::BIGINT + 1,
                                regexp_extract(numeric_range, '\\d+$', 0)::BIGINT
                            ),
                            lambda identifier: identifier::VARCHAR
                        )
                    ))
                )
            )))))"""
        )

    map_entry_sql = ",\n".join(map_entries)
    role_range_pattern = (
        rf"{identifier_pattern}\s*{range_separator}\s*{identifier_pattern}"
    )
    partial_role_pattern = rf"\bPART\s+(?:{role_pattern})\b"
    uncertain_pattern = _scope_uncertainty_pattern(partial_role_pattern)
    return f"""
    SELECT
        * EXCLUDE (__premise_segments, __premise_parse_address),
        map_from_entries(list_filter(
            [{map_entry_sql}], lambda entry: len(struct_extract(entry, 'value')) > 0
        )) AS premise_keys,
        len(map_keys(premise_keys)) > 0 AND (
            regexp_matches(__premise_parse_address, '{uncertain_pattern}')
            OR len(list_filter(__premise_segments, lambda segment:
                regexp_matches(segment, '{role_range_pattern}')
            )) > 0
        ) AS premise_scope_uncertain
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
        * EXCLUDE (__floor_parse_address, __leading_sub_premise_floor),
        list_sort(list_distinct(list_concat(list_transform(
            flatten(list_transform(
                regexp_extract_all(__floor_parse_address, '{floor_list_pattern}', 0),
                lambda segment: regexp_extract_all(segment, '{floor_token_pattern}', 0)
            )),
            lambda floor_name: {floor_normalisation}
        ), [__leading_sub_premise_floor]))) AS floor_keys,
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
            list_transform(distinguishing_adj_start_tokens, lambda token:
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
                lambda position: list_extract(
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
            lambda position: list_extract(distinguishing_adj_start_tokens, position)
        ) AS distinguishing_structural_tokens,
        list_transform(
            distinguishing_adj_start_tokens,
            lambda token, position: struct_pack(
                token := token,
                is_lexical := NOT list_contains(__structural_positions, position)
            )
        ) AS distinguishing_token_parts,
        list_filter(
            distinguishing_adj_start_tokens,
            lambda token, position: NOT list_contains(__structural_positions, position)
        ) AS distinguishing_lexical_tokens
    FROM marked
    """
