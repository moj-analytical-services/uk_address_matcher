from __future__ import annotations

from typing import Final

from uk_address_matcher.cleaning.steps.regexes import (
    construct_nested_call,
    move_flat_to_front,
    remove_apostrophes,
    remove_commas_periods,
    remove_multiple_spaces,
    replace_fwd_slash_with_dash,
    separate_letter_num,
    standarise_num_letter,
    trim,
)
from uk_address_matcher.sql_pipeline.helpers import package_resource_read_sql
from uk_address_matcher.sql_pipeline.steps import CTEStep, pipeline_stage


@pipeline_stage(
    name="ensure_ukam_address_id",
    description="Assign a unique UUID to each row for safe joining without duplicates",
    tags=["setup"],
)
def _add_ukam_address_id():
    return """
    SELECT
        *,
        uuid() AS ukam_address_id
    FROM {input}
    """


@pipeline_stage(
    name="rename_and_select_columns",
    description="Rename and select key columns for downstream processing and assign ukam_address_id",
    tags=["setup"],
)
def _rename_and_select_columns() -> str:
    sql = r"""
    SELECT
        address_concat as original_address_concat,
        postcode,
        ukam_address_id,
        * EXCLUDE (address_concat, postcode, ukam_address_id)
    FROM {input}
    """
    return sql


@pipeline_stage(
    name="trim_whitespace_address_and_postcode",
    description="Remove leading and trailing whitespace from address and postcode fields",
    tags=["normalisation", "cleaning"],
)
def _trim_whitespace_address_and_postcode() -> str:
    sql = r"""
    SELECT
        * EXCLUDE (original_address_concat, postcode),
        TRIM(original_address_concat) AS original_address_concat,
        TRIM(postcode)       AS postcode
    FROM {input}
    """
    return sql


@pipeline_stage(
    name="canonicalise_postcode",
    description="Standardise UK postcodes by ensuring single space between outward and inward codes",
    tags=["normalisation", "cleaning"],
)
def _canonicalise_postcode() -> str:
    """
    Ensures that any postcode matching the UK format has a single space
    separating the outward and inward codes. Assumes 'postcode' is trimmed and uppercased.
    """
    uk_postcode_regex: Final[str] = r"^([A-Z]{1,2}\d[A-Z\d]?|GIR)\s*(\d[A-Z]{2})$"
    sql = f"""
    SELECT
        * EXCLUDE (postcode),
        regexp_replace(
            postcode,
            '{uk_postcode_regex}',
            '\\1 \\2'
        ) AS postcode
    FROM {{input}}
    """
    return sql


@pipeline_stage(
    name="upper_case_address_and_postcode",
    description="Convert address and postcode fields to uppercase for consistent formatting",
    tags=["normalisation", "formatting"],
)
def _upper_case_address_and_postcode() -> str:
    sql = r"""
    SELECT
        * EXCLUDE (postcode),
        UPPER(original_address_concat) AS clean_full_address,
        UPPER(postcode)       AS postcode
    FROM {input}
    """
    return sql


@pipeline_stage(
    name="clean_address_string_first_pass",
    description="Apply initial address cleaning operations: remove punctuation, standardise separators, and normalise formatting",
    tags=["cleaning", "normalisation"],
)
def _clean_address_string_first_pass() -> str:
    fn_call = construct_nested_call(
        "clean_full_address",
        [
            remove_commas_periods,
            remove_apostrophes,
            remove_multiple_spaces,
            replace_fwd_slash_with_dash,
            # standarise_num_dash_num,  # left commented as in original
            separate_letter_num,
            standarise_num_letter,
            move_flat_to_front,
            # remove_repeated_tokens,   # left commented as in original
            trim,
        ],
    )
    sql = f"""
    WITH cleaned AS (
        SELECT
            *,
            {fn_call} AS __clean_address
        FROM {{input}}
    )
    SELECT
        * EXCLUDE (__clean_address, clean_full_address),
        __clean_address AS clean_full_address
    FROM cleaned
    """
    return sql


@pipeline_stage(
    name="remove_duplicate_end_tokens",
    description="Remove duplicated tokens at the end of addresses (e.g. 'HIGH STREET ST ALBANS ST ALBANS' -> 'HIGH STREET ST ALBANS')",
    tags=["cleaning"],
)
def _remove_duplicate_end_tokens() -> str:
    """
    Removes duplicated tokens at the end of the address.
    E.g. 'HIGH STREET ST ALBANS ST ALBANS' -> 'HIGH STREET ST ALBANS'
    """
    sql = r"""
    WITH tokenised AS (
        SELECT *, string_split(clean_full_address, ' ') AS cleaned_tokenised
        FROM {input}
    )
    SELECT
        * EXCLUDE (cleaned_tokenised, clean_full_address),
        CASE
            WHEN array_length(cleaned_tokenised) >= 2
                 AND cleaned_tokenised[-1] = cleaned_tokenised[-2]
            THEN array_to_string(cleaned_tokenised[:-2], ' ')
            WHEN array_length(cleaned_tokenised) >= 4
                 AND cleaned_tokenised[-4] = cleaned_tokenised[-2]
                 AND cleaned_tokenised[-3] = cleaned_tokenised[-1]
            THEN array_to_string(cleaned_tokenised[:-3], ' ')
            ELSE clean_full_address
        END AS clean_full_address
    FROM tokenised
    """
    return sql


@pipeline_stage(
    name="clean_address_string_second_pass",
    description="Apply final cleaning operations to address without numbers: remove extra spaces and trim",
    tags=["cleaning"],
)
def _clean_address_string_second_pass() -> str:
    fn_call = construct_nested_call(
        "address_without_numbers",
        [remove_multiple_spaces, trim],
    )
    sql = f"""
    SELECT
        * EXCLUDE (address_without_numbers),
        {fn_call} AS address_without_numbers
    FROM {{input}}
    """
    return sql


@pipeline_stage(
    name="normalise_abbreviations_and_units",
    description="Normalise address abbreviations (RD->ROAD) and unit types using a vectorised map lookup",
    tags=["normalisation", "cleaning"],
)
def _normalise_abbreviations_and_units() -> list[CTEStep]:
    """Normalise address abbreviations (RD->ROAD) and unit types using a vectorised map lookup

    - 1. Load lookup (upper-case keys for case-insensitive match)
    - 2. Build a single-row MAP (hashmap) using list aggregations
    - 3. Vectorised transform over token list, then join back to a string
    """

    read_abbr_sql = package_resource_read_sql(
        "uk_address_matcher.data", "address_abbreviations.json"
    )
    # 1) Load lookup (upper-case keys for case-insensitive match)
    abbr_lookup_sql = f"""
    SELECT
        UPPER(TRIM(token))       AS token,
        TRIM(replacement)        AS replacement
    FROM ({read_abbr_sql})
    WHERE token IS NOT NULL AND replacement IS NOT NULL
    """

    # 2) Build a single-row MAP using list aggregations (works on DuckDB without map_agg)
    abbr_map_sql = """
    SELECT map(list(token), list(replacement)) AS abbr_map
    FROM {abbr_lookup}
    """

    # 3) Vectorised transform over token list, then join back to a string
    cleaned_sql = """
    SELECT
      address.* EXCLUDE (clean_full_address),
      array_to_string(
        list_transform(
        string_split(COALESCE(address.clean_full_address, ''), ' '),
        x -> COALESCE(map_extract(m.abbr_map, x)[1], x)
        ),
        ' '
      ) AS clean_full_address
    FROM {input} address
    CROSS JOIN {abbr_map} m
    """

    steps = [
        CTEStep("abbr_lookup", abbr_lookup_sql),
        CTEStep("abbr_map", abbr_map_sql),
        CTEStep("with_cleaned_address", cleaned_sql),
    ]
    return steps


@pipeline_stage(
    name="classify_non_traditional_address",
    description="Classify addresses as non-traditional types (e.g. bus shelters, telephone boxes, substations)",
    tags=["classification"],
)
def _classify_non_traditional_address() -> list[CTEStep]:
    """Classify addresses that represent non-traditional address types.

    Loads classification patterns from a JSON file and scans the address for
    bigrams/trigrams that match known non-traditional address patterns
    (e.g. BUS SHELTER, TELEPHONE BOX, ELECTRICITY SUBSTATION).

    Adds a `non_traditional_address_type` column with the classification or NULL
    if the address appears to be a traditional building address.
    """
    read_non_trad_sql = package_resource_read_sql(
        "uk_address_matcher.data", "non_traditional_address_types.json"
    )
    load_lookup_sql = f"""
    WITH json_data AS (
        {read_non_trad_sql}
    ),
    unpivoted AS (
        UNPIVOT json_data
        ON COLUMNS(*)
        INTO NAME classification VALUE patterns
    )
    SELECT
        UPPER(TRIM(unnest(patterns))) AS pattern,
        LOWER(classification) AS classification
    FROM unpivoted
    WHERE patterns IS NOT NULL
    """

    # Checks if any non-traditional patterns match the address tokens string
    classify_sql = """
    SELECT
        input_data.*,
        (
            SELECT classification
            FROM {non_trad_lookup} lookup
            WHERE array_to_string(input_data.address_tokens, ' ') LIKE '%' || lookup.pattern || '%'
            LIMIT 1
        ) AS non_traditional_address_type
    FROM {input} input_data
    """

    return [
        CTEStep("non_trad_lookup", load_lookup_sql),
        CTEStep("with_classification", classify_sql),
    ]


@pipeline_stage(
    name="peel_common_uk_end_tokens",
    description="Iteratively remove common UK locality tokens (cities, counties, boroughs) from the end of addresses",
    tags=["cleaning", "normalisation"],
)
def _peel_common_uk_end_tokens(fuzzy_threshold: int = 1) -> list[CTEStep]:
    """Peel common UK end tokens using an efficient single-pass approach.

    This uses a streamlined approach:
    1. Pre-compute a lookup table with pattern lengths and typo variants
    2. For each address, try to match end tokens in a single pass using CASE
    3. Repeat for a fixed number of iterations (5 covers 99%+ of cases)

    Pre-computing ensures we get a fuzzy matching solution deletion/transposition variants for O(1) lookup
    """
    if fuzzy_threshold > 1:
        raise ValueError(
            f"fuzzy_threshold={fuzzy_threshold} is not supported. "
            "Maximum supported value is 1. See docs/adr/fuzzy_token_peeling_performance.md"
        )

    # Build lookup with pre-computed typo variants for efficient hash-based JOINs
    # Distance 1: deletions (remove 1 char) + transpositions (swap adjacent chars)
    read_end_tokens_sql = package_resource_read_sql(
        "uk_address_matcher.data", "common_uk_end_tokens.json"
    )
    load_lookup_sql = f"""
        WITH json_data AS (
            {read_end_tokens_sql}
        ),
        single_tokens AS (
            SELECT UPPER(TRIM(unnest(single_tokens))) AS pattern, 1 AS token_count
            FROM json_data
        ),
        multi_tokens AS (
            SELECT
                UPPER(TRIM(unnest(multi_tokens))) AS pattern,
                length(pattern) - length(replace(pattern, ' ', '')) + 1 AS token_count
            FROM json_data
        ),
        all_patterns AS (
            SELECT DISTINCT pattern, token_count
            FROM (
                SELECT * FROM single_tokens
                UNION ALL
                SELECT * FROM multi_tokens
            )
        ),
        -- Generate exact matches (always included)
        exact_keys AS (
            SELECT pattern, pattern AS lookup_key, token_count, 0 AS edit_dist
            FROM all_patterns
        ),
        -- Generate deletion variants (remove one char) for single tokens only
        -- E.g. "LONDON" -> "ONDON", "LNDON", "LODON", "LONON", "LONDN", "LONDO"
        deletion_keys AS (
            SELECT
                pattern,
                substr(pattern, 1, i - 1) || substr(pattern, i + 1) AS lookup_key,
                token_count,
                1 AS edit_dist
            FROM all_patterns, generate_series(1, length(pattern)) AS t(i)
            WHERE token_count = 1
              AND length(pattern) >= 5  -- Deletion gives 4+ char key
              AND {fuzzy_enabled}
        ),
        -- Generate transposition variants (swap adjacent chars)
        -- E.g. "LONDON" -> "OLNDON", "LNODON", "LODNON", "LONODN", "LONDNO"
        transposition_keys AS (
            SELECT
                pattern,
                substr(pattern, 1, i - 1) ||
                substr(pattern, i + 1, 1) ||
                substr(pattern, i, 1) ||
                substr(pattern, i + 2) AS lookup_key,
                token_count,
                1 AS edit_dist
            FROM all_patterns, generate_series(1, length(pattern) - 1) AS t(i)
            WHERE token_count = 1
              AND length(pattern) >= 4
              AND {fuzzy_enabled}
        ),
        -- Combine all keys, excluding fuzzy keys that collide with exact patterns
        all_keys AS (
            SELECT * FROM exact_keys
            UNION ALL
            SELECT * FROM deletion_keys
                WHERE lookup_key NOT IN (SELECT pattern FROM all_patterns)
            UNION ALL
            SELECT * FROM transposition_keys
                WHERE lookup_key NOT IN (SELECT pattern FROM all_patterns)
        )
        -- Deduplicate, preferring exact matches (lower edit_dist)
        SELECT DISTINCT ON (lookup_key)
            pattern,
            lookup_key,
            token_count
        FROM all_keys
        ORDER BY lookup_key, edit_dist
        """.format(fuzzy_enabled="TRUE" if fuzzy_threshold >= 1 else "FALSE")

    # Tokenise input
    tokenise_sql = """
    SELECT
        *,
        string_split(clean_full_address, ' ') AS __tokens,
        CAST([] AS VARCHAR[]) AS __peeled
    FROM {input}
    """

    # Build a single peel iteration using JOIN-based lookup
    # Both exact and fuzzy matching use the same JOIN approach - fuzzy just has more keys
    def make_peel_sql(prev_cte: str, iter_num: int) -> str:
        # Both exact and fuzzy use the same JOIN-based approach now
        # The difference is in the lookup table (fuzzy has pre-generated typo variants)
        return f"""
        WITH __with_ends AS (
            SELECT
                *,
                len(__tokens) AS __n,
                -- Extract potential end tokens
                CASE WHEN len(__tokens) >= 1 THEN __tokens[len(__tokens)] ELSE NULL END AS end1,
                CASE WHEN len(__tokens) >= 2
                     THEN array_to_string(list_slice(__tokens, len(__tokens) - 1, len(__tokens)), ' ')
                     ELSE NULL END AS end2,
                CASE WHEN len(__tokens) >= 3
                     THEN array_to_string(list_slice(__tokens, len(__tokens) - 2, len(__tokens)), ' ')
                     ELSE NULL END AS end3
            FROM {{{prev_cte}}}
        ),
        __matched AS (
            SELECT
                e.*,
                l3.pattern AS match3,
                l2.pattern AS match2,
                l1.pattern AS match1
            FROM __with_ends e
            -- Multi-token patterns still use exact match (lookup_key = pattern for these)
            LEFT JOIN {{uk_end_tokens_lookup}} l3 ON l3.token_count = 3 AND l3.lookup_key = e.end3
            LEFT JOIN {{uk_end_tokens_lookup}} l2 ON l2.token_count = 2 AND l2.lookup_key = e.end2
            -- Single-token uses lookup_key which includes fuzzy variants when fuzzy_threshold > 0
            LEFT JOIN {{uk_end_tokens_lookup}} l1 ON l1.token_count = 1 AND l1.lookup_key = e.end1
        )
        SELECT
            * EXCLUDE (__n, end1, end2, end3, match1, match2, match3, __tokens, __peeled),
            -- Apply the best match found (prefer longer patterns)
            CASE
                WHEN match3 IS NOT NULL THEN list_slice(__tokens, 1, __n - 3)
                WHEN match2 IS NOT NULL THEN list_slice(__tokens, 1, __n - 2)
                WHEN match1 IS NOT NULL THEN list_slice(__tokens, 1, __n - 1)
                ELSE __tokens
            END AS __tokens,
            CASE
                WHEN match3 IS NOT NULL THEN list_prepend(match3, __peeled)
                WHEN match2 IS NOT NULL THEN list_prepend(match2, __peeled)
                WHEN match1 IS NOT NULL THEN list_prepend(match1, __peeled)
                ELSE __peeled
            END AS __peeled
        FROM __matched
        """

    # Final cleanup
    final_sql = """
    SELECT
        * EXCLUDE (__tokens, __peeled),
        __peeled AS peeled_tokens_list
    FROM {peel_4}
    """

    # Build all CTEs - only 7 total (lookup + tokenise + 5 iterations)
    steps = [
        CTEStep("uk_end_tokens_lookup", load_lookup_sql),
        CTEStep("tokenised", tokenise_sql),
    ]

    prev = "tokenised"
    for i in range(5):
        steps.append(CTEStep(f"peel_{i}", make_peel_sql(prev, i)))
        prev = f"peel_{i}"

    steps.append(CTEStep("with_peeled_tokens", final_sql))
    return steps
