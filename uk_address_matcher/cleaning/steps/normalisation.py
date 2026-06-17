from __future__ import annotations

from typing import Final

from uk_address_matcher.cleaning.steps.regexes import (
    construct_nested_call,
    move_flat_to_front,
    remove_apostrophes,
    remove_commas_periods,
    remove_multiple_spaces,
    replace_fwd_slash_with_dash,
    replace_non_numeric_adjacent_underscores,
    separate_letter_num,
    standarise_num_letter,
    trim,
)
from uk_address_matcher.sql_pipeline.helpers import package_resource_read_sql
from uk_address_matcher.sql_pipeline.steps import CTEStep, pipeline_stage


@pipeline_stage(
    name="preserve_original_address_concat",
    description="Preserve the original address string for downstream outputs",
    tags=["setup"],
)
def _preserve_original_address_concat() -> str:
    return """
    SELECT
        *,
        address_concat AS original_address_concat,
    FROM {input}
    """


@pipeline_stage(
    name="extract_postcode_from_address",
    description=(
        "Extract UK postcode from address_concat and remove it from the address string"
    ),
    tags=["setup", "parsing"],
)
def _extract_postcode_from_address() -> str:
    """Extract UK postcode and remove it from the address string.

    This stage handles three scenarios:
    1. If a postcode column exists with a non-empty value, use that postcode
    2. If postcode is empty/NULL, extract postcode from address_concat
    3. Always remove the postcode from address_concat to avoid duplication

    Uses a regex to find valid UK postcodes (including GIR 0AA special case).

    Note: The postcode column must exist before this stage runs. This is ensured
    by _ensure_postcode_column() in pipelines.py at pipeline entry.
    """
    # UK postcode regex: matches standard formats and GIR 0AA
    uk_postcode_regex = r"\b(?:GIR ?0AA|[A-Z][A-HJ-Y]?\d[A-Z\d]? ?\d[A-Z]{2})\b"

    # Extract postcode from address only when one has not been supplied.
    # When a postcode is supplied, strip postcode-like suffixes only. Running
    # the broad regex over the whole address can remove valid fragments such as
    # "D1 2ND" before floor normalisation runs.
    return f"""
    WITH prepared AS (
        SELECT
            *,
            NULLIF(TRIM(CAST(postcode AS VARCHAR)), '') AS __existing_postcode
        FROM {{input}}
    )
    SELECT
        * EXCLUDE (
            postcode,
            address_concat,
            __existing_postcode
        ),
        TRIM(
            CASE
                WHEN __existing_postcode IS NOT NULL THEN regexp_replace(
                    address_concat,
                    '\\s+{uk_postcode_regex}$',
                    '',
                    'gi'
                )
                ELSE regexp_replace(
                    address_concat,
                    '{uk_postcode_regex}',
                    '',
                    'gi'
                )
            END
        ) AS address_concat,
        CASE
            WHEN __existing_postcode IS NOT NULL THEN __existing_postcode
            ELSE NULLIF(
                UPPER(regexp_extract(UPPER(address_concat), '{uk_postcode_regex}')),
                ''
            )
        END AS postcode
    FROM prepared
    """


@pipeline_stage(
    name="rename_and_select_columns",
    description=(
        "Rename and select key columns for downstream processing "
        "and assign ukam_address_id"
    ),
    tags=["setup"],
)
def _rename_and_select_columns() -> str:
    sql = r"""
    SELECT
        unique_id,
        original_address_concat,
        address_concat,
        postcode,
        ukam_address_id,
        * EXCLUDE (
            unique_id,
            original_address_concat,
            address_concat,
            postcode,
            ukam_address_id
        )
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
        * EXCLUDE (address_concat, postcode),
        TRIM(address_concat) AS address_concat,
        TRIM(postcode)       AS postcode
    FROM {input}
    """
    return sql


@pipeline_stage(
    name="canonicalise_postcode",
    description=(
        "Standardise UK postcodes by ensuring single space "
        "between outward and inward codes"
    ),
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
    description=(
        "Convert address and postcode fields to uppercase for consistent formatting"
    ),
    tags=["normalisation", "formatting"],
)
def _upper_case_address_and_postcode() -> str:
    sql = r"""
    SELECT
        * EXCLUDE (postcode, address_concat),
        UPPER(address_concat) AS clean_full_address,
        UPPER(postcode)       AS postcode
    FROM {input}
    """
    return sql


@pipeline_stage(
    name="clean_address_string_first_pass",
    description=(
        "Apply initial address cleaning operations: remove punctuation, "
        "standardise separators, and normalise formatting"
    ),
    tags=["cleaning", "normalisation"],
)
def _clean_address_string_first_pass() -> str:
    fn_call = construct_nested_call(
        "clean_full_address",
        [
            remove_commas_periods,
            remove_apostrophes,
            replace_non_numeric_adjacent_underscores,
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
    name="strip_country_suffix",
    description=(
        "Strip trailing country/high-level denomination suffixes from clean_full_address"
    ),
    tags=["cleaning", "normalisation"],
)
def _strip_country_suffix() -> str:
    suffix_regex = (
        r"(?:\s+(?:UNITED KINGDOM|GREAT BRITAIN|NORTHERN IRELAND|"
        r"UK|BRITAIN|ENGLAND|SCOTLAND|WALES))+$"
    )
    sql = f"""
    SELECT
        * EXCLUDE (clean_full_address),
        TRIM(
            regexp_replace(
                ' ' || TRIM(clean_full_address),
                '{suffix_regex}',
                ''
            )
        ) AS clean_full_address
    FROM {{input}}
    """
    return sql


@pipeline_stage(
    name="remove_duplicate_end_tokens",
    description=(
        "Remove duplicated tokens at the end of addresses "
        "(e.g. 'HIGH STREET ST ALBANS ST ALBANS' -> 'HIGH STREET ST ALBANS')"
    ),
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
    description=(
        "Apply final cleaning operations to address without numbers: "
        "remove extra spaces and trim"
    ),
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
    description=(
        "Normalise address abbreviations (RD->ROAD) and "
        "unit types using a vectorised map lookup"
    ),
    tags=["normalisation", "cleaning"],
)
def _normalise_abbreviations_and_units() -> list[CTEStep]:
    """Normalise address abbreviations (RD->ROAD).

    Also normalise unit types using a vectorised map lookup.

    - 1. Load lookup (upper-case keys for case-insensitive match), extended with
         generated compact-floor rows so the JSON file only carries genuinely
         arbitrary abbreviations.
    - 2. Build a single-row MAP (hashmap) using list aggregations
    - 3. Vectorised transform over token list, then join back to a string
    """

    read_abbr_sql = package_resource_read_sql(
        "uk_address_matcher.data", "address_abbreviations.json"
    )

    # Compact-floor tokens are a regular cartesian product: floor code x suffix.
    # Forward slashes are already normalised to dashes by an earlier cleaning
    # pass, so only dash forms need expanding here.
    floor_names = [
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
    ]
    floor_codes = ["G"] + [str(i) for i in range(1, len(floor_names))]
    compact_floor_rows: list[tuple[str, str]] = []
    for code, name in zip(floor_codes, floor_names):
        compact_floor_rows.append((f"{code}FR", f"{name} FLOOR"))
        compact_floor_rows.append((f"{code}F-R", f"{name} FLOOR RIGHT"))
        compact_floor_rows.append((f"{code}F-L", f"{name} FLOOR LEFT"))
        compact_floor_rows.append((f"{code}R-R", f"{name} FLOOR RIGHT"))
        compact_floor_rows.append((f"{code}L-L", f"{name} FLOOR LEFT"))
    compact_floor_values = ", ".join(
        f"('{token}', '{replacement}')" for token, replacement in compact_floor_rows
    )

    # 1) Load lookup (upper-case keys for case-insensitive match)
    abbr_lookup_sql = f"""
    SELECT
        UPPER(TRIM(token))       AS token,
        TRIM(replacement)        AS replacement
    FROM ({read_abbr_sql})
    WHERE token IS NOT NULL AND replacement IS NOT NULL
    UNION ALL
    SELECT token, replacement
    FROM (VALUES {compact_floor_values}) AS compact_floors(token, replacement)
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
    name="join_excluding_with_next_token",
    description=(
        "Join EXCLUDING with the following token to avoid false basement/studio "
        "matches during tokenisation"
    ),
    tags=["normalisation", "cleaning"],
)
def _join_excluding_with_next_token() -> str:
    sql = r"""
    SELECT
        * EXCLUDE (clean_full_address),
        regexp_replace(
            clean_full_address,
            '(^|[ (])EXCLUDING +([^ )]+)',
            '\1EXCLUDING\2',
            'g'
        ) AS clean_full_address
    FROM {input}
    """
    return sql
