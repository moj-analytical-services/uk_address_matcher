from __future__ import annotations

import importlib.resources as pkg_resources
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
        unique_id,
        address_concat as original_address_concat,
        postcode,
        ukam_address_id,
        * EXCLUDE (unique_id, address_concat, postcode, ukam_address_id)
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
        * EXCLUDE (original_address_concat, postcode),
        UPPER(original_address_concat) AS original_address_concat,
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
        "original_address_concat",
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
        * EXCLUDE (__clean_address, original_address_concat),
        __clean_address AS original_address_concat,
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

    with pkg_resources.path(
        "uk_address_matcher.data", "address_abbreviations.json"
    ) as json_path:
        # 1) Load lookup (upper-case keys for case-insensitive match)
        abbr_lookup_sql = f"""
        SELECT
          UPPER(TRIM(token))       AS token,
          TRIM(replacement)        AS replacement
        FROM read_json_auto('{json_path}')
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
