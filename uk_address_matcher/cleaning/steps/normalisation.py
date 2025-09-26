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
from uk_address_matcher.sql_pipeline.steps import pipeline_stage


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
        * EXCLUDE (address_concat, postcode),
        UPPER(address_concat) AS address_concat,
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
        "address_concat",
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
    SELECT
        * EXCLUDE (address_concat),
        {fn_call} AS address_concat
    FROM {{input}}
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
        SELECT *, string_split(address_concat, ' ') AS cleaned_tokenised
        FROM {input}
    )
    SELECT
        * EXCLUDE (cleaned_tokenised, address_concat),
        CASE
            WHEN array_length(cleaned_tokenised) >= 2
                 AND cleaned_tokenised[-1] = cleaned_tokenised[-2]
            THEN array_to_string(cleaned_tokenised[:-2], ' ')
            WHEN array_length(cleaned_tokenised) >= 4
                 AND cleaned_tokenised[-4] = cleaned_tokenised[-2]
                 AND cleaned_tokenised[-3] = cleaned_tokenised[-1]
            THEN array_to_string(cleaned_tokenised[:-3], ' ')
            ELSE address_concat
        END AS address_concat
    FROM tokenised
    """
    return sql


@pipeline_stage(
    name="derive_original_address_concat",
    description="Create a backup copy of the cleaned address before further processing",
    tags=["data_preparation", "cleaning"],
)
def _derive_original_address_concat() -> str:
    sql = r"""
    SELECT
        *,
        address_concat AS original_address_concat
    FROM {input}
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
