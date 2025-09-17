from uk_address_matcher.cleaning.regexes import (
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
from uk_address_matcher.cleaning_v2.pipeline import (
    CTEStep,
    Stage,
)


def trim_whitespace_address_and_postcode() -> Stage:
    sql = """
    select
        * exclude (address_concat, postcode),
        trim(address_concat) as address_concat,
        trim(postcode) as postcode
    from {input}
    """
    step = CTEStep("1", sql)
    return Stage(
        name="trim_whitespace_address_and_postcode",
        steps=[step],
    )


def canonicalise_postcode() -> Stage:
    uk_postcode_regex = r"^([A-Z]{1,2}\d[A-Z\d]?|GIR)\s*(\d[A-Z]{2})$"
    sql = f"""
    select
        * exclude (postcode),
        regexp_replace(
            postcode,
            '{uk_postcode_regex}',
            '\\1 \\2'
        ) as postcode
    from {{input}}
    """
    step = CTEStep("1", sql)
    return Stage(name="canonicalise_postcode", steps=[step])


def upper_case_address_and_postcode() -> Stage:
    sql = """
    select
        * exclude (address_concat, postcode),
        upper(address_concat) as address_concat,
        upper(postcode) as postcode
    from {input}
    """
    step = CTEStep("1", sql)
    return Stage(name="upper_case_address_and_postcode", steps=[step])


def clean_address_string_first_pass() -> Stage:
    fn_call = construct_nested_call(
        "address_concat",
        [
            remove_commas_periods,
            remove_apostrophes,
            remove_multiple_spaces,
            replace_fwd_slash_with_dash,
            separate_letter_num,
            standarise_num_letter,
            move_flat_to_front,
            trim,
        ],
    )
    sql = f"""
    select
        * exclude (address_concat),
        {fn_call} as address_concat
    from {{input}}
    """
    step = CTEStep("1", sql)
    return Stage(name="clean_address_string_first_pass", steps=[step])


def remove_duplicate_end_tokens() -> Stage:
    sql = """
    with tokenised as (
    select *, string_split(address_concat, ' ') as cleaned_tokenised
    from {input}
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
    step = CTEStep("1", sql)
    return Stage(name="remove_duplicate_end_tokens", steps=[step])


def derive_original_address_concat() -> Stage:
    sql = """
    SELECT
        *,
        address_concat AS original_address_concat
    FROM {input}
    """
    step = CTEStep("1", sql)
    return Stage(name="derive_original_address_concat", steps=[step])


def parse_out_flat_position_and_letter() -> Stage:
    floor_positions = r"\b(BASEMENT|GROUND FLOOR|FIRST FLOOR|SECOND FLOOR|THIRD FLOOR|TOP FLOOR|GARDEN)\b"
    flat_letter = r"\b\d{0,4}([A-Za-z])\b"
    leading_letter = r"^\s*\d+([A-Za-z])\b"

    flat_number = r"\b(FLAT|UNIT|APARTMENT)\s+(\S*\d\S*)\s+\S*\d\S*\b"
    sql = f"""
    WITH step1 AS (
        SELECT
            *,
            regexp_extract(address_concat, '{floor_positions}', 1) as floor_pos,
            regexp_extract(address_concat, '{flat_letter}', 1) as flat_letter,
            regexp_extract(address_concat, '{leading_letter}', 1) as leading_letter,
            regexp_extract(address_concat, '{flat_number}', 1) as flat_number
        FROM {{input}}
    )
    SELECT
        * EXCLUDE (floor_pos, flat_letter, leading_letter, flat_number),
        NULLIF(floor_pos, '') as flat_positional,
        NULLIF(COALESCE(
                NULLIF(flat_letter, ''),
                NULLIF(leading_letter, ''),
                CASE
                    WHEN LENGTH(flat_number) <= 4 THEN flat_number
                    ELSE NULL
                END
            ), '') as flat_letter
    FROM step1
    """
    step = CTEStep("1", sql)
    return Stage(name="parse_out_flat_position_and_letter", steps=[step])


def parse_out_numbers() -> Stage:
    regex_pattern = (
        r"\b"  # Word boundary
        # Prioritize matching number ranges first
        r"(\d{1,5}-\d{1,5}|[A-Za-z]?\d{1,5}[A-Za-z]?)"
        r"\b"  # Word boundary
    )
    sql = f"""
    SELECT
        * EXCLUDE (address_concat),
        regexp_replace(address_concat, '{regex_pattern}', '', 'g') AS address_without_numbers,
        CASE
            WHEN flat_letter IS NOT NULL AND flat_letter ~ '^\\d+$' THEN
            regexp_extract_all(address_concat, '{regex_pattern}')[2:]
            ELSE
                regexp_extract_all(address_concat, '{regex_pattern}')
        END AS numeric_tokens
    FROM {{input}}
    """
    step = CTEStep("1", sql)
    return Stage(name="parse_out_numbers", steps=[step])


def clean_address_string_second_pass() -> Stage:
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
    step = CTEStep("1", sql)
    return Stage(name="clean_address_string_second_pass", steps=[step])


def split_numeric_tokens_to_cols() -> Stage:
    sql = """
    SELECT
        * EXCLUDE (numeric_tokens),
        regexp_extract_all(array_to_string(numeric_tokens, ' '), '\\d+')[1] as numeric_token_1,
        regexp_extract_all(array_to_string(numeric_tokens, ' '), '\\d+')[2] as numeric_token_2,
        regexp_extract_all(array_to_string(numeric_tokens, ' '), '\\d+')[3] as numeric_token_3
    FROM {input}
    """
    step = CTEStep("1", sql)
    return Stage(name="split_numeric_tokens_to_cols", steps=[step])


def tokenise_address_without_numbers() -> Stage:
    sql = """
    select
        * exclude (address_without_numbers),
        regexp_split_to_array(trim(address_without_numbers), '\\s+')
            AS address_without_numbers_tokenised
    from {input}
    """
    step = CTEStep("1", sql)
    return Stage(name="tokenise_address_without_numbers", steps=[step])
