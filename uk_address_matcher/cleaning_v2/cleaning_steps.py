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
