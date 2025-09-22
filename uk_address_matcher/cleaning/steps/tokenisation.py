from __future__ import annotations

from uk_address_matcher.core.sql_pipeline import CTEStep, Stage


def _split_numeric_tokens_to_cols() -> Stage:
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


def _tokenise_address_without_numbers() -> Stage:
    sql = """
    select
        * exclude (address_without_numbers),
        regexp_split_to_array(trim(address_without_numbers), '\\s+')
            AS address_without_numbers_tokenised
    from {input}
    """
    step = CTEStep("1", sql)
    return Stage(name="tokenise_address_without_numbers", steps=[step])
