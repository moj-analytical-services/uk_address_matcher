from __future__ import annotations

from uk_address_matcher.sql_pipeline.steps import pipeline_stage


# TODO(ThomasHepworth): Do we want this as a separate stage?
@pipeline_stage(
    name="create_tokenised_address_concat",
    description="Combine our cleaned address into an array of tokens to be used for exact matching and token-level matching",
    tags="tokenisation",
)
def _create_tokenised_address_concat():
    sql = """
    SELECT
        *,
       string_split(clean_full_address, ' ') AS address_tokens
    FROM {input}
    """
    return sql


@pipeline_stage(
    name="split_numeric_tokens_to_cols",
    description="Split numeric tokens array into separate columns (numeric_token_1, numeric_token_2, numeric_token_3)",
    tags="tokenisation",
)
def _split_numeric_tokens_to_cols():
    sql = """
    SELECT
        *,
        regexp_extract_all(array_to_string(numeric_tokens, ' '), '\\d+')[1] as numeric_token_1,
        regexp_extract_all(array_to_string(numeric_tokens, ' '), '\\d+')[2] as numeric_token_2,
        regexp_extract_all(array_to_string(numeric_tokens, ' '), '\\d+')[3] as numeric_token_3
    FROM {input}
    """
    return sql


@pipeline_stage(
    name="tokenise_address_without_numbers",
    description="Split the address_without_numbers field into an array of tokens",
    tags="tokenisation",
)
def _tokenise_address_without_numbers():
    sql = """
    select
        * exclude (address_without_numbers),
        regexp_split_to_array(trim(address_without_numbers), '\\s+')
            AS address_without_numbers_tokenised
    from {input}
    """
    return sql
