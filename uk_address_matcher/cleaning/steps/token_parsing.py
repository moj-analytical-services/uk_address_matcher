from __future__ import annotations

from uk_address_matcher.cleaning.steps.regexes import (
    construct_nested_call,
    remove_multiple_spaces,
    trim,
)
from uk_address_matcher.sql_pipeline.steps import CTEStep, Stage


def _separate_distinguishing_start_tokens_from_with_respect_to_adjacent_records() -> (
    Stage
):
    """
    Identifies common suffixes between addresses and separates them into unique and common parts.
    This function analyzes each address in relation to its neighbors (previous and next addresses
    when sorted by unique_id) to find common suffix patterns. It then splits each address into:
    - unique_tokens: The tokens that are unique to this address (typically the beginning part)
    - common_tokens: The tokens that are shared with neighboring addresses (typically the end part)
    Args:
        ddb_pyrel (DuckDBPyRelation): The input relation
        con (DuckDBPyConnection): The DuckDB connection
    Returns:
        DuckDBPyRelation: The modified table with unique_tokens and common_tokens fields
    """
    tokens_sql = """
    SELECT
        ['FLAT', 'APARTMENT', 'UNIT'] AS __tokens_to_remove,
        list_filter(
            regexp_split_to_array(address_concat, '\\s+'),
            x -> NOT list_contains(__tokens_to_remove, x)
        ) AS __tokens,
        row_number() OVER (ORDER BY reverse(address_concat)) AS row_order,
        *
    FROM {input}
    """

    neighbors_sql = """
    SELECT
        lag(__tokens) OVER (ORDER BY row_order) AS __prev_tokens,
        lead(__tokens) OVER (ORDER BY row_order) AS __next_tokens,
        *
    FROM {tokens}
    """

    suffix_lengths_sql = """
    SELECT
        len(__tokens) AS __token_count,
        CASE WHEN __prev_tokens IS NOT NULL THEN
            (
                SELECT max(i)
                FROM range(0, least(len(__tokens), len(__prev_tokens))) AS t(i)
                WHERE list_slice(list_reverse(__tokens), 1, i + 1) =
                    list_slice(list_reverse(__prev_tokens), 1, i + 1)
            )
        ELSE 0 END AS prev_common_suffix,
        CASE WHEN __next_tokens IS NOT NULL THEN
            (
                SELECT max(i)
                FROM range(0, least(len(__tokens), len(__next_tokens))) AS t(i)
                WHERE list_slice(list_reverse(__tokens), 1, i + 1) =
                    list_slice(list_reverse(__next_tokens), 1, i + 1)
            )
        ELSE 0 END AS next_common_suffix,
        *
    FROM {with_neighbors}
    """

    unique_parts_sql = """
    SELECT
        *,
        greatest(prev_common_suffix, next_common_suffix) AS max_common_suffix,
        list_filter(
            __tokens,
            (token, i) -> i < __token_count - greatest(prev_common_suffix, next_common_suffix)
        ) AS unique_tokens,
        list_filter(
            __tokens,
            (token, i) -> i >= __token_count - greatest(prev_common_suffix, next_common_suffix)
        ) AS common_tokens
    FROM {with_suffix_lengths}
    """

    final_sql = """
    SELECT
        * EXCLUDE (
            __tokens,
            __prev_tokens,
            __next_tokens,
            __token_count,
            __tokens_to_remove,
            max_common_suffix,
            next_common_suffix,
            prev_common_suffix,
            row_order,
            common_tokens,
            unique_tokens
        ),
        COALESCE(unique_tokens, ARRAY[]) AS distinguishing_adj_start_tokens,
        COALESCE(common_tokens, ARRAY[]) AS common_adj_start_tokens
    FROM {with_unique_parts}
    """

    steps = [
        CTEStep("tokens", tokens_sql),
        CTEStep("with_neighbors", neighbors_sql),
        CTEStep("with_suffix_lengths", suffix_lengths_sql),
        CTEStep("with_unique_parts", unique_parts_sql),
        CTEStep("final", final_sql),
    ]

    return Stage(
        name="separate_distinguishing_start_tokens_from_with_respect_to_adjacent_recrods",
        steps=steps,
        output="final",
    )


def _parse_out_flat_position_and_letter() -> Stage:
    """
    Extracts flat positions and letters from address strings into separate columns.


    Args:
        ddb_pyrel: The input relation
        con: The DuckDB connection

    Returns:
        DuckDBPyRelation: The modified table with flat_positional and flat_letter fields
    """
    floor_positions = r"\b(BASEMENT|GROUND FLOOR|FIRST FLOOR|SECOND FLOOR|THIRD FLOOR|TOP FLOOR|GARDEN)\b"
    flat_letter = r"\b\d{0,4}([A-Za-z])\b"
    leading_letter = r"^\s*\d+([A-Za-z])\b"

    flat_number = r"\b(FLAT|UNIT|APARTMENT)\s+(\S*\d\S*)\s+\S*\d\S*\b"

    extract_sql = f"""
    SELECT
        *,
        regexp_extract(address_concat, '{floor_positions}', 1) as floor_pos,
        regexp_extract(address_concat, '{flat_letter}', 1) as flat_letter,
        regexp_extract(address_concat, '{leading_letter}', 1) as leading_letter,
        regexp_extract(address_concat, '{flat_number}', 1) as flat_number
    FROM {{input}}
    """

    final_sql = """
    SELECT
        * EXCLUDE (floor_pos, flat_letter, leading_letter, flat_number),
        NULLIF(floor_pos, '') as flat_positional,
        NULLIF(
            COALESCE(
                NULLIF(flat_letter, ''),
                NULLIF(leading_letter, ''),
                CASE
                    WHEN LENGTH(flat_number) <= 4 THEN flat_number
                    ELSE NULL
                END
            ),
            ''
        ) as flat_letter
    FROM {extract_step}
    """

    steps = [
        CTEStep("extract_step", extract_sql),
        CTEStep("final", final_sql),
    ]

    return Stage(name="parse_out_flat_position_and_letter", steps=steps, output="final")


def _parse_out_numbers() -> Stage:
    """
    Extracts and processes numeric tokens from address strings, ensuring the max length
    of the number+letter is 6 with no more than 1 letter which can be at the start or end.
    It also captures ranges like '1-2', '12-17', '98-102' as a single 'number', and
    matches patterns like '20A', 'A20', '20', and '20-21'.

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


def _clean_address_string_second_pass() -> Stage:
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


GENERALISED_TOKEN_ALIASES_CASE_STATEMENT = """
    CASE
        WHEN token in ('FIRST', 'SECOND', 'THIRD', 'TOP') THEN ['UPPERFLOOR', 'LEVEL']
        WHEN token in ('GARDEN', 'GROUND') THEN ['GROUNDFLOOR', 'LEVEL']
        WHEN token in ('BASEMENT') THEN ['LEVEL']
        ELSE [TOKEN]
    END

"""


def _generalised_token_aliases() -> Stage:
    """
    Maps specific tokens to more general categories to create a generalised representation
    of the unique tokens in an address.

    The idea is to guide matches away from implausible matches and towards
    possible matches

    The real tokens always take precidence over genearlised

    For example sometimes a 2nd floor flat will match to top floor.  Whilst 'top floor'
    is often ambiguous (is the 2nd floor the top floor), we know that
    'top floor' cannot match to 'ground' or 'basement'

    This function applies the following mappings:

    [FIRST, SECOND, THIRD, TOP] -> [UPPERFLOOR, LEVEL]

    [GARDEN, GROUND] -> [GROUNDFLOOR, LEVEL]


    This function applies the following mappings:
    - Single letters (A-E) -> UNIT_NUM_LET
    - Single digits (1-5) -> UNIT_NUM_LET
    - Floor indicators (FIRST, SECOND, THIRD) -> LEVEL
    - Position indicators (TOP, FIRST, SECOND, THIRD) -> TOP
    The following tokens are filtered out completely:
    - FLAT, APARTMENT, UNIT
    Args:
        ddb_pyrel (DuckDBPyRelation): The input relation with unique_tokens field
        con (DuckDBPyConnection): The DuckDB connection
    Returns:
        DuckDBPyRelation: The modified table with generalised_unique_tokens field
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
    step = CTEStep("1", sql)
    return Stage(name="generalised_token_aliases", steps=[step])


def _get_token_frequeny_table() -> Stage:
    """Moved to term_frequencies module; kept for backward-compat imports."""
    raise NotImplementedError(
        "Use uk_address_matcher.cleaning.steps.term_frequencies.get_token_frequeny_table instead"
    )
