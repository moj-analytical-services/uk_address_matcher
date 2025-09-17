import importlib.resources as pkg_resources


from uk_address_matcher.cleaning.regexes import (
    construct_nested_call,
    move_flat_to_front,
    remove_apostrophes,
    remove_commas_periods,
    remove_multiple_spaces,
    # remove_repeated_tokens, # TODO: Restore functionality if needed
    replace_fwd_slash_with_dash,
    separate_letter_num,
    # standarise_num_dash_num, # TODO: Restore functionality if needed
    standarise_num_letter,
    trim,
)
from uk_address_matcher.cleaning.pipeline import (
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
    """
    Ensures that any postcode matching the UK format has a single space
    separating the outward and inward codes. It assumes the postcode has
    already been trimmed and converted to uppercase.

    Args:
        ddb_pyrel (DuckDBPyRelation): The input relation, expected to have an
                                      uppercase 'postcode' column.
        con (DuckDBPyConnection): The DuckDB connection.

    Returns:
        DuckDBPyRelation: Relation with the 'postcode' column canonicalised.
    """
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
            # standarise_num_dash_num,
            separate_letter_num,
            standarise_num_letter,
            move_flat_to_front,
            # remove_repeated_tokens,
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
    """
    Removes duplicated tokens at the end of the address.
    E.g. 'HIGH STREET ST ALBANS ST ALBANS' -> 'HIGH STREET ST ALBANS'
    """
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


def separate_distinguishing_start_tokens_from_with_respect_to_adjacent_recrods() -> (
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


def parse_out_flat_position_and_letter() -> Stage:
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


def parse_out_numbers() -> Stage:
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


def add_term_frequencies_to_address_tokens() -> Stage:
    """Compute token-level relative frequencies and attach them to each record."""

    base_sql = """
    SELECT * FROM {input}
    """

    rel_tok_freq_cte_sql = """
    SELECT
        token,
        count(*) / sum(count(*)) OVER () AS rel_freq
    FROM (
        SELECT
            unnest(address_without_numbers_tokenised) AS token
        FROM {base}
    )
    GROUP BY token
    """

    addresses_exploded_sql = """
    SELECT
        unique_id,
        unnest(address_without_numbers_tokenised) AS token,
        generate_subscripts(address_without_numbers_tokenised, 1) AS token_order
    FROM {base}
    """

    address_groups_sql = """
    SELECT
        {addresses_exploded}.*,
        COALESCE({rel_tok_freq_cte}.rel_freq, 5e-5) AS rel_freq
    FROM {addresses_exploded}
    LEFT JOIN {rel_tok_freq_cte}
        ON {addresses_exploded}.token = {rel_tok_freq_cte}.token
    """

    token_freq_lookup_sql = """
    SELECT
        unique_id,
        list_transform(
            list_zip(
                array_agg(token ORDER BY unique_id, token_order ASC),
                array_agg(rel_freq ORDER BY unique_id, token_order ASC)
            ),
            x -> struct_pack(tok := x[1], rel_freq := x[2])
        ) AS token_rel_freq_arr
    FROM {address_groups}
    GROUP BY unique_id
    """

    final_sql = """
    SELECT
        base.* EXCLUDE (address_without_numbers_tokenised),
        lookup.token_rel_freq_arr
    FROM {base} AS base
    INNER JOIN {token_freq_lookup} AS lookup
        ON base.unique_id = lookup.unique_id
    """

    steps = [
        CTEStep("base", base_sql),
        CTEStep("rel_tok_freq_cte", rel_tok_freq_cte_sql),
        CTEStep("addresses_exploded", addresses_exploded_sql),
        CTEStep("address_groups", address_groups_sql),
        CTEStep("token_freq_lookup", token_freq_lookup_sql),
        CTEStep("final", final_sql),
    ]

    return Stage(
        name="add_term_frequencies_to_address_tokens", steps=steps, output="final"
    )


def add_term_frequencies_to_address_tokens_using_registered_df() -> Stage:
    """Attach precomputed token frequencies registered as rel_tok_freq."""

    base_sql = """
    SELECT * FROM {input}
    """

    addresses_exploded_sql = """
    SELECT
        unique_id,
        unnest(address_without_numbers_tokenised) AS token,
        generate_subscripts(address_without_numbers_tokenised, 1) AS token_order
    FROM {base}
    """

    address_groups_sql = """
    SELECT
        {addresses_exploded}.*,
        COALESCE(rel_tok_freq.rel_freq, 5e-5) AS rel_freq
    FROM {addresses_exploded}
    LEFT JOIN rel_tok_freq
        ON {addresses_exploded}.token = rel_tok_freq.token
    """

    token_freq_lookup_sql = """
    SELECT
        unique_id,
        list_transform(
            list_zip(
                array_agg(token ORDER BY unique_id, token_order ASC),
                array_agg(rel_freq ORDER BY unique_id, token_order ASC)
            ),
            x -> struct_pack(tok := x[1], rel_freq := x[2])
        ) AS token_rel_freq_arr
    FROM {address_groups}
    GROUP BY unique_id
    """

    final_sql = """
    SELECT
        base.* EXCLUDE (address_without_numbers_tokenised),
        lookup.token_rel_freq_arr
    FROM {base} AS base
    INNER JOIN {token_freq_lookup} AS lookup
        ON base.unique_id = lookup.unique_id
    """

    steps = [
        CTEStep("base", base_sql),
        CTEStep("addresses_exploded", addresses_exploded_sql),
        CTEStep("address_groups", address_groups_sql),
        CTEStep("token_freq_lookup", token_freq_lookup_sql),
        CTEStep("final", final_sql),
    ]

    return Stage(
        name="add_term_frequencies_to_address_tokens_using_registered_df",
        steps=steps,
        output="final",
    )


GENERALISED_TOKEN_ALIASES_CASE_STATEMENT = """
    CASE
        WHEN token in ('FIRST', 'SECOND', 'THIRD', 'TOP') THEN ['UPPERFLOOR', 'LEVEL']
        WHEN token in ('GARDEN', 'GROUND') THEN ['GROUNDFLOOR', 'LEVEL']
        WHEN token in ('BASEMENT') THEN ['LEVEL']
        ELSE [TOKEN]
    END

"""


def generalised_token_aliases() -> Stage:
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


def move_common_end_tokens_to_field() -> Stage:
    """
    Move frequently occurring trailing tokens (e.g. counties) into a dedicated field and
    remove them from the token frequency array so missing endings aren't over-penalised.
    """

    base_sql = """
    SELECT * FROM {input}
    """

    with pkg_resources.path(
        "uk_address_matcher.data", "common_end_tokens.csv"
    ) as csv_path:
        common_end_tokens_sql = f"""
            select array_agg(token) as end_tokens_to_remove
            from read_csv_auto('{csv_path}')
            where token_count > 3000
            """

    joined_sql = """
    SELECT *
    FROM {base}
    CROSS JOIN {common_end_tokens}
    """

    end_tokens_included_sql = """
    SELECT
        * EXCLUDE (end_tokens_to_remove),
        list_filter(
            token_rel_freq_arr[-3:],
            x -> list_contains(end_tokens_to_remove, x.tok)
        ) AS common_end_tokens
    FROM {joined}
    """

    remove_end_tokens_expr = """
    list_filter(
        token_rel_freq_arr,
        (x, i) -> NOT (
            i > len(token_rel_freq_arr) - 2
            AND list_contains(list_transform(common_end_tokens, x -> x.tok), x.tok)
        )
    )
    """

    final_sql = f"""
    SELECT
        * EXCLUDE (token_rel_freq_arr),
        {remove_end_tokens_expr} AS token_rel_freq_arr
    FROM {{end_tokens_included}}
    """

    steps = [
        CTEStep("base", base_sql),
        CTEStep("common_end_tokens", common_end_tokens_sql),
        CTEStep("joined", joined_sql),
        CTEStep("end_tokens_included", end_tokens_included_sql),
        CTEStep("final", final_sql),
    ]

    return Stage(
        name="move_common_end_tokens_to_field",
        steps=steps,
        output="final",
    )


def first_unusual_token() -> Stage:
    """Attach the first token below the frequency threshold (0.001) if present."""

    first_token_expr = (
        "list_any_value(list_filter(token_rel_freq_arr, x -> x.rel_freq < 0.001))"
    )

    sql = f"""
    SELECT
        {first_token_expr} AS first_unusual_token,
        *
    FROM {{input}}
    """
    step = CTEStep("1", sql)
    return Stage(name="first_unusual_token", steps=[step])


def use_first_unusual_token_if_no_numeric_token() -> Stage:
    """Fallback to the unusual token when numeric_token_1 is missing."""

    sql = """
    SELECT
        * EXCLUDE (numeric_token_1, token_rel_freq_arr, first_unusual_token),
        CASE
            WHEN numeric_token_1 IS NULL THEN first_unusual_token.tok
            ELSE numeric_token_1
        END AS numeric_token_1,
        CASE
            WHEN numeric_token_1 IS NULL THEN
                list_filter(
                    token_rel_freq_arr,
                    x -> coalesce(x.tok != first_unusual_token.tok, TRUE)
                )
            ELSE token_rel_freq_arr
        END AS token_rel_freq_arr
    FROM {input}
    """
    step = CTEStep("1", sql)
    return Stage(
        name="use_first_unusual_token_if_no_numeric_token",
        steps=[step],
    )


def separate_unusual_tokens() -> Stage:
    """Split token list into frequency bands for matching heuristics."""

    sql = """
    SELECT
        *,
        list_transform(
            list_filter(
                list_select(
                    token_rel_freq_arr,
                    list_grade_up(list_transform(token_rel_freq_arr, x -> x.rel_freq))
                ),
                x -> x.rel_freq < 1e-4 AND x.rel_freq >= 5e-5
            ),
            x -> x.tok
        ) AS unusual_tokens_arr,
        list_transform(
            list_filter(
                list_select(
                    token_rel_freq_arr,
                    list_grade_up(list_transform(token_rel_freq_arr, x -> x.rel_freq))
                ),
                x -> x.rel_freq < 5e-5 AND x.rel_freq >= 1e-7
            ),
            x -> x.tok
        ) AS very_unusual_tokens_arr,
        list_transform(
            list_filter(
                list_select(
                    token_rel_freq_arr,
                    list_grade_up(list_transform(token_rel_freq_arr, x -> x.rel_freq))
                ),
                x -> x.rel_freq < 1e-7
            ),
            x -> x.tok
        ) AS extremely_unusual_tokens_arr
    FROM {input}
    """
    step = CTEStep("1", sql)
    return Stage(name="separate_unusual_tokens", steps=[step])


def final_column_order() -> Stage:
    """Reorder and aggregate columns to match the legacy final layout."""

    sql = """
    SELECT
        unique_id,
        numeric_token_1,
        numeric_token_2,
        numeric_token_3,
        list_aggregate(token_rel_freq_arr, 'histogram') AS token_rel_freq_arr_hist,
        list_aggregate(common_end_tokens, 'histogram') AS common_end_tokens_hist,
        postcode,
        * EXCLUDE (
            unique_id,
            numeric_token_1,
            numeric_token_2,
            numeric_token_3,
            token_rel_freq_arr,
            common_end_tokens,
            postcode
        )
    FROM {input}
    """
    step = CTEStep("1", sql)
    return Stage(name="final_column_order", steps=[step])


def get_token_frequeny_table() -> Stage:
    sql = """
    SELECT
        token_counts.token,
        token_counts.rel_freq
    FROM (
        SELECT
            token,
            COUNT(*) AS count,
            COUNT(*) / (SELECT COUNT(*) FROM {unnested}) AS rel_freq
        FROM {unnested}
        GROUP BY token
    ) AS token_counts
    ORDER BY count DESC
    """

    concatenated_sql = """
    SELECT
        unique_id,
        list_concat(
            array_filter(
                [numeric_token_1, numeric_token_2, numeric_token_3],
                x -> x IS NOT NULL
            ),
            address_without_numbers_tokenised
        ) AS all_tokens
    FROM {input}
    """

    unnested_sql = """
    SELECT unnest(all_tokens) AS token
    FROM {concatenated_tokens}
    """

    steps = [
        CTEStep("concatenated_tokens", concatenated_sql),
        CTEStep("unnested", unnested_sql),
        CTEStep("final", sql),
    ]

    return Stage(name="get_token_frequeny_table", steps=steps, output="final")
