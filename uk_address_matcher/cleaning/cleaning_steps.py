import importlib.resources as pkg_resources

from duckdb import DuckDBPyConnection, DuckDBPyRelation

from .regexes import (
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


def upper_case_address_and_postcode(
    ddb_pyrel: DuckDBPyRelation, con: DuckDBPyConnection
) -> DuckDBPyRelation:
    sql = """
    select
        * exclude (address_concat, postcode),
        upper(address_concat) as address_concat,
        upper(postcode) as postcode
    from ddb_pyrel
    """

    return con.sql(sql)


def derive_original_address_concat(
    ddb_pyrel: DuckDBPyRelation, con: DuckDBPyConnection
) -> DuckDBPyRelation:
    sql = """
    SELECT
        *,
        address_concat AS original_address_concat
    FROM ddb_pyrel
    """

    return con.sql(sql)


def trim_whitespace_address_and_postcode(
    ddb_pyrel: DuckDBPyRelation, con: DuckDBPyConnection
) -> DuckDBPyRelation:
    sql = """
    select
        * exclude (address_concat, postcode),
        trim(address_concat) as address_concat,
        trim(postcode) as postcode
    from ddb_pyrel
    """

    return con.sql(sql)


def canonicalise_postcode(
    ddb_pyrel: DuckDBPyRelation, con: DuckDBPyConnection
) -> DuckDBPyRelation:
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
    SELECT
        * EXCLUDE (postcode),
        regexp_replace(
            postcode,
            '{uk_postcode_regex}',
            '\\1 \\2'  -- Insert space between captured groups
        ) AS postcode
    FROM ddb_pyrel
    """
    return con.sql(sql)


def clean_address_string_first_pass(
    ddb_pyrel: DuckDBPyRelation, con: DuckDBPyConnection
) -> DuckDBPyRelation:
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
        {fn_call} as address_concat,

    from ddb_pyrel
    """

    return con.sql(sql)


def parse_out_flat_position_and_letter(
    ddb_pyrel: DuckDBPyRelation, con: DuckDBPyConnection
) -> DuckDBPyRelation:
    """
    Extracts flat positions and letters from address strings into separate columns.


    Args:
        ddb_pyrel (DuckDBPyRelation): The input relation
        con (DuckDBPyConnection): The DuckDB connection

    Returns:
        DuckDBPyRelation: The modified table with flat_positional and flat_letter fields
    """
    # Define regex patterns
    floor_positions = r"\b(BASEMENT|GROUND FLOOR|FIRST FLOOR|SECOND FLOOR|THIRD FLOOR|TOP FLOOR|GARDEN)\b"
    flat_letter = r"\b\d{0,4}([A-Za-z])\b"
    leading_letter = r"^\s*\d+([A-Za-z])\b"

    flat_number = r"\b(FLAT|UNIT|APARTMENT)\s+(\S*\d\S*)\s+\S*\d\S*\b"

    sql = f"""
    WITH step1 AS (
        SELECT
            *,
            -- Get floor position if present
            regexp_extract(address_concat, '{floor_positions}', 1) as floor_pos,
            -- Get letter after FLAT/UNIT/etc if present
            regexp_extract(address_concat, '{flat_letter}', 1) as flat_letter,
            -- Get just the letter part of leading number+letter combination
            regexp_extract(address_concat, '{leading_letter}', 1) as leading_letter,
            regexp_extract(address_concat, '{flat_number}', 1) as flat_number
        FROM ddb_pyrel
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
    return con.sql(sql)


def parse_out_numbers(
    ddb_pyrel: DuckDBPyRelation, con: DuckDBPyConnection
) -> DuckDBPyRelation:
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
            WHEN flat_letter IS NOT NULL AND flat_letter ~ '^\d+$' THEN
                -- If flat_letter is numeric, ignore the first number token
                -- Use list_filter with index to skip the first element
            regexp_extract_all(address_concat, '{regex_pattern}')[2:]
            ELSE
                regexp_extract_all(address_concat, '{regex_pattern}')
        END AS numeric_tokens
    FROM ddb_pyrel
    """
    return con.sql(sql)


def clean_address_string_second_pass(
    ddb_pyrel: DuckDBPyRelation, con: DuckDBPyConnection
) -> DuckDBPyRelation:
    fn_call = construct_nested_call(
        "address_without_numbers",
        [remove_multiple_spaces, trim],
    )
    sql = f"""
    select
        * exclude (address_without_numbers),
        {fn_call} as address_without_numbers,
    from ddb_pyrel
    """

    return con.sql(sql)


def split_numeric_tokens_to_cols(
    ddb_pyrel: DuckDBPyRelation, con: DuckDBPyConnection
) -> DuckDBPyRelation:
    sql = """
    SELECT
        * EXCLUDE (numeric_tokens),
        regexp_extract_all(array_to_string(numeric_tokens, ' '), '\\d+')[1] as numeric_token_1,
        regexp_extract_all(array_to_string(numeric_tokens, ' '), '\\d+')[2] as numeric_token_2,
        regexp_extract_all(array_to_string(numeric_tokens, ' '), '\\d+')[3] as numeric_token_3
    FROM ddb_pyrel
    """

    return con.sql(sql)


def tokenise_address_without_numbers(
    ddb_pyrel: DuckDBPyRelation, con: DuckDBPyConnection
) -> DuckDBPyRelation:
    sql = """
    select
        * exclude (address_without_numbers),
        regexp_split_to_array(trim(address_without_numbers), '\\s+')
            AS address_without_numbers_tokenised
    from ddb_pyrel
    """

    return con.sql(sql)


def remove_duplicate_end_tokens(
    ddb_pyrel: DuckDBPyRelation, con: DuckDBPyConnection
) -> DuckDBPyRelation:
    """
    Removes duplicated tokens at the end of the address.
    E.g. 'HIGH STREET ST ALBANS ST ALBANS' -> 'HIGH STREET ST ALBANS'
    """
    sql = """
    with tokenised as (
    select *, string_split(address_concat, ' ') as cleaned_tokenised
    from ddb_pyrel
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
    return con.sql(sql)


def get_token_frequeny_table(
    ddb_pyrel: DuckDBPyRelation, con: DuckDBPyConnection
) -> DuckDBPyRelation:
    sql = """
    WITH concatenated_tokens AS (
        SELECT
            unique_id,
            list_concat(
                array_filter(
                    [numeric_token_1, numeric_token_2, numeric_token_3],
                    x -> x IS NOT NULL
                ),
                address_without_numbers_tokenised
            ) AS all_tokens
        FROM ddb_pyrel
    ),
    unnested as (
    SELECT
            unnest(all_tokens) AS token
            FROM concatenated_tokens
    ),
    token_counts AS (
        SELECT
            token,
            count(*) AS count,
             count(*)  / (select count(*) from unnested) rel_freq
        from unnested
        GROUP BY token
    )
    SELECT
        token, rel_freq
    FROM token_counts
    ORDER BY count DESC
    """
    return con.sql(sql)


def _tokens_with_freq_sql(
    ddb_pyrel_name: str, rel_tok_freq_name: str = "rel_tok_freq"
) -> str:
    return f"""
     addresses_exploded AS (
        SELECT
            unique_id,
            unnest(address_without_numbers_tokenised) as token,
            generate_subscripts(address_without_numbers_tokenised, 1) AS token_order
        FROM ddb_pyrel_alias
    ),
    address_groups AS (
        SELECT addresses_exploded.*,
        COALESCE({rel_tok_freq_name}.rel_freq, 5e-5) AS rel_freq
        FROM addresses_exploded
        LEFT JOIN {rel_tok_freq_name}
        ON addresses_exploded.token = {rel_tok_freq_name}.token
    ),
    token_freq_lookup AS (
        SELECT
            unique_id,
            -- This guarantees preservation of token order vs.
            -- list(struct_pack(token := token, rel_freq := rel_freq))
            list_transform(
                list_zip(
                    array_agg(token order by unique_id, token_order asc),
                    array_agg(rel_freq order by unique_id, token_order asc)
                    ),
                x-> struct_pack(tok:= x[1], rel_freq:= x[2])
            )
            as token_rel_freq_arr
        FROM address_groups
        GROUP BY unique_id
    )
    SELECT
        d.* EXCLUDE (address_without_numbers_tokenised),
        r.token_rel_freq_arr
    FROM
        ddb_pyrel_alias as d
    INNER JOIN token_freq_lookup as r
    ON d.unique_id = r.unique_id
    """


def add_term_frequencies_to_address_tokens(
    ddb_pyrel: DuckDBPyRelation, con: DuckDBPyConnection
) -> DuckDBPyRelation:
    # Compute relative term frequencies amongst the tokens
    # TODO - if we're removing numbers we should also remove flat positionals
    con.register("ddb_pyrel_alias", ddb_pyrel)
    sql = f"""
    WITH rel_tok_freq_cte AS (
        SELECT
            token,
            count(*)  / sum(count(*)) OVER() as rel_freq
        FROM (
            SELECT
                unnest(address_without_numbers_tokenised) as token
            FROM ddb_pyrel_alias
        )
        GROUP BY token
    ),
    {_tokens_with_freq_sql("ddb_pyrel", rel_tok_freq_name="rel_tok_freq_cte")}
    """

    return con.sql(sql)


def add_term_frequencies_to_address_tokens_using_registered_df(
    ddb_pyrel: DuckDBPyRelation, con: DuckDBPyConnection
) -> DuckDBPyRelation:
    # For some reason this is necessary to avoid a
    # BinderException: Binder Error: Max expression depth limit of 1000 exceeded.

    con.register("ddb_pyrel_alias", ddb_pyrel)
    sql = f"""
    WITH

    {_tokens_with_freq_sql("ddb_pyrel_name")}

    """

    return con.sql(sql)


def first_unusual_token(
    ddb_pyrel: DuckDBPyRelation, con: DuckDBPyConnection
) -> DuckDBPyRelation:
    # Get first below freq
    first_token = (
        "list_any_value(list_filter(token_rel_freq_arr, x -> x.rel_freq < 0.001))"
    )

    sql = f"""
    select
    {first_token} as first_unusual_token,
    *
    from ddb_pyrel
    """
    return con.sql(sql)


def use_first_unusual_token_if_no_numeric_token(
    ddb_pyrel: DuckDBPyRelation, con: DuckDBPyConnection
) -> DuckDBPyRelation:
    sql = """
    select
        * exclude (numeric_token_1, token_rel_freq_arr, first_unusual_token),
        case
            when numeric_token_1 is null then first_unusual_token.tok
            else numeric_token_1
            end as numeric_token_1,

    case
        when numeric_token_1 is null
        then list_filter(token_rel_freq_arr, x -> coalesce(x.tok != first_unusual_token.tok, true))
        else token_rel_freq_arr
    end
    as token_rel_freq_arr
    from ddb_pyrel
    """

    return con.sql(sql)


def final_column_order(
    ddb_pyrel: DuckDBPyRelation, con: DuckDBPyConnection
) -> DuckDBPyRelation:
    sql = """
    select
        unique_id,
        numeric_token_1,
        numeric_token_2,
        numeric_token_3,
        -- token_rel_freq_arr,

        list_aggregate(token_rel_freq_arr, 'histogram') as token_rel_freq_arr_hist,
        list_aggregate(common_end_tokens,'histogram') AS common_end_tokens_hist,

        --common_end_tokens,
        postcode,

        * exclude (
            unique_id,
            numeric_token_1,
            numeric_token_2,
            numeric_token_3,
            token_rel_freq_arr,
            common_end_tokens,
            postcode
                )

    from ddb_pyrel
    """

    return con.sql(sql)


def move_common_end_tokens_to_field(
    ddb_pyrel: DuckDBPyRelation, con: DuckDBPyConnection
) -> DuckDBPyRelation:
    # Want to put common tokens towards the end of the address
    # into their own field.  These tokens (e.g. SOMERSET or LONDON)
    # are often ommitted from so 'punishing' lack of agreement is probably
    # not necessary

    # For some reason this is necessary to avoid a
    # BinderException: Binder Error: Max expression depth limit of 1000 exceeded.
    con.register("ddb_pyrel_alias2", ddb_pyrel)
    with pkg_resources.path(
        "uk_address_matcher.data", "common_end_tokens.csv"
    ) as csv_path:
        sql = f"""
        select array_agg(token) as end_tokens_to_remove
        from read_csv_auto("{csv_path}")
        where token_count > 3000
        """
    common_end_tokens = con.sql(sql)

    # Not sure why this is needed
    con.register("common_end_tokens", common_end_tokens)

    end_tokens_as_array = """
    list_transform(common_end_tokens, x -> x.tok)
    """

    remove_end_tokens = f"""
    list_filter(token_rel_freq_arr,
        (x,i) ->
            not
            (
            i > len(token_rel_freq_arr) - 2
            and
            list_contains({end_tokens_as_array}, x.tok)
            )
    )
    """

    sql = f"""
    with

    joined as (
        select *
    from ddb_pyrel_alias2
    cross join common_end_tokens
    ),

    end_tokens_included as (
    select
    * exclude (end_tokens_to_remove),
    list_filter(token_rel_freq_arr[-3:],
        x ->  list_contains(end_tokens_to_remove, x.tok)
    )
    as common_end_tokens
    from joined
    )

    select
        * exclude (token_rel_freq_arr),
        {remove_end_tokens} as token_rel_freq_arr
    from end_tokens_included

    """

    return con.sql(sql)


def separate_unusual_tokens(
    ddb_pyrel: DuckDBPyRelation, con: DuckDBPyConnection
) -> DuckDBPyRelation:
    sql = """
    SELECT
        *,
        list_transform(list_filter(
            list_select(
                token_rel_freq_arr,
                list_grade_up(list_transform(token_rel_freq_arr, x -> x.rel_freq))
            ),
            x -> x.rel_freq < 1e-4 and x.rel_freq >= 5e-5
        ), x-> x.tok) AS unusual_tokens_arr,
        list_transform(list_filter(
            list_select(
                token_rel_freq_arr,
                list_grade_up(list_transform(token_rel_freq_arr, x -> x.rel_freq))
            ),
            x -> x.rel_freq < 5e-5 and x.rel_freq >= 1e-7
        ), x-> x.tok) AS very_unusual_tokens_arr,
        list_transform(list_filter(
            list_select(
                token_rel_freq_arr,
                list_grade_up(list_transform(token_rel_freq_arr, x -> x.rel_freq))
            ),
            x -> x.rel_freq < 1e-7
        ), x-> x.tok) AS extremely_unusual_tokens_arr
    FROM ddb_pyrel
    """
    return con.sql(sql)


def separate_distinguishing_start_tokens_from_with_respect_to_adjacent_recrods(
    ddb_pyrel: DuckDBPyRelation, con: DuckDBPyConnection
) -> DuckDBPyRelation:
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
    sql = """
    WITH tokens AS (
        SELECT
            ['FLAT', 'APARTMENT', 'UNIT'] AS __tokens_to_remove,
            list_filter(
                regexp_split_to_array(address_concat, '\\s+'),
                x -> not list_contains(__tokens_to_remove, x)
            )
                AS __tokens,
            row_number() OVER (ORDER BY reverse(address_concat)) AS row_order,
            *
        FROM ddb_pyrel
    ),
    with_neighbors AS (
        SELECT
            lag(__tokens) OVER (ORDER BY row_order) AS __prev_tokens,
            lead(__tokens) OVER (ORDER BY row_order) AS __next_tokens,
            *
        FROM tokens
    ),
    with_suffix_lengths AS (
        SELECT
            len(__tokens) AS __token_count,
            -- Calculate common suffix length with previous address
            CASE WHEN __prev_tokens IS NOT NULL THEN
                (SELECT max(i)
                FROM range(0, least(len(__tokens), len(__prev_tokens))) AS t(i)
                WHERE list_slice(list_reverse(__tokens), 1, i+1) =
                    list_slice(list_reverse(__prev_tokens), 1, i+1))
            ELSE 0 END AS prev_common_suffix,
            -- Calculate common suffix length with next address
            CASE WHEN __next_tokens IS NOT NULL THEN
                (SELECT max(i)
                FROM range(0, least(len(__tokens), len(__next_tokens))) AS t(i)
                WHERE list_slice(list_reverse(__tokens), 1, i+1) =
                    list_slice(list_reverse(__next_tokens), 1, i+1))
            ELSE 0 END AS next_common_suffix,
            *
        FROM with_neighbors
    ),
    with_unique_parts AS (
        SELECT
            *,
            -- Find the maximum common suffix length
            greatest(prev_common_suffix, next_common_suffix) AS max_common_suffix,
            -- Use list_filter with index to keep only the unique part at the beginning
            list_filter(__tokens, (token, i) -> i < __token_count - greatest(prev_common_suffix, next_common_suffix)) AS unique_tokens,
            -- Use list_filter with index to keep only the common part at the end
            list_filter(__tokens, (token, i) -> i >= __token_count - greatest(prev_common_suffix, next_common_suffix)) AS common_tokens
        FROM with_suffix_lengths
    )
    SELECT
        * EXCLUDE (__tokens, __prev_tokens, __next_tokens, __token_count, __tokens_to_remove, max_common_suffix, next_common_suffix, prev_common_suffix, row_order, common_tokens,unique_tokens),
        COALESCE(unique_tokens, ARRAY[]) AS distinguishing_adj_start_tokens,
        COALESCE(common_tokens, ARRAY[]) AS common_adj_start_tokens,
    FROM
    with_unique_parts
    """

    return con.sql(sql)


# Located here because this is reused in comparisons
GENERALISED_TOKEN_ALIASES_CASE_STATEMENT = """
    CASE
        WHEN token in ('FIRST', 'SECOND', 'THIRD', 'TOP') THEN ['UPPERFLOOR', 'LEVEL']
        WHEN token in ('GARDEN', 'GROUND') THEN ['GROUNDFLOOR', 'LEVEL']
        WHEN token in ('BASEMENT') THEN ['LEVEL']
        ELSE [TOKEN]
    END

"""


def generalised_token_aliases(
    ddb_pyrel: DuckDBPyRelation, con: DuckDBPyConnection
) -> DuckDBPyRelation:
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
    FROM ddb_pyrel
    """

    return con.sql(sql)
