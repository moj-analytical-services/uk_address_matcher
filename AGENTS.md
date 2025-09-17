Consider [the old cleaning steps](uk_address_matcher/cleaning/cleaning_steps.py) which were run using [run_pipeline](uk_address_matcher/cleaning/run_pipeline.py)

The core logic was to run a loop where each function took a DuckDBPyRelation as an input and outputted a transformed DuckDBPyRelation

for i, cleaning_function in enumerate(cleaning_queue):
        ddb_pyrel = cleaning_function(ddb_pyrel, con)

I am refactoring this to generate a CTE pipeline using SQL strings using the code in [pipeline v2](uk_address_matcher/cleaning_v2/pipeline.py)

I have set up one function so far in [cleaning_steps V2](uk_address_matcher/cleaning_v2/cleaning_steps.py)

I want to move all of these functions over one at a time.

As we go, I want to import the functions into my [test script](try_new_pipeline.py) to make sure it's working.

Here's our progress, make sure you update this as we proceed

Note that you can run this testing script as we proceed with
uv run try_new_pipeline.py


trim_whitespace_address_and_postcode [DONE]
canonicalise_postcode [DONE]
upper_case_address_and_postcode [DONE]
clean_address_string_first_pass [DONE]
remove_duplicate_end_tokens [DONE]
derive_original_address_concat [DONE]
separate_distinguishing_start_tokens_from_with_respect_to_adjacent_recrods [DONE]
generalised_token_aliases [DONE]
parse_out_flat_position_and_letter [DONE]
parse_out_numbers [DONE]
clean_address_string_second_pass [DONE]
split_numeric_tokens_to_cols [DONE]
tokenise_address_without_numbers [DONE]
move_common_end_tokens_to_field [DONE]
first_unusual_token,
use_first_unusual_token_if_no_numeric_token,
separate_unusual_tokens,
final_column_order,



- Ensure that you port over docstrings
- Make sure you copy over the same regex code, looking carefully at the number of \, and keeping the same number

IMPORTANT:  You'll see in my [test script](try_new_pipeline.py) that i assert equality between the old and new way of doing things.  Ensure as you migrate functions over, you continue this pattern i.e. migrate the function, add it to the queue in the script so the new function is used as part of the asssertion.


Example of migration:

OLD:
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

NEW:
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
