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
canonicalise_postcode,
upper_case_address_and_postcode,
clean_address_string_first_pass,
remove_duplicate_end_tokens,
derive_original_address_concat,
parse_out_flat_position_and_letter,
parse_out_numbers,
clean_address_string_second_pass,
split_numeric_tokens_to_cols,
tokenise_address_without_numbers,
