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



- Ensure that you port over docstrings
- Make sure you copy over the same regex code, looking carefully at the number of \, and keeping the same number

IMPORTANT:  You'll see in my [test script](try_new_pipeline.py) that i assert equality between the old and new way of doing things.  Ensure as you migrate functions over, you continue this pattern i.e. migrate the function, add it to the queue in the script so the new function is used as part of the asssertion.
