from uk_address_matcher.cleaning_v2.pipeline import (
    DuckDBPipeline,
)
from uk_address_matcher.cleaning_v2.cleaning_steps import (
    canonicalise_postcode,
    clean_address_string_first_pass,
    clean_address_string_second_pass,
    derive_original_address_concat,
    parse_out_flat_position_and_letter,
    parse_out_numbers,
    remove_duplicate_end_tokens,
    split_numeric_tokens_to_cols,
    tokenise_address_without_numbers,
    trim_whitespace_address_and_postcode,
    upper_case_address_and_postcode,
)

from uk_address_matcher.cleaning.cleaning_steps import (
    canonicalise_postcode as canonicalise_postcode_v1,
    clean_address_string_first_pass as clean_address_string_first_pass_v1,
    clean_address_string_second_pass as clean_address_string_second_pass_v1,
    derive_original_address_concat as derive_original_address_concat_v1,
    parse_out_flat_position_and_letter as parse_out_flat_position_and_letter_v1,
    parse_out_numbers as parse_out_numbers_v1,
    split_numeric_tokens_to_cols as split_numeric_tokens_to_cols_v1,
    tokenise_address_without_numbers as tokenise_address_without_numbers_v1,
    trim_whitespace_address_and_postcode as trim_whitespace_address_and_postcode_v1,
    upper_case_address_and_postcode as upper_case_address_and_postcode_v1,
    remove_duplicate_end_tokens as remove_duplicate_end_tokens_v1,
)
from uk_address_matcher.cleaning.run_pipeline import run_pipeline

import duckdb
from time import perf_counter
from pandas.testing import assert_frame_equal

queue = [
    (trim_whitespace_address_and_postcode, trim_whitespace_address_and_postcode_v1),
    (canonicalise_postcode, canonicalise_postcode_v1),
    (upper_case_address_and_postcode, upper_case_address_and_postcode_v1),
    (clean_address_string_first_pass, clean_address_string_first_pass_v1),
    (remove_duplicate_end_tokens, remove_duplicate_end_tokens_v1),
    (derive_original_address_concat, derive_original_address_concat_v1),
    (parse_out_flat_position_and_letter, parse_out_flat_position_and_letter_v1),
    (parse_out_numbers, parse_out_numbers_v1),
    (clean_address_string_second_pass, clean_address_string_second_pass_v1),
    (split_numeric_tokens_to_cols, split_numeric_tokens_to_cols_v1),
    (tokenise_address_without_numbers, tokenise_address_without_numbers_v1),
]

con = duckdb.connect()

full_os_path = "secret_data/ord_surv/raw/add_gb_builtaddress_sorted_zstd.parquet"

sql = f"""
create or replace table os as
select
   uprn as unique_id,
   'canonical' as source_dataset,
   regexp_replace(fulladdress, ',[^,]*$', '') AS address_concat,
   postcode,
   latitude as lat,
   longitude as lng
from read_parquet('{full_os_path}')
limit 5000

"""
con.execute(sql)
os_df = con.table("os")


pipe = DuckDBPipeline(con, os_df)

for v2, v1 in queue:
    pipe.add_step(v2())

t0 = perf_counter()
clean_v2 = pipe.run(pretty_print_sql=False)
clean_v2_df = clean_v2.df()
end_time = perf_counter()
print(f"Time taken: {end_time - t0:.2f} seconds")


queue_v2 = [v[1] for v in queue]
start_time = perf_counter()
clean_v1 = run_pipeline(
    os_df,
    con=con,
    cleaning_queue=queue_v2,
)
clean_v1_df = clean_v1.df()

end_time = perf_counter()
print(f"Time taken: {end_time - start_time:.2f} seconds")
clean_v1_df


assert_frame_equal(
    clean_v2_df,
    clean_v1_df,
    check_dtype=False,
    check_exact=False,
    rtol=1e-8,
    atol=1e-12,
)
