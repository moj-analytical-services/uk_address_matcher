from uk_address_matcher.cleaning_v2.pipeline import (
    DuckDBPipeline,
)
from uk_address_matcher.cleaning_v2.cleaning_steps import (
    canonicalise_postcode,
    clean_address_string_first_pass,
    trim_whitespace_address_and_postcode,
    upper_case_address_and_postcode,
)
import duckdb
import time


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
limit 5

"""
con.execute(sql)
os_df = con.table("os")

start_time = time.time()

pipe = DuckDBPipeline(con, os_df)

pipe.add_step(trim_whitespace_address_and_postcode())
pipe.add_step(canonicalise_postcode())
pipe.add_step(upper_case_address_and_postcode())
pipe.add_step(clean_address_string_first_pass())

out = pipe.run(pretty_print_sql=False)
print("Result:")
out.show()

end_time = time.time()
print(f"Time taken: {end_time - start_time:.2f} seconds")
