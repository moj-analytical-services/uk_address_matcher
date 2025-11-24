# Pre-clean the full OS dataset so it doesn't need to be cleaned on the fly
# in subsequent runs
import time

import duckdb

from uk_address_matcher import (
    clean_data_on_the_fly,
    clean_data_with_term_frequencies,
)

overall_start_time = time.time()
con = duckdb.connect(":default:")


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

"""
con.execute(sql)
df_os = con.table("os")
df_os


df_os_clean = clean_data_on_the_fly(df_os, con=con)
df_os_clean = clean_data_with_term_frequencies(df_os, con=con)
df_os_clean.write_parquet("secret_data/ord_surv/os_clean.parquet")
df_os_clean_from_file = duckdb.read_parquet("secret_data/ord_surv/os_clean.parquet")
df_os_clean_from_file.show(max_width=50000)
