# add one directory up to cwd so we can see the uk_address_matcher package
import sys

import duckdb

sys.path.append("..")


from uk_address_matcher.cleaning import (
    clean_address_string_first_pass,
    clean_address_string_second_pass,
    parse_out_numbers,
    split_numeric_tokens_to_cols,
    tokenise_address_without_numbers,
    trim_whitespace_address_and_postcode,
    upper_case_address_and_postcode,
)
from uk_address_matcher.run_pipeline import run_pipeline

path = "../fhrs_companies_house/BasicCompanyDataAsOneFile-2024-05-01.csv"
sql = f"""
SELECT
    "CompanyNumber" AS unique_id,
    'companies_house' as source_dataset,
    CONCAT(
        COALESCE("RegAddress.AddressLine1", ''), ' ',
        COALESCE("RegAddress.AddressLine2", ''), ' ',
        COALESCE("RegAddress.PostTown", ''), ' ',
        COALESCE("RegAddress.County", '')
    ) AS address_concat,
    "RegAddress.PostCode" AS postcode
FROM read_csv_auto('{path}')
"""
df_in = duckdb.sql(sql)

sql = """
select *, address_concat as original_address_concat
from df_in
"""
df_in_2 = duckdb.sql(sql)
cleaning_queue = [
    trim_whitespace_address_and_postcode,
    upper_case_address_and_postcode,
    clean_address_string_first_pass,
    parse_out_numbers,
    clean_address_string_second_pass,
    split_numeric_tokens_to_cols,
    tokenise_address_without_numbers,
]
tokensied = run_pipeline(df_in_2, cleaning_queue)


sql = """
select
count(*) as token_count,
address_without_numbers_tokenised[-1:][1] as token,
from tokensied
group by
address_without_numbers_tokenised[-1:][1],
having count(*) > 100
order by count(*) desc

"""
common_end_tokens = duckdb.sql(sql)
common_end_tokens.df()

sql = """
COPY common_tokens
TO './common_end_tokens.csv' (FORMAT CSV);

"""
duckdb.sql(sql)
