import os
import time

import duckdb

from uk_address_matcher import (
    clean_data_using_precomputed_rel_tok_freq,
    get_linker,
)
from uk_address_matcher.post_linkage.analyse_results import (
    best_matches_summary,
)
from uk_address_matcher.post_linkage.identify_distinguishing_tokens import (
    improve_predictions_using_distinguishing_tokens,
)

overall_start_time = time.time()

# -----------------------------------------------------------------------------
# Step 1: Load data
# -----------------------------------------------------------------------------

con = duckdb.connect(":default:")

# The os.getenv can be ignored, is just so this script can be run in the test suite
epc_path = os.getenv(
    "EPC_PATH",
    "read_csv('secret_data/epc/raw/domestic-*/certificates.csv', filename=true)",
)
full_os_path = os.getenv(
    "FULL_OS_PATH",
    "read_parquet('secret_data/ord_surv/raw/add_gb_builtaddress_sorted_zstd.parquet')",
)


sql = f"""
create or replace table epc_data_raw as
select
   LMK_KEY as unique_id,
   concat_ws(' ', ADDRESS1, ADDRESS2, ADDRESS3) as address_concat,
   POSTCODE as postcode,
   UPRN as uprn,
   UPRN_SOURCE as uprn_source
from {epc_path}
-- where lower(filename) like '%hammersmith%'
"""
con.execute(sql)

sql = f"""
create or replace table os as
select
    try_cast(uprn AS BIGINT) as unique_id,
   regexp_replace(fulladdress, ',[^,]*$', '') AS address_concat,
   postcode
from {full_os_path}
where postcode in
(select distinct postcode from epc_data_raw)
and
description != 'Non Addressable Object'

"""
con.execute(sql)
df_os = con.table("os")


df_epc_data = con.sql("select * exclude (uprn,uprn_source) from epc_data_raw")


messy_count = df_epc_data.count("*").fetchall()[0][0]
canonical_count = df_os.count("*").fetchall()[0][0]


# -----------------------------------------------------------------------------
# Step 2: Clean data
# -----------------------------------------------------------------------------


df_epc_data_clean = clean_data_using_precomputed_rel_tok_freq(df_epc_data, con=con)
df_os_clean = clean_data_using_precomputed_rel_tok_freq(df_os, con=con)

end_time = time.time()
print(f"Time to load/clean: {end_time - overall_start_time} seconds")

# -----------------------------------------------------------------------------
# Step 3: Link data - pass 1
# -----------------------------------------------------------------------------


linker = get_linker(
    df_addresses_to_match=df_epc_data_clean,
    df_addresses_to_search_within=df_os_clean,
    con=con,
    include_full_postcode_block=False,
    include_outside_postcode_block=True,
    retain_intermediate_calculation_columns=True,
)


df_predict = linker.inference.predict(threshold_match_weight=-100)
df_predict_ddb = df_predict.as_duckdbpyrelation()

# -----------------------------------------------------------------------------
# Step 4: Pass 2: There's an optimisation we can do post-linking to improve score
# described here https://github.com/RobinL/uk_address_matcher/issues/14
# -----------------------------------------------------------------------------


start_time = time.time()

USE_BIGRAMS = True


df_predict_improved = improve_predictions_using_distinguishing_tokens(
    df_predict=df_predict_ddb,
    con=con,
    match_weight_threshold=-15,
    top_n_matches=5,
    use_bigrams=USE_BIGRAMS,
)


end_time = time.time()
print(f"Improve time taken: {end_time - start_time} seconds")
print(
    f"Full time taken: {end_time - overall_start_time} seconds to match "
    f"{messy_count:,.0f} messy addresses to {canonical_count:,.0f} canonical addresses "
    f"at a rate of {messy_count / (end_time - overall_start_time):,.0f} "
    "addresses per second"
)

# -----------------------------------------------------------------------------
# Step 5: Inspect results
# -----------------------------------------------------------------------------

dsum_1 = best_matches_summary(
    df_predict=df_predict_ddb,
    df_addresses_to_match=df_epc_data,
    con=con,
)
dsum_1.show(max_width=500)


# # -----------------------------------------------------------------------------
# # Step 6: Inspect single rows
# # This code is specific to the EPC data where we have a UPRN from the EPC data
# # that we can compare the the one we found
# # -----------------------------------------------------------------------------

sql = """
CREATE OR REPLACE TABLE matches_with_epc_and_os as

select
    m.*,
    e.uprn as epc_uprn,
    e.uprn_source as epc_source,
    concat(o.original_address_concat, ' ', o.postcode) as os_address,
    case
        when trim(o.original_address_concat) = '' or o.original_address_concat is null
        then 'no_os_address'
        when e.uprn = unique_id_l then 'agree'
        else 'disagree'
    end as ours_theirs_agreement
from df_predict_improved m
left join epc_data_raw e on m.unique_id_r = e.unique_id
left join df_os_clean o on e.uprn = o.unique_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id_r ORDER BY match_weight DESC, unique_id_l) =1

"""
con.execute(sql)

con.table("matches_with_epc_and_os").show()


sql = """
select ours_theirs_agreement, count(*) as count
from matches_with_epc_and_os
group by 1
"""
con.sql(sql).show(max_width=400, max_rows=40)
rows = con.sql(sql).fetchall()
agree_count = sum(row[1] for row in rows if row[0] == "agree")
disagree_count = sum(row[1] for row in rows if row[0] == "disagree")
perc = agree_count / (agree_count + disagree_count)
print(
    f"Agree: {agree_count:,}, Disagree: {disagree_count:,}, Agreement Rate: {perc:.2%}"
)


bi_cols = ""
if USE_BIGRAMS:
    bi_cols += """
    ,
    overlapping_bigrams_this_l_and_r_filtered,

    bigrams_elsewhere_in_block_but_not_this_filtered
    """


sql = """
select unique_id_r as epc_id, unique_id_l as our_match, epc_uprn as their_match
from matches_with_epc_and_os
where ours_theirs_agreement = 'disagree'
order by random()
limit 1
"""

epc_row_id, our_uprn_match, their_uprn_match = con.sql(sql).fetchall()[0]


cols = """
    concat_ws(' ', original_address_concat, postcode) as original_address_concat,
    flat_positional,
    flat_letter,
    numeric_token_1,
    numeric_token_2,
    numeric_token_3,
    token_rel_freq_arr_hist as tok_arr,
    common_end_tokens_hist as cet_arr,
    unique_id
    """
sql = f"""
select
    'epc' as source, {cols}
from df_epc_data_clean
where unique_id = '{epc_row_id}'
UNION ALL
select
    'our_match' as source, {cols}
from df_os_clean
where unique_id = '{our_uprn_match}'
UNION ALL
select
    'their_match' as source, {cols}
from df_os_clean
where unique_id = '{their_uprn_match}'

"""
con.sql(sql).show(max_width=1000)

sql = f"""
select uprn_source
from epc_data_raw
where unique_id = '{epc_row_id}'
"""

con.sql(sql).show(max_width=1000)


# Find out top n matches

sql = f"""
select
    concat_ws(' ', original_address_concat_r, postcode_r) as messy_address,
    concat_ws(' ', original_address_concat_l, postcode_l) as our_match,
    match_weight as match_weight_short,
    mw_adjustment,
    match_weight_original as match_weight_orig
    {bi_cols},
    overlapping_tokens_this_l_and_r,
    tokens_elsewhere_in_block_but_not_this ,
    missing_tokens,


from df_predict_improved
where unique_id_r = '{epc_row_id}'
order by match_weight desc

"""

con.sql(sql).show(max_width=1000)


sql = f"""
select *
from df_predict_ddb
where unique_id_r = '{epc_row_id}' and unique_id_l in ('{our_uprn_match}', '{their_uprn_match}')
order by match_weight desc
limit 10
"""

res = con.sql(sql)


linker.visualisations.waterfall_chart(
    res.df().to_dict(orient="records"), filter_nulls=False
)
