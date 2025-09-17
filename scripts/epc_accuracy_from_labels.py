import time
import duckdb
from uk_address_matcher import (
    clean_data_using_precomputed_rel_tok_freq,
    get_linker,
)
from uk_address_matcher.post_linkage.identify_distinguishing_tokens import (
    improve_predictions_using_distinguishing_tokens,
)
from IPython.display import display
from uk_address_matcher.linking_model.training import get_settings_for_training

overall_start_time = time.time()

# -----------------------------------------------------------------------------
# Step 1: Load data
# -----------------------------------------------------------------------------

con = duckdb.connect(":memory:")

# The os.getenv can be ignored, is just so this script can be run in the test suite
epc_path = "read_csv('secret_data/epc/raw/domestic-*/certificates.csv', filename=true)"

full_os_path = (
    "read_parquet('secret_data/ord_surv/raw/add_gb_builtaddress_sorted_zstd.parquet')"
)

labels_path = "read_csv('secret_data/epc/labels_2000.csv', all_varchar=true)"


sql = f"""
select * from {labels_path}
where confidence = 'epc_splink_agree'
and hash(messy_id) % 10 = 0
UNION ALL
select * from {labels_path}
where confidence in ('likely', 'certain')


"""
labels_filtered = con.sql(sql)
labels_filtered.count("*").fetchall()[0][0]

# labels_filtered = labels_filtered.limit(100)

sql = f"""
create or replace table epc_data_raw as
select
   LMK_KEY as unique_id,
   concat_ws(' ', ADDRESS1, ADDRESS2, ADDRESS3) as address_concat,
   POSTCODE as postcode,
   UPRN as uprn,
   UPRN_SOURCE as uprn_source
from {epc_path}
where unique_id in (select messy_id from labels_filtered)

"""
con.execute(sql)

sql = f"""
create or replace table os as
select
   uprn as unique_id,
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
print(f"messy_count: {messy_count:,}, canonical_count: {canonical_count:,}")

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
settings = get_settings_for_training()

linker = get_linker(
    df_addresses_to_match=df_epc_data_clean,
    df_addresses_to_search_within=df_os_clean,
    con=con,
    include_full_postcode_block=True,
    include_outside_postcode_block=True,
    retain_intermediate_calculation_columns=True,
    additional_columns_to_retain=[],
    settings=settings,
)


df_predict = linker.inference.predict(threshold_match_weight=-50)
df_predict_ddb = df_predict.as_duckdbpyrelation()

display(linker.visualisations.match_weights_chart())
# -----------------------------------------------------------------------------
# Step 4: Pass 2: There's an optimisation we can do post-linking to improve score
# described here https://github.com/RobinL/uk_address_matcher/issues/14
# -----------------------------------------------------------------------------
df_predict_ddb

start_time = time.time()

USE_BIGRAMS = True


df_predict_improved = improve_predictions_using_distinguishing_tokens(
    df_predict=df_predict_ddb,
    con=con,
    match_weight_threshold=-10,
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


# # -----------------------------------------------------------------------------
# # Step 6: Inspect single rows
# # This code is specific to the EPC data where we have a UPRN from the EPC data
# # that we can compare the the one we found
# # -----------------------------------------------------------------------------


sql = """
CREATE OR REPLACE TABLE matches_with_epc_and_os_predict as

select
    m.*,
    e.uprn as epc_uprn,
    e.uprn_source as epc_source,
    l.correct_uprn as correct_uprn,
    l.confidence as label_confidence,
    case
        when m.unique_id_l = l.correct_uprn then 'true positive'
        else 'false positive'
    end as truth_status
from df_predict_ddb m
left join epc_data_raw e on m.unique_id_r = e.unique_id
left join labels_filtered l on m.unique_id_r = l.messy_id
QUALIFY ROW_NUMBER()
OVER (PARTITION BY unique_id_r
ORDER BY match_weight DESC, unique_id_l) =1

"""
con.execute(sql)


sql = """
select truth_status, count(*) as count
from matches_with_epc_and_os_predict
group by truth_status
"""
con.sql(sql).show(max_width=400, max_rows=40)
rows = con.sql(sql).fetchall()
true_positive_count = sum(row[1] for row in rows if row[0] == "true positive")
false_positive_count = sum(row[1] for row in rows if row[0] == "false positive")
perc = true_positive_count / (true_positive_count + false_positive_count)
print(
    f"True positive: {true_positive_count:,}, False positive: {false_positive_count:,}, Accuracy: {perc:.2%}"
)

sql = """
select truth_status, label_confidence, count(*) as count
from matches_with_epc_and_os_predict
group by truth_status, label_confidence
order by label_confidence, truth_status desc
"""
con.sql(sql).show(max_width=400, max_rows=40)


sql = """
CREATE OR REPLACE TABLE matches_with_epc_and_os as

select
    m.*,
    e.uprn as epc_uprn,
    e.uprn_source as epc_source,
    l.correct_uprn as correct_uprn,
    l.confidence as label_confidence,
    case
        when m.unique_id_l = l.correct_uprn then 'true positive'
        else 'false positive'
    end as truth_status
from df_predict_improved m
left join epc_data_raw e on m.unique_id_r = e.unique_id
left join labels_filtered l on m.unique_id_r = l.messy_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_id_r ORDER BY match_weight DESC, unique_id_l) =1

"""
con.execute(sql)


sql = """
select truth_status, count(*) as count
from matches_with_epc_and_os
group by truth_status
"""
con.sql(sql).show(max_width=400, max_rows=40)
rows = con.sql(sql).fetchall()
true_positive_count = sum(row[1] for row in rows if row[0] == "true positive")
false_positive_count = sum(row[1] for row in rows if row[0] == "false positive")
perc = true_positive_count / (true_positive_count + false_positive_count)
print(
    f"True positive: {true_positive_count:,}, False positive: {false_positive_count:,}, Accuracy: {perc:.2%}"
)

sql = """
select truth_status, label_confidence, count(*) as count
from matches_with_epc_and_os
group by truth_status, label_confidence
order by label_confidence, truth_status desc
"""
con.sql(sql).show(max_width=400, max_rows=40)


# -----------------------------------------------------------------------------
# LOOK AT FALSE POSITIVES
# -----------------------------------------------------------------------------
# %%

print("=" * 200)
# Want to see waterfall for correct and wrong

sql = """
select unique_id_r as epc_id, unique_id_l as our_match, epc_uprn as their_match, correct_uprn as correct_uprn
from matches_with_epc_and_os
where 1=1
and truth_status = 'false positive'
--and epc_id = '732941539062014031115561564858454'
and label_confidence = 'epc_splink_agree'
order by random()
limit 1
"""

epc_row_id, our_uprn_match, their_uprn_match, correct_uprn = con.sql(sql).fetchall()[0]

# show original address

sql = f"""
select address_concat as epc_raw_address, postcode as epc_raw_postcode, unique_id
from epc_data_raw
where unique_id = '{epc_row_id}'
"""

con.sql(sql).show()

sql = f"""
select *
from df_epc_data_clean
where unique_id = '{epc_row_id}'
"""

con.sql(sql).show(max_width=100000)


sql = f"""
select fulladdress as llm_correct_match
from {full_os_path}
 where uprn = '{correct_uprn}'
"""
con.sql(sql).show(max_width=100000)


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
where substr(unique_id, 1, 12) = '{epc_row_id}'
UNION ALL
select
    'our_match' as source, {cols}
from df_os_clean
where unique_id = '{our_uprn_match}'
UNION ALL
select
    'correct_match' as source, {cols}
from df_os_clean
where unique_id = '{correct_uprn}'
UNION ALL
select
    'epc_match' as source, {cols}
from df_os_clean
where unique_id = '{their_uprn_match}'

"""
con.sql(sql).show(max_width=1000)


sql = f"""
select
    concat_ws(' ', original_address_concat_r, postcode_r) as messy_address,
    concat_ws(' ', original_address_concat_l, postcode_l) as our_match,
    match_weight as match_weight_short,
    mw_adjustment,
    match_weight_original as match_weight_orig,
    overlapping_bigrams_this_l_and_r_filtered,
    bigrams_elsewhere_in_block_but_not_this_filtered
    overlapping_tokens_this_l_and_r,
    tokens_elsewhere_in_block_but_not_this ,
    missing_tokens,

from df_predict_improved
where unique_id_r = '{epc_row_id}'
order by match_weight desc

"""

con.sql(sql).show(max_width=1000)

sql = f"""
select
    *

from df_predict_ddb
where unique_id_r = '{epc_row_id}'
order by match_weight desc
limit 5
"""

con.sql(sql).show(max_width=10000)


# Waterfall
sql = f"""
select *
from df_predict_ddb
where unique_id_r = '{epc_row_id}' and unique_id_l in ('{our_uprn_match}')
order by match_weight desc
limit 10
"""

res = con.sql(sql)

print(f"Our match: {our_uprn_match}")
display(
    linker.visualisations.waterfall_chart(
        res.df().to_dict(orient="records"), filter_nulls=False
    )
)

# Waterfall
sql = f"""
select *
from df_predict_ddb
where unique_id_r = '{epc_row_id}' and unique_id_l in ('{correct_uprn}')
order by match_weight desc
limit 10
"""

res = con.sql(sql)

print(f"Correct match: {correct_uprn}")
display(
    linker.visualisations.waterfall_chart(
        res.df().to_dict(orient="records"), filter_nulls=False
    )
)

sql = f"""
select 'correct' as source, uprn, fulladdress
from {full_os_path}
where uprn = '{correct_uprn}'
UNION ALL
select 'our_match' as source, uprn, fulladdress
from {full_os_path}
where uprn = '{our_uprn_match}'
"""

con.sql(sql).show(max_width=100000)


# %%


# sql = f"""
# select unique_id, address_concat
# from epc_data_raw
# where upper(address_concat) like '%FLAT A 119%'
# """

# con.sql(sql).show(max_width=100000)
