from tests.utils import prepare_combined_test_data
from uk_address_matcher import clean_data_using_precomputed_rel_tok_freq, get_linker
from uk_address_matcher.post_linkage.identify_distinguishing_tokens import (
    improve_predictions_using_distinguishing_tokens,
)
import duckdb
from splink import block_on
from IPython.display import display

MATCH_WEIGHT_THRESHOLD_PREDICT = -50
MATCH_WEIGHT_THRESHOLD_IMPROVE = -20

duckdb_con = duckdb.connect(database=":memory:")
yaml_path = "tests/test_addresses.yaml"

# Prepare data
messy_addresses, canonical_addresses = prepare_combined_test_data(yaml_path, duckdb_con)


test_block = 11
USE_BIGRAMS = True

messy_addresses = messy_addresses.filter(f"test_block = {test_block}")
messy_addresses
canonical_addresses = canonical_addresses.filter(f"test_block = {test_block}")
canonical_addresses
# Clean the input data
messy_clean = clean_data_using_precomputed_rel_tok_freq(messy_addresses, con=duckdb_con)
canonical_clean = clean_data_using_precomputed_rel_tok_freq(
    canonical_addresses, con=duckdb_con
)

# Configure the linker
columns_to_retain = ["original_address_concat", "true_match_id"]
linker = get_linker(
    df_addresses_to_match=messy_clean,
    df_addresses_to_search_within=canonical_clean,
    con=duckdb_con,
    include_full_postcode_block=True,
    additional_columns_to_retain=columns_to_retain,
)
linker._settings_obj._blocking_rules_to_generate_predictions = [
    block_on("test_block").get_blocking_rule("duckdb")
]

# Predict matches (first pass)
predicted_matches = linker.inference.predict(
    threshold_match_weight=MATCH_WEIGHT_THRESHOLD_PREDICT,
).as_duckdbpyrelation()


# Improve predictions (second pass)
improved_matches = improve_predictions_using_distinguishing_tokens(
    df_predict=predicted_matches,
    con=duckdb_con,
    match_weight_threshold=MATCH_WEIGHT_THRESHOLD_IMPROVE,
    use_bigrams=USE_BIGRAMS,
)


sql = """
select distinct concat_ws(' ', original_address_concat_r, postcode_r) as messy_address
from improved_matches
"""

duckdb_con.sql(sql).show(max_width=700)

# Is best mrach true match?

sql = """
with best_match as (
    select unique_id_r, unique_id_l from improved_matches
    order by match_weight desc
    limit 1
)
select b.unique_id_l, b.unique_id_r, p.true_match_id_r
  from predicted_matches p
inner join best_match b
on p.unique_id_r = b.unique_id_r
and p.unique_id_l = b.unique_id_l

"""
best_match_id, messy_id, true_match_id = duckdb_con.sql(sql).fetchone()

bi_tri_cols = ""
if USE_BIGRAMS:
    bi_tri_cols += """
    ,
    overlapping_bigrams_this_l_and_r,
    bigrams_elsewhere_in_block_but_not_this,
    overlapping_bigrams_this_l_and_r_filtered
    ,
    bigrams_elsewhere_in_block_but_not_this_filtered



    """


sql = f"""
select
match_weight,
match_weight_original,
mw_adjustment,
concat(original_address_concat_l, ' ', postcode_l) as canonical_address,
case
when unique_id_l = '{true_match_id}' then '✅'
else ''
end as true_match,
overlapping_tokens_this_l_and_r,
tokens_elsewhere_in_block_but_not_this
{bi_tri_cols},
missing_tokens
from improved_matches
order by match_weight desc
"""

duckdb_con.sql(sql).show(max_width=700)


# Next print out how the messy address and the matches were parsed
cols = """
    concat_ws(' ', original_address_concat, postcode) as original_address_concat,
    flat_positional,
    flat_letter,
    numeric_token_1,
    numeric_token_2,
    numeric_token_3,
    array_transform(token_rel_freq_arr, x -> x.tok) as tok_arr,
    array_transform(common_end_tokens, x -> x.tok) as cet_arr,
    unique_id
    """

additional_sql = ""
true_match_text = "true match and best match"
if true_match_id != best_match_id:
    additional_sql = f"""
    UNION ALL
    select 'best match' as source, {cols}
    from canonical_clean
    where unique_id = '{best_match_id}'
    """
    true_match_text = "true match"


sql = f"""
select 'messy' as source, {cols}
from messy_clean
where unique_id = '{messy_id}'
{additional_sql}
UNION ALL
select '{true_match_text}' as source, {cols}
from canonical_clean
where unique_id = '{true_match_id}'
"""

# duckdb_con.sql(sql).show(max_width=700)


# print("Waterfall chart for true match")
recs = (
    predicted_matches.filter("true_match_id_r = unique_id_l")
    .df()
    .to_dict(orient="records")
)
# display(linker.visualisations.waterfall_chart(recs, filter_nulls=False))


if true_match_id != best_match_id:
    # print("Waterfall chart for best match")
    recs = (
        predicted_matches.filter(f"unique_id_l = {best_match_id}")
        .df()
        .to_dict(orient="records")
    )
    # display(linker.visualisations.waterfall_chart(recs, filter_nulls=False))


# print("Pre-second pass weights")
sql = f"""
with t as (
select
match_weight,
case
when unique_id_l = '{true_match_id}' then '✅'
else ''
end as true_match,
COLUMNS('^(t[^f_].*|b[^f_].*|[^tb].*[_l|_r]$)') AS '\\1'
from predicted_matches
)
select * EXCLUDE (source_dataset_l, source_dataset_r) from t
order by match_weight desc
"""

# duckdb_con.sql(sql).show(max_width=700)


# sql = """
# WITH t1 AS (
#     SELECT
#         overlapping_tokens_this_l_and_r
#             .map_entries()
#             .list_filter(x -> x.value IN (1) OR 1=1)
#             .map_from_entries() AS overlapping_tokens_this_l_and_r_count_1,

#         tokens_elsewhere_in_block_but_not_this,

#         overlapping_bigrams_this_l_and_r
#             .map_entries()
#             .list_filter(x -> x.value IN (1) OR 1=1)
#             .map_from_entries() AS overlapping_bigrams_this_l_and_r_count_1,

#         bigrams_elsewhere_in_block_but_not_this,
#         missing_tokens
#     FROM improved_matches
#     ORDER BY match_weight DESC
# )

# SELECT
#     overlapping_bigrams_this_l_and_r_count_1
#         .map_entries()
#         .list_filter(x ->
#             NOT (
#                 (
#                     map_contains(overlapping_tokens_this_l_and_r_count_1, x.key[1])
#                     AND overlapping_tokens_this_l_and_r_count_1[x.key[1]] <= x.value
#                 )
#                 OR
#                 (
#                     map_contains(overlapping_tokens_this_l_and_r_count_1, x.key[2])
#                     AND overlapping_tokens_this_l_and_r_count_1[x.key[2]] <= x.value
#                 )
#             )
#         )
#         .map_from_entries() AS a
# FROM t1;
# """

# duckdb_con.sql(sql).show(max_width=700)
