import altair as alt
import time
import duckdb
import os
from uk_address_matcher import (
    clean_data_using_precomputed_rel_tok_freq,
    get_linker,
)
from uk_address_matcher.post_linkage.analyse_results import (
    best_matches_summary,
    best_matches_with_distinguishability,
)
from uk_address_matcher.post_linkage.identify_distinguishing_tokens import (
    improve_predictions_using_distinguishing_tokens,
)

from IPython.display import display

# -----------------------------------------------------------------------------
# Step 1: Load data
# -----------------------------------------------------------------------------
overall_start_time = time.time()
con = duckdb.connect(":default:")

# The os.getenv can be ignored, is just so this script can be run in the test suite
messy_path = os.getenv(
    "FHRS_PATH", "read_parquet('secret_data/fhrs/fhrs_data.parquet')"
)


sql = f"""
create or replace table df_messy as
select
    fhrsid as unique_id,
    concat_ws(' ', AddressLine1, AddressLine2, AddressLine3, AddressLine4)
        as address_concat,
    PostCode,
    lat as lat,
    lng as lng
from {messy_path}
order by random()
limit 10000
"""

con.execute(sql)
df_messy = con.table("df_messy")


## If you've pre-cleaned the OS data, you can load it directly without having to re-clean
# sql = """
# create or replace view os_clean as
# select * from read_parquet('secret_data/ord_surv/os_clean.parquet')
# where postcode in (
# select distinct postcode from df_messy
# )
# """
# con.execute(sql)
# df_os_clean = con.table("os_clean")


# Otherwise load raw os data and clean it

full_os_path = os.getenv(
    "FULL_OS_PATH",
    "read_parquet('secret_data/ord_surv/raw/add_gb_builtaddress_sorted_zstd.parquet')",
)

sql = f"""
create or replace table os as
select
    uprn as unique_id,
    'canonical' as source_dataset,
   regexp_replace(fulladdress, ',[^,]*$', '') AS address_concat,
   postcode,
    latitude as lat,
    longitude as lng
from {full_os_path}
where postcode in
(select distinct postcode from df_messy)

and description != 'Non Addressable Object'

"""
con.execute(sql)
df_os = con.table("os")
df_os_clean = clean_data_using_precomputed_rel_tok_freq(df_os, con=con)

# -----------------------------------------------------------------------------
# Step 2: Clean data
# -----------------------------------------------------------------------------


messy_count = df_messy.count("*").fetchall()[0][0]
canonical_count = df_os_clean.count("*").fetchall()[0][0]

df_messy_clean = clean_data_using_precomputed_rel_tok_freq(df_messy, con=con)


end_time = time.time()
print(f"Time to load/clean: {end_time - overall_start_time} seconds")


# -----------------------------------------------------------------------------
# Step 3: Link data - pass 1
# -----------------------------------------------------------------------------

linker = get_linker(
    df_addresses_to_match=df_messy_clean,
    df_addresses_to_search_within=df_os_clean,
    con=con,
    include_full_postcode_block=True,
    include_outside_postcode_block=True,
    additional_columns_to_retain=["lat", "lng"],
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
    additional_columns_to_retain=["lat", "lng"],
)

end_time = time.time()
print(f"Improve time taken: {end_time - start_time} seconds")
print(
    f"Full time taken: {end_time - overall_start_time} seconds to match {messy_count:,.0f} messy addresses to {canonical_count:,.0f} canonical addresses at a rate of {messy_count / (end_time - overall_start_time):,.0f} addresses per second"
)

# -----------------------------------------------------------------------------
# Step 5: Inspect results
# -----------------------------------------------------------------------------

best_matches = best_matches_with_distinguishability(
    df_predict=df_predict_ddb,
    df_addresses_to_match=df_messy,
    con=con,
    additional_columns_to_retain=["lat", "lng"],
)


dsum_1 = best_matches_summary(
    df_predict=df_predict_ddb, df_addresses_to_match=df_messy, con=con
)
dsum_1.show(max_width=500)


# -----------------------------------------------------------------------------
# Step 6: Compare geocoding of original data vs new matches
# -----------------------------------------------------------------------------

best_matches.show(max_width=500)

record_number_to_view = 1

sql = """
SELECT
    bm.unique_id_r AS messy_id,
    bm.unique_id_l AS canonical_id,
    concat_ws(' ', bm.original_address_concat_l, bm.postcode_l) AS messy_address,
    concat_ws(' ', bm.address_concat_r, bm.postcode_r) AS canonical_address,

    lat_r, lat_l, lng_r, lng_l,
    -- Calculate great circle distance in kilometers using the Haversine formula
    6371 * 2 * ASIN(
        SQRT(
            POWER(SIN((RADIANS(lat_l) - RADIANS(lat_r)) / 2), 2) +
            COS(RADIANS(lat_r)) * COS(RADIANS(lat_l)) *
            POWER(SIN((RADIANS(lng_l) - RADIANS(lng_r)) / 2), 2)
        )
    ) AS distance_km,
    bm.match_weight,
    bm.distinguishability,
from best_matches bm
ORDER BY distance_km DESC
"""

geocoding_comparison = con.sql(sql)


sql = """
select * from geocoding_comparison
where match_weight > -2
and distinguishability > 5
and distance_km is not null
and distance_km < 5
"""
df_distances = con.sql(sql)

# Create histogram of distances
distance_histogram = (
    alt.Chart(df_distances.to_df())
    .mark_bar()
    .encode(
        alt.X("distance_km:Q", bin=alt.Bin(maxbins=30), title="Distance (km)"),
        alt.Y("count()", title="Number of Addresses"),
        tooltip=["count()", alt.Tooltip("distance_km:Q", bin=alt.Bin(maxbins=30))],
    )
    .properties(
        title="Distribution of Distances Between Matched Addresses",
        width=600,
        height=400,
    )
)

display(distance_histogram)

# Pull out a list of addresses that we matched that didn't have lat lng
# and a list of addresses we matched where we think we have a better geocode

sql = """
select *
from geocoding_comparison

where

-- they don't have a geocode
(

    (lat_r is null or distance_km > 0.05)
    and
    (
    (match_weight > 10 and distinguishability > 8)
    or
    (match_weight > 15 and distinguishability is null)
    )
)
order by random()
"""
con.sql(sql).show(max_width=5000)


# -----------------------------------------------------------------------------
# Step 7: Compare geocoding of original data vs new matches, single records
# -----------------------------------------------------------------------------


sql = """
select messy_id
from geocoding_comparison
where distance_km is not null
order by random()
limit 1
"""
record_to_view = con.sql(sql).fetchone()[0]


sql = f"""
select unique_id_l
from df_predict_improved
where unique_id_r = '{record_to_view}'
order by match_weight desc
limit 1
"""
best_match_id = con.sql(sql).fetchone()[0]
best_match_id


sql = f"""
with t1 as (
select *
 from geocoding_comparison


where messy_id = '{record_to_view}'
)
select
    'messy' as source,
    messy_address as address,
    lat_r as lat,
    lng_r as lng,
    match_weight,
    distinguishability,


from t1

UNION ALL
select
    'canonical' as source,
    canonical_address as address,
    lat_l as lat,
    lng_l as lng,
    match_weight,
    distinguishability,

from t1
"""
con.sql(sql).show(max_width=700)

# Create improved map URLs with centered view and appropriate zoom level
sql_improved_map = f"""
SELECT
    messy_address,
    canonical_address,
    lat_r,
    lng_r,
    lat_l,
    lng_l,
    distance_km,
    match_weight,
    distinguishability,
    -- Calculate center point as average of coordinates
    (lat_r + lat_l) / 2 AS center_lat,
    (lng_r + lng_l) / 2 AS center_lng
FROM geocoding_comparison

where messy_id = '{record_to_view}'

"""

map_examples = con.sql(sql_improved_map).to_df()


for i, row in map_examples.iterrows():
    center_lat = row["center_lat"]
    center_lng = row["center_lng"]
    lat_r = row["lat_r"]
    lng_r = row["lng_r"]
    lat_l = row["lat_l"]
    lng_l = row["lng_l"]

    map_url = (
        f"https://www.robinlinacre.com/url-map/"
        f"?marker={lat_r},{lng_r}"
        f"&marker={lat_l},{lng_l}"
        f"&center={center_lat},{center_lng}"
        f"&zoom=16"
    )

    google_maps_url = (
        f"https://www.google.com/maps/dir/?api=1"
        f"&origin={lat_r},{lng_r}"
        f"&destination={lat_l},{lng_l}"
    )

    print(f"\nDistance: {row['distance_km']:.2f} km")
    print(f"Messy address: {row['messy_address']}")
    print(f"Canonical address: {row['canonical_address']}")
    print(f"Map URL (red=messy, blue=canonical) {map_url}")
    print(f"Google Maps directions: {google_maps_url}")
