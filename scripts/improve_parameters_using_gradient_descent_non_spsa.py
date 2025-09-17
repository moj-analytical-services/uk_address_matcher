import numpy as np
import os
import pandas as pd
import altair as alt
from IPython.display import display, clear_output
import duckdb
from uk_address_matcher import (
    clean_data_using_precomputed_rel_tok_freq,
    get_linker,
    best_matches_with_distinguishability,
)
from uk_address_matcher.post_linkage.identify_distinguishing_tokens import (
    improve_predictions_using_distinguishing_tokens,
)
from uk_address_matcher.linking_model.training import get_settings_for_training

import logging
import json

from datetime import datetime

#  We will clean the data once and store it
if os.path.exists("del.duckdb"):
    os.remove("del.duckdb")
con_disk = duckdb.connect("del.duckdb")

full_os_path = (
    "read_parquet('secret_data/ord_surv/raw/add_gb_builtaddress_sorted_zstd.parquet')"
)


path = "secret_data/labels/*.jsonl"


sql = f"""
CREATE TABLE labels_raw AS
select * from
read_json('{path}', filename=labels_filename, columns={{
    'messy_id': 'VARCHAR',
    'correct_uprn': 'VARCHAR',
    'confidence': 'VARCHAR'
}})
where  not contains(correct_uprn, '[')
and nullif(correct_uprn, '') is not null
and try_cast(correct_uprn as BIGINT) in (select uprn from {full_os_path})
"""
con_disk.execute(sql)

sql = """
CREATE TABLE labels_all AS

-- Labels from LLM labelling of fhrs data
select *
from labels_raw
WHERE CONTAINS(labels_filename, 'fhrs') and CONTAINS(labels_filename, 'llm')
and confidence = 'certain'

UNION ALL
-- Labels from LLM labelling of EPC data where splink and EPC did not match
select *
from labels_raw
WHERE CONTAINS(labels_filename, 'epc') and CONTAINS(labels_filename, 'llm')
and confidence = 'certain'

UNION ALL
-- Labels from auto agreement of EPC and splink matcher
SELECT * FROM (
    SELECT
        *
    FROM labels_raw
    WHERE CONTAINS(labels_filename, 'epc') AND CONTAINS(labels_filename, 'auto')
    and
    messy_id not in
    (
    select messy_id from labels_raw
     WHERE CONTAINS(labels_filename, 'epc') and CONTAINS(labels_filename, 'llm')
    )
    ORDER BY substr(messy_id, 4, 4)
    LIMIT 3000
)
"""
con_disk.execute(sql)
labels = con_disk.table("labels_all")
labels
labels.aggregate(
    "coalesce(labels_filename, 'total') as labels_count, count(*)",
    "cube(labels_filename)",
).show()

# labels like
# ┌────────────────────────────────┬──────────────┬──────────────────────┬───────────────────────────────────────────────┐
# │            messy_id            │ correct_uprn │      confidence      │                labels_filename                │
# │            varchar             │   varchar    │       varchar        │                    varchar                    │
# ├────────────────────────────────┼──────────────┼──────────────────────┼───────────────────────────────────────────────┤
# │ 1000661                        │ 100052007636 │ certain              │ secret_data/labels/llm_labels_3000_fhrs.jsonl │
# │ 1

epc_path = (
    "read_csv('secret_data/epc/raw/domestic-*/certificates.csv', filename=epc_filename)"
)


fhrs_path = "read_parquet('secret_data/fhrs/fhrs_data.parquet')"

sql = f"""
select DISTINCT ON (fhrsid) *
from {fhrs_path}
"""
fhrs_deduped = con_disk.sql(sql)

# FHRS data has duplicate rows for some reason


sql = f"""
CREATE OR REPLACE TABLE messy_data AS
-- Labels from LLM labelling of fhrs data
SELECT
    f.fhrsid AS unique_id,
    CONCAT_WS(' ', f.BusinessName, f.AddressLine1, f.AddressLine2, f.AddressLine3, f.AddressLine4) AS address_concat,
    f.PostCode AS postcode,
    l.labels_filename
FROM fhrs_deduped f
INNER JOIN labels l ON f.fhrsid = l.messy_id
WHERE CONTAINS(l.labels_filename, 'fhrs') AND CONTAINS(l.labels_filename, 'llm')

UNION ALL

-- Labels from LLM labelling of EPC data where splink and EPC did not match
SELECT
    e.LMK_KEY AS unique_id,
    CONCAT_WS(' ', e.ADDRESS1, e.ADDRESS2, e.ADDRESS3) AS address_concat,
    e.POSTCODE AS postcode,
    l.labels_filename
FROM {epc_path} e
INNER JOIN labels l ON e.LMK_KEY = l.messy_id
WHERE CONTAINS(l.labels_filename, 'epc') AND CONTAINS(l.labels_filename, 'llm')

UNION ALL

-- Labels from auto agreement of EPC and splink matcher
SELECT
    e.LMK_KEY AS unique_id,
    CONCAT_WS(' ', e.ADDRESS1, e.ADDRESS2, e.ADDRESS3) AS address_concat,
    e.POSTCODE AS postcode,
    l.labels_filename
FROM {epc_path} e
INNER JOIN labels l ON e.LMK_KEY = l.messy_id
WHERE CONTAINS(l.labels_filename, 'epc') AND CONTAINS(l.labels_filename, 'auto')

"""

con_disk.execute(sql)
messy_data = con_disk.table("messy_data")


messy_data.aggregate(
    "coalesce(labels_filename, 'total') as messy_data_count, count(*)",
    "cube(labels_filename)",
).show()


# Messy data like
# ┌──────────────────────┬────────────────────────────────────┬──────────┬───────────────────────────────────────────────┐
# │      unique_id       │           address_concat           │ postcode │                labels_filename                │
# │       varchar        │              varchar               │ varchar  │                    varchar                    │
# ├──────────────────────┼────────────────────────────────────┼──────────┼───────────────────────────────────────────────┤
# │ 52714                │ White Heather Hotel White Heathe…  │ LL30 2NR │ secret_data/labels/llm_labels_3000_fhrs.jsonl │
# │ 1701094              │ Champs Cafe Champs Cafe 64 Drome…  │ CH5 2LR  │ secret_data/labels/llm_labels_3000_fhrs.jsonl │


sql = f"""
create or replace table os as
select
uprn as unique_id,
regexp_replace(fulladdress, ',[^,]*$', '') AS address_concat,
postcode
from {full_os_path}
where
(postcode in (select distinct postcode from messy_data))
or
(uprn in (select try_cast(correct_uprn as BIGINT) from labels))


"""
con_disk.execute(sql)
df_os = con_disk.table("os")

# df os like:
# ┌─────────────┬────────────────────────────────────────────────────────────────────────────────────┬──────────┐
# │  unique_id  │                                   address_concat                                   │ postcode │
# │    int64    │                                      varchar                                       │ varchar  │
# ├─────────────┼────────────────────────────────────────────────────────────────────────────────────┼──────────┤
# │    25035526 │ 10,  SOME ADDRESS GOES HERE.                                                       │ MK14 6EF │


messy_count = messy_data.count("*").fetchall()[0][0]
canonical_count = df_os.count("*").fetchall()[0][0]
print(f"messy_count: {messy_count:,}, canonical_count: {canonical_count:,}")


# -----------------------------------------------------------------------------
# Step 2: Clean data
# -----------------------------------------------------------------------------

df_messy_data_clean = clean_data_using_precomputed_rel_tok_freq(
    messy_data, con=con_disk
)
sql = """
create table messy_data_clean as
select * exclude (labels_filename) from df_messy_data_clean
"""
con_disk.execute(sql)


df_os_clean = clean_data_using_precomputed_rel_tok_freq(df_os, con=con_disk)
sql = """
create table os_clean as
select * from df_os_clean
"""
con_disk.execute(sql)


def black_box(
    *,
    IMPROVE_DISTINGUISHING_MWT=-10,
    REWARD_MULTIPLIER=3,
    PUNISHMENT_MULTIPLIER=1.5,
    BIGRAM_REWARD_MULTIPLIER=3,
    BIGRAM_PUNISHMENT_MULTIPLIER=1.5,
    MISSING_TOKEN_PENALTY=0.1,
    NUM_1_WEIGHT_1=6.57,
    NUM_1_WEIGHT_2=6.57,
    NUM_1_WEIGHT_3=4,
    NUM_1_WEIGHT_4=1 / 16,
    NUM_1_WEIGHT_5=1 / 256,
    NUM_2_WEIGHT_1=10,
    NUM_2_WEIGHT_2=1,
    NUM_2_WEIGHT_3=1 / 16,
    NUM_2_WEIGHT_4=1 / 256,
    FLAT_POSITIONAL_WEIGHT_1=6.57,
    FLAT_POSITIONAL_WEIGHT_2=6.57,
    FLAT_POSITIONAL_WEIGHT_3=0,
    FLAT_POSITIONAL_WEIGHT_4=0,
    FLAT_POSITIONAL_WEIGHT_5=-5,
    REL_FREQ_START_EXP=4,
    REL_FREQ_START_WEIGHT=-4,
    REL_FREQ_SEGMENT_1=8,
    REL_FREQ_SEGMENT_2=8,
    REL_FREQ_SEGMENT_3=8,
    REL_FREQ_SEGMENT_4=10,
    REL_FREQ_DELTA_WEIGHT_1=1,
    REL_FREQ_DELTA_WEIGHT_2=1,
    REL_FREQ_DELTA_WEIGHT_3=0.25,
    REL_FREQ_DELTA_WEIGHT_4=0.25,
    REL_FREQ_PUNISHMENT_MULTIPLIER=0.33,
    FIRST_N_TOKENS_WEIGHT_1=10,
    FIRST_N_TOKENS_WEIGHT_2=5,
    FIRST_N_TOKENS_WEIGHT_3=0,
    FIRST_N_TOKENS_WEIGHT_4=0,
    FIRST_N_TOKENS_WEIGHT_5=-2,
    output_match_weights_chart=False,
):
    start_time = datetime.now()
    con = duckdb.connect(":memory:")
    sql = "attach 'del.duckdb' as cleaned;"
    con.execute(sql)

    sql = """
    create table labels_all as
    select * from cleaned.labels_all
    """
    con.execute(sql)
    labels_all = con.table("labels_all")

    sql = """
    create table df_messy_data_clean as
    select * from cleaned.messy_data_clean
    """
    con.execute(sql)
    df_messy_data_clean = con.table("df_messy_data_clean")

    sql = """

    create table df_os_clean as
    select * from cleaned.os_clean
    """
    con.execute(sql)
    df_os_clean = con.table("df_os_clean")

    sql = """
    create table messy_data as
    select * from cleaned.messy_data
    """
    con.execute(sql)

    messy_data = con.table("messy_data")

    con.execute("detach cleaned")
    df_os_clean = con.table("df_os_clean")

    end_time = datetime.now()
    # print(
    #     f"Time taken to load data: {round((end_time - start_time).total_seconds(), 2)}"
    # )

    start_time = datetime.now()
    settings = get_settings_for_training(
        num_1_weights={
            "WEIGHT_1": NUM_1_WEIGHT_1,
            "WEIGHT_2": NUM_1_WEIGHT_2,
            "WEIGHT_3": NUM_1_WEIGHT_3,
            "WEIGHT_4": NUM_1_WEIGHT_4,
            "WEIGHT_5": NUM_1_WEIGHT_5,
        },
        num_2_weights={
            "WEIGHT_1": NUM_2_WEIGHT_1,
            "WEIGHT_2": NUM_2_WEIGHT_2,
            "WEIGHT_3": NUM_2_WEIGHT_3,
            "WEIGHT_4": NUM_2_WEIGHT_4,
        },
        flat_positional_weights={
            "WEIGHT_1": FLAT_POSITIONAL_WEIGHT_1,
            "WEIGHT_2": FLAT_POSITIONAL_WEIGHT_2,
            "WEIGHT_3": FLAT_POSITIONAL_WEIGHT_3,
            "WEIGHT_4": FLAT_POSITIONAL_WEIGHT_4,
            "WEIGHT_5": FLAT_POSITIONAL_WEIGHT_5,
        },
        first_n_tokens_weights={
            "WEIGHT_1": FIRST_N_TOKENS_WEIGHT_1,
            "WEIGHT_2": FIRST_N_TOKENS_WEIGHT_2,
            "WEIGHT_3": FIRST_N_TOKENS_WEIGHT_3,
            "WEIGHT_4": FIRST_N_TOKENS_WEIGHT_4,
            "WEIGHT_5": FIRST_N_TOKENS_WEIGHT_5,
        },
        token_rel_freq_arr_comparison={
            "START_EXP": REL_FREQ_START_EXP,
            "START_WEIGHT": REL_FREQ_START_WEIGHT,
            "SEGMENTS": [
                REL_FREQ_SEGMENT_1,
                REL_FREQ_SEGMENT_2,
                REL_FREQ_SEGMENT_3,
                REL_FREQ_SEGMENT_4,
            ],
            "DELTA_WEIGHTS_WITHIN_SEGMENTS": [
                REL_FREQ_DELTA_WEIGHT_1,
                REL_FREQ_DELTA_WEIGHT_2,
                REL_FREQ_DELTA_WEIGHT_3,
                REL_FREQ_DELTA_WEIGHT_4,
            ],
        },
    )
    # import json

    # print(json.dumps(settings.get_settings("duckdb").as_dict(), indent=4))

    linker = get_linker(
        df_addresses_to_match=df_messy_data_clean,
        df_addresses_to_search_within=df_os_clean,
        con=con,
        include_full_postcode_block=False,
        include_outside_postcode_block=True,
        retain_intermediate_calculation_columns=True,
        settings=settings,
    )
    logging.getLogger("splink").setLevel(logging.ERROR)
    if output_match_weights_chart:
        c = linker.visualisations.match_weights_chart()
        c.save("match_weights_chart.html")

    end_time = datetime.now()

    # print(
    #     f"Time taken to run match weights chart: {round((end_time - start_time).total_seconds(), 2)}"
    # )

    start_time = datetime.now()

    df_predict = linker.inference.predict(threshold_match_weight=-20)
    df_predict_ddb = df_predict.as_duckdbpyrelation()

    end_time = datetime.now()
    # print(
    #     f"Time taken to run predict: {round((end_time - start_time).total_seconds(), 2)}"
    # )

    start_time = datetime.now()

    USE_BIGRAMS = True

    start_time = datetime.now()
    df_predict_improved = improve_predictions_using_distinguishing_tokens(
        df_predict=df_predict_ddb,
        con=con,
        match_weight_threshold=IMPROVE_DISTINGUISHING_MWT,
        top_n_matches=5,
        use_bigrams=USE_BIGRAMS,
        REWARD_MULTIPLIER=REWARD_MULTIPLIER,
        PUNISHMENT_MULTIPLIER=PUNISHMENT_MULTIPLIER,
        BIGRAM_REWARD_MULTIPLIER=BIGRAM_REWARD_MULTIPLIER,
        BIGRAM_PUNISHMENT_MULTIPLIER=BIGRAM_PUNISHMENT_MULTIPLIER,
        MISSING_TOKEN_PENALTY=MISSING_TOKEN_PENALTY,
    )

    end_time = datetime.now()
    # print(
    #     f"Time taken to run improve predictions: {round((end_time - start_time).total_seconds(), 2)}"
    # )

    # How many of the labels are in df_predict_improved

    start_time = datetime.now()

    weight_bounds_sql = """
    SELECT
        MIN(match_weight) AS min_weight,
        MAX(match_weight) AS max_weight
    FROM df_predict_improved
    """
    weight_bounds = con.sql(weight_bounds_sql)
    min_weight, max_weight = weight_bounds.fetchall()[0]

    # Step 2: Create improved_with_labels using the weight bounds
    # Get the min and max weight values from the weight_bounds table

    improved_with_labels_sql = f"""
    SELECT
        l.messy_id AS messy_id,
        p.unique_id_r,
        p.unique_id_l,
        (p.match_weight - {min_weight}) / NULLIF({max_weight} - {min_weight}, 0) AS normalized_match_weight,
        l.correct_uprn
    FROM labels_all l
    LEFT JOIN df_predict_improved p ON l.messy_id = p.unique_id_r
    """
    improved_with_labels = con.sql(improved_with_labels_sql)

    # Step 3: Create ranked data
    ranked_sql = """
    SELECT
        messy_id,
        unique_id_r,
        unique_id_l,
        normalized_match_weight,
        correct_uprn,
        ROW_NUMBER() OVER (PARTITION BY messy_id ORDER BY normalized_match_weight DESC) AS row_num
    FROM improved_with_labels
    """
    ranked = con.sql(ranked_sql)

    # Step 4: Create aggregated data
    aggregated_sql = """
    SELECT
        messy_id,
        MAX(normalized_match_weight) AS best_match_weight,
        MAX(CASE WHEN row_num = 1 THEN unique_id_l END) AS best_match_id,
        MAX(CASE WHEN unique_id_l = correct_uprn THEN normalized_match_weight END) AS true_match_weight,
        MAX(CASE WHEN unique_id_l = correct_uprn THEN unique_id_l END) AS true_match_id,
        MAX(CASE WHEN row_num = 2 THEN normalized_match_weight END) AS second_best_match_weight
    FROM ranked
    GROUP BY messy_id
    """
    aggregated = con.sql(aggregated_sql)

    # Step 5: Calculate rewards
    rewards_sql = """
    SELECT
        messy_id,
        CASE
            WHEN true_match_weight IS NULL THEN -0.2
            WHEN true_match_id != best_match_id THEN GREATEST(true_match_weight - best_match_weight, -0.2)
            WHEN true_match_id = best_match_id THEN LEAST(best_match_weight - second_best_match_weight, 0.2)
        END AS reward
    FROM aggregated
    """
    rewards = con.sql(rewards_sql)

    # Step 6: Add truth status
    to_score_sql = """
    SELECT
        messy_id,
        reward,
        CASE
            WHEN reward = 0 THEN 'indistinguishable true positive'
            WHEN reward > 0 THEN 'true positive'
            WHEN reward < 0 THEN 'false positive'
        END AS truth_status
    FROM rewards
    """
    to_score = con.sql(to_score_sql)

    # print count of to score
    # print("count of to score")
    # to_score.count("*").show()

    end_time = datetime.now()
    # print(
    #     f"Time taken to run to_score: {round((end_time - start_time).total_seconds(), 2)}"
    # )

    start_time = datetime.now()

    score = to_score.sum("reward").fetchall()[0][0]
    num_labels = labels_all.count("*").fetchall()[0][0]
    score = 5 * score / num_labels

    num_matches = (
        to_score.filter("truth_status = 'true positive'").count("*").fetchall()[0][0]
    )
    num_indeterminate = (
        to_score.filter("truth_status = 'indistinguishable true positive'")
        .count("*")
        .fetchall()[0][0]
    )
    num_non_matches = (
        to_score.filter("truth_status = 'false positive'").count("*").fetchall()[0][0]
    )

    print(
        f"Score: {score:,.2f},    Num Matches: {num_matches:,.0f}, Num Non Matches: {num_non_matches:,.0f}, Num Indeterminate: {num_indeterminate:,.0f}"
    )
    sum_of_matches_non_matches_ind = num_matches + num_non_matches + num_indeterminate
    print(
        f"Sum of matches, non matches and indeterminate: {sum_of_matches_non_matches_ind:,.0f}"
    )

    return {
        "score": score,
        "num_matches": num_matches,
        "num_non_matches": num_non_matches,
        "num_indeterminate": num_indeterminate,
    }


def get_params_dict(params):
    params_dict = {
        name: config["initial"]
        for name, config in param_config.items()
        if not config["optimize"]
    }
    optimized_params = dict(zip(param_names, params))
    params_dict.update(optimized_params)
    return params_dict


def black_box_reward(params, output_match_weights_chart=False):
    params_dict = get_params_dict(params)
    params_dict["output_match_weights_chart"] = output_match_weights_chart
    result = black_box(**params_dict)
    return result["score"]


def create_chart(history_df, iteration):
    line_chart = (
        alt.Chart(history_df)
        .mark_line(point=True)
        .encode(
            x=alt.X("iteration:O", title="Iteration"),
            y=alt.Y("value:Q", title="Value", scale=alt.Scale(zero=False)),
            tooltip=["iteration", "value"],
        )
    )
    text_chart = (
        alt.Chart(history_df)
        .mark_text(align="left", baseline="middle", dx=7, dy=-7, fontSize=10)
        .encode(
            x=alt.X("iteration:O"),
            y=alt.Y("value:Q"),
            text=alt.Text("value:Q", format=".3f"),
            tooltip=["iteration", "value"],
        )
    )
    combined_chart = alt.layer(line_chart, text_chart, data=history_df)

    # Define variable order with categories
    metrics = ["score", "num_matches", "num_non_matches", "num_indeterminate"]
    optimization_params = [
        name for name, config in param_config.items() if config["optimize"]
    ]
    fixed_params = [
        name
        for name, config in param_config.items()
        if not config["optimize"] and name not in metrics
    ]

    variable_order = metrics + optimization_params + fixed_params

    # Filter to only show metrics and optimized parameters by default
    filtered_df = history_df[history_df["variable"].isin(metrics + optimization_params)]

    facet_chart = (
        combined_chart.properties(height=50)
        .facet(
            row=alt.Row("variable:N", sort=variable_order, title=None).header(
                labelAngle=0, labelAlign="left"
            )
        )
        .resolve_scale(y="independent")
        .properties(title="Parameter Values by Iteration")
    )
    return facet_chart


# Parameter configuration and initialization remain unchanged
param_config = {
    # "IMPROVE_DISTINGUISHING_MWT": {
    #     "initial": -10,
    #     "optimize": False,
    #     "bounds": (-30, 5),
    #     "perturb": 1.0,
    # },
    "REWARD_MULTIPLIER": {
        "initial": 3,
        "optimize": True,
        "bounds": (0, 20),
        "perturb": 0.5,
    },
    "PUNISHMENT_MULTIPLIER": {
        "initial": 1.5,
        "optimize": True,
        "bounds": (0.0, 20),
        "perturb": 0.5,
    },
    "BIGRAM_REWARD_MULTIPLIER": {
        "initial": 3,
        "optimize": True,
        "bounds": (0, 20),
        "perturb": 0.5,
    },
    "BIGRAM_PUNISHMENT_MULTIPLIER": {
        "initial": 1.5,
        "optimize": True,
        "bounds": (0, 20),
        "perturb": 0.5,
    },
    "MISSING_TOKEN_PENALTY": {
        "initial": 0.1,
        "optimize": True,
        "bounds": (0.01, 10),
        "perturb": 0.05,
    },
    "NUM_1_WEIGHT_1": {
        "initial": 6.57,
        "optimize": True,
        "bounds": (1, 30),
        "perturb": 1.0,
    },
    "NUM_1_WEIGHT_2": {
        "initial": 6.57,
        "optimize": True,
        "bounds": (1, 30),
        "perturb": 1.0,
    },
    "NUM_1_WEIGHT_3": {
        "initial": 2,
        "optimize": True,
        "bounds": (0.1, 20),
        "perturb": 1.0,
    },
    "NUM_1_WEIGHT_4": {
        "initial": -4,
        "optimize": True,
        "bounds": (-10, 1),
        "perturb": 1.0,
    },
    "NUM_1_WEIGHT_5": {
        "initial": -8,
        "optimize": True,
        "bounds": (-20, -0.1),
        "perturb": 1.0,
    },
    "NUM_2_WEIGHT_1": {
        "initial": 6.57,
        "optimize": True,
        "bounds": (1, 20),
        "perturb": 1.0,
    },
    "NUM_2_WEIGHT_2": {
        "initial": 0,
        "optimize": True,
        "bounds": (-5, 10),
        "perturb": 1.0,
    },
    "NUM_2_WEIGHT_3": {
        "initial": -2,
        "optimize": True,
        "bounds": (-10, 0),
        "perturb": 1.0,
    },
    "NUM_2_WEIGHT_4": {
        "initial": -4,
        "optimize": True,
        "bounds": (-10, 1),
        "perturb": 1.0,
    },
    "FLAT_POSITIONAL_WEIGHT_1": {
        "initial": 6.57,
        "optimize": True,
        "bounds": (1, 30),
        "perturb": 1.0,
    },
    "FLAT_POSITIONAL_WEIGHT_2": {
        "initial": 6.57,
        "optimize": True,
        "bounds": (1, 30),
        "perturb": 1.0,
    },
    "FLAT_POSITIONAL_WEIGHT_3": {
        "initial": 0,
        "optimize": True,
        "bounds": (-10, 10),
        "perturb": 1.0,
    },
    "FLAT_POSITIONAL_WEIGHT_4": {
        "initial": 0,
        "optimize": True,
        "bounds": (-10, 10),
        "perturb": 1.0,
    },
    "FLAT_POSITIONAL_WEIGHT_5": {
        "initial": -5,
        "optimize": True,
        "bounds": (-10, 10),
        "perturb": 1.0,
    },
    # "FIRST_N_TOKENS_WEIGHT_1": {
    #     "initial": 8,
    #     "optimize": True,
    #     "bounds": (0, 20),
    #     "perturb": 1.0,
    # },
    # "FIRST_N_TOKENS_WEIGHT_2": {
    #     "initial": 4,
    #     "optimize": True,
    #     "bounds": (0, 20),
    #     "perturb": 1.0,
    # },
    # "FIRST_N_TOKENS_WEIGHT_3": {
    #     "initial": 3,
    #     "optimize": True,
    #     "bounds": (0, 20),
    #     "perturb": 1.0,
    # },
    # "FIRST_N_TOKENS_WEIGHT_4": {
    #     "initial": 0,
    #     "optimize": True,
    #     "bounds": (0, 20),
    #     "perturb": 1.0,
    # },
    # "FIRST_N_TOKENS_WEIGHT_5": {
    #     "initial": -2,
    #     "optimize": True,
    #     "bounds": (-10, 0),
    #     "perturb": 1.0,
    # },
    # "REL_FREQ_START_EXP": {
    #     "initial": 4,
    #     "optimize": False,
    #     "bounds": (0, 10),
    #     "perturb": 0.5,
    # },
    "REL_FREQ_START_WEIGHT": {
        "initial": -4,
        "optimize": True,
        "bounds": (-10, 0),
        "perturb": 0.5,
    },
    # "REL_FREQ_SEGMENT_1": {
    #     "initial": 8,
    #     "optimize": False,
    #     "bounds": (1, 20),
    #     "perturb": 1,
    # },
    # "REL_FREQ_SEGMENT_2": {
    #     "initial": 8,
    #     "optimize": False,
    #     "bounds": (1, 20),
    #     "perturb": 1,
    # },
    # "REL_FREQ_SEGMENT_3": {
    #     "initial": 8,
    #     "optimize": False,
    #     "bounds": (1, 20),
    #     "perturb": 1,
    # },
    # "REL_FREQ_SEGMENT_4": {
    #     "initial": 10,
    #     "optimize": False,
    #     "bounds": (1, 20),
    #     "perturb": 1,
    # },
    "REL_FREQ_DELTA_WEIGHT_1": {
        "initial": 1,
        "optimize": True,
        "bounds": (0.01, 5),
        "perturb": 0.1,
    },
    "REL_FREQ_DELTA_WEIGHT_2": {
        "initial": 1,
        "optimize": True,
        "bounds": (0.01, 5),
        "perturb": 0.1,
    },
    "REL_FREQ_DELTA_WEIGHT_3": {
        "initial": 0.25,
        "optimize": True,
        "bounds": (0.01, 2),
        "perturb": 0.1,
    },
    "REL_FREQ_DELTA_WEIGHT_4": {
        "initial": 0.25,
        "optimize": True,
        "bounds": (0.01, 2),
        "perturb": 0.1,
    },
    # "REL_FREQ_PUNISHMENT_MULTIPLIER": {
    #     "initial": 0.33,
    #     "optimize": True,
    #     "bounds": (0, 1),
    #     "perturb": 0.03,
    # },
}

# Optionally randomise the initial parameters within the bounds
# for name, config in param_config.items():
#     if config["optimize"]:
#         config["initial"] = np.random.uniform(config["bounds"][0], config["bounds"][1])
#         config["initial"] = config["bounds"][0]


param_names = [name for name, config in param_config.items() if config["optimize"]]
initial_params_array = [param_config[name]["initial"] for name in param_names]
lower_bounds = np.array([param_config[name]["bounds"][0] for name in param_names])
upper_bounds = np.array([param_config[name]["bounds"][1] for name in param_names])
perturb_scale = np.array([param_config[name]["perturb"] for name in param_names])

alpha = 4.0
alpha_decay = 0.995
min_alpha = 0.0001
momentum = (
    0.1  # Note: Momentum is not used in the provided loop, velocity isn't updated/used
)
num_iterations = 400
perterb_multiplier_to_compute_gradient = 0.3

params = np.array(initial_params_array)
num_params = len(params)
velocity = np.zeros(
    num_params
)  # Initialize velocity even if not used in the provided loop snippet

# Initial computation (optional but good practice)
initial_params_dict = get_params_dict(params)
initial_result = black_box(**initial_params_dict)
initial_score = initial_result["score"]
best_score = initial_score
best_params = params.copy()


history = []


print("Starting Optimization Loop...")
# Optimization loop
for iteration in range(num_iterations):
    iteration_start_time = datetime.now()  # Use a different name
    print(f"\n--- Iteration {iteration} ---")
    alpha = max(alpha * alpha_decay, min_alpha)
    gradient = np.zeros(num_params)

    # --- Calculate Base Reward and Details ---
    print("Calculating base reward...")
    base_params_dict = get_params_dict(params)
    base_result = black_box(**base_params_dict)  # Call full function
    base_score = base_result["score"]
    base_num_matches = base_result["num_matches"]
    print(f"  Base Score: {base_score:.10f}, Base Matches: {base_num_matches}")

    print("Calculating gradients...")
    # --- Gradient Calculation Loop with Detailed Debugging ---
    for idx in range(num_params):
        param_name = param_names[idx]

        # Skip if this parameter is not to be optimized
        if not param_config[param_name]["optimize"]:
            print(
                f"  Skipping gradient for {param_name}: Parameter not set for optimization."
            )
            gradient[idx] = 0
            continue

        perturb_size = perterb_multiplier_to_compute_gradient * perturb_scale[idx]

        if perturb_scale[idx] == 0 or perturb_size == 0:
            print(f"  Skipping gradient for {param_name}: Zero perturbation scale.")
            gradient[idx] = 0
            continue  # Skip if no perturbation defined

        # Create perturbed parameters
        params_perturbed = params.copy()
        params_perturbed[idx] += perturb_size

        # --- Crucial: Clip the perturbed parameter *before* evaluation ---
        # This mimics what your original code did and might be the cause of zero gradients
        original_param_val = params[idx]
        perturbed_val_before_clip = params_perturbed[idx]
        params_perturbed[idx] = np.clip(
            params_perturbed[idx], lower_bounds[idx], upper_bounds[idx]
        )
        perturbed_val_after_clip = params_perturbed[idx]

        # Check if clipping made the perturbation ineffective
        if perturbed_val_after_clip == original_param_val:
            print(
                f"  Gradient for {param_name}: 0.000000 (Perturbation clipped back to original value)"
            )
            gradient[idx] = 0
            # Optionally: You could try a backward difference here if needed
            # perturb_size = -perturb_size ... recompute params_perturbed ... etc.
        else:
            # Evaluate with the (potentially clipped) perturbed parameters
            params_plus_dict = get_params_dict(
                params_perturbed
            )  # Use the modified params array
            result_plus = black_box(**params_plus_dict)
            score_plus = result_plus["score"]
            num_matches_plus = result_plus["num_matches"]

            # Calculate gradient
            gradient[idx] = (
                score_plus - base_score
            ) / perturb_size  # Use the *intended* perturb size

            # --- Detailed Print ---
            print(f"  Gradient for {param_name}:")
            print(
                f"    Base          -> Score: {base_score:.10f}, Matches: {base_num_matches}"
            )
            # --- ADDED original_param_val HERE ---
            print(
                f"    Values        -> Original: {original_param_val:.6f}, Perturbed (Before Clip): {perturbed_val_before_clip:.6f}, Perturbed (After Clip): {perturbed_val_after_clip:.6f}"
            )
            # --- END ADDITION ---
            print(
                f"    Perturbed(+)  -> Score: {score_plus:.10f}, Matches: {num_matches_plus}"
            )
            print(
                f"    Score Diff: {(score_plus - base_score):.10f}, Perturb Size: {perturb_size:.6f}"
            )
            print(f"    Calculated Gradient: {gradient[idx]:.10f}")

    print(f"\n  Iteration {iteration} Summary:")
    # Using list comprehension for cleaner formatting
    print(f"    Gradient: {[f'{g:.6f}' for g in gradient]}")
    print(f"    Parameters (before update): {[f'{p:.6f}' for p in params]}")
    print(f"    Current alpha: {alpha:.6f}")

    # --- Parameter Update ---
    # Compute the update step (Note: Original loop code didn't update velocity)
    # If you want momentum, you need: velocity = momentum * velocity + alpha * gradient
    # update_step = velocity
    # Otherwise, just use the gradient:
    update_step = alpha * gradient

    params_before_update = params.copy()  # Keep track
    params_after_momentum = params + update_step  # If using momentum: params + velocity
    # params = params_after_momentum # If only using update_step
    # Apply momentum-enhanced update (as per original loop structure) - MAKE SURE VELOCITY IS UPDATED IF USING THIS
    # Assuming you *want* momentum as in the original description:
    velocity = momentum * velocity + alpha * gradient  # Standard momentum update
    params_after_momentum = params + velocity  # Apply new velocity
    params = np.clip(
        params_after_momentum, lower_bounds, upper_bounds
    )  # Keep parameters in bounds

    # --- Post-Update Evaluation and Logging ---
    print(f"    Parameters (after update): {[f'{p:.6f}' for p in params]}")

    # Evaluate final state for this iteration (optional, could use base_result from next iter)
    final_params_dict = get_params_dict(params)
    final_result = black_box(**final_params_dict, output_match_weights_chart=True)
    score = final_result["score"]
    num_matches = final_result["num_matches"]
    num_non_matches = final_result["num_non_matches"]
    num_indeterminate = final_result["num_indeterminate"]

    print(f"  Score (End of Iter): {score:,.4f}")  # Use 4dp for final score report
    print(
        f"  Num Matches: {num_matches:,.0f}, Num Non Matches: {num_non_matches:,.0f}, Num Indeterminate: {num_indeterminate:,.0f}"
    )

    # --- History Update ---
    history.append({"iteration": iteration, "variable": "score", "value": score})
    history.append(
        {"iteration": iteration, "variable": "num_matches", "value": num_matches}
    )
    history.append(
        {
            "iteration": iteration,
            "variable": "num_non_matches",
            "value": num_non_matches,
        }
    )
    history.append(
        {
            "iteration": iteration,
            "variable": "num_indeterminate",
            "value": num_indeterminate,
        }
    )
    current_param_dict_for_history = get_params_dict(
        params
    )  # Get full dict including fixed params
    for name, value in current_param_dict_for_history.items():
        history.append({"iteration": iteration, "variable": name, "value": value})

    # --- Charting ---
    if (
        iteration % 5 == 0 or score > best_score
    ):  # Update chart less frequently or on improvement
        try:
            history_df = pd.DataFrame(history)
            chart = create_chart(history_df, iteration)
            chart.save("iteration.html")
            # display(chart) # Optional inline display
            print("  Chart updated.")
        except Exception as e:
            print(f"  Error generating chart: {e}")

    # --- Best Score Tracking ---
    if score > best_score:
        best_score = score
        best_params = params.copy()
        print(f"----> New best score found: {best_score:,.4f} at iteration {iteration}")

        best_params_dict = get_params_dict(best_params)
        best_params_dict["score"] = best_score
        best_params_dict["iteration"] = iteration
        best_params_dict["timestamp"] = datetime.now().isoformat()
        try:
            with open("optimisation.jsonl", "a") as f:
                f.write(json.dumps(best_params_dict) + "\n")
        except Exception as e:
            print(f"Error writing best params to file: {e}")

    # --- Convergence Check (using parameter change) ---
    param_change = params - params_before_update
    param_change_magnitude = np.linalg.norm(param_change)
    print(f"  Parameter change magnitude (norm): {param_change_magnitude:.6f}")
    # print(f"  Velocity (end of iter): {[f'{v:.6f}' for v in velocity]}") # Add if using momentum

    if param_change_magnitude < 1e-5 and iteration > 10:
        print(f"Converged at iteration {iteration} - parameter changes too small")
        break

    iteration_end_time = datetime.now()
    # print(
    #     f"Time taken for Iteration {iteration}: {(iteration_end_time - iteration_start_time).total_seconds():.2f}s"
    # )


# --- Final Results ---
print(f"\nOptimization completed. Best Score: {best_score:,.4f}")
print(f"Parameter names: {param_names}")
print("Final best parameters:")
final_params_dict = {
    name: config["initial"]
    for name, config in param_config.items()
    if not config["optimize"]
}
best_params_dict = dict(zip(param_names, best_params))
final_params_dict.update(best_params_dict)

# Group parameters by category for better readability
print("\nOptimized Parameters:")
for name in param_names:
    value = best_params_dict[name]
    print(f"  {name}: {value:.4f}")

print("\nFixed Parameters:")
fixed_params = {
    name: value for name, value in final_params_dict.items() if name not in param_names
}
# Group numeric token weights
num_1_weights = {k: v for k, v in fixed_params.items() if k.startswith("NUM_1_")}
num_2_weights = {k: v for k, v in fixed_params.items() if k.startswith("NUM_2_")}
other_fixed = {
    k: v
    for k, v in fixed_params.items()
    if not (k.startswith("NUM_1_") or k.startswith("NUM_2_"))
}

if other_fixed:
    print("  Other:")
    for name, value in other_fixed.items():
        print(f"    {name}: {value:.4f}")

if num_1_weights:
    print("  Numeric Token 1 Weights:")
    for name, value in sorted(num_1_weights.items()):
        print(f"    {name}: {value:.4f}")

if num_2_weights:
    print("  Numeric Token 2 Weights:")
    for name, value in sorted(num_2_weights.items()):
        print(f"    {name}: {value:.4f}")
