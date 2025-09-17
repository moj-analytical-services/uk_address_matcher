# In the end we didn't really train the model
# so much as hard code all the match weights since training
# doesn't really work
import os

import duckdb


from secret_data.secrets import epc_addresses_path
from uk_address_matcher import clean_data_using_precomputed_rel_tok_freq
from uk_address_matcher.linking_model.training import get_settings_for_training

from splink import Linker, DuckDBAPI, block_on

LIMIT = 100000000

con = duckdb.connect(":default:")


sql = f"""
create or replace table epc_addresses as
select
    substr(unique_id,1,12) as unique_id,
    'epc' as source_dataset,
    address_concat,
    postcode
from read_parquet('{epc_addresses_path}')
limit {LIMIT}
"""
con.execute(sql)
epc_addresses = con.table("epc_addresses")


epc_addresses_clean = clean_data_using_precomputed_rel_tok_freq(epc_addresses, con=con)


db_api = DuckDBAPI(connection=con)

linker = Linker(
    [epc_addresses_clean, epc_addresses_clean],
    settings=get_settings_for_training(),
    db_api=db_api,
)

linker.visualisations.match_weights_chart()


model_path = "uk_address_matcher/data/splink_model.json"
if os.path.exists(model_path):
    os.remove(model_path)

m = linker.misc.save_model_to_json("uk_address_matcher/data/splink_model.json")

# linker.training.estimate_u_using_random_sampling(
#     max_pairs=1e8
# )

# linker.training.estimate_parameters_using_expectation_maximisation(
#     block_on("postcode")
# )


# linker = train_splink_model(
#     df_epc_pd,
#     df_os_pd,
#     additional_columns_to_retain=["uprn", "uprn_source"],
#     label_colname="uprn",
#     max_pairs=1e7,
# )
# linker.match_weights_chart()
# linker.m_u_parameters_chart()
# linker.save_model_to_json("os_trained_model_2.json")
# # df_predict = linker.predict(threshold_match_probability=0.9)
