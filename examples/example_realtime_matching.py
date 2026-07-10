import duckdb

from uk_address_matcher import (
    ExactMatchStage,
    RealTimeAddressMatcher,
    SplinkStage,
    prepare_canonical_folder_for_realtime,
    ukam_datasets,
)

con = duckdb.connect(database=":memory:")

# Realtime matching uses the same SplinkStage API, but requires a canonical
# folder prepared with realtime blocker artefacts.
df_messy, df_canonical = ukam_datasets.fictional_london

prepare_canonical_folder_for_realtime(
    df_canonical,
    output_folder="./ukam_prepared_canonical_realtime",
    con=con,
    overwrite=True,
)

matcher = RealTimeAddressMatcher(
    canonical_addresses="./ukam_prepared_canonical_realtime",
    addresses_to_match=df_messy.limit(10),
    con=con,
    stages=[
        ExactMatchStage(),
        SplinkStage(
            predict_threshold_match_weight=-20,
            final_match_weight_threshold=12,
            include_full_postcode_block=True,
        ),
    ],
)

match_result = matcher.match()

match_result.matches().limit(10).show(max_width=500)
