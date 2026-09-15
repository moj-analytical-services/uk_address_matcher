from __future__ import annotations

import time

import duckdb

from uk_address_matcher import (
    AddressMatcher,
    ExactMatchStage,
    SplinkStage,
    ukam_datasets,
)
from uk_address_matcher.labelling import _launch_labelling_app_beta

start_time = time.time()
messy, canonical = ukam_datasets.fictional_london
con = duckdb.connect(database=":memory:")

matcher = AddressMatcher(
    canonical_addresses=canonical,
    addresses_to_match=messy,
    con=con,
    stages=[
        ExactMatchStage(),
        SplinkStage(),
    ],
)

result = matcher.match()
bundle_path = result._export_labelling_bundle_beta(overwrite=True)

print(f"Labelling bundle written to: {bundle_path}")  # noqa: T201
print(f"Execution time: {time.time() - start_time:.1f} seconds")  # noqa: T201

con.sql(f"""
    select * from
    read_parquet('{bundle_path}/review_data.parquet')
""").show(max_width=10000, max_rows=1)

_launch_labelling_app_beta(labelling_bundle_path=bundle_path)
