from __future__ import annotations

import time

import duckdb
import pyarrow as pa

from benchmarking.config.datasets import get_dataset_definition, load_dataset
from benchmarking.config.sources import resolve_data_source
from benchmarking.settings import CANONICAL_PATH
from uk_address_matcher import AddressMatcher, ExactMatchStage, SplinkStage
from uk_address_matcher.labelling import _launch_labelling_app_beta

HACKNEY_GSS_CODE = "E09000012"
CLASSIFICATION_CODE_PREFIX = "R"
HACKNEY_DATASET = get_dataset_definition("hackney")
HACKNEY_INPUT_PATH = resolve_data_source(
    HACKNEY_DATASET["data_path_env"],
    HACKNEY_DATASET["file_name"],
)


start_time = time.time()
con = duckdb.connect(database=":memory:")
hackney_addresses = load_dataset(con, dataset_key="hackney")

canonical_address_filter = (
    f"lowertierlocalauthoritygsscode = '{HACKNEY_GSS_CODE}' "
    f"AND substr(classificationcode, 1, 1) = '{CLASSIFICATION_CODE_PREFIX}'"
)

matcher = AddressMatcher(
    canonical_addresses=CANONICAL_PATH,
    canonical_address_filter=canonical_address_filter,
    addresses_to_match=hackney_addresses,
    con=con,
    stages=[
        ExactMatchStage(),
        SplinkStage(),
    ],
)

result = matcher.match()
bundle_path = result._export_labelling_bundle_beta(overwrite=True)
accuracy_table = result.accuracy_analysis(
    output_type="table",
    add_metrics=["f1"],
    match_weight_round_to_nearest=1,
)
accuracy_relation = con.from_arrow(pa.Table.from_pylist(accuracy_table))

print(f"Labelling bundle written to: {bundle_path}")
print(f"Execution time: {time.time() - start_time:.1f} seconds")
accuracy_relation.show(max_width=100_000, max_rows=100_000)
con.sql(f"""
    select * from
    read_parquet('{bundle_path}/review_data.parquet')
""").show(max_width=10000, max_rows=1)

_launch_labelling_app_beta(
    labelling_bundle_path="ukam_labelling_bundle",
    input_dataset_path=HACKNEY_INPUT_PATH,
    input_dataset_label_column="UPRN",
    canonical_address_path=CANONICAL_PATH,
)
