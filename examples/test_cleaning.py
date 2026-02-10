import os
import time

import duckdb
import pandas as pd
from IPython.display import display

from uk_address_matcher import (
    best_matches_summary,
    best_matches_with_distinguishability,
    calculate_match_metrics,
    clean_data_with_term_frequencies,
    get_linker,
    improve_predictions_using_distinguishing_tokens,
)
from uk_address_matcher.cleaning.chunking_strategies import (
    clean_data_with_minimal_steps,
)
from uk_address_matcher.linking_model.exact_matching import (
    available_deterministic_stages,
    run_deterministic_match_pass,
)
from uk_address_matcher.post_linkage.match_candidate_selection import (
    select_top_match_candidates,
)

pd.options.display.max_colwidth = 1000

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Step 1: Load in some example data
# -----------------------------------------------------------------------------

# If you're using your own data you need the following columns:
# +-------------------+----------------------+----------------------------------------+
# |      Column       | DuckDB dtype         |               Description               |
# +-------------------+----------------------+----------------------------------------+
# | unique_id         | BIGINT or VARCHAR    | Unique identifier for each record       |
# | source_dataset    | VARCHAR              | Source dataset label, e.g. 'epc'        |
# | address_concat    | VARCHAR              | Full address (without postcode)         |
# | postcode          | VARCHAR              | Postcode                                |
# +-------------------+----------------------+----------------------------------------+


# Any additional columns should be retained as-is by the cleaning code

p_ch = "./example_data/companies_house_addresess_postcode_overlap.parquet"
p_fhrs = "./example_data/fhrs_addresses_sample.parquet"

con = duckdb.connect(database=":memory:")

# Read our example data in and ensure unique_id is the correct data type
df_ch = con.read_parquet(p_ch)
df_fhrs = con.read_parquet(p_fhrs)

# Apply limit if TEST_LIMIT environment variable is set
if os.getenv("TEST_LIMIT"):
    df_ch = df_ch.limit(250)
    df_fhrs = df_fhrs.limit(250)

# -----------------------------------------------------------------------------
# Step 2: Clean the data/feature engineering to prepare for matching model
# -----------------------------------------------------------------------------
df_ch_clean = clean_data_with_term_frequencies(df_ch, con=con)
df_fhrs_clean = clean_data_with_term_frequencies(df_fhrs, con=con)

# df_ch_ccc = clean_data_with_minimal_steps(df_ch, con=con)
# df_fhrs_ccc = clean_data_with_minimal_steps(df_fhrs, con=con)
