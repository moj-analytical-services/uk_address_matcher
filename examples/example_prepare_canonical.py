import duckdb

from uk_address_matcher import AddressMatcher, prepare_canonical_folder
from uk_address_matcher.datasets import ukam_datasets

con = duckdb.connect(database=":memory:")

# Pre-prepare canonical data for faster repeated matching.

# Cleaning a large canonical dataset (e.g. AddressBase) is expensive.
# Use `prepare_canonical_folder` to do this once and write the
# artefacts to disk. Subsequent matching runs can then load the prepared
# folder directly, skipping cleaning entirely.

# Step 1: Load raw canonical addresses
df_messy, df_canonical = ukam_datasets.fictional_london

# Step 2: Prepare and persist to a folder (one-time operation)
prepare_canonical_folder(
    df_canonical,
    output_folder="./ukam_prepared_canonical",
    con=con,
    cleaning_num_chunks=10,
    overwrite=True,
)

print("Prepared canonical data written to ./ukam_prepared_canonical/")

# Step 3: Match using the prepared folder
matcher = AddressMatcher(
    canonical_addresses="./ukam_prepared_canonical",
    addresses_to_match=df_messy,
    con=con,
)

with matcher.match() as match_result:
    print("\n=== First 10 matched records ===")
    match_result.matches().limit(10).show(max_width=500)
con.close()
