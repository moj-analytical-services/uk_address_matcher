import duckdb

from uk_address_matcher import AddressMatcher, prepare_canonical_folder

con = duckdb.connect(database=":memory:")

# Pre-prepare canonical data for faster repeated matching.

# Cleaning a large canonical dataset (e.g. AddressBase) is expensive.
# Use `prepare_canonical_folder` to do this once and write the
# artefacts to disk. Subsequent matching runs can then load the prepared
# folder directly, skipping cleaning entirely.

# Step 1: Load raw canonical addresses
df_canonical = con.read_parquet(
    "./example_data/companies_house_addresess_postcode_overlap.parquet"
)

# Step 2: Prepare and persist to a folder (one-time operation)
prepare_canonical_folder(
    df_canonical,
    output_folder="./ukam_prepared_canonical",
    con=con,
    overwrite=True,
)

print("Prepared canonical data written to ./ukam_prepared_canonical/")

# Step 3: Match using the prepared folder
df_messy = con.read_parquet("./example_data/fhrs_addresses_sample.parquet")

matcher = AddressMatcher(
    canonical_addresses="./ukam_prepared_canonical",
    addresses_to_match=df_messy,
    con=con,
)

match_result = matcher.match()
result = match_result.matches

print("\n=== First 10 matched records ===")
result.limit(10).show(max_width=500)
