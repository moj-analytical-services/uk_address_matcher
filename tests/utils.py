import duckdb
import pyarrow as pa
import yaml


def load_test_cases(yaml_path):
    """Load test cases from a YAML file"""
    with open(yaml_path, "r") as file:
        data = yaml.safe_load(file)
    return data["addresses"]


def prepare_combined_test_data(yaml_path, con=None):
    """
    Prepare all test cases from a YAML file into a single combined dataset.

    Args:
        yaml_path: Path to the YAML file with test cases
        con: Optional DuckDB connection

    Returns:
        tuple: (df_messy_combined, df_canonical_combined)
    """
    if con is None:
        con = duckdb.connect(database=":memory:")

    test_cases = load_test_cases(yaml_path)

    # Prepare data for all messy addresses
    messy_data = []
    canonical_data = []

    for test_block_id, test_case in enumerate(test_cases, 1):
        # Calculate the true match ID (first canonical address in the list)
        true_match_id = test_block_id * 1000 + 1

        # Add messy address with test_block identifier and true_match_id
        messy_address = test_case["messy_address"]
        messy_data.append(
            {
                "unique_id": test_block_id,  # Use test_block_id as unique_id for messy addresses
                "source_dataset": "messy",
                "address_concat": messy_address[0],
                "postcode": messy_address[1],
                "test_block": test_block_id,
                "true_match_id": true_match_id,  # Add the true match ID as a column
                "ukam_address_id": test_block_id,
            }
        )

        canonical_addresses = test_case["canonical_addresses"]

        for i, addr in enumerate(canonical_addresses, 1):
            canonical_data.append(
                {
                    "unique_id": test_block_id * 1000
                    + i,  # Create unique IDs within each test block
                    "source_dataset": "canonical",
                    "address_concat": addr[0],
                    "postcode": addr[1],
                    "test_block": test_block_id,
                    "true_match_id": None,
                    "ukam_address_id": test_block_id + (i * 1000),
                }
            )

    messy_table = pa.Table.from_pylist(messy_data)
    canonical_table = pa.Table.from_pylist(canonical_data)

    con.register("messy_table_combined", messy_table)
    con.register("canonical_table_combined", canonical_table)

    df_messy_combined = con.table("messy_table_combined")
    df_canonical_combined = con.table("canonical_table_combined")

    return df_messy_combined, df_canonical_combined
