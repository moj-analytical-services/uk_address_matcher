import os
import subprocess

import duckdb
import pytest

from uk_address_matcher import clean_data_with_term_frequencies


def test_full_example():
    env = os.environ.copy()

    env["EPC_PATH"] = (
        f"read_csv('{os.path.abspath('tests/test_data/epc_fake.csv')}', filename=true)"
    )
    env["FULL_OS_PATH"] = (
        f"read_csv('{os.path.abspath('tests/test_data/os_fake.csv')}', filename=true)"
    )

    result = subprocess.run(
        ["python", "examples/match_epc_to_os.py"],
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert result.returncode == 0, (
        f"Script failed!\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
    )


@pytest.mark.parametrize(
    "path, postcode",
    [
        ("tests/test_data/one_clean_row_downing_street.parquet", "SW1A 2AA"),
        (
            "tests/test_data/one_clean_exact_matching_row_downing_street.parquet",
            "SW1A 3BC",
        ),
    ],
    ids=["splink_pass", "exact_match"],
)
def test_match_one(path, postcode):
    env = os.environ.copy()

    # If we don't run this every time, any changes to cleaning will not be picked up
    con = duckdb.connect(":memory:")
    canon_data = con.sql(
        f"""
    select
        '1' as unique_id,
        '10 downing street westminster london' as address_concat,
        '{postcode}' as postcode
    """
    )
    canon_data = clean_data_with_term_frequencies(canon_data, con=con)
    con.sql(
        f"COPY ({canon_data.sql_query()}) TO '{os.path.abspath(path)}' (FORMAT 'parquet')"
    )

    # We need to provide a way to override the hardcoded path in match_one.py
    env["OS_CLEAN_PATH"] = f"read_parquet('{os.path.abspath(path)}')"

    result = subprocess.run(
        ["python", "examples/match_one.py"],
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert result.returncode == 0, (
        f"Script failed!\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
    )


def test_match_fhrs_to_os():
    env = os.environ.copy()

    # Override the hardcoded paths in match_fhrs_to_os.py
    env["FHRS_PATH"] = (
        f"read_csv('{os.path.abspath('tests/test_data/fhrs_fake.csv')}', filename=true)"
    )
    env["FULL_OS_PATH"] = (
        f"read_csv('{os.path.abspath('tests/test_data/os_fake.csv')}', filename=true)"
    )

    result = subprocess.run(
        ["python", "examples/fhrs/match_fhrs_to_os.py"],
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert result.returncode == 0, (
        f"Script failed!\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
    )


def test_example_matching():
    env = os.environ.copy()

    # Set flag to limit the number of records for testing
    env["TEST_LIMIT"] = "1"

    result = subprocess.run(
        ["python", "examples/example_matching.py"],
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert result.returncode == 0, (
        f"Script failed!\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
    )
