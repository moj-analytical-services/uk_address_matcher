import os
import subprocess

import pytest


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
    "path",
    [
        "tests/test_data/one_clean_row_downing_street.parquet",
        "tests/test_data/one_clean_exact_matching_row_downing_street.parquet",
    ],
    ids=["splink_pass", "exact_match"],
)
def test_match_one(path):
    env = os.environ.copy()
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
