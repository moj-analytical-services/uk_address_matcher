import logging
from unittest.mock import patch

import duckdb
import pytest

# Keep verbose dependency logs out of test output.
logging.getLogger("splink").setLevel(logging.ERROR)
logging.getLogger("sqlglot").setLevel(logging.WARNING)


def _test_calculate_chunk_size(total_records: int, num_of_chunks: int) -> int:
    """Test-friendly chunk size calculation without the 10k minimum constraint.

    This allows tests with small datasets to actually exercise multi-chunk behaviour.
    """
    if total_records <= 0:
        raise ValueError(
            "Supplied address table has no records. Please provide a non-empty table."
        )
    num_of_chunks = max(1, num_of_chunks)
    chunk_size = (total_records + num_of_chunks - 1) // num_of_chunks
    return max(1, chunk_size)


@pytest.fixture
def duck_con():
    con = duckdb.connect(database=":memory:")

    # Patch chunk size calculation to allow small test datasets to be chunked
    with patch(
        "uk_address_matcher.cleaning.chunking_strategies._calculate_chunk_size",
        _test_calculate_chunk_size,
    ):
        yield con
    con.close()
