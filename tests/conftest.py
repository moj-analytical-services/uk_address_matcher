import logging

import duckdb
import pytest

# Disable Splink warnings in tests
logging.getLogger("splink").setLevel(logging.ERROR)


@pytest.fixture
def duck_con():
    con = duckdb.connect(database=":memory:")
    con.execute("INSTALL splink_udfs FROM community; LOAD splink_udfs;")

    yield con
    con.close()
