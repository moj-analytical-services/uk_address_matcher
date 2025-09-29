import duckdb
import pytest


@pytest.fixture
def duck_con():
    con = duckdb.connect(database=":memory:")
    yield con
    con.close()
