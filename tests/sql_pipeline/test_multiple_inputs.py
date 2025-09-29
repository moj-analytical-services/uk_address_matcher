import duckdb
import pytest

from uk_address_matcher.sql_pipeline.runner import (
    DuckDBPipeline,
    InputBinding,
    create_sql_pipeline,
)
from uk_address_matcher.sql_pipeline.steps import pipeline_stage


@pipeline_stage()
def join_lookup_stage():
    return """
		SELECT a.id, b.extra
		FROM {primary} AS a
		JOIN {lookup} AS b
		  ON a.id = b.id
	"""


@pipeline_stage()
def root_join_lookup_stage():
    return """
        SELECT p.id, l.extra
        FROM {root} AS p
        JOIN {lookup} AS l
          ON p.id = l.id
    """


def _setup_primary_lookup(duck_con: duckdb.DuckDBPyConnection) -> None:
    duck_con.execute("CREATE TABLE primary_input (id INTEGER)")
    duck_con.execute("INSERT INTO primary_input VALUES (1),(2)")
    duck_con.execute("CREATE TABLE lookup_input (id INTEGER, extra INTEGER)")
    duck_con.execute("INSERT INTO lookup_input VALUES (1, 100),(2, 200)")


def test_multiple_input_bindings_register_aliases(duck_con):
    _setup_primary_lookup(duck_con)

    bindings = [
        InputBinding("primary", duck_con.table("primary_input")),
        InputBinding("lookup", duck_con.table("lookup_input")),
    ]

    pipeline = DuckDBPipeline(duck_con, bindings)
    pipeline.add_step(join_lookup_stage())

    result = pipeline.run().df().sort_values("id").reset_index(drop=True)

    assert list(result.extra) == [100, 200]

    alias_map = pipeline.input_alias_map
    assert set(alias_map) >= {"primary", "lookup", "root"}
    assert alias_map["root"] == alias_map["primary"]
    for name in ("primary", "lookup"):
        duck_con.execute(f"SELECT COUNT(*) FROM {alias_map[name]}")

    assert pipeline.root_alias == alias_map["root"]


@pipeline_stage()
def slugged_join_stage():
    return (
        "joined",
        """
			SELECT p.id, l.extra
			FROM {primary_input} AS p
			JOIN {lookup_table} AS l
			  ON p.id = l.id
		""",
    )


def test_input_bindings_with_slugged_placeholders(duck_con):
    _setup_primary_lookup(duck_con)

    bindings = [
        InputBinding("Primary Input", duck_con.table("primary_input")),
        InputBinding("lookup-table", duck_con.table("lookup_input")),
    ]

    pipeline = create_sql_pipeline(
        duck_con,
        bindings,
        [slugged_join_stage],
        pipeline_name="slug-demo",
    )

    result = pipeline.run().df().sort_values("id").reset_index(drop=True)

    assert list(result.extra) == [100, 200]

    alias_map = dict(pipeline.input_alias_map)
    assert "Primary Input" not in alias_map
    assert "lookup-table" not in alias_map
    assert alias_map.keys() >= {"primary_input", "lookup_table", "root"}
    for name in ("primary_input", "lookup_table"):
        duck_con.execute(f"SELECT COUNT(*) FROM {alias_map[name]}")


def test_multi_input_pipeline_allows_root_placeholder(duck_con):
    _setup_primary_lookup(duck_con)

    bindings = [
        InputBinding("primary", duck_con.table("primary_input")),
        InputBinding("lookup", duck_con.table("lookup_input")),
    ]

    pipeline = DuckDBPipeline(duck_con, bindings)
    pipeline.add_step(root_join_lookup_stage())

    result = pipeline.run().df().sort_values("id").reset_index(drop=True)
    assert list(result.extra) == [100, 200]


def test_duplicate_input_placeholder_raises(duck_con):
    _setup_primary_lookup(duck_con)

    bindings = [
        InputBinding("primary", duck_con.table("primary_input")),
        InputBinding("primary", duck_con.table("lookup_input")),
    ]

    with pytest.raises(ValueError) as err:
        DuckDBPipeline(duck_con, bindings)

    assert "Duplicate input placeholder" in str(err.value)
