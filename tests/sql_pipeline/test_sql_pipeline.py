import pytest

from uk_address_matcher.cleaning.pipelines import (
    QUEUE_PRE_TF,
    QUEUE_PRE_TF_MATERIALIZED,
)
from uk_address_matcher.sql_pipeline.runner import DuckDBPipeline
from uk_address_matcher.sql_pipeline.steps import CTEStep, Stage, pipeline_stage


# ---------- Helpers ----------
@pytest.fixture
def base_rel(duck_con):
    duck_con.execute("CREATE TABLE base (id INTEGER, val INTEGER)")
    duck_con.execute("INSERT INTO base VALUES (1,10),(2,20),(3,30)")
    return duck_con.table("base")


# ---------- Stage definitions for tests ----------
@pipeline_stage()
def add_ten():
    return "SELECT id, val + 10 AS val FROM {input}"


@pipeline_stage()
def name_tuple_stage():
    return ("double", "SELECT id, val*2 AS val FROM {input}")


@pipeline_stage()
def mixed_stage():
    return [
        ("triple", "SELECT id, val*3 AS val FROM {input}"),
        CTEStep(name="quad", sql="SELECT id, val*4 AS val FROM {triple}"),
    ]


@pipeline_stage(stage_output="triple")
def multi_output_stage():
    return [
        ("triple", "SELECT id, val*3 AS val FROM {input}"),
        ("ignore", "SELECT * FROM {triple}"),
    ]


@pipeline_stage()
def duplicate_names_stage():
    return [
        ("dup", "SELECT * FROM {input}"),
        ("dup", "SELECT * FROM {input}"),
    ]


def test_stage_return_forms_normalised():
    s1 = add_ten()
    s2 = name_tuple_stage()
    s3 = mixed_stage()
    s4 = multi_output_stage()

    assert len(s1.steps) == 1 and isinstance(s1.steps[0], CTEStep)
    # name_tuple_stage returns a single (name, sql) pair
    assert [st.name for st in s2.steps] == ["double"]
    assert [st.name for st in s3.steps] == ["triple", "quad"]
    assert s4.output == "triple"


def test_pipeline_execution_order(duck_con, base_rel):
    pipe = DuckDBPipeline(duck_con, base_rel)
    pipe.add_step(add_ten())  # val + 10
    pipe.add_step(name_tuple_stage())  # (val+10)*2
    # final result using run
    result = pipe.run()
    out_df = result.df().sort_values("id").reset_index(drop=True)
    assert list(out_df.val) == [40, 60, 80]


def test_debug_materialise_matches_final(duck_con, base_rel, capsys):
    pipe = DuckDBPipeline(duck_con, base_rel)
    pipe.add_step(add_ten())
    pipe.add_step(name_tuple_stage())

    # materialising debug
    rel_debug = pipe.debug(materialise=True, return_last=True)
    debug_vals = rel_debug.df().sort_values("id").val.tolist()

    # Need a fresh pipeline (previous one now spent when generating full sql)
    pipe2 = DuckDBPipeline(duck_con, base_rel)
    pipe2.add_step(add_ten())
    pipe2.add_step(name_tuple_stage())
    rel_run = pipe2.run()
    run_vals = rel_run.df().sort_values("id").val.tolist()
    assert debug_vals == run_vals


def test_materialized_stage_emits_boundary_and_preserves_results(duck_con, base_rel):
    @pipeline_stage(materialized=True)
    def materialized_copy():
        return "SELECT id, val + 1 AS val FROM {input}"

    pipeline = DuckDBPipeline(duck_con, base_rel)
    pipeline.add_step(materialized_copy())
    sql = pipeline.generate_cte_pipeline_sql(mark_spent=False)

    assert "AS MATERIALIZED" in sql
    values = pipeline.run().df().sort_values("id").val.tolist()
    assert values == [11, 21, 31]


def test_default_pre_tf_queue_is_fused_with_explicit_materialized_variant():
    def is_materialized(stage_spec):
        return (
            stage_spec.materialized
            if isinstance(stage_spec, Stage)
            else stage_spec().materialized
        )

    assert not any(is_materialized(stage) for stage in QUEUE_PRE_TF)
    assert any(is_materialized(stage) for stage in QUEUE_PRE_TF_MATERIALIZED)


def test_debug_logical_outputs_sql(duck_con, base_rel, capsys):
    pipe = DuckDBPipeline(duck_con, base_rel)
    stages = [
        add_ten,
        name_tuple_stage,
    ]
    for stage in stages:
        pipe.add_step(stage())

    pipeline_len = len(stages)

    pipe.debug(materialise=False, show_sql=True, max_rows=1)
    captured = capsys.readouterr().out
    # Expect markers for each non-seed step (2) - +1 as we have an initial input step
    assert captured.count("DEBUG STEP") == pipeline_len + 1
    assert "SELECT * FROM" in captured
    # Verify alias pattern appears (s<idx>_)
    assert (
        len([m for m in captured.split() if m.startswith("s0_") or m.startswith("s1_")])
        >= 1
    )


def test_duplicate_names_error():
    # duplicate_names_stage should raise because normalisation detects duplicates
    with pytest.raises(ValueError):
        duplicate_names_stage()


def test_materialise_debug_parity(duck_con, base_rel):
    pipe = DuckDBPipeline(duck_con, base_rel)
    pipe.add_step(add_ten())
    pipe.add_step(name_tuple_stage())
    debug_rel = pipe.debug(materialise=True, return_last=True)
    debug_vals = debug_rel.df().sort_values("id").val.tolist()

    # fresh pipeline for normal run
    pipe2 = DuckDBPipeline(duck_con, base_rel)
    pipe2.add_step(add_ten())
    pipe2.add_step(name_tuple_stage())
    run_vals = pipe2.run().df().sort_values("id").val.tolist()
    assert debug_vals == run_vals


@pipeline_stage()
def select_extra_col():
    return "SELECT * EXCLUDE (extra), extra FROM {input}"


def test_projected_input_relation_columns_preserved(duck_con):
    """A projected relation (e.g. _ensure_postcode_column) must not be silently
    collapsed back to the base table alias, which would drop the new column."""
    duck_con.execute("CREATE TABLE src_tbl (id INTEGER, val INTEGER)")
    duck_con.execute("INSERT INTO src_tbl VALUES (1, 10)")
    base = duck_con.table("src_tbl")
    # Project an extra column on top of the base table
    projected = base.select("*, 99 AS extra")

    pipe = DuckDBPipeline(duck_con, projected)
    pipe.add_step(select_extra_col())
    result = pipe.run()
    assert "extra" in result.columns
    assert result.fetchone()[2] == 99
