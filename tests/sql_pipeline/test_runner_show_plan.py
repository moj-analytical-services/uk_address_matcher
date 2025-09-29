import duckdb

from uk_address_matcher.sql_pipeline.runner import create_sql_pipeline
from uk_address_matcher.sql_pipeline.steps import pipeline_stage


def _make_stage(name: str, description: str = "", **kwargs):
    tags = kwargs.pop("tags", None)
    depends_on = kwargs.pop("depends_on", None)
    checkpoint = kwargs.pop("checkpoint", False)

    @pipeline_stage(
        name=name,
        description=description,
        tags=tags,
        depends_on=depends_on,
        checkpoint=checkpoint,
    )
    def stage_factory():
        return "SELECT * FROM {input}"

    return stage_factory()


def _capture_plan_output(pipeline, monkeypatch):
    captured: list[str] = []

    def fake_emit(message: str):
        captured.append(message)
        return None

    monkeypatch.setattr("uk_address_matcher.sql_pipeline.runner._emit_debug", fake_emit)

    pipeline.show_plan()
    assert captured, "show_plan should emit formatted output"
    return captured[0]


def test_show_plan_renders_stage_metadata(monkeypatch):
    con = duckdb.connect()
    try:
        rel = con.sql("SELECT 1 AS id")
        stage_one = _make_stage(
            "stage_one",
            description="First stage",
            tags="alpha",
        )
        stage_two = _make_stage(
            "stage_two",
            description="Second stage",
            tags=["beta", "gamma"],
            depends_on="stage_one",
            checkpoint=True,
        )

        pipeline = create_sql_pipeline(
            con,
            rel,
            [stage_one, stage_two],
            pipeline_name="Example pipeline",
            pipeline_description="Demo pipeline",
        )

        output = _capture_plan_output(pipeline, monkeypatch)
    finally:
        con.close()

    assert "Example pipeline" in output
    assert "stage_one [alpha]" in output
    assert "↳ First stage" in output
    assert "stage_two [beta, gamma]" in output
    assert "depends on:" in output and "• stage_one" in output
    assert "checkpoint" in output


def test_show_plan_handles_empty_pipeline(monkeypatch):
    con = duckdb.connect()
    try:
        rel = con.sql("SELECT 1 AS id")
        pipeline = create_sql_pipeline(
            con,
            rel,
            [],
            pipeline_name="Empty pipeline",
        )

        output = _capture_plan_output(pipeline, monkeypatch)
    finally:
        con.close()

    assert "Empty pipeline" in output
    assert "(no stages added)" in output
