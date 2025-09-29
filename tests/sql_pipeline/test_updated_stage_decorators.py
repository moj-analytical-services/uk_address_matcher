import pytest

from uk_address_matcher.sql_pipeline.steps import (
    CTEStep,
    Stage,
    pipeline_stage,
)


def test_pipeline_stage_from_string_returns_stage():
    @pipeline_stage(
        name="random_stage",
        description="A random stage for testing",
        tags="test",
        depends_on="seed_stage",
    )
    def random_stage():
        return "SELECT 1 AS col"

    stage = random_stage()

    assert isinstance(stage, Stage)
    assert stage.name == "random_stage"
    assert len(stage.steps) == 1
    assert stage.steps[0].sql == "SELECT 1 AS col"
    assert stage.stage_metadata.description == "A random stage for testing"
    assert stage.stage_metadata.tags == ["test"]
    assert stage.stage_metadata.depends_on == ["seed_stage"]


def test_pipeline_stage_from_named_tuple_spec():
    @pipeline_stage()
    def named_fragment_stage():
        return ("custom_fragment", "SELECT 2 AS col")

    stage = named_fragment_stage()

    assert isinstance(stage, Stage)
    assert stage.steps[0].name == "custom_fragment"
    assert stage.steps[0].sql == "SELECT 2 AS col"


def test_pipeline_stage_from_iterable_spec():
    @pipeline_stage()
    def iterable_stage():
        return [
            ("first_fragment", "SELECT 1 AS col"),
            CTEStep("second_fragment", "SELECT * FROM {first_fragment}"),
        ]

    stage = iterable_stage()

    assert [step.name for step in stage.steps] == [
        "first_fragment",
        "second_fragment",
    ]
    assert stage.steps[1].sql == "SELECT * FROM {first_fragment}"


def test_pipeline_stage_copies_registers_and_preludes():
    sentinel = object()
    registers = {"temp_rel": sentinel}
    preludes = [lambda con: None]

    @pipeline_stage(
        stage_registers=registers,
        preludes=preludes,
        checkpoint=True,
        stage_output="final_output",
        tags=("alpha", "beta"),
    )
    def stage_with_registers():
        return "SELECT 1"

    stage = stage_with_registers()

    assert stage.registers == registers
    assert stage.registers is not registers
    assert stage.preludes is not preludes
    assert callable(stage.preludes[0])
    assert stage.checkpoint is True
    assert stage.output == "final_output"
    assert stage.stage_metadata.tags == ["alpha", "beta"]


def test_pipeline_stage_invalid_return_type_raises():
    @pipeline_stage()
    def bad_stage():
        return {"invalid": "spec"}

    with pytest.raises(TypeError):
        bad_stage()
