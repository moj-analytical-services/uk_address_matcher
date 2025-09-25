from __future__ import annotations

from dataclasses import dataclass
from functools import wraps
from typing import TYPE_CHECKING, Callable, Dict, Iterable, List, Optional, Tuple, Union

from typing_extensions import ParamSpec

from uk_address_matcher.sql_pipeline.helpers import _uid

if TYPE_CHECKING:
    import duckdb


@dataclass(frozen=True)
class CTEStep:
    name: str
    sql: str  # may reference {input} and prior fragment names as {frag_name}

    @classmethod
    def from_return_value(cls, ret: object) -> Tuple["CTEStep", ...]:
        """Normalise a user stage return value into a tuple of CTESteps.

        Accepted forms:
          1. raw SQL str -> single CTEStep with random name
          2. (name, sql) tuple -> single CTEStep
          3. CTEStep instance -> returned as-is (single element tuple)
          4. list/tuple (iterable) of CTEStep and/or (name, sql) tuples (mixed allowed)

        Empty collections are rejected.
        """
        if isinstance(ret, str):
            return (cls(name=f"frag_{_uid(5)}", sql=ret),)

        if isinstance(ret, CTEStep):
            return (ret,)

        if isinstance(ret, (list, tuple)):
            # Treat a 2-tuple of strings as a single step
            if (
                isinstance(ret, tuple)
                and len(ret) == 2
                and all(isinstance(x, str) for x in ret)
            ):
                name, sql_text = ret  # type: ignore[misc]
                return (cls(name=name, sql=sql_text),)

            converted: List[CTEStep] = []
            for idx, item in enumerate(ret):
                if isinstance(item, CTEStep):
                    converted.append(item)
                elif (
                    isinstance(item, tuple)
                    and len(item) == 2
                    and all(isinstance(x, str) for x in item)
                ):
                    n, s = item  # type: ignore[misc]
                    converted.append(cls(name=n, sql=s))
                else:
                    raise TypeError(
                        "Stage return iterable items must be CTEStep or (name, sql) tuple; "
                        f"got {item!r} at index {idx}"
                    )
            if not converted:
                raise ValueError("Stage returned an empty iterable of steps")
            return tuple(converted)

        raise TypeError(
            "Unsupported stage return type. Expected one of: str, CTEStep, (str,str), "
            "or iterable of these"
        )


@dataclass
class StageMeta:
    description: Optional[str] = None
    group: Optional[str] = None
    depends_on: Optional[List[str]] = None


@dataclass
class Stage:
    name: str
    # Make steps immutable so Stage can be safely hashed
    steps: Tuple[CTEStep, ...]
    # Debugging information / metadata
    stage_metadata: Optional[StageMeta] = None
    output: Optional[str] = None
    # DuckDB-specific helpers
    registers: Optional[Dict[str, duckdb.DuckDBPyRelation]] = None
    checkpoint: bool = False
    # Optional list of callables executed before the step (referenced in pipeline)
    preludes: Optional[List[Callable[[duckdb.DuckDBPyConnection], None]]] = None

    # Let dataclass generate eq; supply a hash consistent with eq but stable.
    def __hash__(self) -> int:
        return hash((self.name, self.steps, self.output, self.checkpoint))


SQLSpec = Union[str, Iterable[Tuple[str, str]], Iterable[CTEStep]]

P = ParamSpec("P")


def _normalise_sql_step(spec: SQLSpec) -> List[CTEStep]:
    if isinstance(spec, str):
        return [CTEStep("frag_00", spec)]
    # Treat a 2-tuple of strings as a single (name, sql) specification
    if (
        isinstance(spec, tuple)
        and len(spec) == 2
        and all(isinstance(x, str) for x in spec)
    ):
        return [CTEStep(spec[0], spec[1])]
    out: List[CTEStep] = []
    for i, item in enumerate(spec or []):
        out.append(item if isinstance(item, CTEStep) else CTEStep(item[0], item[1]))
    # basic duplicate check
    names = [s.name for s in out]
    if len(names) != len(set(names)):
        raise ValueError(f"Duplicate CTE names: {names}")
    return out


def pipeline_stage(
    *,
    name: Optional[str] = None,
    description: str = "",
    group: Optional[str] = None,
    depends_on: Optional[list[str]] = None,
    checkpoint: bool = False,
    stage_output: Optional[str] = None,
    stage_registers: Optional[dict] = None,
    preludes: Optional[list] = None,
) -> Callable[[Callable[P, SQLSpec]], Callable[P, "Stage"]]:
    def deco(fn: Callable[P, SQLSpec]) -> Callable[P, "Stage"]:
        stage_name = name or fn.__name__
        deps = tuple(depends_on or ())

        @wraps(fn)
        def factory(*args: P.args, **kwargs: P.kwargs) -> "Stage":
            spec = fn(*args, **kwargs)
            steps = _normalise_sql_step(spec)
            return Stage(
                name=stage_name,
                steps=steps,
                stage_metadata=StageMeta(
                    description=description, group=group, depends_on=deps
                ),
                output=stage_output,
                registers=dict(stage_registers) if stage_registers else None,
                checkpoint=checkpoint,
                preludes=list(preludes) if preludes else None,
            )

        return factory

    return deco
