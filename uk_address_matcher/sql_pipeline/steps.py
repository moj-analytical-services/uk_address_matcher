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
    tags: Optional[List[str]] = None
    depends_on: Optional[List[str]] = None

    def __post_init__(self):
        # Coerce tags/depends_on into lists of strings for consistency
        if isinstance(self.tags, str):
            self.tags = [self.tags]
        elif self.tags is not None and not isinstance(self.tags, list):
            # Accept tuples/sets or any iterable of strings
            self.tags = list(self.tags)

        if isinstance(self.depends_on, str):
            self.depends_on = [self.depends_on]
        elif self.depends_on is not None and not isinstance(self.depends_on, list):
            self.depends_on = list(self.depends_on)


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

    def format_plan_block(self, max_name: int = 60, dep_width: int = 60) -> str:
        """Render a formatted multi-line summary block for this stage.

        This is used by the pipeline plan view to present each queued SQL stage
        in a human-friendly way.

        For example, a `Stage` titled "tokenise_addresses" might render as:
        tokenise_addresses [cleaning]
        ↳ Split address into tokens
        │ depends on: load_raw
        │ (checkpoint)
        """
        meta = self.stage_metadata or StageMeta()
        display_name = (
            self.name
            if len(self.name) <= max_name
            else self.name[: max_name - 3] + "..."
        )
        lines: List[str] = []
        tags_part = f" [{', '.join(meta.tags)}]" if meta.tags else ""
        lines.append(f"{display_name}{tags_part}")
        if meta.description:
            lines.append(f"↳ {meta.description}")
        # (input/output columns intentionally omitted for now)
        if meta.depends_on:
            deps = ", ".join(meta.depends_on)
            if len(deps) > dep_width:
                deps = deps[: dep_width - 3] + "..."
            lines.append(f"│ depends on: {deps}")
        if self.checkpoint:
            lines.append("│ (checkpoint)")
        return "\n".join(lines)


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

    if spec is None:
        raise TypeError("Stage callable returned None; expected SQL specification.")

    out: List[CTEStep] = []
    for i, item in enumerate(spec):
        if isinstance(item, CTEStep):
            out.append(item)
        elif (
            isinstance(item, tuple)
            and len(item) == 2
            and all(isinstance(x, str) for x in item)
        ):
            out.append(CTEStep(item[0], item[1]))
        else:
            raise TypeError(
                "Stage return iterable items must be CTEStep or (name, sql) tuple; "
                f"got {item!r} at index {i}"
            )

    if not out:
        raise ValueError("Stage returned an empty iterable of steps")

    # basic duplicate check
    names = [s.name for s in out]
    if len(names) != len(set(names)):
        raise ValueError(f"Duplicate CTE names: {names}")
    return out


def pipeline_stage(
    *,
    name: Optional[str] = None,
    description: str = "",
    tags: Optional[Union[str, Iterable[str]]] = None,
    depends_on: Optional[Union[str, Iterable[str]]] = None,
    checkpoint: bool = False,
    stage_output: Optional[str] = None,
    stage_registers: Optional[dict] = None,
    preludes: Optional[list] = None,
) -> Callable[[Callable[P, SQLSpec]], Callable[P, "Stage"]]:
    def deco(fn: Callable[P, SQLSpec]) -> Callable[P, "Stage"]:
        stage_name = name or fn.__name__

        # normalise depends_on/tags to lists of strings
        def _norm_list(v: Optional[Union[str, Iterable[str]]]) -> Optional[List[str]]:
            if v is None:
                return None
            if isinstance(v, str):
                return [v]
            return list(v)

        deps_list = _norm_list(depends_on) or []
        tags_list = _norm_list(tags)

        @wraps(fn)
        def factory(*args: P.args, **kwargs: P.kwargs) -> "Stage":
            spec = fn(*args, **kwargs)
            steps = _normalise_sql_step(spec)
            return Stage(
                name=stage_name,
                steps=steps,
                stage_metadata=StageMeta(
                    description=description,
                    tags=tags_list,
                    depends_on=deps_list,
                ),
                output=stage_output,
                registers=dict(stage_registers) if stage_registers else None,
                checkpoint=checkpoint,
                preludes=list(preludes) if preludes else None,
            )

        return factory

    return deco
