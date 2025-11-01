from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from time import perf_counter
from types import MappingProxyType
from typing import (
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    Union,
)

import duckdb

from uk_address_matcher.sql_pipeline.helpers import (
    TimingReport,
    _duckdb_table_exists,
    _emit_debug,
    _explain_debug,
    _format_duration,
    _pretty_sql,
    _slug,
    _uid,
)
from uk_address_matcher.sql_pipeline.steps import Stage

logger = logging.getLogger("uk_address_matcher")

StageFactory = Callable[[], Stage]
StageLike = Union[Stage, StageFactory]


class InputBinding(NamedTuple):
    name: str
    relation: duckdb.DuckDBPyRelation

    def normalised_placeholder(self) -> str:
        name = _slug(self.name)
        if not name:
            raise ValueError(
                "InputBinding placeholder must be a non-empty, slug-compatible string"
            )
        if name[0].isdigit():
            name = f"t_{name}"
        return name

    def register(
        self,
        con: duckdb.DuckDBPyConnection,
        registered_aliases: set[str],
    ) -> str:
        alias_candidate = getattr(self.relation, "alias", None) or self.name
        alias_candidate = _slug(alias_candidate) or self.normalised_placeholder()
        if alias_candidate[0].isdigit():
            alias_candidate = f"t_{alias_candidate}"

        alias = alias_candidate
        while alias in registered_aliases:
            alias = f"{alias_candidate}_{_uid(4)}"

        if not _duckdb_table_exists(con, alias):
            con.register(alias, self.relation)

        registered_aliases.add(alias)
        return alias

    def __str__(self) -> str:
        # TODO(ThomasHepworth): improve representation at some point
        return (
            f"InputBinding(placeholder={self.name!r},\n"
            f"  relation=\n{self.relation.limit(5)})"
        )

    __repr__ = __str__


@dataclass
class QueuedFragment:
    """Fragments provider richer descriptions of pipeline stages, simplifying
    debugging and plan logging."""

    sql: str
    alias: str
    stage_name: Optional[str] = None
    fragment_name: Optional[str] = None
    stage_description: Optional[str] = None


class CTEPipeline:
    def __init__(self):
        # queue holds CTE fragments alongside stage metadata
        self.queue: List[QueuedFragment] = []
        self.spent = False  # one-shot guard
        # records (sql_text, materialised_temp_table_name) for checkpoints
        self._materialised_sql_blocks: List[Tuple[str, str]] = []

    def enqueue_sql(
        self,
        sql: str,
        output_table_name: str,
        *,
        stage_name: Optional[str] = None,
        fragment_name: Optional[str] = None,
        stage_description: Optional[str] = None,
    ) -> None:
        if self.spent:
            raise ValueError("This pipeline has already been used (spent=True).")
        self.queue.append(
            QueuedFragment(
                sql=sql,
                alias=output_table_name,
                stage_name=stage_name,
                fragment_name=fragment_name,
                stage_description=stage_description,
            )
        )

    def _compose_with_sql_from(self, items: List[QueuedFragment]) -> str:
        """
        Compose a WITH chain from the given CTE items, returning:
        WITH a AS (...), b AS (...), ...
        SELECT * FROM <last_alias>
        """
        if not items:
            raise ValueError("Cannot compose SQL from an empty CTE list.")
        with_ctes_str = ",\n\n".join(
            f"{item.alias} AS (\n{item.sql}\n)" for item in items
        )
        return f"WITH\n{with_ctes_str}\n\nSELECT * FROM {items[-1].alias}"

    def generate_cte_pipeline_sql(self, *, mark_spent: bool = True) -> str:
        if mark_spent:
            self.spent = True
        if not self.queue:
            raise ValueError("Empty pipeline.")
        return self._compose_with_sql_from(self.queue)

    @property
    def output_table_name(self) -> str:
        if not self.queue:
            raise ValueError("Empty pipeline.")
        return self.queue[-1].alias


def render_step_to_ctes(
    step: Stage,
    step_idx: int,
    prev_alias: str,
    alias_map: Dict[str, str],
) -> Tuple[List[QueuedFragment], str, str]:
    """Instantiate templated fragments into concrete, namespaced CTEs."""

    ctes: List[QueuedFragment] = []
    frag_aliases: Dict[str, str] = {}
    base_mapping = {"input": prev_alias, **alias_map}
    stage_description = None
    if step.stage_metadata and step.stage_metadata.description:
        stage_description = step.stage_metadata.description

    for frag in step.steps:
        alias = f"s{step_idx}_{_slug(step.name)}__{_slug(frag.name)}"
        replacements = {**base_mapping, **frag_aliases}

        sql = frag.sql
        for key, target in replacements.items():
            sql = sql.replace(f"{{{key}}}", target)

        ctes.append(
            QueuedFragment(
                sql=sql,
                alias=alias,
                stage_name=step.name,
                fragment_name=frag.name,
                stage_description=stage_description if not ctes else None,
            )
        )
        frag_aliases[frag.name] = alias

    default_output_name = step.steps[-1].name
    if step.output and step.output in frag_aliases:
        out_alias = frag_aliases[step.output]
    else:
        out_alias = frag_aliases[default_output_name]

    return ctes, out_alias, frag_aliases[default_output_name]


@dataclass
class DebugOptions:
    pretty_print_sql: bool = False
    debug_mode: bool = False
    debug_show_sql: bool = False
    debug_max_rows: Optional[int] = None
    debug_incremental: bool = False  # materialise each CTE one-by-one

    @staticmethod
    def _getenv_bool(name: str, default: bool) -> bool:
        val = os.getenv(name)
        if val is None:
            return default
        return val.strip().lower() in {"1", "true", "yes", "on"}

    @staticmethod
    def _getenv_int(name: str, default: Optional[int]) -> Optional[int]:
        val = os.getenv(name)
        if val is None or val == "":
            return default
        try:
            return int(val)
        except ValueError:
            return default

    @classmethod
    def from_env(cls) -> DebugOptions:
        return cls(
            pretty_print_sql=cls._getenv_bool("UKAM_PRETTY_PRINT_SQL", False),
            debug_mode=cls._getenv_bool("UKAM_DEBUG_MODE", False),
            debug_show_sql=cls._getenv_bool("UKAM_DEBUG_SHOW_SQL", False),
            debug_max_rows=cls._getenv_int("UKAM_DEBUG_MAX_ROWS", None),
            debug_incremental=cls._getenv_bool("UKAM_DEBUG_INCREMENTAL", False),
        )

    def __str__(self) -> str:
        return (
            f"RunOptions(pretty_print_sql={self.pretty_print_sql}, "
            f"debug_mode={self.debug_mode}, "
            f"debug_show_sql={self.debug_show_sql}, "
            f"debug_max_rows={self.debug_max_rows}, "
            f"debug_incremental={self.debug_incremental})"
        )


class DuckDBPipeline(CTEPipeline):
    def __init__(
        self,
        con: duckdb.DuckDBPyConnection,
        input_rel: Union[
            duckdb.DuckDBPyRelation,
            Sequence[InputBinding],
        ],
        *,
        name: Optional[str] = None,
        description: str = "",
    ):
        super().__init__()
        self.con = con

        bindings = self._normalise_inputs(input_rel)
        if not bindings:
            raise ValueError("input_rel must contain at least one DuckDB relation.")

        self._registered_relation_aliases: set[str] = set()
        self._input_alias_map: Dict[str, str] = {}
        self._input_alias_map_view: Mapping[str, str]

        self._bootstrap_inputs(bindings)

        self._src_name = self._root_alias
        seed = f"seed_{_uid()}"
        self.enqueue_sql(f"SELECT * FROM {self._src_name}", seed)
        self._current_output_alias = seed
        self._step_counter = 0
        # Defaults for run options (read from environment by default)
        self._default_debug_options = DebugOptions.from_env()
        self.name = name or f"pipeline_{_uid()}"
        self.description = description
        # Keep an ordered list of stages as they are added (excluding seed)
        self._stages: List[Stage] = []
        # Track stage signatures to guard against duplicates
        self._stage_signatures: set[tuple] = set()

    @staticmethod
    def _stage_signature(stage: Stage) -> tuple:
        """Generate a hashable signature representing the stage's logical contents."""

        return stage.fingerprint

    def _normalise_inputs(
        self,
        input_rel: Union[
            duckdb.DuckDBPyRelation,
            Sequence[InputBinding],
        ],
    ) -> List[InputBinding]:
        if isinstance(input_rel, duckdb.DuckDBPyRelation):
            return [InputBinding("root", input_rel)]
        if isinstance(input_rel, Sequence) and not isinstance(input_rel, (str, bytes)):
            if not input_rel:
                raise ValueError("input_rel sequence must not be empty.")
            bindings: List[InputBinding] = []
            for idx, item in enumerate(input_rel):
                if not isinstance(item, InputBinding):
                    raise TypeError(
                        "Sequences of inputs must contain InputBinding instances; "
                        f"got {type(item)!r} at index {idx}."
                    )
                bindings.append(item)
            if len(bindings) == 1:
                if not bindings[0].name:
                    raise ValueError(
                        "Single InputBinding must define a placeholder or provide a standalone relation."
                    )
                return bindings

            for binding in bindings:
                if not binding.name:
                    raise ValueError(
                        "All InputBinding entries must have an explicit placeholder when providing multiple inputs."
                    )
            return bindings
        else:
            raise TypeError(
                "input_rel must be a DuckDBPyRelation or a sequence of InputBinding entries."
            )

    def _bootstrap_inputs(self, bindings: Sequence[InputBinding]) -> None:
        alias_map: Dict[str, str] = {}
        seen_placeholders: set[str] = set()

        for binding in bindings:
            key = binding.normalised_placeholder()
            if key == "input":
                raise ValueError(
                    "The placeholder name 'input' is reserved; please choose a different alias."
                )
            if key in seen_placeholders:
                raise ValueError(f"Duplicate input placeholder detected: {key}")
            seen_placeholders.add(key)

            alias = binding.register(self.con, self._registered_relation_aliases)
            alias_map[key] = alias

        first_key = next(iter(alias_map))
        root_alias = alias_map[first_key]

        alias_map.setdefault("root", root_alias)

        self._input_bindings: Tuple[InputBinding, ...] = tuple(bindings)
        self._input_alias_map = alias_map
        self._root_alias = root_alias
        self._input_alias_map_view = MappingProxyType(self._input_alias_map)

    @property
    def input_alias_map(self) -> Mapping[str, str]:
        return self._input_alias_map_view

    @property
    def root_alias(self) -> str:
        return self._root_alias

    @property
    def input_bindings(self) -> Tuple[InputBinding, ...]:
        return self._input_bindings

    def show_plan(self) -> None:
        """Return a human-friendly multi-line description of the pipeline.

        Format example:
            ┌──────────────────────────────┐
            │ 🔧 Pipeline Plan (11 stages) │
            │ My pipeline's name           │
            └──────────────────────────────┘

            A description of the pipeline.
            -----------------------------

             1. stage_one [group]
                ↳ <Description>
                │ depends on: other_stage
                | other metadata...
        """
        lines: List[str] = []

        stage_count = len(self._stages)
        title = (
            f"🔧 Pipeline Plan ({stage_count} stage{'s' if stage_count != 1 else ''})"
        )
        name_line = self.name
        # Construct the header box with proper padding
        content_width = max(len(title), len(name_line))
        top_bottom = "─" * (content_width + 3)  # +3 for side padding spaces
        lines.append(f"┌{top_bottom}┐")
        lines.append(f"│ {title.ljust(content_width)} │")
        # Only show pipeline name line if distinct from title wording
        if name_line and name_line != title:
            lines.append(f"│ {name_line.ljust(content_width + 1)} │")
        lines.append(f"└{top_bottom}┘")
        lines.append("")

        # Show description only if it exists and isn't just the name repeated
        if (
            self.description
            and self.description.strip().lower() != self.name.strip().lower()
        ):
            lines.append(self.description)
            lines.append("-" * len(self.description))
            lines.append("")
        if not self._stages:
            lines.append("(no stages added)")
        for idx, stage in enumerate(self._stages, start=1):
            block = stage.format_plan_block()
            first_line, *rest = block.splitlines()
            lines.append(f"{idx:2d}. {first_line}")
            for rl in rest:
                lines.append(f"    {rl}")
            if idx < len(self._stages):
                lines.append("")
        plan_text = "\n".join(lines)
        _emit_debug(plan_text)

    def add_step(self, step: Stage, *, explain: bool = False) -> None:
        signature = self._stage_signature(step)
        # TODO(ThomasHepworth): Temporarily disable duplicate stage detection
        # if signature in self._stage_signatures:
        #     raise ValueError(
        #         "Duplicate stage detected: "
        #         f"stage '{step.name}' has already been added to pipeline '{self.name}'."
        #     )
        # run any preludes / registers
        if step.registers:
            for k, rel in step.registers.items():
                self.con.register(k, rel)
        if step.preludes:
            for fn in step.preludes:
                fn(self.con)

        prev_alias = self.output_table_name
        step_idx = self._step_counter
        ctes, out_alias, default_alias = render_step_to_ctes(
            step,
            step_idx,
            prev_alias,
            self._input_alias_map,
        )
        for fragment in ctes:
            self.enqueue_sql(
                fragment.sql,
                fragment.alias,
                stage_name=fragment.stage_name,
                fragment_name=fragment.fragment_name,
                stage_description=fragment.stage_description,
            )
        self._current_output_alias = out_alias
        self._step_counter += 1

        if step.output:
            self._input_alias_map[step.output] = out_alias
            last_step_name = step.steps[-1].name
            self._input_alias_map.setdefault(last_step_name, default_alias)
        else:
            last_step_name = step.steps[-1].name
            self._input_alias_map[last_step_name] = default_alias

        # TODO(ThomasHepworth): Debugging does not currently support checkpoints
        # If we are explaining, skip checkpoint materialisation
        # as we want to print out a single final plan
        if step.checkpoint and not explain:
            self._materialise_checkpoint()
        # Record the stage for plan display
        self._stages.append(step)
        self._stage_signatures.add(signature)

    def _materialise_checkpoint(self) -> None:
        # Compose without marking spent
        current_fragments = list(self.queue)
        sql = self.generate_cte_pipeline_sql(mark_spent=False)
        tmp = f"__seg_{_uid()}"
        self._materialised_sql_blocks.append((sql, tmp))
        self.con.execute(f"CREATE OR REPLACE TEMP TABLE {tmp} AS {sql}")
        # reset the queue with a fresh seed reading from tmp
        self.queue.clear()
        seed = f"seed_{_uid()}"
        self.enqueue_sql(f"SELECT * FROM {tmp}", seed)
        self._current_output_alias = seed

        # Replace any alias references produced before the checkpoint with the new seed
        previous_aliases = {fragment.alias for fragment in current_fragments}
        for placeholder, alias in list(self._input_alias_map.items()):
            if alias in previous_aliases:
                self._input_alias_map[placeholder] = seed

        # Update root alias so subsequent references use the materialised relation
        self._root_alias = seed

    def debug(
        self,
        *,
        show_sql: bool = False,
        max_rows: Optional[int] = None,
        materialise: bool = False,
        return_last: bool = False,
    ) -> Optional[duckdb.DuckDBPyRelation]:
        """Debug the pipeline.

        Modes:
          * Logical (materialise=False): For each CTE build a partial WITH chain
            up to that CTE and show result (no temp table persistence per step).
          * Materialising (materialise=True): For each SQL fragment run
            `CREATE OR REPLACE TEMP TABLE <alias> AS <sql>`.

        If `return_last` and `materialise` are both True, a relation for the final
        materialised alias is returned.
        """
        if not self.queue:
            logger.debug("No CTEs enqueued.")
            return None

        timing_report = TimingReport()

        if not materialise:
            total = len(self.queue)
            for index in range(1, total + 1):
                subset = self.queue[:index]
                current_fragment = subset[-1]
                sql = self._compose_with_sql_from(subset)

                header = f"\n=== DEBUG STEP {index}/{total} — alias `{current_fragment.alias}`"
                if current_fragment.stage_name:
                    header += f" · stage `{current_fragment.stage_name}`"
                if current_fragment.fragment_name:
                    header += f" · cte `{current_fragment.fragment_name}`"
                header += " ===\n"
                _emit_debug(header)
                if current_fragment.stage_description:
                    _emit_debug(f"↳ {current_fragment.stage_description}\n")
                if show_sql:
                    _emit_debug(_pretty_sql(sql))
                    _emit_debug("\n--------------------------------------------\n")

                start = perf_counter()
                rel = self.con.sql(sql)
                if max_rows is not None:
                    rel.show(max_rows=max_rows)
                else:
                    rel.show()
                elapsed = perf_counter() - start

                timing_report.add_timing(
                    step_number=index,
                    alias=current_fragment.alias,
                    duration_seconds=elapsed,
                    stage_name=current_fragment.stage_name,
                    fragment_name=current_fragment.fragment_name,
                )

                _emit_debug(
                    f"Step {index}/{total} executed in {_format_duration(elapsed)}"
                )

            _emit_debug(timing_report.format_report())
            return None

        if len(self.queue) == 1:  # only seed
            return self.con.table(self.queue[0].alias) if return_last else None

        work_items = self.queue
        total = len(work_items)
        for idx, fragment in enumerate(work_items, start=1):
            # Always emit a header during materialised debug to mirror non-materialised mode
            header = f"\n=== DEBUG STEP {idx}/{total} — alias `{fragment.alias}`"
            if fragment.stage_name:
                header += f" · stage `{fragment.stage_name}`"
            if fragment.fragment_name:
                header += f" · cte `{fragment.fragment_name}`"
            header += " ===\n"
            _emit_debug(header)
            if fragment.stage_description:
                _emit_debug(f"↳ {fragment.stage_description}\n")
            if show_sql:
                _emit_debug(_pretty_sql(fragment.sql))
                _emit_debug("\n--------------------------------------------\n")
            start = perf_counter()
            # TODO(ThomasHepworth): ParserException: Parser Error: syntax error at or near "{"
            # is a common error, typically indicating that a placeholder wasn't replaced
            # correctly. There are various ways we could try to catch this earlier.
            self.con.execute(
                f"CREATE OR REPLACE TEMP TABLE {fragment.alias} AS {fragment.sql}"
            )
            rel = self.con.table(fragment.alias)
            if max_rows is not None:
                rel.show(max_rows=max_rows)
            else:
                rel.show()

            rel.count("*").show()
            elapsed = perf_counter() - start

            timing_report.add_timing(
                step_number=idx,
                alias=fragment.alias,
                duration_seconds=elapsed,
                stage_name=fragment.stage_name,
                fragment_name=fragment.fragment_name,
            )

            _emit_debug(f"Step {idx}/{total} executed in {_format_duration(elapsed)}")

        _emit_debug(timing_report.format_report())

        if return_last:
            return self.con.table(work_items[-1].alias)
        return None

    def run(
        self,
        options: Optional[DebugOptions] = None,
        *,
        explain: bool = False,
    ) -> duckdb.DuckDBPyRelation:
        """Run the pipeline using the provided options (or defaults)."""

        if options is None:
            options = self._default_debug_options
        elif not isinstance(options, DebugOptions):
            raise TypeError(
                "options must be a DebugOptions instance when provided; "
                f"got {type(options)!r}."
            )
        # Incremental/materialising path
        if options.debug_incremental:
            return self.debug(
                show_sql=options.debug_show_sql,
                max_rows=options.debug_max_rows,
                materialise=True,
                return_last=True,
            )

        # Non-incremental debug preview
        if options.debug_mode:
            self.debug(
                show_sql=options.debug_show_sql,
                max_rows=options.debug_max_rows,
                materialise=False,
            )

        final_sql = self.generate_cte_pipeline_sql()
        final_alias = self.output_table_name
        segments: List[Tuple[str, str]] = [
            *self._materialised_sql_blocks,
            (final_sql, final_alias),
        ]
        checkpoint_count = len(self._materialised_sql_blocks)

        def _segment_label(idx: int, alias: str) -> str:
            """Generate a descriptive label for a SQL segment."""
            if idx <= checkpoint_count:
                return f"materialised checkpoint saved as {alias}"
            if checkpoint_count:
                return f"final segment after checkpoints (current alias {alias})"
            return f"final segment (current alias {alias})"

        if options.pretty_print_sql:
            for idx, (sql, alias) in enumerate(segments, start=1):
                label = _segment_label(idx, alias)
                _emit_debug(f"\n=== SQL SEGMENT {idx} ({label}) ===\n")
                _emit_debug(_pretty_sql(sql))
                _emit_debug("\n===============================\n")

        if explain:
            for idx, (sql, alias) in enumerate(segments, start=1):
                label = _segment_label(idx, alias)
                _explain_debug(self.con, sql)
            return None

        return self.con.sql(final_sql)


def _ensure_stage(stage_like: StageLike) -> Stage:
    """Normalise a stage reference into a concrete `Stage` instance."""
    if isinstance(stage_like, Stage):
        return stage_like

    if callable(stage_like):
        candidate = stage_like()
        if isinstance(candidate, Stage):
            return candidate
        raise TypeError(
            "Stage factory callable must return a `Stage` instance; "
            f"got {type(candidate)!r}."
        )

    raise TypeError(
        "Stages must be provided as `Stage` instances or zero-argument factories that "
        "return a `Stage`."
    )


def create_sql_pipeline(
    con: duckdb.DuckDBPyConnection,
    input_rel: Union[duckdb.DuckDBPyRelation, Sequence[InputBinding]],
    stage_specs: Iterable[StageLike],
    *,
    pipeline_name: str | None = None,
    pipeline_description: str | None = None,
) -> DuckDBPipeline:
    """Construct a `DuckDBPipeline` from the provided stage specifications.

    Parameters
    ----------
    con:
        Active DuckDB connection used for executing SQL.
    input_rel:
        Either a single `DuckDBPyRelation` (for simple pipelines) or a sequence
        of `InputBinding` objects when multiple named inputs are required.
    stage_specs:
        Iterable of stages or stage factories to add to the pipeline in order.
    pipeline_name / pipeline_description:
        Optional metadata used when rendering plans or debug output.
    """

    pipeline = DuckDBPipeline(
        con,
        input_rel,
        name=pipeline_name,
        description=pipeline_description or "",
    )

    for stage_like in stage_specs:
        pipeline.add_step(_ensure_stage(stage_like))

    return pipeline
