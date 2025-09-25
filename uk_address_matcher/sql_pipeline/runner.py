from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Tuple

from uk_address_matcher.sql_pipeline.helpers import (
    _duckdb_table_exists,
    _emit_debug,
    _pretty_sql,
    _slug,
    _uid,
)
from uk_address_matcher.sql_pipeline.steps import Stage

if TYPE_CHECKING:
    import duckdb


logger = logging.getLogger("uk_address_matcher")

StageFactory = Callable[[], Stage]


class CTEPipeline:
    def __init__(self):
        # queue holds tuples of (sql, output_alias)
        self.queue: List[Tuple[str, str]] = []
        self.spent = False  # one-shot guard
        # records (sql_text, materialised_temp_table_name) for checkpoints
        self._materialised_sql_blocks: List[Tuple[str, str]] = []

    def enqueue_sql(self, sql: str, output_table_name: str) -> None:
        if self.spent:
            raise ValueError("This pipeline has already been used (spent=True).")
        self.queue.append((sql, output_table_name))

    def _compose_with_sql_from(self, items: List[Tuple[str, str]]) -> str:
        """
        Compose a WITH chain from the given CTE items, returning:
        WITH a AS (...), b AS (...), ...
        SELECT * FROM <last_alias>
        """
        if not items:
            raise ValueError("Cannot compose SQL from an empty CTE list.")
        with_ctes_str = ",\n\n".join(
            f"{alias} AS (\n{sql}\n)" for (sql, alias) in items
        )
        return f"WITH\n{with_ctes_str}\n\nSELECT * FROM {items[-1][1]}"

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
        return self.queue[-1][1]


def render_step_to_ctes(
    step: Stage, step_idx: int, prev_alias: str
) -> Tuple[List[Tuple[str, str]], str]:
    """Instantiate templated fragments into concrete, namespaced CTEs."""
    ctes: List[Tuple[str, str]] = []
    frag_aliases: Dict[str, str] = {}
    mapping = {"input": prev_alias}

    for frag in step.steps:
        alias = f"s{step_idx}_{_slug(step.name)}__{_slug(frag.name)}"

        # apply placeholders
        sql = frag.sql.replace("{input}", mapping["input"])
        # then any prior fragment references
        for k, v in frag_aliases.items():
            sql = sql.replace(f"{{{k}}}", v)

        ctes.append((sql, alias))
        frag_aliases[frag.name] = alias

    out_alias = frag_aliases[step.output or step.steps[-1].name]
    return ctes, out_alias


@dataclass
class RunOptions:
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
    def from_env(cls) -> "RunOptions":
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
        input_rel: duckdb.DuckDBPyRelation,
        *,
        name: Optional[str] = None,
        description: str = "",
    ):
        super().__init__()
        self.con = con
        self._src_name = input_rel.alias or f"src_{_uid()}"
        if not _duckdb_table_exists(self.con, self._src_name):
            self.con.register(self._src_name, input_rel)

        seed = f"seed_{_uid()}"
        self.enqueue_sql(f"SELECT * FROM {self._src_name}", seed)
        self._current_output_alias = seed
        self._step_counter = 0
        # Defaults for run options (read from environment by default)
        self._default_run_options = RunOptions.from_env()

    def log_pipeline(self) -> None: ...

    def add_step(self, step: Stage) -> None:
        # run any preludes / registers
        if step.registers:
            for k, rel in step.registers.items():
                self.con.register(k, rel)
        if step.preludes:
            for fn in step.preludes:
                fn(self.con)

        prev_alias = self.output_table_name
        step_idx = self._step_counter
        ctes, out_alias = render_step_to_ctes(step, step_idx, prev_alias)
        for sql, alias in ctes:
            self.enqueue_sql(sql, alias)
        self._current_output_alias = out_alias
        self._step_counter += 1

        if step.checkpoint:
            self._materialise_checkpoint()

    def _materialise_checkpoint(self) -> None:
        # Compose without marking spent
        sql = self.generate_cte_pipeline_sql(mark_spent=False)
        tmp = f"__seg_{_uid()}"
        self._materialised_sql_blocks.append((sql, tmp))
        self.con.execute(f"CREATE OR REPLACE TEMP TABLE {tmp} AS {sql}")
        # reset the queue with a fresh seed reading from tmp
        self.queue.clear()
        seed = f"seed_{_uid()}"
        self.enqueue_sql(f"SELECT * FROM {tmp}", seed)
        self._current_output_alias = seed

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

        if not materialise:
            total = len(self.queue)
            for index in range(1, total + 1):
                subset = self.queue[:index]
                alias = subset[-1][1]
                sql = self._compose_with_sql_from(subset)

                _emit_debug(f"\n=== DEBUG STEP {index}/{total} — alias `{alias}` ===\n")
                if show_sql:
                    _emit_debug(_pretty_sql(sql))
                    _emit_debug("\n--------------------------------------------\n")

                rel = self.con.sql(sql)
                if max_rows is not None:
                    rel.show(max_rows=max_rows)
                else:
                    rel.show()
            return None

        if len(self.queue) == 1:  # only seed
            return self.con.table(self.queue[0][1]) if return_last else None

        work_items = self.queue
        total = len(work_items)
        for idx, (sql, alias) in enumerate(work_items, start=1):
            if show_sql:
                _emit_debug(f"\n=== DEBUG STEP {idx}/{total} — alias `{alias}` ===\n")
                _emit_debug(_pretty_sql(sql))
                _emit_debug("\n--------------------------------------------\n")
            self.con.execute(f"CREATE OR REPLACE TEMP TABLE {alias} AS {sql}")
            rel = self.con.table(alias)
            if max_rows is not None:
                rel.show(max_rows=max_rows)
            else:
                rel.show()

        if return_last:
            return self.con.table(work_items[-1][1])
        return None

    def run_with_options(self, options: RunOptions):
        """Preferred entry: run pipeline using the given RunOptions."""
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
        if options.pretty_print_sql:
            final_alias = self.output_table_name
            segments: List[Tuple[str, str]] = [
                *self._materialised_sql_blocks,
                (final_sql, final_alias),
            ]
            checkpoint_count = len(self._materialised_sql_blocks)
            for idx, (sql, materialised_name) in enumerate(segments, start=1):
                if idx <= checkpoint_count:
                    label = f"materialised checkpoint saved as {materialised_name}"
                else:
                    if checkpoint_count:
                        label = f"final segment after checkpoints (current alias {materialised_name})"
                    else:
                        label = f"final segment (current alias {materialised_name})"
                _emit_debug(f"\n=== SQL SEGMENT {idx} ({label}) ===\n")
                _emit_debug(_pretty_sql(sql))
                _emit_debug("\n===============================\n")
        return self.con.sql(final_sql)

    def run(
        self,
        *,
        pretty_print_sql: Optional[bool] = None,
        debug_mode: Optional[bool] = None,
        debug_show_sql: Optional[bool] = None,
        debug_max_rows: Optional[int] = None,
        debug_incremental: Optional[bool] = None,
    ):
        """
        Backwards-compatible wrapper that builds RunOptions from args merged
        with environment-derived defaults. Prefer run_with_options.
        """
        opts = RunOptions(
            pretty_print_sql=(
                pretty_print_sql
                if pretty_print_sql is not None
                else self._default_run_options.pretty_print_sql
            ),
            debug_mode=(
                debug_mode
                if debug_mode is not None
                else self._default_run_options.debug_mode
            ),
            debug_show_sql=(
                debug_show_sql
                if debug_show_sql is not None
                else self._default_run_options.debug_show_sql
            ),
            debug_max_rows=(
                debug_max_rows
                if debug_max_rows is not None
                else self._default_run_options.debug_max_rows
            ),
            debug_incremental=(
                debug_incremental
                if debug_incremental is not None
                else self._default_run_options.debug_incremental
            ),
        )
        return self.run_with_options(opts)
