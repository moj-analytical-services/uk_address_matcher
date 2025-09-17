from __future__ import annotations

import random
import re
import string
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import duckdb


def _uid(n: int = 6) -> str:
    return "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in range(n)
    )


def _slug(s: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", s.lower())


def _pretty_sql(sql: str) -> str:
    # Hook for pretty printers if desired
    return sql


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


@dataclass
class CTEStep:
    name: str
    sql: str  # may reference {input} and prior fragment names as {frag_name}


@dataclass
class Stage:
    name: str
    steps: List[CTEStep]
    output: Optional[str] = None
    # DuckDB-specific helpers
    registers: Dict[str, duckdb.DuckDBPyRelation] = None
    preludes: List = None
    checkpoint: bool = False


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


# ---------------------------
# duckdb-oriented runner with checkpoints and debug mode
# ---------------------------
class DuckDBPipeline(CTEPipeline):
    def __init__(
        self, con: duckdb.DuckDBPyConnection, input_rel: duckdb.DuckDBPyRelation
    ):
        super().__init__()
        self.con = con
        self._src_name = f"__src_{_uid()}"
        self.con.register(self._src_name, input_rel)
        seed = f"seed_{_uid()}"
        self.enqueue_sql(f"SELECT * FROM {self._src_name}", seed)
        self._current_output_alias = seed
        self._step_counter = 0

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

    def debug(self, *, show_sql: bool = False, max_rows: Optional[int] = None) -> None:
        """
        Execute and display the result of each CTE in order, using a partial WITH chain
        up to that CTE. Does NOT mark the pipeline as spent.

        Note: if checkpoints have been used, this shows intermediates only for the
        current segment (after the last checkpoint).
        """
        if not self.queue:
            print("No CTEs enqueued.")
            return

        total = len(self.queue)
        for i in range(1, total + 1):
            subset = self.queue[:i]
            alias = subset[-1][1]
            sql = self._compose_with_sql_from(subset)

            print(f"\n=== DEBUG STEP {i}/{total} — alias `{alias}` ===\n")
            if show_sql:
                print(_pretty_sql(sql))
                print("\n--------------------------------------------\n")

            rel = self.con.sql(sql)
            if max_rows is not None:
                rel.show(max_rows=max_rows)
            else:
                rel.show()

    def run(
        self,
        *,
        pretty_print_sql: bool = True,
        debug_mode: bool = False,
        debug_show_sql: bool = False,
        debug_max_rows: Optional[int] = None,
    ):
        # Optional debug pass over all intermediates before final execution
        if debug_mode:
            self.debug(show_sql=debug_show_sql, max_rows=debug_max_rows)

        final_sql = self.generate_cte_pipeline_sql()
        if pretty_print_sql:
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
                print(f"\n=== SQL SEGMENT {idx} ({label}) ===\n")
                print(_pretty_sql(sql))
                print("\n===============================\n")
        return self.con.sql(final_sql)
