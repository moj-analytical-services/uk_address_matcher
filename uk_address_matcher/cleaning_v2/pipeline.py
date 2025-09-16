# try_this_out.py
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
    return sql


class CTEPipeline:
    def __init__(self):
        self.queue: List[Tuple[str, str]] = []
        self.spent = False  # one-shot guard
        self._materialised_sql_blocks: List[Tuple[str, str]] = []

    def enqueue_sql(self, sql: str, output_table_name: str) -> None:
        if self.spent:
            raise ValueError("This pipeline has already been used (spent=True).")
        self.queue.append((sql, output_table_name))

    def generate_cte_pipeline_sql(self, *, mark_spent: bool = True) -> str:
        if mark_spent:
            self.spent = True
        if not self.queue:
            raise ValueError("Empty pipeline.")

        # include ALL CTEs in the WITH
        ctes_str = ",\n\n".join(
            f"{output_table_name} AS (\n{sql}\n)"
            for sql, output_table_name in self.queue
        )
        sql = f"WITH\n{ctes_str}\n\nSELECT * FROM {self.queue[-1][1]}"
        return sql

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
# duckdb-oriented runner with checkpoints
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
        sql = self.generate_cte_pipeline_sql(mark_spent=False)
        tmp = f"__seg_{_uid()}"
        self._materialised_sql_blocks.append((sql, tmp))
        self.con.execute(f"CREATE OR REPLACE TEMP TABLE {tmp} AS {sql}")
        # reset the queue with a fresh seed reading from tmp
        self.queue.clear()
        seed = f"seed_{_uid()}"
        self.enqueue_sql(f"SELECT * FROM {tmp}", seed)
        self._current_output_alias = seed

    def run(self, *, pretty_print_sql: bool = True):
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


# ---------------------------
# demo steps (3 simple ones)
# ---------------------------
def step_uppercase_name() -> Stage:
    uppercased_sql = """
        SELECT
            id,
            UPPER(name) AS name,
            age
        FROM {input}
        """
    uppercased_fragment = CTEStep("uppercased", uppercased_sql)
    return Stage(
        name="upper_case_name",
        steps=[uppercased_fragment],
        output="uppercased",
    )


def step_add_one_to_age() -> Stage:
    aged_sql = """
        SELECT
            id,
            name,
            age + 1 AS age
        FROM {input}
        """
    aged_fragment = CTEStep("aged", aged_sql)
    return Stage(
        name="add_one_to_age",
        steps=[aged_fragment],
        output="aged",
    )


def step_filter_age_ge_30() -> Stage:
    filtered_sql = """
        SELECT *
        FROM {input}
        WHERE age >= 30
        """
    filtered_fragment = CTEStep("filtered", filtered_sql)
    return Stage(
        name="filter_age_ge_30",
        steps=[filtered_fragment],
        output="filtered",
    )


def step_filter_and_aggregate_age_ge_30() -> Stage:
    filtered_sql = """
        SELECT
            id,
            name,
            age
        FROM {input}
        WHERE age >= 30
        """
    aggregated_sql = """
        SELECT
            COUNT(*) AS cnt_over_30,
            AVG(age) AS avg_age_over_30
        FROM {filtered_over_30}
        """
    filtered_fragment = CTEStep("filtered_over_30", filtered_sql)
    aggregated_fragment = CTEStep("aggregated", aggregated_sql)
    return Stage(
        name="filter_and_aggregate_age_ge_30",
        steps=[filtered_fragment, aggregated_fragment],
        output="aggregated",
    )
