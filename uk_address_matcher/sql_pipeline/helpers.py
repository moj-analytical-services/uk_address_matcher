import importlib.resources as pkg_resources
import logging
import random
import re
import string
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import List

import duckdb

logger = logging.getLogger("uk_address_matcher")

_CON_INPUT_RELATION_ALIAS_CACHE: dict[int, dict[str, str]] = {}


@dataclass
class StageTimingRecord:
    """Records execution timing for a pipeline stage."""

    step_number: int
    alias: str
    stage_name: str | None
    fragment_name: str | None
    duration_seconds: float


@dataclass
class TimingReport:
    """Collects and formats timing information for pipeline stages."""

    records: List[StageTimingRecord] = field(default_factory=list)

    def add_timing(
        self,
        step_number: int,
        alias: str,
        duration_seconds: float,
        stage_name: str | None = None,
        fragment_name: str | None = None,
    ) -> None:
        """Record a stage execution time."""
        self.records.append(
            StageTimingRecord(
                step_number=step_number,
                alias=alias,
                stage_name=stage_name,
                fragment_name=fragment_name,
                duration_seconds=duration_seconds,
            )
        )

    def format_report(self) -> str:
        """Generate a formatted timing report."""
        if not self.records:
            return "(no timing data collected)"

        lines = ["\n" + "=" * 80]
        lines.append("⏱️  PIPELINE TIMING REPORT")
        lines.append("=" * 80)

        total_duration = sum(r.duration_seconds for r in self.records)

        # Column headers
        lines.append(
            f"{'Step':<6} {'Duration':<12} {'%':<7} {'Alias':<25} {'Stage / Fragment'}"
        )
        lines.append("-" * 80)

        for record in self.records:
            percentage = (
                (record.duration_seconds / total_duration * 100)
                if total_duration > 0
                else 0
            )
            duration_str = _format_duration(record.duration_seconds)

            stage_info = ""
            if record.stage_name and record.fragment_name:
                stage_info = f"{record.stage_name} / {record.fragment_name}"
            elif record.stage_name:
                stage_info = record.stage_name
            elif record.fragment_name:
                stage_info = record.fragment_name

            lines.append(
                f"{record.step_number:<6} {duration_str:<12} {percentage:>5.1f}%  "
                f"{record.alias:<25} {stage_info}"
            )

        lines.append("-" * 80)
        lines.append(f"{'TOTAL':<6} {_format_duration(total_duration):<12}")
        lines.append("=" * 80 + "\n")

        return "\n".join(lines)


def _format_duration(seconds: float) -> str:
    if seconds >= 60:
        minutes, rem = divmod(seconds, 60)
        return f"{int(minutes)}m {rem:05.2f}s"
    return f"{seconds:.2f}s"


def _emit_debug(msg: str) -> None:
    """Emit debug output via logger if configured, else stdout.

    Many users won't configure logging in quick scripts, so when debug/pretty-print
    is enabled we print to stdout to ensure visibility.
    """
    if logger.handlers and logger.isEnabledFor(logging.DEBUG):
        logger.debug(msg)
    else:
        if msg.endswith("\n"):
            sys.stdout.write(msg)
        else:
            sys.stdout.write(f"{msg}\n")
        sys.stdout.flush()


def _uid(n: int = 6) -> str:
    return "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in range(n)
    )


def _slug(s: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", s.lower())


def _pretty_sql(sql: str) -> str:
    # Hook for pretty printers if desired
    return sql


def package_resource_read_sql(package: str, filename: str) -> str:
    """Build a SELECT statement for a packaged resource based on file suffix.

    Supports JSON, CSV, and Parquet inputs via DuckDB reader functions.
    """
    suffix = Path(filename).suffix.lower()
    readers = {
        ".json": "read_json_auto",
        ".csv": "read_csv_auto",
        ".parquet": "read_parquet",
    }
    reader = readers.get(suffix)
    if reader is None:
        raise ValueError(f"Unsupported resource type '{suffix}' for '{filename}'.")

    with pkg_resources.path(package, filename) as resource_path:
        return f"SELECT * FROM {reader}('{resource_path}')"


def _duckdb_table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    result = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table_name],
    ).fetchone()
    return result[0] > 0


def _quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _relation_from_registered_alias(
    con: duckdb.DuckDBPyConnection,
    alias: str,
) -> duckdb.DuckDBPyRelation:
    """Read a registered alias via SQL rather than ``con.table(alias)``.

    DuckDB 1.3.2 returns a debug-style ``sql_query()`` string for relations
    created with ``con.table(alias)`` after ``con.register(...)``. Building the
    relation via ``SELECT * FROM <alias>`` keeps ``sql_query()`` valid across
    DuckDB versions, which matters because downstream code nests that SQL.
    """
    return con.sql(f"SELECT * FROM {_quote_identifier(alias)}")


def _drop_table_and_registered_aliases(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
) -> None:
    # information_schema.tables reports temporary tables AND temporary views
    # as 'LOCAL TEMPORARY', so table_type cannot distinguish them. Check
    # duckdb_views() (which covers regular views, temp views, and relations
    # registered via con.register) and fall back to DROP TABLE IF EXISTS.
    view_exists = con.execute(
        "SELECT 1 FROM duckdb_views() WHERE view_name = ?",
        [table_name],
    ).fetchone()
    if view_exists is not None:
        con.execute(f"DROP VIEW IF EXISTS {_quote_identifier(table_name)}")
        return

    con.execute(f"DROP TABLE IF EXISTS {_quote_identifier(table_name)}")


def _register_input_relation_once(
    relation: duckdb.DuckDBPyRelation,
    *,
    con: duckdb.DuckDBPyConnection,
    role: str = "input",
) -> duckdb.DuckDBPyRelation:
    """Ensure a relation is registered on ``con`` and reuse stable aliases.

    Handles relations created by a different DuckDB connection by falling back
    to Arrow registration.
    """
    registration_cache = _CON_INPUT_RELATION_ALIAS_CACHE.setdefault(id(con), {})

    relation_sql = relation.sql_query()
    cached_alias = registration_cache.get(relation_sql)
    if cached_alias and _duckdb_table_exists(con, cached_alias):
        return _relation_from_registered_alias(con, cached_alias)

    relation_alias = getattr(relation, "alias", None)
    if (
        relation_alias
        and not str(relation_alias).startswith("unnamed_relation_")
        and _duckdb_table_exists(con, str(relation_alias))
    ):
        registration_cache[relation_sql] = str(relation_alias)
        return _relation_from_registered_alias(con, str(relation_alias))

    alias = f"__ukam__tmp_input_{role}_{_uid()}"
    while _duckdb_table_exists(con, alias):
        alias = f"__ukam__tmp_input_{role}_{_uid()}"

    try:
        con.register(alias, relation)
    except duckdb.InvalidInputException as exc:
        if "created by another Connection" not in str(exc):
            raise
        con.register(alias, relation.to_arrow_table())

    registration_cache[relation_sql] = alias
    return _relation_from_registered_alias(con, alias)


def _explain_debug(con: duckdb.DuckDBPyConnection, sql: str) -> None:
    _emit_debug(
        f"Generating EXPLAIN plan for final SQL.\n============================\n:{sql}\n"
    )
    _emit_debug(con.sql(f"EXPLAIN {sql}").fetchone()[1])
