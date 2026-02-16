from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Optional

from uk_address_matcher.sql_pipeline.helpers import _uid

if TYPE_CHECKING:
    import duckdb

    from uk_address_matcher.sql_pipeline.runner import DebugOptions


_REQUIRED_COLUMNS = {
    "ukam_address_id",
    "canonical_ukam_address_id",
    "resolved_canonical_id",
}

_IMPORT_MODULE = "uk_address_matcher"


class _StageList(list):
    """A ``list`` subclass whose `repr`/`str` shows stage documentation.

    Behaves identically to a plain `list[type[MatchingStage]]` for iteration,
    indexing, and membership tests, but prints a human-friendly summary when
    displayed interactively or via `print()`.
    """

    @staticmethod
    def _stage_summary(stage_cls: type) -> str:
        doc = stage_cls.__doc__ or ""
        # Take only the first line of the docstring.
        first_line = doc.strip().split("\n")[0].strip()
        return first_line

    def __repr__(self) -> str:
        if not self:
            return "No matching stages registered."

        lines = [f"Available matching stages (import from {_IMPORT_MODULE}):"]
        name_width = max(len(cls.__name__) for cls in self)
        for cls in self:
            summary = self._stage_summary(cls)
            desc = f" — {summary}" if summary else ""
            lines.append(f"  {cls.__name__:<{name_width}}{desc}")

        lines.append("")
        lines.append("Usage:")
        names = ", ".join(cls.__name__ for cls in self)
        lines.append(f"  from {_IMPORT_MODULE} import {names}")

        return "\n".join(lines)

    def __str__(self) -> str:
        return repr(self)


class MatchingStage(ABC):
    """Base class for matching stages.

    Each stage
    maintains a shared DuckDB table (`results_table`) with one row per *messy*
    address record. Stages update
    existing rows keyed by `ukam_address_id`.

    **Results table contract**
    The `results_table` contains at minimum:

    - `ukam_address_id` — internal identifier for the messy record (the update key)
    - `resolved_canonical_id` — the matched canonical record's user-facing ID
        (NULL until matched)
    - `canonical_ukam_address_id` — internal identifier for the matched canonical
        record (NULL until matched)
    - `match_reason` — label describing how the match was obtained (NULL until
        matched; typically a DuckDB `ENUM` backed by
        :class:`uk_address_matcher.sql_pipeline.match_reasons.MatchReason`)

    The table may also contain messy record columns (for example `unique_id` and
    optional `ukam_label`) plus any stage-specific columns added by earlier
    stages.

    **Stage output contract**
    Subclasses implement the method `find_matches` and return a
    `duckdb.DuckDBPyRelation` containing any new matches found

    The returned relation must contain at minimum:

    - `ukam_address_id`
    - `canonical_ukam_address_id`
    - `resolved_canonical_id`

    It may also contain:

    - `match_reason` — if absent, :meth:`run` populates it with `stage_name`.
        (Any value written must be compatible with the type of
        `results_table.match_reason`.)
    - Any additional columns (e.g. `match_weight`, `distinguishability`). The
        base implementation will add these columns to `results_table` on first use
        and then populate them for rows updated by this stage.

    The relation should contain at most one row per `ukam_address_id`. If multiple
    rows are returned for the same record then the update semantics are undefined.

    Note that find_matches always requires df_unmatched and df_canonical to be passed,
    but it considers these tables immutable i.e. it doesn't changes them,
    it just uses them to find matches, and then the matches are updated in the
    results table

    """

    @classmethod
    def available_stages(cls) -> _StageList:
        """All registered ``MatchingStage`` subclasses.

        Returns a list of stage classes.  Printing the result shows each
        stage with its one-line description and an import example::

            >>> MatchingStage.available_stages()
            Available matching stages (from uk_address_matcher):
              ExactMatchStage  – Exact hash-join matching on clean_full_address + postcode.
              SplinkStage      – Splink probabilistic matching stage.
              ...

        The returned object is a regular ``list`` subclass, so you can iterate,
        index, or pass it to any code that expects ``list[type[MatchingStage]]``.
        """
        out: list[type[MatchingStage]] = []
        stack = list(cls.__subclasses__())
        while stack:
            sub = stack.pop()
            out.append(sub)
            stack.extend(sub.__subclasses__())
        out.sort(key=lambda s: s.__name__)
        return _StageList(out)

    @abstractmethod
    def find_matches(
        self,
        con: duckdb.DuckDBPyConnection,
        stage_name: str,
        df_unmatched: duckdb.DuckDBPyRelation,
        df_canonical: duckdb.DuckDBPyRelation,
        debug_options: Optional[DebugOptions] = None,
        explain: bool = False,
    ) -> Optional[duckdb.DuckDBPyRelation]:
        """Return matches for currently unmatched records."""

    def run(
        self,
        con: duckdb.DuckDBPyConnection,
        stage_name: str,
        results_table: str,
        df_unmatched: duckdb.DuckDBPyRelation,
        df_canonical: duckdb.DuckDBPyRelation,
        debug_options: Optional[DebugOptions] = None,
        explain: bool = False,
    ) -> None:
        """Run this stage and write matched rows into the shared results table."""
        stage_matches = self.find_matches(
            con=con,
            stage_name=stage_name,
            df_unmatched=df_unmatched,
            df_canonical=df_canonical,
            debug_options=debug_options,
            explain=explain,
        )

        if explain or stage_matches is None:
            return

        missing = _REQUIRED_COLUMNS - set(stage_matches.columns)
        if missing:
            missing_str = ", ".join(sorted(missing))
            raise ValueError(
                f"Stage '{stage_name}' result missing required columns: {missing_str}"
            )

        self._write_matches_to_results(
            con=con,
            stage_name=stage_name,
            results_table=results_table,
            stage_matches=stage_matches,
        )

    def _write_matches_to_results(
        self,
        con: duckdb.DuckDBPyConnection,
        stage_name: str,
        results_table: str,
        stage_matches: duckdb.DuckDBPyRelation,
    ) -> None:
        tmp_table = f"__ukam_stage_matches_{_uid()}"
        match_reason_expr = (
            "match_reason"
            if "match_reason" in stage_matches.columns
            else f"'{stage_name}' AS match_reason"
        )

        con.execute(f"DROP TABLE IF EXISTS {tmp_table}")
        con.execute(
            f"""
            CREATE TABLE {tmp_table} AS
            SELECT
                ukam_address_id,
                canonical_ukam_address_id,
                resolved_canonical_id,
                {match_reason_expr},
                * EXCLUDE (
                    ukam_address_id,
                    canonical_ukam_address_id,
                    resolved_canonical_id,
                    match_reason
                )
            FROM ({stage_matches.sql_query()})
            WHERE resolved_canonical_id IS NOT NULL
            """
        )

        temp_columns = con.execute(f"DESCRIBE SELECT * FROM {tmp_table}").fetchall()
        temp_column_types = {row[0]: row[1] for row in temp_columns}

        results_columns = {
            row[1]
            for row in con.execute(f"PRAGMA table_info('{results_table}')").fetchall()
        }

        additional_columns = [
            col
            for col in temp_column_types
            if col
            not in {
                "ukam_address_id",
                "canonical_ukam_address_id",
                "resolved_canonical_id",
                "match_reason",
            }
        ]

        for column_name in additional_columns:
            if column_name not in results_columns:
                column_type = temp_column_types[column_name]
                con.execute(
                    f"ALTER TABLE {results_table} ADD COLUMN {column_name} {column_type}"
                )

        set_clauses = [
            "canonical_ukam_address_id = src.canonical_ukam_address_id",
            "resolved_canonical_id = src.resolved_canonical_id",
            "match_reason = src.match_reason",
        ]
        set_clauses.extend(
            f"{column_name} = src.{column_name}" for column_name in additional_columns
        )
        set_sql = ",\n                ".join(set_clauses)

        con.execute(
            f"""
            UPDATE {results_table} AS dst
            SET
                {set_sql}
            FROM {tmp_table} AS src
            WHERE dst.ukam_address_id = src.ukam_address_id
              AND dst.resolved_canonical_id IS NULL
            """
        )

        con.execute(f"DROP TABLE IF EXISTS {tmp_table}")
