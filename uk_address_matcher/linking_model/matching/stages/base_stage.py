"""Abstract base class for matching stages."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Optional

from uk_address_matcher.sql_pipeline.helpers import _uid

if TYPE_CHECKING:
    import duckdb

    from uk_address_matcher.sql_pipeline.runner import DebugOptions

logger = logging.getLogger("uk_address_matcher")

# Columns that every find_matches() relation must include.
_REQUIRED_MATCH_COLUMNS = frozenset(
    {"ukam_address_id", "canonical_ukam_address_id", "resolved_canonical_id"}
)

# Core results-table columns that are not treated as "extra" stage columns.
_CORE_RESULTS_COLUMNS = frozenset(
    {
        "ukam_address_id",
        "canonical_ukam_address_id",
        "resolved_canonical_id",
        "match_reason",
    }
)


class MatchingStage(ABC):
    """Base class for all matching stages.

    Subclasses implement ``find_matches()`` which returns a
    ``DuckDBPyRelation`` containing at minimum:

    - ``ukam_address_id``           — the messy record's internal ID
    - ``canonical_ukam_address_id`` — the matched canonical record's internal ID
    - ``resolved_canonical_id``     — the matched canonical record's user-facing ID

    The relation may also contain:

    - ``match_reason`` — a human-readable label; if absent the stage name is used.
    - Any additional columns (e.g. ``match_weight``, ``distinguishability``)
      which will be automatically added to the results table.

    The inherited ``run()`` method calls ``find_matches()``, validates the
    output, and writes matched rows into the shared results table.
    """

    @abstractmethod
    def find_matches(
        self,
        con: duckdb.DuckDBPyConnection,
        stage_name: str,
        df_unmatched: duckdb.DuckDBPyRelation,
        df_canonical: duckdb.DuckDBPyRelation,
        debug_options: Optional[DebugOptions] = None,
    ) -> Optional[duckdb.DuckDBPyRelation]:
        """Find matches and return a relation of matched pairs.

        Parameters
        ----------
        con :
            Active DuckDB connection.
        stage_name :
            Name of the current stage (e.g. ``"exact_matches"``).
        df_unmatched :
            Cleaned messy addresses that have not yet been matched.
        df_canonical :
            Cleaned canonical addresses to search within.
        debug_options :
            Optional debug/explain settings.

        Returns
        -------
        duckdb.DuckDBPyRelation or None
            A relation with at least the columns ``ukam_address_id``,
            ``canonical_ukam_address_id``, and ``resolved_canonical_id``.
            Return ``None`` (or an empty relation) if no matches were found.
        """
        ...

    # ------------------------------------------------------------------
    # Concrete run() — called by the runner loop
    # ------------------------------------------------------------------

    def run(
        self,
        con: duckdb.DuckDBPyConnection,
        stage_name: str,
        results_table: str,
        df_unmatched: duckdb.DuckDBPyRelation,
        df_canonical: duckdb.DuckDBPyRelation,
        debug_options: Optional[DebugOptions] = None,
    ) -> None:
        """Orchestrate a stage: find matches then update the results table."""
        matches = self.find_matches(
            con, stage_name, df_unmatched, df_canonical, debug_options
        )
        if matches is None:
            return

        _update_results_table(
            con=con,
            results_table=results_table,
            matches=matches,
            stage_name=stage_name,
        )


# ------------------------------------------------------------------
# Shared helper — updates __ukam_results with a matches relation
# ------------------------------------------------------------------


def _update_results_table(
    con: duckdb.DuckDBPyConnection,
    results_table: str,
    matches: duckdb.DuckDBPyRelation,
    stage_name: str,
) -> None:
    """Write matched rows from *matches* into the results table.

    Parameters
    ----------
    con :
        Active DuckDB connection.
    results_table :
        Name of the ``__ukam_results_*`` table to update.
    matches :
        Relation containing at least ``ukam_address_id``,
        ``canonical_ukam_address_id``, and ``resolved_canonical_id``.
        May optionally include ``match_reason`` and any extra columns.
    stage_name :
        Used as the default ``match_reason`` when the column is absent.
    """
    columns = set(matches.columns)

    # Validate required columns
    missing = _REQUIRED_MATCH_COLUMNS - columns
    if missing:
        raise ValueError(
            f"Stage '{stage_name}' find_matches() result is missing required "
            f"columns: {sorted(missing)}. Got: {sorted(columns)}"
        )

    # If match_reason is missing, add a default derived from the stage name
    has_match_reason = "match_reason" in columns
    if not has_match_reason:
        matches = con.sql(f"""
            SELECT *, '{stage_name}' AS match_reason
            FROM ({matches.sql_query()})
        """)
        columns = set(matches.columns)

    # Detect extra columns the stage wants to persist (e.g. match_weight)
    extra_columns = sorted(columns - _CORE_RESULTS_COLUMNS)

    # Materialise into a temp table for the UPDATE join
    uid = _uid()
    tmp_table = f"__ukam_stage_tmp_{stage_name}_{uid}"
    con.execute(f"DROP TABLE IF EXISTS {tmp_table}")

    matched_count = matches.count("*").fetchone()[0]
    if matched_count == 0:
        return

    matches.create(tmp_table)

    # Add any extra columns to the results table
    if extra_columns:
        # Infer types from the temp table
        col_types = {
            row[1]: row[2]
            for row in con.execute(
                f"PRAGMA table_info('{tmp_table}')"
            ).fetchall()
            if row[1] in extra_columns
        }
        for col_name in extra_columns:
            col_type = col_types.get(col_name, "VARCHAR")
            con.execute(
                f"ALTER TABLE {results_table} "
                f"ADD COLUMN IF NOT EXISTS {col_name} {col_type}"
            )

    # Build the SET clause
    set_parts = [
        "resolved_canonical_id = m.resolved_canonical_id",
        "canonical_ukam_address_id = m.canonical_ukam_address_id",
        "match_reason = m.match_reason::VARCHAR",
    ]
    for col_name in extra_columns:
        set_parts.append(f"{col_name} = m.{col_name}")

    set_clause = ",\n            ".join(set_parts)

    con.execute(f"""
        UPDATE {results_table} AS r
        SET
            {set_clause}
        FROM {tmp_table} AS m
        WHERE r.ukam_address_id = m.ukam_address_id
    """)

    con.execute(f"DROP TABLE IF EXISTS {tmp_table}")
