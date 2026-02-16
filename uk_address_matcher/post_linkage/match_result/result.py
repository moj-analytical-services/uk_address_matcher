from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from duckdb import DuckDBPyConnection, DuckDBPyRelation

from uk_address_matcher.post_linkage.analyse_results import (
    calculate_match_metrics,
)
from uk_address_matcher.post_linkage.match_result.splink_inspector import (
    _SplinkInspector,
)
from uk_address_matcher.sql_pipeline.match_reasons import MatchReason


@dataclass
class MatchResult:
    """Wraps match output with connection-scoped inspection helpers.

    The underlying DuckDB relation is accessible via ``.relation``.
    Common DuckDB relation methods (e.g. ``.filter()``, ``.limit()``,
    ``.project()``) are forwarded automatically, so you can treat a
    ``MatchResult`` like a relation for quick exploration.

    Key methods:
        match_metrics         - match-reason breakdown with counts and percentages.
        match_reasons         - distinct match-reason values.
        filter_by_match_reason - filter rows to a single match reason.
        splink_predictions     - raw Splink predictions table (requires SplinkStage).
        splink_waterfall_chart - Splink waterfall chart for sampled records.
    """

    _relation: DuckDBPyRelation
    con: DuckDBPyConnection
    metadata: dict[str, Any]
    _splink_linker: Any | None = None
    _splink_inspector: _SplinkInspector | None = None

    def __post_init__(self) -> None:
        if self._splink_linker is not None:
            self._splink_inspector = _SplinkInspector(
                con=self.con,
                tables=self.metadata.get("splink_tables", {}),
            )

    def __getattr__(self, name: str) -> Any:
        return getattr(self._relation, name)

    def __repr__(self) -> str:
        return f"MatchResult(relation={self._relation!r})"

    @property
    def relation(self) -> DuckDBPyRelation:
        """The underlying DuckDB relation."""
        return self._relation

    def match_metrics(
        self,
        *,
        order: Literal["descending", "ascending"] = "descending",
    ) -> DuckDBPyRelation:
        """Match-reason breakdown with counts and percentages.

        Equivalent to ``calculate_match_metrics(match_result.relation)``.
        """

        return calculate_match_metrics(self._relation, order=order)

    def match_reasons(self) -> list[str]:
        """Distinct non-null match-reason values present in the results."""
        rows = (
            self._relation.aggregate("match_reason", group_expr="match_reason")
            .filter("match_reason IS NOT NULL")
            .fetchall()
        )
        return sorted(row[0] for row in rows)

    def filter_by_match_reason(self, reason: MatchReason | str) -> DuckDBPyRelation:
        """Return rows matching a specific ``match_reason`` value.

        Accepts a ``MatchReason`` enum member or a plain string.  Raises
        ``ValueError`` if the reason is not present in the results.
        """
        if isinstance(reason, MatchReason):
            reason = reason.value

        available = self.match_reasons()
        if reason not in available:
            raise ValueError(
                f"Match reason {reason!r} not found in results. "
                f"Available reasons: {available}"
            )

        escaped = reason.replace("'", "''")
        return self._relation.filter(f"match_reason = '{escaped}'")

    def has_splink(self) -> bool:
        """True when a Splink stage ran and inspection helpers are available."""
        return self._splink_linker is not None

    def splink_predictions(
        self,
        limit: int | None = None,
        ukam_ids: list[str | int] | None = None,
        *,
        threshold_match_probability: float | None = None,
        threshold_match_weight: float | None = None,
    ) -> DuckDBPyRelation:
        """Splink predictions as a DuckDB relation.

        Use ``ukam_ids`` to filter on the input-side identifier.
        """
        return self._splink().predictions(
            limit=limit,
            ukam_ids=ukam_ids,
            threshold_match_probability=threshold_match_probability,
            threshold_match_weight=threshold_match_weight,
        )

    def splink_waterfall_chart(
        self,
        records: Any,
        *,
        filter_nulls: bool = True,
        remove_sensitive_data: bool = False,
        as_dict: bool = False,
    ) -> Any:
        """Splink waterfall chart for prediction records.

        ``records`` must match the structure returned by Splink's
        ``as_record_dict``. DuckDB relations and Splink prediction dataframes
        are converted to dictionaries automatically.

        Requires ``retain_intermediate_calculation_columns=True`` on your
        ``SplinkStage`` so that the comparison-vector columns needed by the
        waterfall are present in the predictions table.
        """
        self._splink()
        record_dicts = _ensure_record_dicts(records)

        try:
            return self._splink_linker.visualisations.waterfall_chart(
                record_dicts,
                filter_nulls=filter_nulls,
                remove_sensitive_data=remove_sensitive_data,
                as_dict=as_dict,
            )
        except ValueError as e:
            if "retain_intermediate_calculation_columns" in str(e):
                raise ValueError(
                    "Waterfall charts require "
                    "retain_intermediate_calculation_columns=True on your "
                    "SplinkStage. For example:\n\n"
                    "    SplinkStage(\n"
                    "        retain_intermediate_calculation_columns=True,\n"
                    "        ...\n"
                    "    )"
                ) from e
            raise

    def _splink(self) -> _SplinkInspector:
        if self._splink_inspector is None:
            raise ValueError(
                "Splink inspection is unavailable. Run a Splink stage to enable it."
            )
        return self._splink_inspector


def _ensure_record_dicts(records: Any) -> list[dict[str, Any]]:
    if isinstance(records, DuckDBPyRelation):
        rows = records.fetchall()
        columns = records.columns
        return [dict(zip(columns, row)) for row in rows]
    if hasattr(records, "as_record_dict"):
        return records.as_record_dict()
    if isinstance(records, list):
        if not records:
            return records
        if isinstance(records[0], dict):
            return records
    raise TypeError(
        "Waterfall charts expect a DuckDB relation, a Splink predictions "
        "dataframe with as_record_dict(), or a list of record dictionaries."
    )
