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


@dataclass
class MatchResult:
    """Wraps match output with connection-scoped inspection helpers.

    Access the underlying DuckDB relation via `.matches`.

    Key methods:
        match_metrics      - match-reason breakdown with counts and percentages.
        match_reasons      - distinct match-reason values.
        splink_predictions - raw Splink predictions table (requires `SplinkStage`).
    """

    _relation: DuckDBPyRelation
    con: DuckDBPyConnection
    _splink_linker: Any | None = None
    _splink_inspector: _SplinkInspector | None = None

    def __post_init__(self) -> None:
        if self._splink_linker is not None:
            self._splink_inspector = _SplinkInspector(
                con=self.con,
                linker=self._splink_linker,
            )

    def __repr__(self) -> str:
        class_name = self.__class__.__name__
        return (
            f"{class_name} object.\n"
            "Use .matches to retrieve your raw results as a DuckDB table."
        )

    @property
    def matches(self) -> DuckDBPyRelation:
        """The underlying DuckDB relation containing match results."""
        return self._relation

    def match_metrics(
        self,
        *,
        order: Literal["descending", "ascending"] = "descending",
    ) -> DuckDBPyRelation:
        """Match-reason breakdown with counts and percentages"""

        return calculate_match_metrics(self._relation, order=order)

    def _has_splink(self) -> bool:
        """True when a Splink stage ran and inspection helpers are available."""
        return self._splink_linker is not None

    def _require_splink(self) -> _SplinkInspector:
        """Return the Splink inspector or raise if unavailable."""
        if self._splink_inspector is None:
            raise ValueError(
                "Splink inspection is unavailable. Run a Splink stage to enable it."
            )
        return self._splink_inspector

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
        return self._require_splink().predictions(
            limit=limit,
            ukam_ids=ukam_ids,
            threshold_match_probability=threshold_match_probability,
            threshold_match_weight=threshold_match_weight,
        )

    def _splink_waterfall_chart(
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
        self._require_splink()
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
