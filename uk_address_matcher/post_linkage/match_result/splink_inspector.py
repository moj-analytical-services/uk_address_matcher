from __future__ import annotations

from typing import TYPE_CHECKING, Any

from duckdb import DuckDBPyConnection, DuckDBPyRelation

if TYPE_CHECKING:
    from uk_address_matcher.linking_model.matching.stages.splink import SplinkStage


def _resolve_predictions_id_column(predictions: DuckDBPyRelation) -> str:
    columns = predictions.columns
    if "ukam_address_id_r" in columns:
        return "ukam_address_id_r"
    if "ukam_address_id" in columns:
        return "ukam_address_id"
    if "unique_id_r" in columns:
        return "unique_id_r"
    if "unique_id" in columns:
        return "unique_id"
    raise ValueError(
        "Splink predictions table is missing expected ID columns for filtering."
    )


def _sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


class _SplinkInspector:
    """Internal helper for Splink-specific inspection utilities."""

    def __init__(
        self,
        *,
        con: DuckDBPyConnection,
        stage: SplinkStage,
    ) -> None:
        self._con = con
        self._stage = stage

    def predictions(
        self,
        *,
        limit: int | None = None,
        ukam_ids: list[str | int] | None = None,
        threshold_match_probability: float | None = None,
        threshold_match_weight: float | None = None,
    ) -> DuckDBPyRelation:
        """Return the Splink predictions as a DuckDB relation.

        All parameters are optional filters. With no arguments the full
        predictions table is returned.
        """
        predictions = self._get_predictions_relation()

        has_filters = (
            limit is not None
            or ukam_ids
            or threshold_match_probability is not None
            or threshold_match_weight is not None
        )
        if not has_filters:
            return predictions
        conditions: list[str] = []

        if ukam_ids:
            id_column = _resolve_predictions_id_column(predictions)
            id_values = ", ".join(_sql_literal(value) for value in ukam_ids)
            conditions.append(f"{id_column} IN ({id_values})")
        if threshold_match_probability is not None:
            conditions.append(f"match_probability >= {threshold_match_probability}")
        if threshold_match_weight is not None:
            conditions.append(f"match_weight >= {threshold_match_weight}")

        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)

        limit_clause = ""
        if limit is not None:
            limit_clause = f"LIMIT {limit}"

        query = (
            "SELECT * "
            f"FROM ({predictions.sql_query()}) AS pred {where_clause} {limit_clause}"
        ).strip()
        return self._con.sql(query)

    def _get_predictions_relation(self) -> DuckDBPyRelation:
        if self._stage.predictions_table is not None:
            return self._con.table(self._stage.predictions_table)
        raise ValueError(
            "SplinkStage has no predictions table. "
            "The stage may not have run far enough to produce predictions."
        )
