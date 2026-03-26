from __future__ import annotations

import json
from importlib.resources import files
from typing import TYPE_CHECKING, Any
from uuid import uuid4

if TYPE_CHECKING:
    import duckdb


def _load_chart_definition(file_name: str) -> dict[str, Any]:
    chart_path = files("uk_address_matcher.analysis.chart_defs").joinpath(file_name)
    with chart_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _visual_chart_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return records used for chart rendering, excluding sentinel thresholds."""
    filtered: list[dict[str, Any]] = []
    for row in records:
        threshold = row.get("truth_threshold")
        if threshold is None:
            filtered.append(row)
            continue
        threshold_value = float(threshold)
        if abs(threshold_value) >= 900:
            continue
        filtered.append(row)
    return filtered


def build_precision_recall_chart_definition(
    records: list[dict[str, Any]],
    add_metrics: list[str] | None = None,
) -> dict[str, Any]:
    del add_metrics
    plot_records = _visual_chart_records(records)
    chart = _load_chart_definition("precision_recall.json")
    chart["data"]["values"] = plot_records
    if chart.get("params"):
        chart["params"][0]["name"] = f"grid_{uuid4().hex}"
    return chart


def build_threshold_selection_chart_definition(
    records: list[dict[str, Any]],
    add_metrics: list[str],
) -> dict[str, Any]:
    plot_records = _visual_chart_records(records)
    chart = _load_chart_definition("threshold_selection_tool.json")
    chart["data"]["values"] = plot_records

    metrics = ["precision", "recall", *add_metrics]
    chart["transform"][0]["fold"] = metrics
    return chart


def build_match_weight_rounding_expression(
    match_weight_round_to_nearest: float | None,
) -> str:
    if match_weight_round_to_nearest is None:
        return "m.match_weight"
    return (
        f"CAST({match_weight_round_to_nearest} AS DOUBLE) "
        f"* round(m.match_weight / {match_weight_round_to_nearest})"
    )


def compute_precision_recall_auc(
    con: duckdb.DuckDBPyConnection,
    threshold_metrics_sql: str,
) -> float | None:
    auc_row = con.sql(
        f"""
        WITH points AS (
            SELECT
                CAST(recall AS DOUBLE) AS recall,
                MAX(CAST(precision AS DOUBLE)) AS precision
            FROM ({threshold_metrics_sql})
            WHERE recall IS NOT NULL
              AND precision IS NOT NULL
            GROUP BY 1
        ),
        ordered AS (
            SELECT
                recall,
                precision,
                LAG(recall) OVER (ORDER BY recall) AS prev_recall,
                LAG(precision) OVER (ORDER BY recall) AS prev_precision
            FROM points
        ),
        auc_integral AS (
            SELECT
                SUM(
                    (recall - prev_recall)
                    * ((precision + prev_precision) / 2.0)
                ) AS auc
            FROM ordered
            WHERE prev_recall IS NOT NULL
        )
        SELECT
            CASE
                WHEN auc IS NULL THEN NULL
                ELSE LEAST(GREATEST(auc, 0.0), 1.0)
            END AS auc
        FROM auc_integral
        """
    ).fetchone()
    if auc_row is None or auc_row[0] is None:
        return None
    return float(auc_row[0])


def render_chart_definition(chart_definition: dict[str, Any]) -> Any:
    """Return an Altair chart when available, otherwise return raw Vega-Lite dict."""
    try:
        import altair as alt
    except ImportError:
        return chart_definition

    return alt.Chart.from_dict(chart_definition)
