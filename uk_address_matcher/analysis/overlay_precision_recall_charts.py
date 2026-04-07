from __future__ import annotations

import json
import re
from collections.abc import Sequence
from importlib.resources import files
from os import PathLike
from pathlib import Path
from typing import Any

_OVERLAY_COLOUR_RANGE = [
    "#4C78A8",
    "#F58518",
    "#54A24B",
    "#E45756",
    "#72B7B2",
    "#EECA3B",
    "#B279A2",
    "#FF9DA6",
]

_ALTAIR_SPEC_ASSIGNMENT_RE = re.compile(r"\b(?:var|const|let)\s+spec\s*=\s*")


def _load_chart_definition(file_name: str) -> dict[str, Any]:
    chart_path = files("uk_address_matcher.analysis.chart_defs").joinpath(file_name)
    with chart_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _prepare_nested_chart_definition(
    chart_definition: dict[str, Any],
) -> dict[str, Any]:
    nested_chart_definition = dict(chart_definition)
    nested_chart_definition.pop("$schema", None)
    nested_chart_definition.pop("config", None)
    nested_chart_definition.pop("padding", None)
    return nested_chart_definition


def _extract_chart_definition_from_altair_html(html_text: str) -> dict[str, Any]:
    match = _ALTAIR_SPEC_ASSIGNMENT_RE.search(html_text)
    if match is None:
        raise ValueError("Could not find an embedded Vega/Vega-Lite spec in HTML")

    decoder = json.JSONDecoder()
    json_start = match.end()
    while json_start < len(html_text) and html_text[json_start].isspace():
        json_start += 1

    chart_definition, _ = decoder.raw_decode(html_text, idx=json_start)
    if not isinstance(chart_definition, dict):
        raise ValueError("Embedded Vega/Vega-Lite spec must be a JSON object")

    return chart_definition


def _load_precision_recall_chart_input(
    chart: Any,
) -> dict[str, Any]:
    if isinstance(chart, dict):
        return chart

    if isinstance(chart, (str, PathLike)):
        chart_path = Path(chart)
        with chart_path.open("r", encoding="utf-8") as f:
            chart_text = f.read()

        try:
            chart_definition = json.loads(chart_text)
        except json.JSONDecodeError:
            chart_definition = _extract_chart_definition_from_altair_html(chart_text)

        if not isinstance(chart_definition, dict):
            raise ValueError("Chart file must contain a Vega/Vega-Lite JSON object")

        return chart_definition

    to_dict = getattr(chart, "to_dict", None)
    if callable(to_dict):
        chart_definition = to_dict()
        if isinstance(chart_definition, dict):
            return chart_definition

    raise TypeError(
        "precision-recall chart input must be an Altair chart, a path to a "
        "Vega-Lite JSON file, or a Vega-Lite chart definition dict"
    )


def _extract_precision_recall_chart_records(
    chart: Any,
) -> list[dict[str, Any]]:
    chart_definition = _load_precision_recall_chart_input(chart)
    data = chart_definition.get("data", {})
    values = data.get("values")

    if values is None:
        dataset_name = data.get("name")
        if dataset_name is not None:
            values = chart_definition.get("datasets", {}).get(dataset_name)

    if not isinstance(values, list):
        raise ValueError("precision-recall chart must contain inline data values")

    extracted: list[dict[str, Any]] = []
    for row in values:
        if not isinstance(row, dict):
            raise ValueError("precision-recall chart data rows must be objects")

        if "precision" not in row or "recall" not in row:
            raise ValueError(
                "precision-recall chart data rows must contain 'precision' and "
                "'recall' fields"
            )

        precision = row["precision"]
        recall = row["recall"]
        if precision is None or recall is None:
            continue

        extracted.append(
            {
                **row,
                "precision": float(precision),
                "recall": float(recall),
            }
        )

    if not extracted:
        raise ValueError("precision-recall chart must contain at least one data row")

    extracted.sort(key=lambda row: (row["recall"], row["precision"]))
    return extracted


def _precision_recall_chart_label(
    chart: Any,
    fallback: str,
) -> str:
    if isinstance(chart, (str, PathLike)):
        return Path(chart).stem

    return fallback


def _normalise_comparison_charts(
    comparison_charts: Any | list[Any],
) -> list[Any]:
    if isinstance(comparison_charts, list):
        charts = comparison_charts
    else:
        charts = [comparison_charts]

    if not charts:
        raise ValueError("comparison_charts must contain at least one chart")

    return charts


def _normalise_comparison_labels(
    comparison_charts: list[Any],
    comparison_labels: str | Sequence[str] | None,
) -> list[str]:
    if comparison_labels is None:
        return [
            _precision_recall_chart_label(chart, f"Comparison {index + 1}")
            for index, chart in enumerate(comparison_charts)
        ]

    if isinstance(comparison_labels, str):
        if len(comparison_charts) != 1:
            raise ValueError(
                "comparison_labels must provide one label per comparison chart"
            )
        return [comparison_labels]

    labels = list(comparison_labels)
    if len(labels) != len(comparison_charts):
        raise ValueError("comparison_labels must provide one label per comparison chart")
    return labels


def _build_curve_records(
    chart: Any,
    *,
    series_id: str,
    series_label: str,
    is_baseline: bool,
) -> list[dict[str, Any]]:
    return [
        {
            **record,
            "series_id": series_id,
            "series_label": series_label,
            "is_baseline": is_baseline,
        }
        for record in _extract_precision_recall_chart_records(chart)
    ]


def _choose_label_record(records: list[dict[str, Any]]) -> dict[str, Any]:
    return max(records, key=lambda row: (row["recall"], row["precision"]))


def _interpolate_precision_for_recall(
    records: list[dict[str, Any]],
    *,
    target_recall: float,
) -> float:
    return _interpolate_numeric_field_for_recall(
        records,
        field_name="precision",
        target_recall=target_recall,
    )


def _interpolate_numeric_field_for_recall(
    records: list[dict[str, Any]],
    *,
    field_name: str,
    target_recall: float,
) -> float | None:
    candidate_values: list[float] = []

    usable_records = [record for record in records if record.get(field_name) is not None]
    if not usable_records:
        return None

    for left, right in zip(usable_records, usable_records[1:]):
        left_recall = float(left["recall"])
        right_recall = float(right["recall"])
        lower_recall = min(left_recall, right_recall)
        upper_recall = max(left_recall, right_recall)
        if not (lower_recall <= target_recall <= upper_recall):
            continue

        left_value = float(left[field_name])
        right_value = float(right[field_name])

        if left_recall == right_recall:
            clamped_value = min(
                max(left_value, min(left_value, right_value)),
                max(left_value, right_value),
            )
            candidate_values.append(clamped_value)
            continue

        interpolation_fraction = (target_recall - left_recall) / (
            right_recall - left_recall
        )
        candidate_values.append(
            left_value + interpolation_fraction * (right_value - left_value)
        )

    if candidate_values:
        return candidate_values[0]

    nearest_record = min(
        usable_records,
        key=lambda record: abs(float(record["recall"]) - target_recall),
    )
    return float(nearest_record[field_name])


def _build_diff_records(
    baseline_records: list[dict[str, Any]],
    comparison_records_by_label: list[tuple[str, list[dict[str, Any]]]],
) -> list[dict[str, Any]]:
    diff_records: list[dict[str, Any]] = []

    for comparison_label, comparison_records in comparison_records_by_label:
        min_comparison_recall = min(
            float(record["recall"]) for record in comparison_records
        )
        max_comparison_recall = max(
            float(record["recall"]) for record in comparison_records
        )

        for baseline_record in baseline_records:
            baseline_recall = float(baseline_record["recall"])
            if not (min_comparison_recall <= baseline_recall <= max_comparison_recall):
                continue

            baseline_precision = float(baseline_record["precision"])
            comparison_precision = _interpolate_precision_for_recall(
                comparison_records,
                target_recall=baseline_recall,
            )
            comparison_fp = _interpolate_numeric_field_for_recall(
                comparison_records,
                field_name="fp",
                target_recall=baseline_recall,
            )
            diff_records.append(
                {
                    "baseline_recall": baseline_recall,
                    "baseline_precision": baseline_precision,
                    "baseline_fp": baseline_record.get("fp"),
                    "comparison_label": comparison_label,
                    "comparison_precision": comparison_precision,
                    "comparison_fp": comparison_fp,
                    "precision_gap_percentage_points": (
                        comparison_precision - baseline_precision
                    )
                    * 100.0,
                    "baseline_truth_threshold": baseline_record.get("truth_threshold"),
                    "baseline_match_probability": baseline_record.get(
                        "match_probability"
                    ),
                }
            )

    return diff_records


def _build_overlay_chart_definition(
    curve_records: list[dict[str, Any]],
    diff_records: list[dict[str, Any]],
    label_records: list[dict[str, Any]],
) -> dict[str, Any]:
    ordered_series_labels = [record["series_label"] for record in label_records]

    top_panel = _prepare_nested_chart_definition(
        _load_chart_definition("precision_recall.json")
    )
    top_panel["data"]["values"] = curve_records
    top_panel.pop("params", None)
    top_panel["height"] = 320
    top_panel["mark"]["fillOpacity"] = 0.08
    top_panel["encoding"]["detail"] = {
        "field": "series_id",
        "type": "nominal",
    }
    top_panel["encoding"]["color"] = {
        "field": "series_label",
        "type": "nominal",
        "title": "Curve",
        "scale": {
            "domain": ordered_series_labels,
            "range": _OVERLAY_COLOUR_RANGE[: len(ordered_series_labels)],
        },
    }
    top_panel["encoding"]["tooltip"] = [
        {
            "field": "series_label",
            "type": "nominal",
            "title": "Curve",
        },
        {
            "field": "fp",
            "type": "quantitative",
            "title": "False positives",
            "format": ".0f",
        },
        *top_panel["encoding"]["tooltip"],
    ]

    top_mark = top_panel.pop("mark")
    top_encoding = top_panel.pop("encoding")
    top_panel["layer"] = [
        {
            "mark": top_mark,
            "encoding": top_encoding,
        },
        {
            "data": {"values": label_records},
            "mark": {
                "type": "text",
                "align": "left",
                "baseline": "middle",
                "dx": 7,
                "fontSize": 11,
                "fontWeight": "bold",
            },
            "encoding": {
                "x": {
                    "field": "recall",
                    "type": "quantitative",
                },
                "y": {
                    "field": "precision",
                    "type": "quantitative",
                },
                "text": {
                    "field": "series_label",
                    "type": "nominal",
                },
                "color": {
                    "field": "series_label",
                    "type": "nominal",
                    "legend": None,
                    "scale": {
                        "domain": ordered_series_labels,
                        "range": _OVERLAY_COLOUR_RANGE[: len(ordered_series_labels)],
                    },
                },
            },
        },
    ]

    bottom_panel = _prepare_nested_chart_definition(
        _load_chart_definition("precision_recall_diff.json")
    )
    bottom_panel["data"]["values"] = diff_records

    return {
        "$schema": "https://vega.github.io/schema/vega-lite/v6.1.0.json",
        "description": (
            "Overlayed precision-recall curves with recall-aligned precision gaps"
        ),
        "title": "Precision-Recall Curve Comparison",
        "padding": {"top": 5, "left": 5, "right": 5, "bottom": 5},
        "vconcat": [
            top_panel,
            bottom_panel,
        ],
        "resolve": {
            "scale": {
                "color": "independent",
            }
        },
        "config": {
            "view": {"stroke": None},
            "axis": {"labelFontSize": 11, "titleFontSize": 12},
            "legend": {"labelFontSize": 11, "titleFontSize": 12},
        },
    }


def _overlay_precision_recall_charts(
    baseline_chart: Any,
    comparison_charts: Any | list[Any],
    *,
    baseline_label: str | None = None,
    comparison_labels: str | Sequence[str] | None = None,
) -> Any:
    normalised_comparison_charts = _normalise_comparison_charts(comparison_charts)
    normalised_comparison_labels = _normalise_comparison_labels(
        normalised_comparison_charts,
        comparison_labels,
    )

    baseline_series_label = baseline_label or "baseline"
    baseline_records = _build_curve_records(
        baseline_chart,
        series_id="baseline",
        series_label=baseline_series_label,
        is_baseline=True,
    )

    curve_records = list(baseline_records)
    label_records = [_choose_label_record(baseline_records)]
    comparison_records_by_label: list[tuple[str, list[dict[str, Any]]]] = []

    for index, (chart, label) in enumerate(
        zip(normalised_comparison_charts, normalised_comparison_labels, strict=True),
        start=1,
    ):
        comparison_records = _build_curve_records(
            chart,
            series_id=f"comparison_{index}",
            series_label=label,
            is_baseline=False,
        )
        curve_records.extend(comparison_records)
        label_records.append(_choose_label_record(comparison_records))
        comparison_records_by_label.append((label, comparison_records))

    diff_records = _build_diff_records(baseline_records, comparison_records_by_label)
    chart_definition = _build_overlay_chart_definition(
        curve_records,
        diff_records,
        label_records,
    )

    try:
        import altair as alt
    except ImportError:
        return chart_definition

    return alt.VConcatChart.from_dict(chart_definition)
