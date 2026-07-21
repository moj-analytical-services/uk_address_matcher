from __future__ import annotations

import json
import re
from collections.abc import Sequence
from importlib.resources import files
from os import PathLike
from pathlib import Path
from typing import Any

_OVERLAY_COLOUR_RANGE = [
    "#6929C4",
    "#1192E8",
    "#005D5D",
    "#9F1853",
    "#FA4D56",
    "#570408",
    "#198038",
    "#002D9C",
    "#EE538B",
    "#B28600",
]

_HOVER_GUIDE_COLOUR = "#A0A5B4"
_LABEL_MIN_VERTICAL_GAP = 0.001

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


def _apply_label_offsets(label_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not label_records:
        return []

    adjusted_records = [
        {
            **record,
            "_label_order": index,
            "label_precision": float(record["precision"]),
            "label_has_connector": False,
        }
        for index, record in enumerate(label_records)
    ]
    adjusted_records.sort(
        key=lambda record: (float(record["precision"]), float(record["recall"]))
    )

    for index in range(1, len(adjusted_records)):
        previous_label_precision = float(adjusted_records[index - 1]["label_precision"])
        current_label_precision = float(adjusted_records[index]["label_precision"])
        minimum_label_precision = previous_label_precision + _LABEL_MIN_VERTICAL_GAP
        if current_label_precision < minimum_label_precision:
            adjusted_records[index]["label_precision"] = minimum_label_precision

    overflow = float(adjusted_records[-1]["label_precision"]) - 1.0
    if overflow > 0:
        for record in adjusted_records:
            record["label_precision"] = max(
                0.0,
                float(record["label_precision"]) - overflow,
            )

        for index in range(len(adjusted_records) - 2, -1, -1):
            next_label_precision = float(adjusted_records[index + 1]["label_precision"])
            current_label_precision = float(adjusted_records[index]["label_precision"])
            maximum_label_precision = next_label_precision - _LABEL_MIN_VERTICAL_GAP
            if current_label_precision > maximum_label_precision:
                adjusted_records[index]["label_precision"] = max(
                    0.0,
                    maximum_label_precision,
                )

    for record in adjusted_records:
        record["label_has_connector"] = (
            abs(float(record["label_precision"]) - float(record["precision"])) > 1e-9
        )

    adjusted_records.sort(key=lambda record: int(record["_label_order"]))
    return [
        {key: value for key, value in record.items() if key != "_label_order"}
        for record in adjusted_records
    ]


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
    comparison_records_by_label: list[tuple[str, str, list[dict[str, Any]]]],
) -> list[dict[str, Any]]:
    diff_records: list[dict[str, Any]] = []

    for comparison_label, series_id, comparison_records in comparison_records_by_label:
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
                    "recall": baseline_recall,
                    "baseline_precision": baseline_precision,
                    "baseline_fp": baseline_record.get("fp"),
                    "comparison_label": comparison_label,
                    "series_id": series_id,
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
    adjusted_label_records = _apply_label_offsets(label_records)
    precision_axis_floor = min(float(record["precision"]) for record in curve_records)
    recall_axis_minimum = max(
        0.0,
        min(float(record["recall"]) for record in curve_records) - 0.05,
    )
    recall_axis_maximum = min(
        1.0,
        max(float(record["recall"]) for record in curve_records) + 0.05,
    )
    recall_axis_minimum = round(recall_axis_minimum, 12)
    recall_axis_maximum = round(recall_axis_maximum, 12)
    ordered_series_labels = [record["series_label"] for record in adjusted_label_records]
    comparison_labels = [
        record["series_label"]
        for record in adjusted_label_records
        if not bool(record["is_baseline"])
    ]
    if len(ordered_series_labels) > len(_OVERLAY_COLOUR_RANGE):
        raise ValueError(
            "Precision-recall overlays support at most "
            f"{len(_OVERLAY_COLOUR_RANGE)} curves; received "
            f"{len(ordered_series_labels)}."
        )

    series_colours = dict(
        zip(
            ordered_series_labels,
            _OVERLAY_COLOUR_RANGE,
            strict=False,
        )
    )
    series_colour_scale = {
        "domain": ordered_series_labels,
        "range": [series_colours[label] for label in ordered_series_labels],
    }
    comparison_colour_scale = {
        "domain": comparison_labels,
        "range": [series_colours[label] for label in comparison_labels],
    }

    top_panel = _prepare_nested_chart_definition(
        _load_chart_definition("precision_recall.json")
    )
    top_panel["data"]["values"] = curve_records
    top_panel.pop("params", None)
    top_panel.pop("title", None)
    top_panel["height"] = 320
    top_panel["encoding"]["detail"] = {
        "field": "series_id",
        "type": "nominal",
    }
    top_panel["encoding"]["order"] = {
        "field": "recall",
        "type": "quantitative",
    }
    top_panel["encoding"]["x"]["scale"]["domain"] = [
        recall_axis_minimum,
        recall_axis_maximum,
    ]
    top_panel["encoding"]["y"]["scale"]["domain"] = [precision_axis_floor, 1.0]
    top_panel["encoding"]["color"] = {
        "field": "series_label",
        "type": "nominal",
        "title": "Curve",
        "scale": series_colour_scale,
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

    top_panel.pop("mark")
    top_encoding = top_panel.pop("encoding")
    line_hover_opacity_conditions = [
        {
            "test": (
                "length(data('label_hover_store')) === 0 && "
                "length(data('gap_hover_store')) === 0"
            ),
            "value": 0.45,
        },
        {
            "param": "label_hover",
            "empty": False,
            "value": 1.0,
        },
        {
            "param": "gap_hover",
            "empty": False,
            "value": 1.0,
        },
    ]
    top_panel["layer"] = [
        {
            "mark": {
                "type": "point",
                "opacity": 0,
                "size": 90,
                "clip": True,
            },
            "encoding": {
                "x": top_encoding["x"],
                "y": {
                    "field": "precision",
                    "type": "quantitative",
                },
                "detail": {
                    "field": "series_id",
                    "type": "nominal",
                },
                "tooltip": top_encoding["tooltip"],
            },
            "params": [
                {
                    "name": "curve_hover",
                    "select": {
                        "type": "point",
                        "on": "mouseover",
                        "clear": "mouseout",
                        "nearest": True,
                        "fields": ["series_id", "recall", "precision"],
                    },
                }
            ],
        },
        {
            "transform": [{"filter": "datum.is_baseline"}],
            "mark": {
                "type": "area",
                "interpolate": "linear",
                "clip": True,
                "line": False,
                "opacity": 0.10,
            },
            "encoding": {
                "x": top_encoding["x"],
                "y": top_encoding["y"],
                "color": top_encoding["color"],
            },
        },
        {
            "transform": [{"filter": "!datum.is_baseline"}],
            "mark": {
                "type": "area",
                "interpolate": "linear",
                "clip": True,
                "line": False,
                "opacity": 0.15,
            },
            "encoding": {
                "x": top_encoding["x"],
                "y": top_encoding["y"],
                "color": top_encoding["color"],
            },
        },
        {
            "transform": [{"filter": "datum.is_baseline"}],
            "mark": {
                "type": "line",
                "interpolate": "linear",
                "clip": True,
                "strokeWidth": 3.25,
            },
            "encoding": {
                "x": top_encoding["x"],
                "y": top_encoding["y"],
                "detail": top_encoding["detail"],
                "order": top_encoding["order"],
                "color": top_encoding["color"],
                "opacity": {
                    "condition": line_hover_opacity_conditions,
                    "value": 0.18,
                },
            },
        },
        {
            "transform": [{"filter": "!datum.is_baseline"}],
            "mark": {
                "type": "line",
                "interpolate": "linear",
                "clip": True,
                "strokeWidth": 2.75,
            },
            "encoding": {
                "x": top_encoding["x"],
                "y": top_encoding["y"],
                "detail": top_encoding["detail"],
                "order": top_encoding["order"],
                "color": top_encoding["color"],
                "opacity": {
                    "condition": line_hover_opacity_conditions,
                    "value": 0.18,
                },
            },
        },
        {
            "data": {"values": adjusted_label_records},
            "transform": [{"filter": "datum.label_has_connector"}],
            "mark": {
                "type": "rule",
                "strokeWidth": 1,
            },
            "encoding": {
                "x": top_encoding["x"],
                "y": {
                    "field": "precision",
                    "type": "quantitative",
                },
                "y2": {
                    "field": "label_precision",
                },
                "color": {
                    "field": "series_label",
                    "type": "nominal",
                    "legend": None,
                    "scale": series_colour_scale,
                },
            },
        },
        {
            "data": {"values": adjusted_label_records},
            "mark": {
                "type": "text",
                "align": "left",
                "baseline": "middle",
                "dx": 7,
                "fontSize": 11,
                "fontWeight": "bold",
            },
            "params": [
                {
                    "name": "label_hover",
                    "select": {
                        "type": "point",
                        "on": "mouseover",
                        "clear": "mouseout",
                        "fields": ["series_id"],
                    },
                }
            ],
            "encoding": {
                "x": top_encoding["x"],
                "y": {
                    "field": "label_precision",
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
                    "scale": series_colour_scale,
                },
                "opacity": {
                    "condition": {
                        "param": "label_hover",
                        "empty": False,
                        "value": 1.0,
                    },
                    "value": 0.75,
                },
            },
        },
        {
            "data": {"values": curve_records},
            "mark": {
                "type": "rule",
                "color": _HOVER_GUIDE_COLOUR,
                "strokeWidth": 1,
            },
            "encoding": {
                "x": top_encoding["x"],
            },
            "transform": [{"filter": {"param": "curve_hover", "empty": False}}],
        },
        {
            "data": {"values": curve_records},
            "mark": {
                "type": "rule",
                "color": _HOVER_GUIDE_COLOUR,
                "strokeWidth": 1,
            },
            "encoding": {
                "y": {
                    "field": "precision",
                    "type": "quantitative",
                },
            },
            "transform": [{"filter": {"param": "curve_hover", "empty": False}}],
        },
    ]

    bottom_panel = _prepare_nested_chart_definition(
        _load_chart_definition("precision_recall_diff.json")
    )
    bottom_panel["data"]["values"] = diff_records
    bottom_panel["layer"][1]["encoding"]["color"]["scale"] = comparison_colour_scale
    bottom_panel["layer"][1]["encoding"]["opacity"] = {
        "condition": line_hover_opacity_conditions,
        "value": 0.18,
    }
    bottom_panel["layer"][1]["mark"]["strokeWidth"] = 2.75
    bottom_panel["layer"][1]["encoding"]["detail"] = {
        "field": "series_id",
        "type": "nominal",
    }
    bottom_panel["layer"][1]["params"] = [
        {
            "name": "gap_hover",
            "select": {
                "type": "point",
                "on": "mouseover",
                "clear": "mouseout",
                "fields": ["series_id"],
            },
        }
    ]
    bottom_panel["layer"][1]["encoding"]["x"]["field"] = "recall"
    bottom_panel["layer"][1]["encoding"]["x"]["scale"]["domain"] = [
        recall_axis_minimum,
        recall_axis_maximum,
    ]
    precision_gap_extent = max(
        (
            abs(float(record["precision_gap_percentage_points"]))
            for record in diff_records
        ),
        default=1.0,
    )
    precision_gap_extent = round(precision_gap_extent, 12)
    if precision_gap_extent == 0:
        precision_gap_extent = 1.0
    bottom_panel["layer"][1]["encoding"]["y"]["scale"] = {
        "domain": [-precision_gap_extent, precision_gap_extent],
        "nice": False,
    }

    return {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "description": (
            "Overlayed precision-recall curves with recall-aligned precision gaps"
        ),
        "title": "Precision-Recall Curve Comparison",
        "background": "#FFFFFF",
        "padding": {"top": 5, "left": 5, "right": 5, "bottom": 5},
        "spacing": 12,
        "vconcat": [
            top_panel,
            bottom_panel,
        ],
        "resolve": {
            "scale": {
                "color": "independent",
                "x": "shared",
            }
        },
        "config": {
            "view": {"stroke": None},
            "axis": {
                "domainColor": "#6B7280",
                "gridColor": "#E5E7EB",
                "gridOpacity": 0.8,
                "labelColor": "#374151",
                "labelFontSize": 11,
                "tickColor": "#9CA3AF",
                "titleColor": "#1F2937",
                "titleFontSize": 12,
                "titleFontWeight": 600,
            },
            "legend": {
                "labelColor": "#374151",
                "labelFontSize": 11,
                "titleColor": "#1F2937",
                "titleFontSize": 12,
            },
            "title": {
                "anchor": "start",
                "fontSize": 16,
            },
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
    comparison_records_by_label: list[tuple[str, str, list[dict[str, Any]]]] = []

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
        comparison_records_by_label.append(
            (label, f"comparison_{index}", comparison_records)
        )

    diff_records = _build_diff_records(baseline_records, comparison_records_by_label)
    chart_definition = _build_overlay_chart_definition(
        curve_records,
        diff_records,
        label_records,
    )
    return chart_definition
