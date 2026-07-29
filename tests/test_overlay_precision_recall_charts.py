from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from uk_address_matcher.analysis.overlay_precision_recall_charts import (
    _LABEL_MIN_VERTICAL_GAP,
    _OVERLAY_COLOUR_RANGE,
    _apply_label_offsets,
    _build_overlay_chart_definition,
    _overlay_precision_recall_charts,
)


def _chart(values: list[dict[str, Any]]) -> dict[str, Any]:
    return {"data": {"values": values}}


def _curve_values(*, precision_offset: float = 0.0) -> list[dict[str, Any]]:
    return [
        {
            "recall": 0.4,
            "precision": 0.9 + precision_offset,
            "truth_threshold": 12.0,
            "match_probability": 0.9998,
            "fp": 10,
        },
        {
            "recall": 0.8,
            "precision": 0.94 + precision_offset,
            "truth_threshold": 8.0,
            "match_probability": 0.9961,
            "fp": 5,
        },
    ]


def test_apply_label_offsets_separates_close_endpoint_labels() -> None:
    adjusted = _apply_label_offsets(
        [
            {
                "series_label": "Hackney 320bdce991462589",
                "recall": 0.962,
                "precision": 0.976,
                "is_baseline": True,
            },
            {
                "series_label": "Hackney 366b7bec7391f5df",
                "recall": 0.958,
                "precision": 0.974,
                "is_baseline": False,
            },
            {
                "series_label": "Hackney 739f3335b49a58f0",
                "recall": 0.812,
                "precision": 0.913,
                "is_baseline": False,
            },
        ]
    )

    adjusted_by_label = {record["series_label"]: record for record in adjusted}
    left = adjusted_by_label["Hackney 320bdce991462589"]
    right = adjusted_by_label["Hackney 366b7bec7391f5df"]

    assert (
        abs(float(left["label_precision"]) - float(right["label_precision"]))
        >= _LABEL_MIN_VERTICAL_GAP
    )
    assert not left["label_has_connector"]
    assert not right["label_has_connector"]


def test_overlay_chart_definition_uses_translucent_comparisons_and_hover_guides() -> None:
    chart_definition = _build_overlay_chart_definition(
        curve_records=[
            {
                "series_id": "baseline",
                "series_label": "Baseline",
                "is_baseline": True,
                "recall": 0.91,
                "precision": 0.96,
            },
            {
                "series_id": "comparison_1",
                "series_label": "Comparison",
                "is_baseline": False,
                "recall": 0.89,
                "precision": 0.955,
            },
        ],
        diff_records=[],
        label_records=[
            {
                "series_label": "Baseline",
                "is_baseline": True,
                "recall": 0.91,
                "precision": 0.96,
            },
            {
                "series_label": "Comparison",
                "is_baseline": False,
                "recall": 0.89,
                "precision": 0.955,
            },
        ],
    )

    top_panel = chart_definition["vconcat"][0]
    assert len(chart_definition["vconcat"]) == 2
    assert "params" not in chart_definition
    assert "params" not in top_panel
    assert "title" not in top_panel

    hover_source_layer = top_panel["layer"][0]
    assert hover_source_layer["params"][0]["name"] == "curve_hover"
    assert hover_source_layer["mark"] == {
        "type": "point",
        "opacity": 0,
        "size": 90,
        "clip": True,
    }

    baseline_area_layer = top_panel["layer"][1]
    comparison_area_layer = top_panel["layer"][2]
    baseline_line_layer = top_panel["layer"][3]
    comparison_line_layer = top_panel["layer"][4]
    assert comparison_area_layer["transform"][-1] == {"filter": "!datum.is_baseline"}
    assert comparison_area_layer["mark"]["opacity"] == 0.14
    assert "y2" not in comparison_area_layer["encoding"]
    assert "detail" not in comparison_area_layer["encoding"]
    assert "order" not in comparison_area_layer["encoding"]
    assert baseline_area_layer["transform"][-1] == {"filter": "datum.is_baseline"}
    assert baseline_area_layer["mark"]["opacity"] == 0.09
    assert "y2" not in baseline_area_layer["encoding"]
    assert "detail" not in baseline_area_layer["encoding"]
    assert "order" not in baseline_area_layer["encoding"]
    assert comparison_line_layer["transform"] == [{"filter": "!datum.is_baseline"}]
    assert comparison_line_layer["encoding"]["opacity"] == {
        "condition": [
            {
                "test": (
                    "length(data('label_hover_store')) === 0 && "
                    "length(data('gap_hover_store')) === 0"
                ),
                "value": 0.45,
            },
            {"param": "label_hover", "empty": False, "value": 1.0},
            {"param": "gap_hover", "empty": False, "value": 1.0},
        ],
        "value": 0.18,
    }
    assert comparison_line_layer["mark"]["strokeWidth"] == 2.75
    assert "strokeDash" not in comparison_line_layer["mark"]
    assert baseline_line_layer["transform"] == [{"filter": "datum.is_baseline"}]
    assert baseline_line_layer["encoding"]["opacity"] == {
        "condition": [
            {
                "test": (
                    "length(data('label_hover_store')) === 0 && "
                    "length(data('gap_hover_store')) === 0"
                ),
                "value": 0.45,
            },
            {"param": "label_hover", "empty": False, "value": 1.0},
            {"param": "gap_hover", "empty": False, "value": 1.0},
        ],
        "value": 0.18,
    }
    assert baseline_line_layer["mark"]["strokeWidth"] == 3.25
    assert baseline_line_layer["encoding"]["detail"] == {
        "field": "series_id",
        "type": "nominal",
    }
    assert baseline_line_layer["encoding"]["order"] == {
        "field": "recall",
        "type": "quantitative",
    }
    assert baseline_line_layer["encoding"]["x"]["scale"]["domain"] == [0.84, 0.96]
    assert baseline_line_layer["encoding"]["y"]["scale"]["domain"] == [0.955, 1.0]
    assert "params" not in baseline_line_layer
    assert chart_definition["resolve"]["scale"]["x"] == "shared"

    hover_rule_layers = [
        layer
        for layer in top_panel["layer"]
        if layer.get("mark", {}).get("type") == "rule"
        and layer.get("transform")
        == [{"filter": {"param": "curve_hover", "empty": False}}]
    ]
    assert len(hover_rule_layers) == 2

    label_layer = next(
        layer
        for layer in top_panel["layer"]
        if layer.get("mark", {}).get("type") == "text"
    )
    assert "stroke" not in label_layer["mark"]
    assert label_layer["encoding"]["y"]["field"] == "label_precision"
    assert label_layer["params"][0]["name"] == "label_hover"
    assert label_layer["params"][0]["select"] == {
        "type": "point",
        "on": "mouseover",
        "clear": "mouseout",
        "fields": ["series_id"],
    }
    assert label_layer["encoding"]["opacity"] == {
        "condition": {"param": "label_hover", "empty": False, "value": 1.0},
        "value": 0.75,
    }

    connector_layer = next(
        layer
        for layer in top_panel["layer"]
        if layer.get("mark", {}).get("type") == "rule"
        and layer.get("encoding", {}).get("y2", {}).get("field") == "label_precision"
    )
    assert connector_layer["transform"] == [{"filter": "datum.label_has_connector"}]

    tooltip_fields = [
        tooltip["field"] for tooltip in hover_source_layer["encoding"]["tooltip"]
    ]
    assert tooltip_fields == [
        "series_label",
        "fp",
        "truth_threshold",
        "match_probability",
        "precision",
        "recall",
    ]

    assert chart_definition["$schema"].endswith("/v5.json")
    assert chart_definition["spacing"] == 12
    assert chart_definition["config"]["view"]["stroke"] is None
    assert chart_definition["config"]["title"]["anchor"] == "start"


def test_overlay_chart_uses_one_ordered_colour_mapping() -> None:
    chart_definition = _overlay_precision_recall_charts(
        _chart(_curve_values()),
        [
            _chart(_curve_values(precision_offset=0.001)),
            _chart(_curve_values(precision_offset=0.002)),
        ],
        baseline_label="Baseline",
        comparison_labels=["First", "Second"],
    )

    top_panel = chart_definition["vconcat"][0]
    (
        _,
        _,
        _,
        comparison_line_layer,
        baseline_line_layer,
        connector_layer,
        label_layer,
        *_,
    ) = top_panel["layer"]
    top_colour_scale = baseline_line_layer["encoding"]["color"]["scale"]
    assert top_colour_scale == {
        "domain": ["Baseline", "First", "Second"],
        "range": _OVERLAY_COLOUR_RANGE[:3],
    }
    assert top_colour_scale["range"] == ["#005D5D", "#FA4D56", "#6929C4"]
    assert len(set(top_colour_scale["range"])) == len(top_colour_scale["range"])
    assert comparison_line_layer["encoding"]["color"]["scale"] == top_colour_scale
    assert connector_layer["encoding"]["color"]["scale"] == top_colour_scale
    assert label_layer["encoding"]["color"]["scale"] == top_colour_scale
    assert connector_layer["encoding"]["color"]["legend"] is None
    assert label_layer["encoding"]["color"]["legend"] is None

    bottom_colour_scale = chart_definition["vconcat"][1]["layer"][1]["encoding"]["color"][
        "scale"
    ]
    assert bottom_colour_scale == {
        "domain": ["First", "Second"],
        "range": _OVERLAY_COLOUR_RANGE[1:3],
    }
    assert chart_definition["vconcat"][1]["layer"][1]["encoding"]["opacity"] == {
        "condition": [
            {
                "test": (
                    "length(data('label_hover_store')) === 0 && "
                    "length(data('gap_hover_store')) === 0"
                ),
                "value": 0.45,
            },
            {"param": "label_hover", "empty": False, "value": 1.0},
            {"param": "gap_hover", "empty": False, "value": 1.0},
        ],
        "value": 0.18,
    }
    assert chart_definition["vconcat"][1]["layer"][1]["encoding"]["detail"] == {
        "field": "series_id",
        "type": "nominal",
    }
    assert chart_definition["vconcat"][1]["layer"][1]["mark"]["strokeWidth"] == 2.75
    assert "point" not in chart_definition["vconcat"][1]["layer"][1]["mark"]
    assert "strokeDash" not in chart_definition["vconcat"][1]["layer"][1]["mark"]
    assert chart_definition["vconcat"][1]["layer"][1]["params"][0]["name"] == "gap_hover"
    assert (
        chart_definition["vconcat"][1]["layer"][1]["encoding"]["x"]["field"] == "recall"
    )
    assert chart_definition["vconcat"][1]["layer"][1]["encoding"]["y"]["scale"] == {
        "domain": [-0.2, 0.2],
        "nice": False,
    }


def test_overlay_chart_rejects_more_curves_than_palette_colours() -> None:
    comparison_charts = [_chart(_curve_values()) for _ in range(10)]

    with pytest.raises(ValueError, match="at most 10 curves; received 11"):
        _overlay_precision_recall_charts(
            _chart(_curve_values()),
            comparison_charts,
        )


def test_overlay_chart_preserves_curve_data_and_precision_gaps() -> None:
    baseline_values = _curve_values()
    comparison_values = [
        {**baseline_values[0], "precision": 0.91, "fp": 9},
        {**baseline_values[1], "precision": 0.95, "fp": 4},
    ]

    chart_definition = _overlay_precision_recall_charts(
        _chart(baseline_values),
        _chart(comparison_values),
        baseline_label="Baseline",
        comparison_labels="Comparison",
    )

    curve_records = chart_definition["vconcat"][0]["data"]["values"]
    assert len(curve_records) == 4
    assert [
        {
            key: record[key]
            for key in (
                "precision",
                "recall",
                "truth_threshold",
                "series_id",
                "series_label",
            )
        }
        for record in curve_records
    ] == [
        {
            "precision": 0.9,
            "recall": 0.4,
            "truth_threshold": 12.0,
            "series_id": "baseline",
            "series_label": "Baseline",
        },
        {
            "precision": 0.94,
            "recall": 0.8,
            "truth_threshold": 8.0,
            "series_id": "baseline",
            "series_label": "Baseline",
        },
        {
            "precision": 0.91,
            "recall": 0.4,
            "truth_threshold": 12.0,
            "series_id": "comparison_1",
            "series_label": "Comparison",
        },
        {
            "precision": 0.95,
            "recall": 0.8,
            "truth_threshold": 8.0,
            "series_id": "comparison_1",
            "series_label": "Comparison",
        },
    ]

    diff_records = chart_definition["vconcat"][1]["data"]["values"]
    precision_gaps = [
        record["precision_gap_percentage_points"] for record in diff_records
    ]
    assert precision_gaps == pytest.approx([1.0, 1.0])
    assert [record["comparison_fp"] for record in diff_records] == [9.0, 4.0]
    json.dumps(chart_definition, allow_nan=False)


@pytest.mark.parametrize("input_kind", ["dict", "json", "html", "object", "named"])
def test_overlay_chart_accepts_supported_input_forms(
    input_kind: str,
    tmp_path: Path,
) -> None:
    values = _curve_values()
    inline_chart = _chart(values)
    chart_input: Any

    if input_kind == "dict":
        chart_input = inline_chart
    elif input_kind == "json":
        chart_path = tmp_path / "curve.json"
        chart_path.write_text(json.dumps(inline_chart), encoding="utf-8")
        chart_input = chart_path
    elif input_kind == "html":
        chart_path = tmp_path / "curve.html"
        chart_path.write_text(
            f"<script>const spec = {json.dumps(inline_chart)};</script>",
            encoding="utf-8",
        )
        chart_input = chart_path
    elif input_kind == "object":

        class ChartLike:
            def to_dict(self) -> dict[str, Any]:
                return inline_chart

        chart_input = ChartLike()
    else:
        chart_input = {
            "data": {"name": "curve_records"},
            "datasets": {"curve_records": values},
        }

    chart_definition = _overlay_precision_recall_charts(
        chart_input,
        inline_chart,
    )

    assert len(chart_definition["vconcat"][0]["data"]["values"]) == 4
