from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from benchmarking.comparisons.comparison_utils import (
    comparison_stage_sort_key,
    delta,
    index_by_stage,
    select_primary_accuracy_row,
    to_float,
    to_int,
)


def _format_count(value: Any) -> str:
    parsed = to_int(value)
    if parsed is None:
        return "n/a"
    return f"{parsed:,}"


def _format_signed_count(value: Any) -> str:
    parsed = to_int(value)
    if parsed is None:
        return "n/a"
    return f"{parsed:+,}"


def _format_seconds(value: Any) -> str:
    parsed = to_float(value)
    if parsed is None:
        return "n/a"
    return f"{parsed:.3f}s"


def _format_signed_seconds(value: Any) -> str:
    parsed = to_float(value)
    if parsed is None:
        return "n/a"
    return f"{parsed:+.3f}s"


def _format_fraction_percent(value: Any) -> str:
    parsed = to_float(value)
    if parsed is None:
        return "n/a"
    return f"{parsed * 100:.4f}%"


def _format_percent(value: Any) -> str:
    parsed = to_float(value)
    if parsed is None:
        return "n/a"
    return f"{parsed:.2f}%"


def _format_signed_pp_from_fraction(value: Any) -> str:
    parsed = to_float(value)
    if parsed is None:
        return "n/a"
    return f"{parsed * 100:+.4f} pp"


def _format_signed_pp(value: Any) -> str:
    parsed = to_float(value)
    if parsed is None:
        return "n/a"
    return f"{parsed:+.2f} pp"


def _format_optional_text(value: str | None) -> str:
    if value is None or not value.strip():
        return "n/a"
    return value


def _format_utc_timestamp(value: str | None) -> str:
    if value is None or not value.strip():
        return "n/a"

    normalised = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalised)
    except ValueError:
        return value

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)

    return parsed.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def _markdown_table(headers: list[str], rows: list[list[str]]) -> str:
    header_row = "| " + " | ".join(headers) + " |"
    divider_row = "| " + " | ".join("---" for _ in headers) + " |"
    body_rows = ["| " + " | ".join(row) + " |" for row in rows]
    return "\n".join([header_row, divider_row, *body_rows])


def _relative_markdown_path(*, markdown_path: Path, target_path: Path) -> str:
    return target_path.relative_to(markdown_path.parent).as_posix()


def _display_stage_name(stage_name: str) -> str:
    return stage_name.replace("_", " ").strip().title()


def _stage_status(
    *,
    baseline_row: dict[str, Any],
    current_row: dict[str, Any],
) -> str:
    if baseline_row and current_row:
        return "Present in both runs"
    if baseline_row:
        return "Removed in current run"
    if current_row:
        return "Added in current run"
    return "n/a"


def _is_non_zero(value: Any, *, tolerance: float = 1e-9) -> bool:
    parsed = to_float(value)
    if parsed is None:
        return False
    return abs(parsed) > tolerance


def _build_chart_embed_blocks(
    *,
    markdown_path: Path,
    chart_paths: list[Path],
) -> list[str]:
    embed_blocks: list[str] = []
    for chart_path in chart_paths:
        chart_title = chart_path.stem.replace("_", " ").title()
        relative_chart_path = _relative_markdown_path(
            markdown_path=markdown_path,
            target_path=chart_path,
        )
        embed_blocks.extend(
            [
                f"### {chart_title}",
                "",
                (
                    f'<iframe src="{relative_chart_path}" title="{chart_title}" '
                    'width="100%" height="860" loading="lazy" '
                    'style="border: 1px solid #d0d7de; border-radius: 8px; '
                    'background: #ffffff;"></iframe>'
                ),
                "",
                f'<p><a href="{relative_chart_path}">Open chart directly</a></p>',
                "",
            ]
        )

    return embed_blocks


def _merge_stage_rows(
    *,
    accuracy_index: dict[str, dict[str, Any]],
    diagnostics_index: dict[str, dict[str, Any]],
    stage_name: str,
) -> dict[str, Any]:
    return {
        **accuracy_index.get(stage_name, {}),
        **diagnostics_index.get(stage_name, {}),
    }


def _sorted_stage_names(
    *,
    baseline_accuracy_rows: list[dict[str, Any]],
    current_accuracy_rows: list[dict[str, Any]],
    baseline_stage_rows: list[dict[str, Any]],
    current_stage_rows: list[dict[str, Any]],
) -> list[str]:
    baseline_accuracy_index = index_by_stage(baseline_accuracy_rows)
    current_accuracy_index = index_by_stage(current_accuracy_rows)
    baseline_stage_index = index_by_stage(baseline_stage_rows)
    current_stage_index = index_by_stage(current_stage_rows)

    stage_names = set(baseline_accuracy_index).union(current_accuracy_index)
    stage_names.update(baseline_stage_index)
    stage_names.update(current_stage_index)

    return sorted(
        stage_names,
        key=lambda stage_name: comparison_stage_sort_key(
            stage_name,
            _merge_stage_rows(
                accuracy_index=baseline_accuracy_index,
                diagnostics_index=baseline_stage_index,
                stage_name=stage_name,
            ),
            _merge_stage_rows(
                accuracy_index=current_accuracy_index,
                diagnostics_index=current_stage_index,
                stage_name=stage_name,
            ),
        ),
    )


def _build_accuracy_delta_rows(
    *,
    stage_names: list[str],
    baseline_rows: list[dict[str, Any]],
    current_rows: list[dict[str, Any]],
) -> list[list[str]]:
    baseline_index = index_by_stage(baseline_rows)
    current_index = index_by_stage(current_rows)

    rows: list[list[str]] = []
    for stage_name in stage_names:
        baseline_row = baseline_index.get(stage_name, {})
        current_row = current_index.get(stage_name, {})
        if not baseline_row and not current_row:
            continue
        rows.append(
            [
                _display_stage_name(stage_name),
                _stage_status(baseline_row=baseline_row, current_row=current_row),
                _format_signed_count(
                    delta(
                        current_row.get("correct_matches"),
                        baseline_row.get("correct_matches"),
                    )
                ),
                _format_signed_pp_from_fraction(
                    delta(
                        current_row.get("precision"),
                        baseline_row.get("precision"),
                    )
                ),
                _format_signed_pp_from_fraction(
                    delta(
                        current_row.get("recall"),
                        baseline_row.get("recall"),
                    )
                ),
                _format_signed_pp_from_fraction(
                    delta(current_row.get("f1"), baseline_row.get("f1"))
                ),
            ]
        )

    return rows


def _build_stage_flow_delta_rows(
    *,
    stage_names: list[str],
    baseline_rows: list[dict[str, Any]],
    current_rows: list[dict[str, Any]],
) -> list[list[str]]:
    baseline_index = index_by_stage(baseline_rows)
    current_index = index_by_stage(current_rows)

    rows: list[list[str]] = []
    for stage_name in stage_names:
        baseline_row = baseline_index.get(stage_name, {})
        current_row = current_index.get(stage_name, {})
        if not baseline_row and not current_row:
            continue
        rows.append(
            [
                _display_stage_name(stage_name),
                _stage_status(baseline_row=baseline_row, current_row=current_row),
                _format_signed_count(
                    delta(
                        current_row.get("rows_entering_stage"),
                        baseline_row.get("rows_entering_stage"),
                    )
                ),
                _format_signed_count(
                    delta(
                        current_row.get("rows_matched_in_stage"),
                        baseline_row.get("rows_matched_in_stage"),
                    )
                ),
                _format_signed_pp(
                    delta(
                        current_row.get("stage_match_rate"),
                        baseline_row.get("stage_match_rate"),
                    )
                ),
                _format_signed_seconds(
                    delta(
                        current_row.get("elapsed_seconds"),
                        baseline_row.get("elapsed_seconds"),
                    )
                ),
            ]
        )

    return rows


def _build_stage_highlight_lines(
    *,
    stage_names: list[str],
    baseline_accuracy_rows: list[dict[str, Any]],
    current_accuracy_rows: list[dict[str, Any]],
    baseline_stage_rows: list[dict[str, Any]],
    current_stage_rows: list[dict[str, Any]],
) -> list[str]:
    baseline_accuracy_index = index_by_stage(baseline_accuracy_rows)
    current_accuracy_index = index_by_stage(current_accuracy_rows)
    baseline_stage_index = index_by_stage(baseline_stage_rows)
    current_stage_index = index_by_stage(current_stage_rows)

    lines: list[str] = []
    for stage_name in stage_names:
        if stage_name == "overall":
            continue

        baseline_accuracy = baseline_accuracy_index.get(stage_name, {})
        current_accuracy = current_accuracy_index.get(stage_name, {})
        baseline_stage = baseline_stage_index.get(stage_name, {})
        current_stage = current_stage_index.get(stage_name, {})

        status = _stage_status(
            baseline_row=baseline_accuracy or baseline_stage,
            current_row=current_accuracy or current_stage,
        )
        display_name = _display_stage_name(stage_name)
        if status != "Present in both runs":
            lines.append(f"{display_name}: {status}.")
            continue

        parts: list[str] = []
        correct_delta = delta(
            current_accuracy.get("correct_matches"),
            baseline_accuracy.get("correct_matches"),
        )
        precision_delta = delta(
            current_accuracy.get("precision"),
            baseline_accuracy.get("precision"),
        )
        recall_delta = delta(
            current_accuracy.get("recall"),
            baseline_accuracy.get("recall"),
        )
        matched_delta = delta(
            current_stage.get("rows_matched_in_stage"),
            baseline_stage.get("rows_matched_in_stage"),
        )
        runtime_delta = delta(
            current_stage.get("elapsed_seconds"),
            baseline_stage.get("elapsed_seconds"),
        )

        if _is_non_zero(correct_delta):
            parts.append(f"{_format_signed_count(correct_delta)} correct matches")
        if _is_non_zero(precision_delta):
            parts.append(f"{_format_signed_pp_from_fraction(precision_delta)} precision")
        if _is_non_zero(recall_delta):
            parts.append(f"{_format_signed_pp_from_fraction(recall_delta)} recall")
        if _is_non_zero(matched_delta):
            parts.append(f"{_format_signed_count(matched_delta)} matched in stage")
        if _is_non_zero(runtime_delta):
            parts.append(f"{_format_signed_seconds(runtime_delta)} runtime")

        if parts:
            lines.append(f"{display_name}: {', '.join(parts)}.")

    if lines:
        return lines
    return ["No stage-level changes beyond the overall headline."]


def _build_stage_breakdown_blocks(
    *,
    stage_names: list[str],
    baseline_accuracy_rows: list[dict[str, Any]],
    current_accuracy_rows: list[dict[str, Any]],
    baseline_stage_rows: list[dict[str, Any]],
    current_stage_rows: list[dict[str, Any]],
) -> list[str]:
    baseline_accuracy_index = index_by_stage(baseline_accuracy_rows)
    current_accuracy_index = index_by_stage(current_accuracy_rows)
    baseline_stage_index = index_by_stage(baseline_stage_rows)
    current_stage_index = index_by_stage(current_stage_rows)

    blocks: list[str] = []
    for stage_name in stage_names:
        baseline_accuracy = baseline_accuracy_index.get(stage_name, {})
        current_accuracy = current_accuracy_index.get(stage_name, {})
        baseline_stage = baseline_stage_index.get(stage_name, {})
        current_stage = current_stage_index.get(stage_name, {})

        blocks.extend(
            [
                f"### {_display_stage_name(stage_name)}",
                "",
                (
                    "Status: "
                    + _stage_status(
                        baseline_row=baseline_accuracy or baseline_stage,
                        current_row=current_accuracy or current_stage,
                    )
                ),
                "",
            ]
        )

        if baseline_accuracy or current_accuracy:
            blocks.extend(
                [
                    "Accuracy",
                    "",
                    _markdown_table(
                        ["Metric", "Baseline", "Current", "Delta"],
                        [
                            [
                                "Correct matches",
                                _format_count(baseline_accuracy.get("correct_matches")),
                                _format_count(current_accuracy.get("correct_matches")),
                                _format_signed_count(
                                    delta(
                                        current_accuracy.get("correct_matches"),
                                        baseline_accuracy.get("correct_matches"),
                                    )
                                ),
                            ],
                            [
                                "Precision",
                                _format_fraction_percent(
                                    baseline_accuracy.get("precision")
                                ),
                                _format_fraction_percent(
                                    current_accuracy.get("precision")
                                ),
                                _format_signed_pp_from_fraction(
                                    delta(
                                        current_accuracy.get("precision"),
                                        baseline_accuracy.get("precision"),
                                    )
                                ),
                            ],
                            [
                                "Recall",
                                _format_fraction_percent(baseline_accuracy.get("recall")),
                                _format_fraction_percent(current_accuracy.get("recall")),
                                _format_signed_pp_from_fraction(
                                    delta(
                                        current_accuracy.get("recall"),
                                        baseline_accuracy.get("recall"),
                                    )
                                ),
                            ],
                            [
                                "F1",
                                _format_fraction_percent(baseline_accuracy.get("f1")),
                                _format_fraction_percent(current_accuracy.get("f1")),
                                _format_signed_pp_from_fraction(
                                    delta(
                                        current_accuracy.get("f1"),
                                        baseline_accuracy.get("f1"),
                                    )
                                ),
                            ],
                        ],
                    ),
                    "",
                ]
            )

        if baseline_stage or current_stage:
            blocks.extend(
                [
                    "Flow and runtime",
                    "",
                    _markdown_table(
                        ["Metric", "Baseline", "Current", "Delta"],
                        [
                            [
                                "Rows entering stage",
                                _format_count(baseline_stage.get("rows_entering_stage")),
                                _format_count(current_stage.get("rows_entering_stage")),
                                _format_signed_count(
                                    delta(
                                        current_stage.get("rows_entering_stage"),
                                        baseline_stage.get("rows_entering_stage"),
                                    )
                                ),
                            ],
                            [
                                "Rows matched in stage",
                                _format_count(
                                    baseline_stage.get("rows_matched_in_stage")
                                ),
                                _format_count(current_stage.get("rows_matched_in_stage")),
                                _format_signed_count(
                                    delta(
                                        current_stage.get("rows_matched_in_stage"),
                                        baseline_stage.get("rows_matched_in_stage"),
                                    )
                                ),
                            ],
                            [
                                "Stage match rate",
                                _format_percent(baseline_stage.get("stage_match_rate")),
                                _format_percent(current_stage.get("stage_match_rate")),
                                _format_signed_pp(
                                    delta(
                                        current_stage.get("stage_match_rate"),
                                        baseline_stage.get("stage_match_rate"),
                                    )
                                ),
                            ],
                            [
                                "Elapsed time",
                                _format_seconds(baseline_stage.get("elapsed_seconds")),
                                _format_seconds(current_stage.get("elapsed_seconds")),
                                _format_signed_seconds(
                                    delta(
                                        current_stage.get("elapsed_seconds"),
                                        baseline_stage.get("elapsed_seconds"),
                                    )
                                ),
                            ],
                        ],
                    ),
                    "",
                ]
            )

    return blocks


def write_comparison_markdown_summary(
    *,
    path: Path,
    dataset_label: str | None,
    baseline_hash: str,
    current_hash: str,
    baseline_git_commit_hash: str | None,
    current_git_commit_hash: str | None,
    baseline_created_at_utc: str | None,
    current_created_at_utc: str | None,
    notes: list[str],
    baseline_accuracy_rows: list[dict[str, Any]],
    current_accuracy_rows: list[dict[str, Any]],
    baseline_stage_rows: list[dict[str, Any]],
    current_stage_rows: list[dict[str, Any]],
    chart_paths: list[Path],
    summary_path: Path,
) -> None:
    overall_baseline = select_primary_accuracy_row(baseline_accuracy_rows)
    overall_current = select_primary_accuracy_row(current_accuracy_rows)
    stage_names = _sorted_stage_names(
        baseline_accuracy_rows=baseline_accuracy_rows,
        current_accuracy_rows=current_accuracy_rows,
        baseline_stage_rows=baseline_stage_rows,
        current_stage_rows=current_stage_rows,
    )

    accuracy_delta_table = _markdown_table(
        ["Stage", "Status", "Correct Δ", "Precision Δ", "Recall Δ", "F1 Δ"],
        _build_accuracy_delta_rows(
            stage_names=stage_names,
            baseline_rows=baseline_accuracy_rows,
            current_rows=current_accuracy_rows,
        ),
    )
    stage_flow_delta_table = _markdown_table(
        [
            "Stage",
            "Status",
            "Entering Δ",
            "Matched Δ",
            "Match rate Δ",
            "Runtime Δ",
        ],
        _build_stage_flow_delta_rows(
            stage_names=stage_names,
            baseline_rows=baseline_stage_rows,
            current_rows=current_stage_rows,
        ),
    )
    stage_highlights = _build_stage_highlight_lines(
        stage_names=stage_names,
        baseline_accuracy_rows=baseline_accuracy_rows,
        current_accuracy_rows=current_accuracy_rows,
        baseline_stage_rows=baseline_stage_rows,
        current_stage_rows=current_stage_rows,
    )
    stage_breakdown_blocks = _build_stage_breakdown_blocks(
        stage_names=stage_names,
        baseline_accuracy_rows=baseline_accuracy_rows,
        current_accuracy_rows=current_accuracy_rows,
        baseline_stage_rows=baseline_stage_rows,
        current_stage_rows=current_stage_rows,
    )

    relative_summary_path = _relative_markdown_path(
        markdown_path=path,
        target_path=summary_path,
    )
    artifact_lines = [f"- Summary JSON: {relative_summary_path}"]
    for chart_path in chart_paths:
        relative_chart_path = _relative_markdown_path(
            markdown_path=path,
            target_path=chart_path,
        )
        artifact_lines.append(f"- Chart: {relative_chart_path}")

    chart_embed_blocks = _build_chart_embed_blocks(
        markdown_path=path,
        chart_paths=chart_paths,
    )

    markdown = "\n".join(
        [
            f"# Benchmark Comparison: {baseline_hash} vs {current_hash}",
            "",
            f"Dataset: {_format_optional_text(dataset_label)}",
            "",
            "## Runs",
            "",
            _markdown_table(
                ["Run", "Hash", "Git commit", "Created at (UTC)"],
                [
                    [
                        "Baseline",
                        baseline_hash,
                        _format_optional_text(baseline_git_commit_hash),
                        _format_utc_timestamp(baseline_created_at_utc),
                    ],
                    [
                        "Current",
                        current_hash,
                        _format_optional_text(current_git_commit_hash),
                        _format_utc_timestamp(current_created_at_utc),
                    ],
                ],
            ),
            "",
            "## Overall Outcome",
            "",
            _markdown_table(
                ["Metric", "Baseline", "Current", "Delta"],
                [
                    [
                        "Correct matches",
                        _format_count(overall_baseline.get("correct_matches")),
                        _format_count(overall_current.get("correct_matches")),
                        _format_signed_count(
                            delta(
                                overall_current.get("correct_matches"),
                                overall_baseline.get("correct_matches"),
                            )
                        ),
                    ],
                    [
                        "Precision",
                        _format_fraction_percent(overall_baseline.get("precision")),
                        _format_fraction_percent(overall_current.get("precision")),
                        _format_signed_pp_from_fraction(
                            delta(
                                overall_current.get("precision"),
                                overall_baseline.get("precision"),
                            )
                        ),
                    ],
                    [
                        "Recall",
                        _format_fraction_percent(overall_baseline.get("recall")),
                        _format_fraction_percent(overall_current.get("recall")),
                        _format_signed_pp_from_fraction(
                            delta(
                                overall_current.get("recall"),
                                overall_baseline.get("recall"),
                            )
                        ),
                    ],
                    [
                        "F1",
                        _format_fraction_percent(overall_baseline.get("f1")),
                        _format_fraction_percent(overall_current.get("f1")),
                        _format_signed_pp_from_fraction(
                            delta(
                                overall_current.get("f1"),
                                overall_baseline.get("f1"),
                            )
                        ),
                    ],
                ],
            ),
            "",
            "## What Changed",
            "",
            *[f"- {note}" for note in notes],
            "",
            "## Stage Highlights",
            "",
            *[f"- {line}" for line in stage_highlights],
            "",
            "## Accuracy Changes By Stage",
            "",
            accuracy_delta_table,
            "",
            "## Flow And Runtime Changes By Stage",
            "",
            stage_flow_delta_table,
            "",
            "## Detailed Stage Breakdown",
            "",
            *stage_breakdown_blocks,
            "## Embedded Charts",
            "",
            (
                "These charts are embedded as HTML iframes. If your markdown renderer "
                "does not display the iframe, use the direct chart link below it."
            ),
            "",
            *chart_embed_blocks,
            "## Artifacts",
            "",
            *artifact_lines,
            "",
        ]
    )
    path.write_text(markdown, encoding="utf-8")
