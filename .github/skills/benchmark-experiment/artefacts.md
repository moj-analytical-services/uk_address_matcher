# Benchmark Experiment Artefacts

Persisted benchmark runs live under:

```text
benchmarking/results/<dataset>/<yyyy-mm-dd>/<run_id>/
```

## Files To Read First

- `manifest.json`
  - Run metadata, stage definition, summary, timings, and chart paths.

- `accuracy_table.json`
  - Precision, recall, F1, PR-AUC, operating-point recall/thresholds, default-threshold error rates, and stage outcome counts.

- `stage_diagnostics.json`
  - Rows entering each stage, matched counts, and timing detail.

- `comparison_summary_<baseline>_vs_<current>.json`
  - Machine-readable deltas for comparison runs.

- `comparison_report_<baseline>_vs_<current>.md`
  - Human-readable comparison summary.

- `charts/precision_recall_overlay_<baseline>_vs_<current>.html`
  - Primary comparison chart.

- `charts/precision_recall_overlay_<baseline>_vs_<current>.vl.json`
  - Vega-Lite spec for the same overlay chart.

## When To Read Which Artefact

- Start with `manifest.json` if you need run IDs, stage settings, summary counts, or output paths.
- Read `accuracy_table.json` early when you need threshold-independent ranking evidence such as PR-AUC / area under the precision-recall curve and operating-point recall.
- Read `comparison_report_*.md` first when the user wants a human-readable summary.
- Read `comparison_summary_*.json` first when you need exact deltas or want to quote numbers programmatically.
- Read the overlay chart or spec when the user asks whether the change is broadly better or worse across thresholds.
- Read `stage_diagnostics.json` when the question is about runtime or stage-level movement.

## AUC-Driven Decision Rules

- Do not decide from one threshold alone when PR-AUC and the overlay chart disagree.
- Use PR-AUC as the first check for whether ranking quality improved overall.
- Then use operating-point recall from `accuracy_table.json` to answer practical questions like recall at `>=99%` precision.
- Use fixed-threshold precision/recall/F1 only after the AUC-level picture is clear.

## Run Comparison Rules

- `run_id` is the user-facing identifier.
- Comparisons are dataset-specific.
- `COMPARISON_BASELINE_RUN_ID = "latest"` is the default comparison mode in normal reruns.
- Persisted artefacts are preferred over notebook output or raw terminal logs.