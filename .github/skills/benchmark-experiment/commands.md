# Benchmark Experiment Commands

These are the default command templates for recurring experiment work.

## Run A Persisted Benchmark

```bash
export UKAM_OS_CANONICAL_PREPARED=/path/to/prepared/canonical
uv run python -m benchmarking.run_benchmarking
```

Notes:

- Run as a module so imports resolve correctly.
- Keep `benchmarking/run_benchmarking.py` edits surgical: dataset selection, stages, and comparison baseline only.

## Rebuild The Reduced Canonical

```bash
uv run python dev_scripts/reduced_canonical.py
```

Use this when upstream cleaned features or canonical-side fields used by matching changed.

## Build A Structural TF Variant

```bash
uv run python -m benchmarking.build_structural_tf_variant \
  --filter-mode unit_only \
  --output-dir /path/to/tmp/variant
```

Common modes:

- `none`
- `broad`
- `unit_only`

## Run A Structural TF Replay Audit

```bash
uv run python benchmarking/structural_tf_recall_audit.py \
  --dataset-key hackney \
  --baseline-run-id <baseline_run_id> \
  --variant-run-id <variant_run_id> \
  --baseline-canonical-path /path/to/baseline/canonical \
  --variant-canonical-path /path/to/variant/canonical \
  --baseline-filter-mode none \
  --variant-filter-mode unit_only \
  --output-dir benchmarking/results/hackney/<yyyy-mm-dd>/<variant_run_id>
```

This writes replayed overlay charts, markdown and JSON audits, and lost-record JSON.

## Generate The Static Loser Viewer

```bash
uv run python benchmarking/structural_tf_loser_viewer.py \
  --lost-records-json benchmarking/results/hackney/<yyyy-mm-dd>/<run_id>/structural_tf_lost_records_<baseline>_vs_<variant>.json \
  --canonical-path /path/to/variant/canonical \
  --output-html docs/findings/hackney_structural_tf_unit_only_loser_viewer.html
```

## Manual Run Comparison

Use `COMPARISON_BASELINE_RUN_ID = "latest"` in `benchmarking/run_benchmarking.py` for the normal case.

If you need a manual comparison helper, inspect `benchmarking/compare_runs.py` and the persisted run directories first.