---
name: benchmark-experiment
description: "Use when: running benchmark experiments, rebuilding reduced canonical data, comparing persisted benchmark runs, generating replay audits, inspecting overlay charts, or regenerating structural TF loser viewers. Provides exact commands, env vars, artefact paths, and workflow selection for uk_address_matcher experiments."
---

# Benchmark Experiment Skill

Use this skill for recurring benchmark workflows in `uk_address_matcher`.

## Goal

- Resolve benchmark experiments, persisted comparisons, and record-level audits without rediscovering commands, env vars, or artefact paths across the repo.

## Success Criteria

- The correct benchmark workflow is chosen.
- Persisted artefacts are used before reruns where they already answer the question.
- Rebuilds and reruns only happen when there is a concrete evidence gap.
- Reports include run IDs, output paths, changed settings, PR-AUC / area-under-the-curve evidence, and validation evidence.

## Start Here

- Read `.github/instructions/benchmark-experiments.instructions.md` first.
- Then use the bundled assets in this skill instead of rediscovering commands across `benchmarking/`, `.github/prompts/`, and `docs/findings/`.

## What This Skill Covers

- Standard persisted benchmark runs via `benchmarking/run_benchmarking.py`
- Reduced canonical rebuilds via `scripts/reduced_canonical.py`
- Structural TF variant builds via `benchmarking/build_structural_tf_variant.py`
- Replay audits and row-level loser analysis
- Persisted artefact lookup under `benchmarking/results/<dataset>/<date>/<run_id>/`
- Static loser-viewer regeneration for findings work

## Bundled Assets

- `commands.md` for exact command templates
- `env-vars.md` for `UKAM_*` and dataset data-path variables
- `artefacts.md` for persisted-run directory contents and which files to read first

## Workflow Selection

- Use `benchmarking/run_benchmarking.py` for normal persisted experiments.
- Use `scripts/reduced_canonical.py` when upstream cleaned features or canonical-side fields changed and the standard reduced canonical needs rebuilding.
- Use `benchmarking/build_structural_tf_variant.py` when the task is a structural TF ablation or variant comparison.
- Use `benchmarking/structural_tf_recall_audit.py` when the user wants replayed threshold analysis or row-level lost-record exports.
- Use `benchmarking/structural_tf_loser_viewer.py` when the user wants a portable HTML review surface from lost-record JSON.

Start from persisted run artefacts whenever the request can be answered from existing outputs.

## Working Rules

- Prefer `uv run python -m ...` for benchmark entrypoints that rely on package imports.
- Prefer persisted artefacts over console summaries.
- Use PR-AUC / area under the precision-recall curve as a primary threshold-independent decision aid, not just fixed-threshold precision/recall.
- Treat the overlay precision-recall chart plus PR-AUC as the minimum pair for deciding whether a variant is broadly better.
- Report run IDs, output directories, exact changed settings, PR-AUC findings, and overlay chart paths.
- If a workflow question is already answered in this skill, do not reopen broad repo exploration.

## Stop Rules

- Do not rebuild reduced canonical data unless upstream cleaned features or canonical-side fields changed.
- Do not rerun a benchmark or replay audit just to restate numbers already available in persisted artefacts.
- When enough persisted evidence exists to answer correctly, answer and stop.