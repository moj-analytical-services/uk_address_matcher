---
description: "Draft an evidence-backed PR summary in British English, with a fictional worked example, council benchmarks, precision-recall overlays and performance costs."
agent: agent
argument-hint: "Feature or commit range; optional benchmark run IDs"
---

# Write a PR summary

Produce a concise, copy-ready Markdown PR summary in British English, without
em dashes. Use the feature or commit range supplied in chat; otherwise use the
current feature commits. Do not include unrelated worktree changes.

Follow [repository workflow](../instructions/repo-workflow.instructions.md),
[benchmark evidence](../instructions/benchmark-experiments.instructions.md)
and [data safety](../instructions/data-handling.instructions.md). Reuse the
[benchmark skill](../skills/benchmark-experiment/SKILL.md) and existing reports
in `docs/experiments/` before running new experiments.

## Required sections

1. **Overview**: explain what changes and how it works in one short paragraph;
   state whether users need new arguments, options or preparation steps.
2. **Worked example**: use fictional UK addresses and a small canonical
   neighbourhood. Show the derived features, the resulting evidence or score
   contribution, and a counterexample or precision guard. Check the example
   against the actual implementation. Never reproduce real personal addresses.
3. **Real-world results**: include each council and the independently measured
   pooled dataset. Report row counts, correct and wrong matches, precision,
   recall, percentage-point deltas and PR-AUC where available. Name the run IDs,
   threshold, controls and feature version; do not present summed councils as a
   pooled run or historical results as validation of untested later changes.
4. **Precision-recall overlay**: link the pooled HTML overlay first, then the
   council overlays and supporting Markdown reports. Explain where the curves
   improve or worsen, including false-positive trade-offs.
5. **Performance and storage**: show before/after runtime and peak resident
   memory with deltas, measurement scope and repetition count. Keep matching,
   source cleaning, canonical preparation and cold end-to-end time separate.
   Measure persisted storage using comparable Parquet files with identical
   rows, compression and unrelated columns; report exact bytes, MiB, percentage
   growth and bytes per canonical alias. List all new persisted columns rather
   than attributing the whole increase to a single column. Separate persisted
   storage from temporary maps, process memory and DuckDB spill.
6. **Validation and limitations**: give the tests run, real-data parity checks,
   scope exclusions and remaining evidence gaps. Identify any failed gates or
   regressions plainly.

## Evidence discipline

- Start with persisted manifests, comparison summaries and measured files.
- Use structured JSON/Parquet readers and `uv run` for Python measurements.
- Keep thresholds, prior, population, row IDs and non-target settings constant.
- Label single timings, different hardware and partially comparable runs.
- If evidence is missing, measure the smallest relevant slice where practical;
  otherwise say "not measured" and specify the check needed. Do not invent it.
- Use relative Markdown links from the summary file and provide a clickable
  workspace link to the pooled overlay in the chat response.
- Put detailed evidence in a supporting Markdown file when needed; keep the PR
  body easy to scan. Do not stage, commit or push generated material unless asked.