---
name: record-level-audit
description: "Use the benchmark-experiment skill for persisted record-level benchmark audits and loser-viewer workflows."
disable-model-invocation: true
---
Use the `benchmark-experiment` skill for persisted benchmark audits.

Goal:

- explain row-level benchmark differences from persisted evidence before rerunning anything

Success means:

- `.github/instructions/benchmark-experiments.instructions.md` is read first
- the persisted run directory is inspected before any replay or regeneration step
- replay-audit and loser-viewer commands come from the benchmark skill assets
- any new audit output stays human-readable and writes the JSON companion when required

Stop rules:

- do not rerun a replay audit if the persisted artefacts already answer the question
- do not force this workflow when the task is not really a persisted benchmark audit