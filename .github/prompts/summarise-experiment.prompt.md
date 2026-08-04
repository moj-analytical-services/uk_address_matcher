---
description: "Use when: writing a Markdown report for a completed benchmark experiment, including measured accuracy, runtime, storage and promotion evidence."
---

# Summarise a benchmark experiment

Write a concise Markdown experiment report in `docs/experiments/` using British
English. Do not use em dashes. Base every reported number on persisted artefacts
or directly measured files. Do not invent missing metrics.

## Experiment context

```text
Question:
${input:question:What hypothesis did the experiment test?}

Change:
${input:change:What implementation, model, feature or threshold changed?}

Baseline:
${input:baseline:What is the baseline run ID and baseline artefact?}

Variants:
${input:variants:Which controls, candidates and ablations were run?}

Datasets:
${input:datasets:Which datasets and row counts were used?}

Canonical rebuild:
${input:rebuild:Was the canonical rebuilt, and from which input and output paths?}

Storage paths:
${input:storage:Which files were measured before and after?}

Artefacts:
${input:artefacts:Where are the persisted manifests, charts, audits and transition tables?}
```

## Required report structure

Use these sections where the evidence exists:

1. `# <Experiment name>`
2. `## Summary`
3. `## What It Does In Practice`
4. `## Experimental Design`
5. `## Results`
6. `## Storage`
7. `## Interpretation`
8. `## Decision`
9. `## Artefacts`
10. `## Validation`

Explain the behaviour in practical terms before discussing implementation
details. State which parts were held constant, including datasets, input rows,
canonical rows, blocking rules, non-target comparisons, thresholds and
post-linkage logic.

## Required measurements

Report a table for every variant containing, where available:

- run ID and model or settings variant;
- messy rows, canonical rows and candidate pairs;
- matched, correct, wrong and unresolved rows;
- precision, recall, F1, specificity, top-k or ranking metrics and PR AUC;
- runtime, peak memory and spill;
- canonical file size and target feature size.

Calculate and label absolute deltas against the unchanged baseline. Report
percentage-point deltas for precision and recall, not only relative percentages.
Include record counts alongside percentages.

For storage experiments, report exact byte counts before and after, approximate
human-readable sizes, bytes saved or added, and the percentage change. Make clear
whether the comparison is a single Parquet file or an entire directory. Do not
compare directory totals when the directories contain different unrelated
artefacts.

For model comparisons, report the precision-recall overlay interpretation,
gamma or Bayes-factor transitions, changed top-ranked candidates and any
record-level audit findings. Distinguish arithmetic effects, representation
effects, level-band effects and complete ablation effects.

## Decision rules

State one of:

- `Promote`
- `Reject`
- `Continue testing`
- `Inconclusive`

Support the decision with explicit gates and evidence. If a candidate improves
precision but loses recall, say so plainly. If ablation is equivalent or better,
recommend removing the feature. If a candidate is promising but fails a gate,
recommend the narrowest justified follow-up rather than suggesting an
unrecorded parameter search.

## Evidence discipline

- Link to existing repository files using workspace-relative Markdown links.
- Include persisted run IDs and artefact paths.
- Mention rebuild provenance and package versions when relevant.
- Report warnings, failed runs and limitations.
- Keep production promotion separate from experimental results.
- Do not claim a storage saving from logical type sizes without measured Parquet
  bytes.
- Do not use em dashes anywhere in the report.