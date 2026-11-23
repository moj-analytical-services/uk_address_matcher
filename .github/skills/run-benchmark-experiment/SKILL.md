---
name: run-benchmark-experiment
description: "Use when: running a persisted benchmark experiment, comparing a model or threshold change against a baseline, or producing benchmark audit artefacts."
disable-model-invocation: true
argument-hint: "Hypothesis, proposed change, baseline run, datasets, stages, threshold and audit option"
---
# Run a benchmark experiment

Follow the full [benchmark experiment instructions](../../instructions/benchmark-experiments.instructions.md).

## Inputs

Resolve the hypothesis, proposed change, baseline run, datasets, matching stages,
primary operating point and record-level audit request from the arguments. Ask for
anything material that is missing. Default the baseline to `latest` and the primary
operating point to MW 10 when those choices are not supplied.

## Objective

Use the baseline run as the control.

Unless this experiment specifies otherwise, assess results at MW 6, 8, 10 and 12.

At MW 10, prefer the formulation that recovers the most true positives while preserving precision.

A variant passes the default promotion criterion only when:

```
TP_new > TP_baseline
F1_new > F1_baseline
precision_new >= precision_baseline - 0.02 percentage points
```

Calculate and report the corresponding maximum permitted false-positive count.

## Required workflow

1. Confirm the hypothesis, exact change, baseline, controls, and whether the reduced canonical requires rebuilding.
2. Make only the smallest experiment-specific changes.
3. Run targeted tests for touched behaviour.
4. Run the persisted benchmark through `benchmarking/run_benchmarking.py`.
5. Compare the experiment against the resolved baseline.
6. Interpret the overlay precision-recall chart.
7. Produce the transition matrix and, where requested, a record-level audit.
8. Persist a concise Markdown report and JSON companion in the run directory.

## Required analysis

Report:

- Baseline and experiment run IDs.
- Exact changes and no-change controls.
- MW 6, 8, 10, and 12 metrics and deltas.
- TP, FP, FN, precision, recall, F1, F0.5, matched, and unmatched.
- MW 10 precision floor and maximum-FP assessment.
- Transition matrix.
- Model effect versus threshold effect.
- Ranking changes versus threshold crossings.
- Truth absent from the candidate set.
- Overlay precision-recall chart interpretation.
- Runtime impact.
- Important newly wrong and recovered examples.
- Artefact paths.
- A clear promote, reject, continue-testing, or inconclusive decision.

When results are promising but inconclusive, inspect changed records and run only a narrowly justified follow-up. Do not perform an unrecorded parameter search.
