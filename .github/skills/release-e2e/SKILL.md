---
name: release-e2e
description: "Validate a release end-to-end and produce the runtime manifest and report."
disable-model-invocation: true
argument-hint: "Target release v2.x.x and private benchmarking/.config.json contents"
---

# Release end-to-end validation

Run this workflow locally from the release branch. Do not publish, push, deploy
cloud infrastructure or expose private inputs, paths, addresses, predictions,
credentials or machine identifiers.

Follow the [repository instructions](../../instructions/repo-workflow.instructions.md),
[benchmark workflow](../benchmark-experiment/SKILL.md),
[data safety rules](../../instructions/data-handling.instructions.md), and
[ADR 0005](../../../docs/adr/0005-local-pre-release-validation.md).

## 1. Ask For The Three Release Inputs

Before running anything, collect only:

1. The target release version and source ref, if they were not already supplied.
2. A filled-in private `benchmarking/.config.json`. Check the file. If a required
   value is missing or still a placeholder, ask the user to update the file
   directly and say when they are ready to continue. Do not ask them to paste the
   file into chat.
3. The local path to the raw OS data to be used for full national precleaning.

The config must contain the prepared canonical path and the five labelled
datasets used by this run:

- `UKAM_OS_CANONICAL_PREPARED`
- `UKAM_HACKNEY_DATA_PATH`
- `UKAM_RHONDDA_DATA_PATH`
- `UKAM_ABERDEENSHIRE_DATA_PATH`
- `UKAM_MID_SUSSEX_DATA_PATH`
- `UKAM_GWYNEDD_DATA_PATH`

After the user supplies these, validate the paths and proceed. Use a fresh
release-specific prepared-output directory; never overwrite raw input or an
existing prepared directory.

## 2. Install And Prepare

1. Create an isolated `uv` environment from the exact release checkout or wheel.
   Record the imported package version and module path, commit, lock, Python and
   DuckDB versions, and the source ref privately.
2. Prepare the complete national input with 10 cleaning chunks, 16GB DuckDB
   memory and supported default indexes/features unless the user overrides them.
   No reduced canonical, sample or cached preparation.
3. Verify the bundle manifest, files, row counts and fresh-connection loading.
4. Measure the preparation E2E wall time and process RSS and allocated spill
   through setup, writes, manifest finalisation and teardown. Sample process
   resources externally at one-second intervals and record OS high-water RSS
   when available. A configured 16GB DuckDB memory limit is not a process-RSS
   limit.
5. Measure preparation stages when practical. If stage instrumentation is not
   available, record the mandatory overall measurements and leave stage entries
   empty rather than estimating them.
6. Use the fresh prepared path for the release runs without changing the user's
   config. The close-down step asks whether to activate it after all work and
   checks have finished.

## 3. Run The Complete Benchmark Matrix

Use the existing benchmark loaders, runner, stages and persistence helpers. Do
not create another matcher or change production defaults. Run every job in a
fresh process and connection with:

- `sample_mode=False`
- `persist_results=True`
- candidates retained down to MW 10
- MW 10, 20 and 30 scored from the same run, with exact and peeled matches fixed
- current stage order and model defaults

Run all 14 jobs:

| Postcode mode | Dataset/view | Canonical filter |
|---|---|---|
| Supplied | Pooled councils, restricted | Standard residential filter |
| Supplied | Pooled councils, unrestricted | No council/classification filter |
| Supplied | Hackney | `E09000012` and `R` |
| Supplied | Rhondda Cynon Taf | `W06000016` and `R` |
| Supplied | Aberdeenshire | `S12000034` and `R` |
| Supplied | Mid Sussex | `E07000228` and `C` |
| Supplied | Gwynedd | `W06000002` and `R` |
| Postcode-less | Pooled councils, restricted | Standard residential filter |
| Postcode-less | Pooled councils, unrestricted | No council/classification filter |
| Postcode-less | Hackney | `E09000012` and `R` |
| Postcode-less | Rhondda Cynon Taf | `W06000016` and `R` |
| Postcode-less | Aberdeenshire | `S12000034` and `R` |
| Postcode-less | Mid Sussex | `E07000228` and `C` |
| Postcode-less | Gwynedd | `W06000002` and `R` |

`pooled_councils` combines Hackney, Rhondda Cynon Taf and Aberdeenshire. The
restricted pool applies the supported standard residential filter, including
classification `R` and existing ancillary-address exclusions. The unrestricted
pool applies no geography or classification restriction. Do not invent either
filter from address text or perform local canonical/TF rebuilds.

Apply regional `lowertierlocalauthoritygsscode` and leading
`classificationcode` filters through `canonical_address_filter` before matching.
Retain national TF and index context. Verify GSS codes, non-empty subsets and
eligible-label coverage.

For every job measure and retain:

- matching workflow runtime, excluding accuracy reporting and persistence;
- E2E runtime through setup, matching, reporting, persistence and teardown;
- sampled peak RSS;
- OS high-water RSS when available;
- sampled peak allocated spill and final spill;
- raw, eligible and excluded rows, with naturally missing-postcode counts;
- exact, peeled, Splink and other-pipeline stage runtimes when available;
- MW 10, 20 and 30 outcomes.

Do not report one runtime as three independently measured threshold runs.

### Input Rules

- Supplied-postcode mode preserves supplied values, including genuine nulls.
- Postcode-less mode clears the input postcode and removes postcode text before
  cleaning. For the relation emitted by the existing dataset loader, use the
  same UK postcode pattern as production normalisation:

  ```sql
  SELECT
      * EXCLUDE (address_concat, postcode),
      regexp_replace(
          address_concat,
          '\b(?:GIR ?0AA|[A-Z][A-HJ-Y]?\d[A-Z\d]? ?\d[A-Z]{2})\b',
          '',
          'gi'
      ) AS address_concat,
      CAST(NULL AS VARCHAR) AS postcode
  FROM loaded_dataset
  ```

  Apply this external test-only transform before matching. Keep canonical
  postcodes, labels, filters and eligible cohorts unchanged. Treat it as an
  artificial stress test.
- Exclude rows with missing UPRN labels or labels absent from the selected
  canonical because they cannot be scored.
- Retain all other rows, including genuine missing postcodes.
- Keep wrong and unmatched observations in the denominator.
- Do not add difficult-record, duplicate-observation or suppression exclusions.

## 4. Reconcile And Persist

Create `docs/runtime/<target-release>/manifest.json` from
[manifest.template.json](../../../docs/runtime/manifest.template.json). Include
all seven dataset keys in both `datasets` and `postcodeless_datasets`; do not
collapse the restricted/unrestricted pooled views.

For every operating point use:

```json
{
  "match_weight": 10,
  "precision": 0,
  "recall": 0,
  "f1": 0,
  "tp": 0,
  "fp": 0,
  "unmatched": 0
}
```

Replace the example values with exact results. Use null only with a material
explanation in `notes`; never substitute zero for unavailable data.

For each job and operating point verify:

- `tp + fp + unmatched = eligible_rows`
- `precision = tp / (tp + fp)`
- `recall = tp / eligible_rows`
- `f1 = 2 * precision * recall / (precision + recall)`

Copy the blank [report.md template](../../../docs/runtime/report.md) into the
target directory and replace every placeholder from `manifest.json`. Keep the
template's headings, tables and ordering. The report must include:

1. release, installed version and status;
2. preparation count, E2E runtime and resource peak;
3. one resource row for every one of the 14 benchmark jobs;
4. MW 10 summaries for supplied and postcode-less modes;
5. MW 10/20/30 tables for all seven dataset views in both modes;
6. short notes describing how the run was produced, practical simplifications,
   timing boundaries and material limitations;
7. a relative link to `manifest.json`.

Keep notes simple. Say what was prepared, what was run, what was simplified and
what could not be measured. Do not turn them into diagnostic essays, PR-AUC
reports or private-environment inventories.

Render seconds to three decimals, percentages to two decimals, counts with
thousands separators and GB as 1,000,000,000 bytes. Reconcile every Markdown
value against JSON using unrounded source numbers.

## 5. Validate

- Re-import the installed package and verify the expected version.
- Re-run the manifest/report reconciliation.
- Confirm there are exactly 14 completed benchmark jobs, 14 complete MW 10/20/30
  score sets (42 operating points), and a sampled RSS value for preparation
  plus every job.
- Confirm all monitored jobs ended with zero remaining spill or document the
  failure clearly.
- Run focused tests for any harness change.
- Run the repository's secret checker on the versioned report and manifest.

Do not claim completion with a missing required run, invalid metrics, failed
checks or absent overall runtime/RSS measurements.

## 6. Performance Documentation

Compare only with genuinely comparable persisted runs. If cohorts, canonical
data, model, dependencies, constraints or hardware differ materially, leave
[performance.md](../../../docs/site/performance.md) unchanged and say so in one
note. Otherwise update only the affected preparation and benchmark figures as
the final, separately reviewed step.

Finish with the release status, report links, config activation status,
performance-documentation decision and exact validation commands/results.

## 7. Close Down And Offer Activation

At the end of the run, calculate the total size of the newly prepared canonical
folder, including all output files. Then ask the user whether they want to update
`UKAM_OS_CANONICAL_PREPARED` in their private `benchmarking/.config.json` to the
new folder. The answer will usually be yes, but do not update it without
confirmation.

Show the new folder's total size in a human-readable unit so the user can make
the choice quickly. If the user says yes, update only
`UKAM_OS_CANONICAL_PREPARED`, preserve every other config value, and verify the
resolved path in a fresh process.
