---
description: "Validate a fresh release end-to-end: national preparation, filtered council benchmarks, laptop runtime manifest, and gated performance-documentation updates."
agent: agent
argument-hint: "Target release v1.x.x, source ref or wheel, raw canonical path, optional DuckDB constraints"
---

# Release end-to-end validation

Execute this workflow from a fresh installation and raw national data through
persisted benchmarks and verified reports. Do not publish, commit, push or deploy
cloud infrastructure. Keep addresses, row-level predictions, credentials, private
paths and serial numbers out of shareable output; retain private diagnostics locally.

Follow the [repository workflow](../instructions/repo-workflow.instructions.md),
[benchmark instructions](../instructions/benchmark-experiments.instructions.md),
[data safety instructions](../instructions/data-handling.instructions.md), and
[benchmark skill](../skills/benchmark-experiment/SKILL.md). Read applicable Python,
SQL and testing instructions before implementing a harness. Rationale:
[Local pre-release validation](../../docs/adr/0005-local-pre-release-validation.md).

## Inputs And Defaults

- Confirm the target release **before creating reports or starting a run**: require
  a user-supplied `v1.x.x` label, not an inferred date/version/ref. Resolve ambiguous
  source refs or wheels; record the installed package version separately.
- Ask for missing raw canonical and five labelled dataset paths. Inspect schemas,
  not address records; use supported `prepare_canonical_folder` inputs. Attach
  DuckDB sources read-only and clarify ambiguous tables/projections.
- Default to **10 preparation chunks** and **`16GB` DuckDB memory**, unless overridden.
  This is not a process-RSS limit. Apply and read back constraints on every
  connection, including `setup_connection()`; record threads, spill cap, output
  chunks and resolved preparation options. Do not silently change resources or sample.
- Choose fresh release-specific prepared-output and private results directories
  with sufficient free space, plus dedicated spill directories per job. Never
  overwrite raw inputs or an existing prepared folder; ask if capacity is uncertain.
- Before importing benchmark settings, validate config/environment paths. Treat
  missing, null, blank or placeholder values as unset; ask rather than invent paths.
  For the primary run, create/update private `benchmarking/.config.json` with a JSON
  parser, preserving unrelated settings and filling validated `UKAM_*_DATA_PATH`
  values. Historical runs must instead follow section 4's config-preservation rule.

## 1. Fresh Installation And National Preparation

1. Record UTC timestamps, Git commit/dirty state, package/Python/DuckDB/dependency
   versions and lock, OS, laptop model, CPU, logical cores and RAM. Use `uv` for an
   isolated installation: build/install the checkout's wheel or install the exact
   published version. Verify imported `uk_address_matcher` version/module path;
   repository benchmark helpers must not shadow it. Retain runnable commands.
2. Convert raw OS extracts with existing `ukam-os-builder` rules for alternate and
   Welsh addresses, metadata and deduplication; retain geography and address variants.
   Record builder provenance, timing and resources separately. Then run
   `prepare_canonical_folder` on the **complete national input**, defaulting to
   `num_of_chunks=10`, `memory_limit='16GB'` and supported default indexes/features/
   road preparation. No reduced rebuild, cached output or smoke sample.
3. Use fresh processes and sample wall time, RSS and spill bytes externally through
   writes/finalisation (default 1 second). Retain private logs, sampled peaks and
   OS high-water RSS where available. State units/method, allocated versus logical
   disk bytes and sampled versus continuous peaks. Separate harness/call timing
   and identify input reading, setup, writes, reporting and teardown boundaries.
4. Use existing hooks or a minimal external harness, not production algorithm
   changes, for the [preparation stage breakdown](../../docs/site/performance.md#canonical-preparation-runtime):
   input coercion; foundational cleaning/IDs; term-frequency aggregation; adjacent
   distinguishing/TF application; inverted index; road catalogue/keys; each Parquet
   write; manifest finalisation; uninstrumented overhead. Record stage time/RSS/spill
   where available, otherwise `null` with a reason; overall measurements are mandatory.
5. Verify manifest, files/indexes and row counts, account for rejected rows and
   load the bundle in a new connection. Close connections and verify spill cleanup.
   Track long-running jobs through tool timeouts with completion receipts; never
   duplicate a job because a tool stopped waiting. Save failures and stop dependent work.

## 2. Activate The Newly Prepared Data

For the primary release only, after bundle verification update **only**
`UKAM_OS_CANONICAL_PREPARED` in private config using a JSON parser. Preserve other
settings and a private recovery copy; never print or commit config. Resolve
environment overrides, which take precedence, and verify
`benchmarking.settings.CANONICAL_PATH` in a fresh process. Leave the new path active
on success; restore the old value if activation validation fails. Skip this step
for historical baselines.

## 3. Matching And Accuracy

Use the [benchmark entrypoint](../../benchmarking/run_benchmarking.py) and existing
runner/persistence/metrics helpers, with `sample_mode=False`, `persist_results=True`
and a fresh process/connection per dataset. A minimal repo-runnable driver may apply
constraints/monitoring; do not create another matcher or change production defaults.
Record direct current-run timings, not deduplicated older persistence receipts.

Run `pooled_councils` first against the full national bundle with
`canonical_address_filter=None` and all implicit canonical filters disabled:
**no council or classification filter**, including no residential-only restriction.
Record actual pooled membership and member breakdowns where supported.

Then run these five datasets separately against that same national prepared folder:

| Dataset | Council | Expected GSS code | Classification |
| --- | --- | --- | --- |
| `hackney` | Hackney | `E09000012` | Residential (`R`) |
| `rhondda` | Rhondda Cynon Taf | `W06000016` | Residential (`R`) |
| `aberdeenshire` | Aberdeenshire | `S12000034` | Residential (`R`) |
| `mid_sussex` | Mid Sussex | `E07000228` | Commercial (`C`) |
| `gwynedd` | Gwynedd | `W06000002` | Residential (`R`) |

Verify codes against metadata/authoritative lookup, non-empty subsets and label
coverage. Pass each regional filter as `canonical_address_filter`, applying
`lowertierlocalauthoritygsscode = '<code>'` and
`substr(classificationcode, 1, 1) = '<classification>'` before matching. If metadata
is missing, use a read-only UPRN join and the supported prepared loading path;
never infer geography from address text or fall back to raw preparation. Preserve
entity/alias semantics and national TF/index context: no council rebuilds or local
TF recomputation. Keep config on the national folder; time filtering/initialisation
separately. Do not copy obsolete APIs/SQL from historical performance examples.

Use existing loaders, documenting any necessary local eligibility correction.
Retain genuinely missing postcodes; exclude missing input UPRN labels and labels
absent from the selected canonical. Record raw/eligible/excluded rows and reasons,
including missing-postcode counts before/after canonical-label filtering. Keep
wrong/unmatched observations in the denominator; do not add difficult-record or
duplicate-observation exclusions, strip supplied postcodes or run suppression variants.

Keep current stage order/model defaults across all runs. Report **MW 8, 10 and 12**,
retaining candidates down to MW 8 and fixed exact/peeled matches at each threshold.
Prefer scoring once with existing metrics helpers; label the scoring threshold
and any separately timed runs. A shared runtime is not three measured runs.
Aggregate accuracy sub-reasons into owning execution stages before reconciliation.
Record stage-only versus cumulative scope; use supported cumulative results or
`null` with a reason rather than inventing stage-only splits.

## 4. Optional Historical Comparison

Use comparable persisted baselines where available; run a historical installation
only when requested, with the same verification/measurement rules and a separate
prepared-output directory. Never activate it: preserve primary config byte-for-byte
and pass paths through child-process environment overrides.

Only with explicit user approval may historical matching reuse the current
canonical: record its producing version/commit/manifest, label the cross-version
canonical replay and omit historical preparation timings. For a failed historical
preparation, compare only completed steps; mark full runtime unavailable with no delta.
Link accepted baseline run IDs; exclude invalid/superseded runs. Report accuracy and
runtime deltas at the same operating points, with current-minus-older differences
calculated from unrounded data. Time source foundational precleaning directly or
use labelled approximate log intervals, never workflow minus matching-stage time.
Different canonical data, cohorts, dependencies, hardware or constraints prevent
a controlled timing claim. Historical documentation timings are context, not a gate.

## 5. Persist Two Release Reports And Verify

Keep exactly two generated files in `docs/runtime/<target-release>/`: a self-contained
aggregate-only `manifest.json` and `report.md` rendered from its values. Overwrite
that version's pair on reruns, touching no other versions; remove obsolete split
reports only after validating consolidation.

Start from [manifest.template.json](../../docs/runtime/manifest.template.json).
The shared manifest is a small summary, not a diagnostic log. Include only:

- `target_release`, installed `software_version`, and `status`.
- `precleaning.canonical_rows` and `precleaning.e2e_seconds`: externally monitored
  national preparation wall time including setup, writes and teardown, excluding
  the separately measured raw OS conversion.
- All six `datasets`, each with `eligible_rows`, `runtime_seconds` (benchmark
  workflow time, excluding accuracy reporting and persistence), and three
  `operating_points` for MW 8, 10 and 12.
- Each operating point: `{match_weight, precision, recall, f1, tp, fp, unmatched}`.
- Short `notes` only for material limitations, timing boundaries, cohort/filter
  context, version mismatches and the performance-documentation decision.

Use exact numbers, seconds and fractions in `[0,1]`; unavailable values are `null`
with a short explanation in `notes`. Do not copy MW 10 outcomes into MW 8 if the
run discarded candidates below MW 10. Mark those MW 8 results unavailable and the
run incomplete; a genuine MW 8 evaluation requires retained candidates down to 8.

Keep all other evidence from sections 1-4 privately: commits, hashes, fingerprints,
run IDs, timestamps, machine details, commands, file paths/names, resource samples,
stage breakdowns, source/exclusion audits and diagnostic artefacts. Do not copy
these into the shared pair, even as `private://` references or placeholders.
Do not add security exceptions or suppressions to accommodate generated reports.

Render a concise, human-readable `report.md` from the summary in this order:

1. Release heading, installed software version and status; state version mismatches.
2. National precleaning canonical count and E2E runtime.
3. One MW 10 summary table: dataset, eligible count, runtime, precision, recall,
   F1, TP, FP and unmatched.
4. One MW 8/10/12 table per dataset: precision, recall, F1, TP, FP and unmatched.
   Use readable headings in the order pooled councils, Hackney, Rhondda Cynon Taf,
   Aberdeenshire, Mid Sussex and Gwynedd.
5. Short notes and a relative link to `manifest.json`. No empty historical
   comparison sections, dense curves, PR-AUC reports or diagnostic dumps.

Display seconds to three decimals, percentages to two, and integer counts with
thousands separators; show `Unavailable` for nulls, never zero.
Use one final outcome per eligible record and `eligible_rows` as the denominator.
Reconcile `tp + fp + unmatched = denominator_rows`,
`precision = tp / (tp + fp)`, `recall = tp / denominator_rows`,
`f1 = 2 * precision * recall / (precision + recall)` and define zero-denominator
handling. Wrong predictions also lose a true match: unmatched is not FN. Use
existing accuracy/diagnostic tables without substituting curve denominators.

Verify all six runs, execution stages, constraints and resources against private
evidence. Parse the shared summary and reconcile every Markdown value and rounding
against it. Check the template's field set and exclude diagnostic metadata.
Run the repository's secret checker on both reports before proposing a commit;
if its tools are unavailable, install the pinned security tools or report the check
as blocked, never claim it passed. Focused checks must validate any new driver.
Apply user-supplied regression budgets; otherwise report regressions without invented
pass thresholds. Execution success is not an accuracy guarantee.

## 6. Final Performance-Documentation Update

After all runs/checks, record an assessment of preparation/stage runtime, RSS/spill,
label coverage, accuracy and unmatched counts against comparable persisted results,
including baseline IDs, comparability differences and anomalies. If preparation is
severely slower, matching is anomalous, required evidence is missing or conclusions
are uncertain, **leave performance.md unchanged** and ask the user to review.

Otherwise update [performance.md](../../docs/site/performance.md) as the final step:

- Replace full-country step 4 preparation timing and affected setup totals/footnotes;
  retain unmeasured download/install timings. Match timings to actual cohort sizes,
  never relabel another workload as 100,000 records.
- Replace the canonical preparation stage table, shares/cumulative times and total
  from this run. Refresh date, counts, hardware/versions/constraints/chunks,
  harness/call and spill/cleanup notes; preserve precision and complete breakdowns.
- Refresh Hackney/Mid Sussex MW 8/10/12 metrics, eligible counts and runtimes. Add or
  update short Aberdeenshire/Gwynedd council sections in the existing style, without
  scripts, postcode-less sections or essays. Label old curves as historical or
  replace them with three-point summaries; do not imply suppression examples ran.
- Use approximate whole-second council runtimes, retaining exact timing boundaries
  privately; if repeated, report the median and retain all repetitions privately. Reuse existing layout/
  filter patterns, briefly correct stale Mid Sussex raw-relation/local-TF claims,
  and add at most one manifest link. No unrelated edits or mixed-run measurements.

Validate the documentation diff, tables and rounding against JSON; record the
documentation decision with a brief reason in `notes`, keeping detailed review
evidence private. Finish with a compact dataset summary, report links,
activation/documentation status and passed/failed/incomplete conclusion.
Never claim an end-to-end pass with failed or
skipped required work, invalid metrics, missing overall measurements or failed checks.