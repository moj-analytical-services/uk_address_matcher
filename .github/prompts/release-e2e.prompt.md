---
description: "Validate a fresh release end-to-end: national preparation, filtered council benchmarks, laptop runtime manifest, and gated performance-documentation updates."
agent: agent
argument-hint: "Target release v1.x.x, source ref or wheel, raw canonical path, optional DuckDB constraints"
---

# Release end-to-end validation

Execute this workflow, not just a plan. Validate a fresh installation of the
requested release from raw canonical data through persisted benchmark results.
Do not publish a release, commit, push, or deploy cloud infrastructure.

Follow the [repository workflow](../instructions/repo-workflow.instructions.md),
[benchmark instructions](../instructions/benchmark-experiments.instructions.md),
[data safety instructions](../instructions/data-handling.instructions.md), and
[benchmark skill](../skills/benchmark-experiment/SKILL.md). Before implementing
any harness, read the applicable Python, SQL, and testing instructions.
The rationale is recorded in
[Local pre-release validation](../../docs/adr/0005-local-pre-release-validation.md).

## Inputs And Defaults

- Raw canonical data: use the user's supplied path; if absent, ask for it before
  starting. Accept the formats supported by `prepare_canonical_folder`; inspect
  the schema, not address records. For a DuckDB database, ask for the source
  table/projection if ambiguous and attach it read-only.
- Target release: **ask the user for the next release version before creating
  reports or starting a run**, unless already supplied. Require an explicit
  `v1.x.x` label such as `v1.6.0`; never infer it from the date, current package
  version or Git ref. Record the source ref/wheel and installed package version
  separately; confirm which source to test if ambiguous.
- Preparation chunks: **10**, unless the user overrides this.
- DuckDB memory limit: **`16GB`**, unless the user overrides this. This is not a
  hard process-RSS limit. Apply and read back constraints on every connection,
  including benchmark connections created by `setup_connection()`.
- Choose a fresh, release-specific output directory with sufficient free space.
  Never overwrite raw inputs or an existing prepared folder. Ask for an output
  location when safe capacity cannot be established. Use a dedicated spill
  directory per job; record threads, spill cap, output chunks, and all resolved
  preparation options. Do not silently increase memory or reduce dataset size.
- Resolve the five labelled dataset paths from private config/environment. Ask
  for any missing paths; never silently skip a dataset or request credentials in chat.

## 1. Fresh Installation And National Preparation

1. Record UTC start time, release/package version, Git commit and dirty state,
   Python/DuckDB/dependency versions, OS, laptop model, CPU, logical cores, and
   physical RAM. Never record serial numbers, credentials, or private config contents.
2. Create a clean isolated environment using `uv`. For a checkout release, build
   and install its wheel rather than relying solely on the existing editable
   environment; for a published release, install that exact version. Verify the
   imported `uk_address_matcher` version and module path before running. Repository
   benchmark helpers may come from this repo, but must not shadow the installed
   package. Record the exact install and execution commands and dependency lock.
3. Run `prepare_canonical_folder` on the **complete raw national input**, with
   `num_of_chunks=10` and `memory_limit='16GB'` by default. Use the supported
   preparation API and preserve its default indexes/features/road preparation;
   do not substitute a reduced-canonical rebuild, cached output, or smoke sample.
4. Monitor wall time, process RSS, and spill-directory bytes throughout the run,
   including writes and finalisation. Sample externally at a documented interval
   (default 1 second) and retain a private resource log. Record sampled peaks plus
   the OS process high-water RSS where available, with explicit units and method.
   Distinguish allocated disk space from logical bytes and sampled spill maxima
   from a continuous peak. Run jobs in fresh processes so RSS peaks are attributable.
5. Capture stage boundaries using existing progress/timing hooks or a minimal
   external harness. Use the stage breakdown in
   [Canonical preparation runtime](../../docs/site/performance.md#canonical-preparation-runtime)
   as the reference: input coercion; foundational cleaning/ID assignment;
   term-frequency aggregation; adjacent distinguishing/TF application;
   inverted-index derivation; road catalogue; road keys; each Parquet write;
   manifest finalisation; uninstrumented overhead. Record elapsed time and sampled
   peak RSS/spill per available stage. Do not change production algorithms just
   for instrumentation. If a boundary is unavailable, record `null` and why;
   overall runtime and sampled RSS/spill are mandatory.
6. Separate harness wall time from preparation-call time; state whether input
   reading, connection setup, writes, reporting, and teardown are included. Verify
   the generated canonical manifest, expected files/indexes and row counts, then
   load the output in a new connection. Account for any rejected input rows.
7. Ensure completion and connection closure; verify spill cleanup. Long-running
   jobs must survive tool timeouts via a tracked process and completion receipt.
   Do not launch a duplicate job because a tool stopped waiting. On failure, save
   measurements/errors and stop dependent steps; do not claim success.

## 2. Activate The Newly Prepared Data

Only after successful verification, update **only** `UKAM_OS_CANONICAL_PREPARED`
in `benchmarking/.config.json` to the new national prepared folder, using a JSON
parser and preserving other settings. Keep a private recovery copy of the old
value; do not print the config or add it to Git. Leave the new value active on
success; restore the previous value if activation validation fails.

Environment variables take precedence over this config. Resolve conflicting
overrides explicitly and start a fresh process to verify that
`benchmarking.settings.CANONICAL_PATH` points to the new folder. Never silently
benchmark against an old environment-supplied path.

## 3. Pooled Councils Against The Full Canonical

Use [the benchmark entrypoint](../../benchmarking/run_benchmarking.py) and its
existing runner/persistence helpers. Set `SELECTED_DATASETS=['pooled_councils']`,
`sample_mode=False`, `persist_results=True`, and
`canonical_address_filter=None`; disable any constants/config filter that would
restrict the canonical. **Full canonical means no council or classification
filter**, not just national residential addresses.

Keep the entrypoint's current stage order and resolved model defaults. Report
accuracy at **MW 8, 10 and 12**, recording effective thresholds and the complete
resolved stage definition. Retain sufficient scored candidates to evaluate MW 8;
do not derive lower-threshold results from output already pruned above MW 8.
Prefer scoring once and evaluating the three operating points through existing
metrics helpers. Include fixed exact/peeled matches at each point. If separate
matching runs are needed, label their thresholds and timings separately; never
present a shared runtime as three measured runs. Do not change production defaults.
Prefer a small repo-runnable driver reusing the runner if needed to
apply constraints/monitoring without permanent changes to benchmark defaults;
do not invent a second matching or metrics implementation.

Use a release-specific results root and fresh process/connection for each run.
Record current-run timings directly, not an older deduplicated persistence receipt.
Persist pooled totals and member-council breakdowns where supported. Record the
resolved pooled membership rather than assuming Mid Sussex belongs to it.

## 4. Five Regionally Filtered Canonical Runs

Run `hackney`, `rhondda`, `aberdeenshire`, `mid_sussex`, and `gwynedd` separately,
unsampled, reusing this release's newly prepared national folder. Follow the
regional filtering SQL patterns in
[Performance overview](../../docs/site/performance.md), not obsolete API calls
or malformed SQL from its historical snippets. Do not run postcode-suppressed variants.

| Dataset | Council | Expected GSS code | Classification |
| --- | --- | --- | --- |
| `hackney` | Hackney | `E09000012` | Residential (`R`) |
| `rhondda` | Rhondda Cynon Taf | `W06000016` | Residential (`R`) |
| `aberdeenshire` | Aberdeenshire | `S12000034` | Residential (`R`) |
| `mid_sussex` | Mid Sussex | `E07000228` | Commercial (`C`) |
| `gwynedd` | Gwynedd | `W06000002` | Residential (`R`) |

Verify each code against available geographic metadata or an authoritative
lookup. Validate non-empty subsets and label coverage before matching. Apply
`lowertierlocalauthoritygsscode = '<code>'` and
`substr(classificationcode, 1, 1) = '<classification>'` before matching.
If those fields were not retained, use a read-only join to source metadata by
UPRN; do not guess geography from address text or copy the malformed historical SQL.

Pass the same national prepared folder to the existing runner with a different
`canonical_address_filter` for each council. Do not rebuild council folders or
recompute local term frequencies/indexes. Preserve entity/alias semantics and
keep the private config pointing to the national folder. These runs retain
national TF/index context: record this origin explicitly and do not describe
them as equivalent to the historical Mid Sussex raw-relation/local-TF setup.
If a metadata join is necessary, materialise only the required filtered prepared
view using the supported loading path; do not silently fall back to raw preparation.

Use existing labelled-data loaders. Record raw rows, eligible labelled rows,
missing labels, labels absent from the selected canonical, and exclusions with
reasons. Preserve current loader behaviour unless a documented local eligibility
filter is needed; never hide coverage losses by reporting only retained rows.
Record regional filtering/initialisation separately from matching time/resources.
Use the same stage/model settings and MW 8, 10 and 12 reporting as the pooled run, not obsolete
settings from the historical examples.

## 5. Persist The Release Manifest And Verify

Write one compact aggregate-only JSON manifest at
`docs/runtime/<target-release>/manifest.json`, for example
`docs/runtime/v1.6.0/manifest.json`. Keep one report set per release version:
overwrite that version's existing generated reports on a rerun, removing obsolete
stage artefacts so results from different runs are not mixed. Do not touch other
versions. Keep timestamps and run IDs in the report metadata, not subdirectories.
Create it at the start and update it after each completed step so failed runs
also have evidence. This sanitised laptop record must live in the repository,
not only in ignored benchmark results; keep detailed/private artefacts under the
release-specific results root. Use a structured JSON writer. Include:

- Schema version, user-confirmed target release, Git provenance/dirty state, UTC start/end,
  overall status (`passed`, `failed`, or `incomplete`), errors and limitations.
- Laptop/OS/CPU/core/RAM specs, package/Python/DuckDB versions, install provenance,
  exact commands or runnable driver path, resolved DuckDB constraints and units.
- National canonical manifest references, regional row counts, filters, options,
  preparation timings, per-stage measurements, resource sampling method/interval,
  overall peak RSS/spill and cleanup status. Redact private input paths in shareable
  output; keep full commands/resource logs private when they contain such paths.
- One run record for the pooled dataset and each of the five regional datasets:
  run ID/artefact paths, canonical scope, input/eligible/excluded counts, resolved
  stages/model/thresholds, total/initialisation/matching/reporting runtimes with
  boundaries, RSS/spill peaks, and overall precision, recall, F1, TP, FP, unmatched.
- An ordered stage array keyed by **execution ordinal and stage name**, including
  runtime, input/output counts, resource measurements when available, and accuracy
  metrics. State whether each accuracy record is stage-only or cumulative. Keep
  repeated stage names distinct; if the existing API lacks stage-only accuracy,
  use supported cumulative results or mark unavailable with a reason, not a guessed split.

Keep stage artefacts clearly labelled under the same run directory:

- `preparation/stages.json`: ordered preparation-stage timings and resources.
- `datasets/<scope>-<dataset>/stage-<ordinal>-<stage-name>.json`: one aggregate
  report per executed matching stage, including repeated stages separately.
- `datasets/<scope>-<dataset>/overall.json`: overall timings and the three
  operating-point summaries for that dataset. Use `full` and `regional` scope labels.

Each JSON report must carry `schema_version`, `target_release`, `run_id`,
`dataset`, `canonical_scope`, `stage_ordinal`, `stage_name`, `metric_scope`,
`elapsed_seconds`, `peak_rss_bytes`, `peak_spill_bytes` and an `operating_points`
array where accuracy applies. Use `null` with a reason for inapplicable fields.
Each operating point carries `match_weight` (8, 10 or 12) and the metric fields
below; stage reports must distinguish stage-only from cumulative results. Link
all reports from the manifest using relative paths. Keep names/units consistent
across releases so these JSON files can drive runtime and precision/recall charts
without parsing console logs. Retain exact numbers, not display-rounded strings.
Do not generate dense curves or PR-AUC reports for this release check; more
detailed curves can be run separately. Any automatically persisted detailed
benchmark outputs remain outside these compact repo reports.

Use metric records shaped as
`{match_weight, scope, denominator_rows, matched, tp, fp, unmatched, precision, recall, f1}`.
Store precision/recall/F1 as fractions in `[0,1]`; label displayed percentages.
Document label eligibility and denominators. For a cohort with one final outcome
per eligible record, reconcile `tp + fp + unmatched = denominator_rows`,
`precision = tp / (tp + fp)`, `recall = tp / denominator_rows`, and
`f1 = 2 * precision * recall / (precision + recall)`. Define zero-denominator
handling explicitly. Unmatched is not the same as FN: wrong predictions also
lose a true match. Use existing accuracy/diagnostic tables; do not silently mix
their curve denominators with headline denominators.

Parse the saved JSON and verify all six required runs, every executed stage,
metric reconciliations, existing artefact references, newly prepared provenance,
effective connection constraints, resource evidence, target release labels and
the MW 8/10/12 records. Keep raw addresses,
row-level predictions and secrets out of the shareable manifest. Validate any
new driver with focused checks and leave its rerun command usable in this repo.

If comparable persisted baselines exist, link their run IDs, report accuracy and
runtime deltas at the same three operating points. Different
canonical data, cohorts, hardware, or constraints invalidate a controlled timing
claim. Never compare these runs to historical documentation timings as a release gate.
Apply user-supplied regression budgets; otherwise report regressions without
inventing numerical pass thresholds. Execution success is not an accuracy guarantee.

## 6. Final Performance-Documentation Update

Only after all runs and manifest checks finish, assess preparation runtime,
stage timings, RSS/spill, label coverage, accuracy and unmatched counts against
comparable persisted results. Document the assessment in the manifest, including
baseline IDs, differences in hardware/constraints/cohorts, anomalies and the
decision. Historical documentation figures are context, not a controlled baseline.
If preparation is severely slower, matching looks anomalous, a required run or
measurement is missing, or the evidence is inconclusive, **leave performance.md
unchanged**, retain the manifest and ask the user to review before publishing.
Do not silently bless suspicious results or choose an arbitrary regression budget.

If the results are credible and the gate passes, update
[performance.md](../../docs/site/performance.md) as the final step:

- In the top timings table, replace the full-country step 4 preparation runtime
  with this run's measured end-to-end preparation time and recalculate affected
  setup totals/footnotes. Update matching timings only for the stated workload:
  do not relabel a differently sized council cohort as 100,000 records. Keep
  unmeasured download/install figures unchanged rather than inventing replacements.
- In `Canonical preparation runtime` / `Show full canonical preparation runtime`,
  replace the existing stage seconds, shares, cumulative seconds and total from
  the same run. Update the existing date, row counts, laptop specs, versions,
  constraints, chunk counts, harness/call timing and spill/cleanup notes in place.
  Reconcile the table with the manifest; do not publish an incomplete stage
  breakdown or combine old and new measurements. Preserve precise values here.
- Refresh Hackney Council and Mid Sussex headline precision/recall/F1 figures,
  labelled cohort sizes and runtimes from their new regionally filtered runs.
  Use MW 8, 10 and 12 from the saved reports. If updating a chart, label it as
  three operating points, not a full curve. Do not present historical curves as
  evidence for new figures; keep them explicitly historical or replace them
  with the compact summaries. Detailed curve generation is a separate task.
- Add short `### Aberdeenshire Council data` and `### Gwynedd Council data`
  sections with the same summary metrics, cohort sizes and approximate runtime,
  providing Scottish and Welsh data points. No long scripts, new postcode-less
  sections, or explanatory essays. Do not refresh existing historical
  postcode-suppressed examples as though they were rerun.
- Use rough whole-second runtimes for council summaries, for example
  `~6 seconds` when supported by the measurement, not separate `5.8 seconds total`
  and `5.6 seconds matching` claims. Keep exact values and timing boundaries in
  the manifest; do not force the example value onto different results. If repeated,
  use the median and retain all repetitions in the manifest.
- Reuse existing SQL/filter patterns and layout. Correct stale Mid Sussex
  raw-relation/local-TF claims and affected snippets briefly to reflect the
  prepared-folder regional filter. Otherwise replace existing figures/notes in
  place, without extra paragraphs or unrelated prose cleanup. Add at most one
  concise link to the repo-stored manifest for provenance.

Validate updated tables, rounding, operating-point consistency and the documentation
diff against the manifest. Record `documentation_update` as `updated` or
`withheld`, with its reason and paths, in the final JSON. No documentation timing
or accuracy figures should change before this gate.

Finish with a compact dataset/stage summary, manifest and driver links, config
activation and documentation-update status, and a clear passed/failed/incomplete
conclusion. Do not claim
an end-to-end pass with a failed/skipped required run, invalid output, missing
mandatory overall measurements, or failed manifest checks.