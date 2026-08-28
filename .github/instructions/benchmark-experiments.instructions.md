---
applyTo:
  - "benchmarking/**/*.py"
  - "benchmarking/**/*.md"
  - "benchmarking/**/*.json"
  - "scripts/reduced_canonical.py"
  - "uk_address_matcher/post_linkage/match_result/**/*.py"
  - "uk_address_matcher/data/splink_model.json"
  - "tests/test_benchmark_run_persistence.py"
---

# Benchmark experiment and audit workflow

- Use this instruction file for:
  - subtype or threshold experiments,
  - benchmark reruns via `benchmarking/run_benchmarking.py`,
  - reduced-canonical rebuilds,
  - persisted run comparisons,
  - record-level audit artefacts.

## Primary control surfaces

- Treat `benchmarking/run_benchmarking.py` as the normal experiment entrypoint.
- Treat `scripts/reduced_canonical.py` as the normal fast rebuild path for the reduced canonical dataset.
- Prefer persisted run artefacts under `benchmarking/results/<dataset>/<date>/<run_id>/` over transient console summaries.

## Reduced canonical rebuild workflow

- If an experiment changes upstream cleaned features or canonical-side fields used by matching, rebuild the reduced canonical before benchmarking.
- Prefer `uv run python scripts/reduced_canonical.py` rather than inventing a bespoke rebuild script.
- Keep the reduced canonical output path stable unless the user explicitly asks for a different location.
- Report:
  - source canonical path,
  - output folder,
  - filtered row count,
  - whether the rebuild was actually required for the requested experiment.

## Benchmark run workflow

- Use `benchmarking/run_benchmarking.py` for persisted experiments unless the user explicitly asks for a one-off ad hoc script.
- When editing that file for an experiment, keep the diff surgical:
  - update `SELECTED_DATASETS`,
  - update `STAGES`,
  - update `COMPARISON_BASELINE_RUN_ID` if needed,
  - avoid unrelated edits.
- Prefer `COMPARISON_BASELINE_RUN_ID = "latest"` unless the user gives a specific run ID to compare against.
- Run benchmark scripts with `uv`, for example `uv run python benchmarking/run_benchmarking.py`.
- Persist results and point the user to the run directory, not just console output.

## Persisted comparison expectations

- Comparison artefacts should capture the resolved stage definition, including resolved default Splink settings, not `settings: null`.
- If comparison provenance is missing, fix persistence/reporting at the root rather than explaining around the gap.
- When comparing runs, distinguish:
  - model change at the old threshold,
  - threshold change under the current model,
  - any canonical-data change.

## Experiment record expectations

- Every benchmark experiment should leave behind a concise text-and-numbers record of:
  - what question the experiment was trying to answer,
  - exactly what changed,
  - exactly what did not change,
  - exactly which run IDs and artefact paths were produced,
  - the headline numerical outcome,
  - how to interpret the result.
- Do not leave the change description implicit. Record the exact operational diff, for example:
  - dataset selection,
  - reduced-canonical rebuild yes/no,
  - changed files,
  - stage list,
  - threshold values,
  - changed Splink comparison JSON block,
  - comparison baseline run ID.
- Prefer recording the exact changed JSON block or stage-definition snapshot rather than paraphrasing it.
- If the model or threshold changed, record both the previous and current values in text and in a small table.

## Experiment record template

- Use a compact section structure like this in experiment summaries and audit preambles:
  - `Question`: what hypothesis was tested.
  - `Runs compared`: baseline run ID, current run ID, dataset, date.
  - `Exact changes made`: bullet list of precise changes.
  - `No-change controls`: what was intentionally held constant.
  - `Headline metrics`: matched rows, correct matches, wrong matches, precision, recall, f1.
  - `Effect split`: model effect at old threshold, threshold effect under current model, canonical-data effect if relevant.
  - `Primary chart read`: what the overlay precision-recall chart says.
  - `Artefacts`: markdown path, JSON path, comparison summary path, overlay chart path.

## Primary chart expectations

- Treat `uk_address_matcher/analysis/overlay_precision_recall_charts.py` as the primary comparison chart for benchmark experiments.
- If persisted comparison charts are generated, the overlay precision-recall chart should be the first chart referenced in summaries.
- When writing up results, explicitly interpret the overlay chart in words, not just by linking it.
- The minimum chart interpretation should state:
  - whether the comparison curve is generally above or below the baseline,
  - whether gains are concentrated at particular recall bands,
  - whether the comparison increases false positives materially,
  - whether the lower panel shows mostly positive or negative reductions in false
    positives.
- If the overlay chart contradicts a simple headline-metric story, say so explicitly.
- Include the overlay chart path in the experiment artefact list whenever it exists.

## Record-level audit workflow

- For row-level explanation, use `MatchResult` outputs and raw Splink predictions rather than relying only on aggregate tables.
- Prefer this sequence:
  1. confirm the persisted run IDs and aggregate metrics,
  2. reconstruct any required baseline variant,
  3. rerun targeted variants with `retain_intermediate_calculation_columns=True`,
  4. extract `MatchResult._splink_predictions()`,
  5. materialise a markdown audit plus a JSON companion in the current run directory.
- Use separate DuckDB connections per variant run to avoid temporary-table reuse issues.
- If the historical baseline canonical no longer matches the current code path, reconstruct the baseline with the current reduced canonical and the older model settings instead of forcing stale artefacts.
- Persist both:
  - a human-readable markdown report,
  - a machine-readable JSON companion.

## Splink prediction handling

- Use `retain_intermediate_calculation_columns=True` when the user asks where the weight came from.
- Read contribution columns from `_splink_predictions()` such as:
  - `match_weight`,
  - `match_probability`,
  - `bf_address_subtype_code`,
  - `bf_flat_identity`,
  - `bf_clean_full_address`,
  - `bf_address_without_numbers`,
  - `bf_postcode`,
  - relevant `gamma_*` and `bf_*` columns.
- Verify left/right orientation before writing conclusions. In this workflow, canonical candidates may not be on the side you first expect.
- Deduplicate candidate rows before presenting them in markdown.

## Human-readable audit formatting

- Optimise the markdown for manual review, not just data exhaust.
- Start each sample section with the original source address in the heading.
- For each reviewed row, show the comparison in this order:
  - messy input,
  - expected canonical,
  - current predicted canonical when applicable.
- Prefer short, explicit tables over raw JSON dumps in the main flow.
- Include concise weight summaries that answer:
  - what happened under baseline,
  - what happened under the current model at the old threshold,
  - what happened under the current threshold,
  - subtype BF on the expected and predicted candidate.
- Include a quick-review table near the top for all newly-wrong rows.
- Keep the markdown elegant:
  - deduplicate candidate lists,
  - avoid repeating identical rows three times,
  - avoid burying the original address below long factor dumps,
  - keep the JSON companion as the place for full structured payloads.

## Wrong-match review expectations

- When auditing newly-wrong rows, provide an opinion field for each row using categories such as:
  - `Likely genuinely wrong`,
  - `Likely source mislabel`,
  - `Possibly source mislabel`,
  - `Unclear`.
- Base that opinion on the messy row, labelled canonical row, predicted canonical row, and the extracted feature/weight evidence.
- Treat the opinion as audit guidance, not ground truth.

## Report minimums

- Always include:
  - baseline and current run IDs,
  - headline summary table,
  - transition matrix,
  - exact subtype comparison weights when subtype work is involved,
  - exact change log for the experiment,
  - primary chart interpretation using the overlay precision-recall chart,
  - quick-review table for newly-wrong rows,
  - file paths for the markdown and JSON outputs.
- If a user asks for “why did this move so much?”, explicitly separate:
  - the feature-change effect,
  - the threshold effect.

## Validation

- Validate any new persistence/reporting code with targeted tests where possible.
- If an existing test module is broken for unrelated reasons, report the blocker precisely rather than silently skipping validation.
- For experiment-only artefact generation, verify the written markdown and JSON directly after generation.