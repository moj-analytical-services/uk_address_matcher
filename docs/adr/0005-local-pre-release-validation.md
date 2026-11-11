# 5. Local pre-release validation

Date: 2026-11-06

## Status

Accepted

## Context

Releases are infrequent. Running the full check on our local laptops is faster
and gives us a more consistent environment than shared CI runners. Dedicated
release machinery would have a cost to build and maintain.

We need a repeatable check of preparation, matching, runtime and accuracy before
each release, with reports we can compare over time.

## Decision

Run the [release E2E prompt](../../.github/prompts/release-e2e.prompt.md) locally
before releasing, rather than adding a formal continuous deployment step.
Existing CI checks still apply; this does not automatically publish a release.

The check covers:

1. Ask for the target release version (`v1.x.x`) and raw canonical data path.
2. Verify a fresh installation and prepare the full national canonical data,
   using 10 chunks and a 16GB DuckDB limit unless explicitly overridden. Measure
   total and stage runtime, peak RSS and spill usage.
3. Verify the prepared output and activate its path in the private benchmark config.
4. Run pooled councils against the full canonical, then Hackney, Rhondda,
   Aberdeenshire, Mid Sussex and Gwynedd against regional filters on that same
   prepared data. Retain national term frequencies and indexes.
5. Record precision, recall, F1, TP, FP and unmatched at MW 8, 10 and 12, alongside
   labelled stage timings and resources. Detailed curves are a separate exercise.
6. Review the results before updating the existing performance figures. Leave
   those figures unchanged if preparation is severely slow, matching looks wrong
   or the evidence is incomplete or inconclusive.

Save exactly two sanitised reports under `docs/runtime/<target-release>/`,
replacing that version's pair on a rerun: a self-contained `manifest.json` and a
readable `report.md` generated from its values. Start from
[manifest.template.json](../runtime/manifest.template.json). The JSON contains only
the release label, installed software version, status, national precleaning count
and E2E runtime, eligible counts and matching runtimes, MW 8/10/12 precision,
recall, F1, TP, FP and unmatched for all six datasets, and short limitation notes.
Keep exact values and consistent units. The Markdown presents these results as
concise tables, not a diagnostic log.

Keep detailed and failed-run evidence privately, including commits, fingerprints,
timestamps, hardware, stage timings, resource samples and artefact references.
Do not include these, addresses, credentials or private paths in shared reports.
Run the repository secret checker before proposing report commits; do not add
scanner exceptions for generated metadata.

## Consequences

We get a lightweight release check and a versioned performance history without
maintaining dedicated runners. Someone must run and review it before releasing.
Laptop or dataset changes can affect comparisons, so reports retain their context.
The three operating points are not a full precision/recall curve.

We can revisit dedicated machinery if releases become frequent or the local
checks become unreliable or too costly to run.