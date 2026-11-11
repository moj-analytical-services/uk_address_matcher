# Conwy: repeated external IDs and row identity

**Status:** the row-identity defects are fixed and covered by regression tests.
The post-fix Conwy run preserves every input row, but its all-input accuracy
remains low. A supported-label-only precision figure reported earlier is not
comparable to the plotted precision-recall curve; the metric denominators and
the source-label caveat are documented below.

## What Was Happening

Conwy contained 54,902 address rows but only 18,763 distinct external
`unique_id` values. That is valid input: external identifiers may repeat;
`ukam_address_id` identifies each individual address row.

Several operations incorrectly treated the external identifier as a row key:

- Inverted-index lookup combined candidate evidence from different input rows
  sharing an ID.
- Road enrichment joined multiple input rows to multiple road-key rows with
  the same external ID. A diagnostic join expanded 54,902 inputs to about
  **799 million rows**, causing excessive memory use, spill and runtime.
- Candidate shortlisting, reranking and best-match selection shared limits
  across repeated IDs, discarding matches belonging to other input rows.

Smaller cleaning chunks reduced individual workloads but did not correct
these joins or rankings. Deduplicating the input would also have lost valid rows.

## What Changed

Input-side evidence, enrichment joins and candidate rankings now use
`ukam_address_id`. External IDs are preserved in the output, and canonical
candidate deduplication retains its separate purpose.

A separate performance fix also prevents legacy prepared canonical data
without `local_key_index` from triggering an unexpected national rebuild of
that feature during matching; it receives neutral local-key evidence instead.

## Validation

- A paired CSV regression matches the same 5,000 addresses with distinct IDs
  and with one ID repeated throughout. Both return all 5,000 correct matches
  with distinct internal IDs, taking approximately 1.3s and 1.1s respectively.
  A generous fivefold slowdown guard catches substantial runtime regressions.
- The matcher and post-linkage regression checks passed: 96 tests.
- Fresh preparation of all 71,438,939 national records completed in **8m 42s**.
- Full benchmarks against that fresh canonical data, without model tuning:

| Dataset | Precision | Recall | Match runtime |
| --- | ---: | ---: | ---: |
| Hackney | 99.57% | 99.15% | 47.6s |
| Pooled councils | 99.02% | 98.09% | 79.9s |

Both benchmark outputs preserved exactly one row per internal ID. Hackney's
standalone and pooled results agreed exactly. Their shared run ID is
`2a1819c373949950`; recall above is correct matches divided by all input rows.

## Conwy Rerun

Run `afb5c1ea7746645a` used the freshly prepared 71.4-million-row canonical
data, the same stages and threshold as Hackney, and the same residential,
no-garage/car-park canonical filter. It completed in **36.95s** and returned
exactly 54,902 rows with 54,902 distinct internal IDs.

| Run | Matched | Correct | Wrong | Unmatched | Precision | Recall | Runtime |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Before final ranking fixes (`d69e484308c0a4ab`) | 42,329 | 17,576 | 24,753 | 12,573 | 41.52% | 32.01% | 63.97s |
| After fixes (`afb5c1ea7746645a`) | 53,884 | 17,578 | 36,306 | 1,018 | 32.62% | 32.02% | 36.95s |

The exact stage's results did not change: 23,552 of its 32,166 labelled
matches were still counted wrong. Splink accepted 11,555 more rows after the
ranking fix, but added only two correct matches. This prompted a row-level
aggregate check of whether each expected UPRN exists in the canonical data.

That check found **36,234 rows across 110 distinct UPRNs** whose labels are
absent from both the previous and freshly prepared canonical datasets. The
largest repeated external-ID group has 28,928 rows and its label is absent.
A further **279 rows** have labels in the full canonical data but are removed
by the configured canonical filter. No raw address or UPRN values are included
in the audit.

Of the 36,306 matches scored wrong, **35,934 (98.98%)** are on rows whose
expected label is absent from canonical or excluded by the filter. The current
scoring treats these labels as negatives, so any returned candidate counts as
a false positive. That is only valid if those source labels genuinely mean
there is no match; a placeholder or canonical version/coverage problem would
make them unscorable instead.

For the 18,389 rows whose labels are present after filtering, the subset-only
diagnostic is:

| Input rows with available labels | Matched | Correct | Wrong | Unmatched | Subset precision | Subset recall |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 18,389 | 17,950 | 17,578 | 372 | 439 | 97.93% | 95.59% |

The **97.93% subset precision** excludes the 35,934 accepted predictions on
unsupported-label rows from its denominator, so it is not the overall or PR
curve precision.

The PR curve uses the standard threshold-sweep definitions: its positive class
is rows whose expected label exists in the filtered canonical; accepted
predictions for labels outside that set count as false positives. At the
default threshold, its point is **32.62% precision** (17,578 / 53,884 matches)
and **95.59% recall** (17,578 / 18,389 positive rows). The curve traces other
thresholds too, so a point around 45% recall can coexist with that default
point. The run summary's `recall` field is a different measure: correct
matches divided by all inputs, 17,578 / 54,902 = **32.02%**. The Conwy run
directory has since been deleted, so the exact threshold for the remembered
45% point cannot be recovered.

The loader maps normalised source `uprn` values to both `unique_id` and
`ukam_label`, retaining non-null values. The aggregate audit does not contain
the label values, so it cannot establish whether the 28,928-row repeated ID is
a valid UPRN or a source placeholder. Validate that with the source definition
before changing ingestion; do not deduplicate the rows.

For a benchmark restricted to labels in the current filtered canonical, use
`FILTER_OUT_MISSING_UPRNS = True` in `benchmarking/constants.py` and select
the ordinary `conwy` dataset. The flag applies to every selected dataset,
preserves repeated rows, and records retained and excluded counts. Set it to
`False` to keep the full input population. Filtered metrics describe only
canonical-covered rows, not source-wide coverage; compare runs using the same
flag and canonical filter settings.

The Conwy run artifacts, including the rendered chart, are no longer present.
The aggregate-only label audit remains at
`.ai_scratch/conwy_output_label_diagnostics.json`.