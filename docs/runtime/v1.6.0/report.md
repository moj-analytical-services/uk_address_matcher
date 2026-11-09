# Release Validation: v1.6.0

**Matching: completed in both postcode modes using `1.6.0`.** Overall status: incomplete.

## National Precleaning

71,438,939 canonical rows; E2E runtime **570.039 seconds** (9 min 30 sec).
Reused `1.5.3` preparation measurements; precleaning was not rerun.

Sampled peak RSS **15.191 GB**; sampled peak allocated spill **43.130 GB**; final spill **0.000 GB**.

### Precleaning Stages

| Stage | Runtime (s) | Sampled peak RSS (GB) | Sampled peak spill (GB) |
| --- | ---: | ---: | ---: |
| Foundational cleaning and IDs | 115.546 | 4.459 | 0.000 |
| Term-frequency aggregation | 2.330 | 5.384 | 0.000 |
| Adjacent distinguishing and TF application | 105.442 | 12.970 | 0.000 |
| Inverted-index derivation | 39.600 | 13.441 | 0.000 |
| Road catalogue and keys | 165.740 | 15.191 | 4.998 |
| Inverted-index write | 7.465 | 8.183 | 4.998 |
| Canonical-address write | 116.369 | 12.191 | 43.130 |
| Manifest finalisation | 1.577 | 7.691 | 38.841 |
| Other preparation work | 12.530 | Unavailable | Unavailable |

## Resource Usage

Pooled councils against the full national canonical.

| Mode | Sampled peak RSS (GB) | Sampled peak spill (GB) |
| --- | ---: | ---: |
| Supplied postcodes | 13.595 | 4.033 |
| Postcode-less | 14.981 | 14.541 |

Both runs finished with zero spill remaining.

## Supplied Postcodes

### Matching Summary At MW 10

| Dataset | Eligible | Runtime (s) | Precision | Recall | F1 | TP | FP | Unmatched |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Pooled councils | 348,284 | 71.986 | 98.91% | 98.07% | 98.49% | 341,568 | 3,768 | 2,948 |
| Hackney | 113,837 | 22.038 | 99.72% | 99.52% | 99.62% | 113,294 | 320 | 223 |
| Rhondda Cynon Taf | 111,247 | 21.235 | 99.13% | 97.73% | 98.43% | 108,724 | 954 | 1,569 |
| Aberdeenshire | 122,301 | 15.179 | 99.17% | 98.44% | 98.81% | 120,392 | 1,003 | 906 |
| Mid Sussex | 3,717 | 7.731 | 96.54% | 87.84% | 91.98% | 3,265 | 117 | 335 |
| Gwynedd | 62,103 | 14.441 | 97.95% | 94.45% | 96.17% | 58,655 | 1,226 | 2,222 |

### Matching Stage Runtimes

| Dataset | Exact (s) | Peeled (s) | Splink (s) | Other pipeline work (s) |
| --- | ---: | ---: | ---: | ---: |
| Pooled councils | 1.163 | 0.653 | 57.167 | 11.568 |
| Hackney | 0.451 | 0.187 | 14.085 | 6.573 |
| Rhondda Cynon Taf | 0.282 | 0.392 | 13.271 | 6.648 |
| Aberdeenshire | 0.474 | 0.175 | 8.133 | 5.800 |
| Mid Sussex | 0.156 | 0.088 | 3.399 | 3.356 |
| Gwynedd | 0.258 | 0.273 | 8.230 | 4.928 |

### MW 8, 10 And 12 Results

#### Pooled councils

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | 98.85% | 98.21% | 98.53% | 342,039 | 3,993 | 2,252 |
| 10 | 98.91% | 98.07% | 98.49% | 341,568 | 3,768 | 2,948 |
| 12 | 98.99% | 97.83% | 98.41% | 340,742 | 3,486 | 4,056 |

#### Hackney

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | 99.70% | 99.60% | 99.65% | 113,378 | 340 | 119 |
| 10 | 99.72% | 99.52% | 99.62% | 113,294 | 320 | 223 |
| 12 | 99.73% | 99.45% | 99.59% | 113,207 | 302 | 328 |

#### Rhondda Cynon Taf

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | 99.09% | 97.88% | 98.48% | 108,886 | 998 | 1,363 |
| 10 | 99.13% | 97.73% | 98.43% | 108,724 | 954 | 1,569 |
| 12 | 99.23% | 97.48% | 98.35% | 108,445 | 837 | 1,965 |

#### Aberdeenshire

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | 99.11% | 98.54% | 98.82% | 120,514 | 1,086 | 701 |
| 10 | 99.17% | 98.44% | 98.81% | 120,392 | 1,003 | 906 |
| 12 | 99.24% | 98.30% | 98.76% | 120,219 | 926 | 1,156 |

#### Mid Sussex

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | 96.30% | 88.81% | 92.40% | 3,301 | 127 | 289 |
| 10 | 96.54% | 87.84% | 91.98% | 3,265 | 117 | 335 |
| 12 | 96.89% | 87.03% | 91.70% | 3,235 | 104 | 378 |

#### Gwynedd

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | 97.85% | 94.68% | 96.24% | 58,800 | 1,295 | 2,008 |
| 10 | 97.95% | 94.45% | 96.17% | 58,655 | 1,226 | 2,222 |
| 12 | 98.06% | 94.14% | 96.06% | 58,462 | 1,156 | 2,485 |

## Postcode-less Inputs

### Matching Summary At MW 10

| Dataset | Eligible | Runtime (s) | Precision | Recall | F1 | TP | FP | Unmatched |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Pooled councils | 348,284 | 94.735 | 96.99% | 79.47% | 87.36% | 276,787 | 8,595 | 62,902 |
| Hackney | 113,837 | 22.299 | 99.58% | 86.41% | 92.53% | 98,362 | 410 | 15,065 |
| Rhondda Cynon Taf | 111,247 | 21.792 | 95.81% | 72.28% | 82.40% | 80,409 | 3,514 | 27,324 |
| Aberdeenshire | 122,301 | 22.475 | 99.18% | 86.77% | 92.56% | 106,123 | 881 | 15,297 |
| Mid Sussex | 3,717 | 6.607 | 97.69% | 74.04% | 84.24% | 2,752 | 65 | 900 |
| Gwynedd | 62,103 | 16.138 | 97.60% | 74.30% | 84.37% | 46,143 | 1,137 | 14,823 |

### Matching Stage Runtimes

| Dataset | Exact (s) | Peeled (s) | Splink (s) | Other pipeline work (s) |
| --- | ---: | ---: | ---: | ---: |
| Pooled councils | 0.223 | 0.206 | 81.171 | 11.677 |
| Hackney | 0.232 | 0.175 | 15.253 | 5.843 |
| Rhondda Cynon Taf | 0.215 | 0.183 | 14.738 | 5.973 |
| Aberdeenshire | 0.202 | 0.190 | 16.076 | 5.349 |
| Mid Sussex | 0.136 | 0.071 | 3.009 | 2.676 |
| Gwynedd | 0.170 | 0.128 | 10.776 | 4.251 |

### MW 8, 10 And 12 Results

#### Pooled councils

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | 96.20% | 80.84% | 87.85% | 281,539 | 11,132 | 55,613 |
| 10 | 96.99% | 79.47% | 87.36% | 276,787 | 8,595 | 62,902 |
| 12 | 97.57% | 77.01% | 86.08% | 268,209 | 6,689 | 73,386 |

#### Hackney

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | 99.45% | 88.38% | 93.59% | 100,611 | 555 | 12,671 |
| 10 | 99.58% | 86.41% | 92.53% | 98,362 | 410 | 15,065 |
| 12 | 99.67% | 83.78% | 91.04% | 95,368 | 311 | 18,158 |

#### Rhondda Cynon Taf

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | 94.33% | 73.10% | 82.37% | 81,317 | 4,888 | 25,042 |
| 10 | 95.81% | 72.28% | 82.40% | 80,409 | 3,514 | 27,324 |
| 12 | 96.98% | 70.73% | 81.80% | 78,684 | 2,447 | 30,116 |

#### Aberdeenshire

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | 98.91% | 87.47% | 92.84% | 106,979 | 1,175 | 14,147 |
| 10 | 99.18% | 86.77% | 92.56% | 106,123 | 881 | 15,297 |
| 12 | 99.33% | 85.58% | 91.94% | 104,667 | 709 | 16,925 |

#### Mid Sussex

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | 97.37% | 75.76% | 85.22% | 2,816 | 76 | 825 |
| 10 | 97.69% | 74.04% | 84.24% | 2,752 | 65 | 900 |
| 12 | 98.02% | 71.78% | 82.87% | 2,668 | 54 | 995 |

#### Gwynedd

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | 96.95% | 75.79% | 85.08% | 47,070 | 1,482 | 13,551 |
| 10 | 97.60% | 74.30% | 84.37% | 46,143 | 1,137 | 14,823 |
| 12 | 98.01% | 72.22% | 83.16% | 44,851 | 909 | 16,343 |

## Notes

- Matching completed for all six datasets in both modes using a locally built 1.6.0 wheel from a modified checkout, retaining Splink candidates down to MW 8. Exact and peeled matches remain fixed at each threshold.
- Precleaning and raw conversion were not rerun, as requested. Their original measurements are retained; preparation used package 1.5.3. Overall validation remains incomplete because preparation rejected-row accounting and historical derived-count differences are unresolved.
- Supplied-postcode mode preserves all inputs, including genuinely missing postcodes. Postcode-less mode clears the input postcode field and removes postcode text before cleaning; no input postcode was recovered. Canonical postcodes are unchanged.
- Both modes use identical eligible cohorts and labels. Missing labels and labels absent from the selected canonical are excluded; eligible rows are the accuracy denominator. Pooled councils use the full national canonical; regional filters are residential except Mid Sussex commercial.
- MW 8/10/12 counts use exact threshold overrides on the retained match decisions, not rounded chart lookup. All MW 8 points are now measured. Exact MW 12 counts supersede the earlier rounded-curve estimates.
- Workflow runtime excludes accuracy reporting and persistence. Matching E2E covers the monitored process, including setup, reporting, persistence, threshold evaluation and teardown. Each mode has one scored run per dataset.
- Resources use one-second process-tree RSS samples and OS child high-water RSS separately; sampled peaks are not continuous maxima. GB means 1,000,000,000 bytes. Spill is allocated disk bytes; raw conversion observed zero spill with logical-size sampling. All twelve new jobs and both reused jobs ended with zero spill.
- Stage resource peaks are process totals sampled during a stage, not incremental allocations. Benchmark sub-stage RSS/spill was not measured. Other work is a timing residual. Preparation tables omit input coercion and the term-frequency/road-catalogue writes without changing E2E timing; matching stage totals exclude initialisation and reporting.
- The existing performance documentation is unchanged; these are matching reruns against reused preparation, not a fresh full 1.6.0 preparation validation.

[Manifest](manifest.json)
