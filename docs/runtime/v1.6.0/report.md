# Release Validation: v1.6.0

**Status: incomplete.** Installed software: `1.5.3`.

## National Precleaning

71,438,939 canonical rows; E2E runtime **570.039 seconds** (9 min 30 sec).

## Matching Summary At MW 10

| Dataset | Eligible | Runtime (s) | Precision | Recall | F1 | TP | FP | Unmatched |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Pooled councils | 348,284 | 67.801 | 98.91% | 98.07% | 98.49% | 341,568 | 3,768 | 2,948 |
| Hackney | 113,837 | 21.727 | 99.72% | 99.52% | 99.62% | 113,294 | 320 | 223 |
| Rhondda Cynon Taf | 111,247 | 20.849 | 99.13% | 97.73% | 98.43% | 108,724 | 954 | 1,569 |
| Aberdeenshire | 122,301 | 14.786 | 99.17% | 98.44% | 98.81% | 120,392 | 1,003 | 906 |
| Mid Sussex | 3,717 | 7.442 | 96.54% | 87.84% | 91.98% | 3,265 | 117 | 335 |
| Gwynedd | 62,103 | 14.120 | 97.95% | 94.45% | 96.17% | 58,655 | 1,226 | 2,222 |

## MW 8, 10 And 12 Results

### Pooled councils

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | Unavailable | Unavailable | Unavailable | Unavailable | Unavailable | Unavailable |
| 10 | 98.91% | 98.07% | 98.49% | 341,568 | 3,768 | 2,948 |
| 12 | 98.99% | 97.85% | 98.41% | 340,779 | 3,494 | 4,011 |

### Hackney

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | Unavailable | Unavailable | Unavailable | Unavailable | Unavailable | Unavailable |
| 10 | 99.72% | 99.52% | 99.62% | 113,294 | 320 | 223 |
| 12 | 99.73% | 99.45% | 99.59% | 113,209 | 303 | 325 |

### Rhondda Cynon Taf

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | Unavailable | Unavailable | Unavailable | Unavailable | Unavailable | Unavailable |
| 10 | 99.13% | 97.73% | 98.43% | 108,724 | 954 | 1,569 |
| 12 | 99.23% | 97.48% | 98.35% | 108,448 | 840 | 1,959 |

### Aberdeenshire

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | Unavailable | Unavailable | Unavailable | Unavailable | Unavailable | Unavailable |
| 10 | 99.17% | 98.44% | 98.81% | 120,392 | 1,003 | 906 |
| 12 | 99.23% | 98.30% | 98.77% | 120,224 | 928 | 1,149 |

### Mid Sussex

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | Unavailable | Unavailable | Unavailable | Unavailable | Unavailable | Unavailable |
| 10 | 96.54% | 87.84% | 91.98% | 3,265 | 117 | 335 |
| 12 | 96.89% | 87.06% | 91.71% | 3,236 | 104 | 377 |

### Gwynedd

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | Unavailable | Unavailable | Unavailable | Unavailable | Unavailable | Unavailable |
| 10 | 97.95% | 94.45% | 96.17% | 58,655 | 1,226 | 2,222 |
| 12 | 98.06% | 94.14% | 96.06% | 58,464 | 1,157 | 2,482 |

## Notes

- Target v1.6.0 was tested using package 1.5.3 from a modified checkout, not a v1.6.0 release artifact.
- Precleaning E2E is the monitored national preparation process, including setup and teardown; raw OS conversion is excluded.
- Matching runtime is the benchmark workflow time, excluding accuracy reporting and persistence; one run per dataset.
- MW 8 is unavailable: the runs retained candidates only down to MW 10. The earlier MW 8 figures repeated MW 10 and were not valid MW 8 measurements.
- Pooled councils used the full national canonical; other datasets used regional residential filters, except Mid Sussex commercial.
- Missing postcodes were retained. Missing labels and labels absent from the selected canonical were excluded; eligible rows are the accuracy denominator.
- Rejected-row accounting and differences from historical derived counts remain unresolved. Performance documentation is unchanged.

[Manifest](manifest.json)
