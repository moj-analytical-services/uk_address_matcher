# Release validation: v2.0.0.dev3

**Matching completed in supplied-postcode and postcode-less modes.** Overall status: completed.

Target release: **v2.0.0.dev3**. Installed/source software version: **2.0.0.dev3**.

## Preparation

73,058,999 canonical rows; E2E runtime **628.281 seconds**.

Sampled peak RSS **16.455 GB**; sampled peak allocated spill **43.827 GB**; final spill **0.000 GB**.

### Preparation Stages

| Stage | Runtime (s) |
|---|---:|
| Overall full-canonical preparation | 628.281 |
| Individual preparation stages | Unavailable |

## Resource Usage

| Mode | Dataset | Runtime (s) | Peak RSS (GB) | Peak spill (GB) |
|---|---|---:|---:|---:|
| Supplied postcodes | Pooled councils (filtered) | 106.403 | 17.111 | 3.436 |
| Supplied postcodes | Pooled councils (unfiltered) | 91.966 | 12.684 | 0.000 |
| Supplied postcodes | Hackney | 38.050 | 7.652 | 0.000 |
| Supplied postcodes | Rhondda Cynon Taf | 44.682 | 6.332 | 0.000 |
| Supplied postcodes | Aberdeenshire | 29.631 | 6.108 | 0.000 |
| Supplied postcodes | Mid Sussex | 8.012 | 5.535 | 0.000 |
| Supplied postcodes | Gwynedd | 22.891 | 6.024 | 0.000 |
| Postcode-less | Pooled councils (filtered) | 108.694 | 17.786 | 2.325 |
| Postcode-less | Pooled councils (unfiltered) | 98.948 | 17.559 | 4.162 |
| Postcode-less | Hackney | 35.421 | 6.523 | 0.000 |
| Postcode-less | Rhondda Cynon Taf | 33.452 | 6.523 | 0.000 |
| Postcode-less | Aberdeenshire | 38.176 | 6.190 | 0.000 |
| Postcode-less | Mid Sussex | 7.232 | 5.368 | 0.000 |
| Postcode-less | Gwynedd | 23.005 | 6.090 | 0.000 |

All monitored jobs finished with zero spill remaining.

## Supplied Postcodes

### Matching Summary At MW 10

| Dataset | Eligible | Runtime (s) | Precision | Recall | F1 | TP | FP | Unmatched |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Pooled councils (filtered) | 347,343 | 106.403 | 99.48% | 98.76% | 99.12% | 343,031 | 1,800 | 2,512 |
| Pooled councils (unfiltered) | 348,199 | 91.966 | 99.05% | 98.40% | 98.72% | 342,622 | 3,291 | 2,286 |
| Hackney | 113,773 | 38.050 | 99.80% | 99.69% | 99.75% | 113,423 | 229 | 121 |
| Rhondda Cynon Taf | 111,241 | 44.682 | 99.38% | 98.35% | 98.86% | 109,403 | 679 | 1,159 |
| Aberdeenshire | 122,259 | 29.631 | 99.30% | 98.43% | 98.86% | 120,344 | 854 | 1,061 |
| Mid Sussex | 3,701 | 8.012 | 96.63% | 88.38% | 92.32% | 3,271 | 114 | 316 |
| Gwynedd | 62,408 | 22.891 | 98.32% | 95.00% | 96.63% | 59,288 | 1,010 | 2,110 |

### MW 10, 20 And 30 Results

#### Pooled councils (filtered)

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
|---:|---:|---:|---:|---:|---:|---:|
| 10 | 99.48% | 98.76% | 99.12% | 343,031 | 1,800 | 2,512 |
| 20 | 99.64% | 97.33% | 98.47% | 338,075 | 1,207 | 8,061 |
| 30 | 99.80% | 92.86% | 96.21% | 322,557 | 647 | 24,139 |

#### Pooled councils (unfiltered)

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
|---:|---:|---:|---:|---:|---:|---:|
| 10 | 99.05% | 98.40% | 98.72% | 342,622 | 3,291 | 2,286 |
| 20 | 99.31% | 96.76% | 98.02% | 336,922 | 2,345 | 8,932 |
| 30 | 99.59% | 92.08% | 95.68% | 320,605 | 1,329 | 26,265 |

#### Hackney

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
|---:|---:|---:|---:|---:|---:|---:|
| 10 | 99.80% | 99.69% | 99.75% | 113,423 | 229 | 121 |
| 20 | 99.86% | 98.79% | 99.32% | 112,393 | 152 | 1,228 |
| 30 | 99.93% | 92.96% | 96.32% | 105,769 | 69 | 7,935 |

#### Rhondda Cynon Taf

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
|---:|---:|---:|---:|---:|---:|---:|
| 10 | 99.38% | 98.35% | 98.86% | 109,403 | 679 | 1,159 |
| 20 | 99.60% | 96.46% | 98.00% | 107,298 | 433 | 3,510 |
| 30 | 99.77% | 92.27% | 95.87% | 102,639 | 236 | 8,366 |

#### Aberdeenshire

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
|---:|---:|---:|---:|---:|---:|---:|
| 10 | 99.30% | 98.43% | 98.86% | 120,344 | 854 | 1,061 |
| 20 | 99.51% | 97.54% | 98.52% | 119,255 | 586 | 2,418 |
| 30 | 99.72% | 94.18% | 96.87% | 115,148 | 322 | 6,789 |

#### Mid Sussex

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
|---:|---:|---:|---:|---:|---:|---:|
| 10 | 96.63% | 88.38% | 92.32% | 3,271 | 114 | 316 |
| 20 | 97.68% | 81.79% | 89.03% | 3,027 | 72 | 602 |
| 30 | 98.67% | 70.04% | 81.92% | 2,592 | 35 | 1,074 |

#### Gwynedd

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
|---:|---:|---:|---:|---:|---:|---:|
| 10 | 98.32% | 95.00% | 96.63% | 59,288 | 1,010 | 2,110 |
| 20 | 98.76% | 92.57% | 95.56% | 57,772 | 728 | 3,908 |
| 30 | 99.25% | 86.48% | 92.42% | 53,969 | 410 | 8,029 |

## Postcode-less Inputs

### Matching Summary At MW 10

| Dataset | Eligible | Runtime (s) | Precision | Recall | F1 | TP | FP | Unmatched |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Pooled councils (filtered) | 347,343 | 108.694 | 97.31% | 80.63% | 88.19% | 280,062 | 7,743 | 59,538 |
| Pooled councils (unfiltered) | 348,199 | 98.948 | 97.19% | 80.25% | 87.91% | 279,421 | 8,079 | 60,699 |
| Hackney | 113,773 | 35.421 | 99.65% | 86.70% | 92.72% | 98,638 | 344 | 14,791 |
| Rhondda Cynon Taf | 111,241 | 33.452 | 95.61% | 72.57% | 82.51% | 80,727 | 3,705 | 26,809 |
| Aberdeenshire | 122,259 | 38.176 | 99.37% | 87.97% | 93.32% | 107,550 | 679 | 14,030 |
| Mid Sussex | 3,701 | 7.232 | 97.85% | 72.39% | 83.21% | 2,679 | 59 | 963 |
| Gwynedd | 62,408 | 23.005 | 97.88% | 76.10% | 85.63% | 47,493 | 1,029 | 13,886 |

### MW 10, 20 And 30 Results

#### Pooled councils (filtered)

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
|---:|---:|---:|---:|---:|---:|---:|
| 10 | 97.31% | 80.63% | 88.19% | 280,062 | 7,743 | 59,538 |
| 20 | 99.22% | 65.68% | 79.04% | 228,143 | 1,797 | 117,403 |
| 30 | 99.75% | 26.49% | 41.86% | 92,016 | 231 | 255,096 |

#### Pooled councils (unfiltered)

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
|---:|---:|---:|---:|---:|---:|---:|
| 10 | 97.19% | 80.25% | 87.91% | 279,421 | 8,079 | 60,699 |
| 20 | 99.07% | 64.37% | 78.04% | 224,129 | 2,093 | 121,977 |
| 30 | 99.65% | 24.84% | 39.77% | 86,508 | 302 | 261,389 |

#### Hackney

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
|---:|---:|---:|---:|---:|---:|---:|
| 10 | 99.65% | 86.70% | 92.72% | 98,638 | 344 | 14,791 |
| 20 | 99.89% | 66.89% | 80.13% | 76,100 | 80 | 37,593 |
| 30 | 99.96% | 32.54% | 49.10% | 37,023 | 15 | 76,735 |

#### Rhondda Cynon Taf

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
|---:|---:|---:|---:|---:|---:|---:|
| 10 | 95.61% | 72.57% | 82.51% | 80,727 | 3,705 | 26,809 |
| 20 | 98.97% | 59.40% | 74.24% | 66,080 | 690 | 44,471 |
| 30 | 99.60% | 25.80% | 40.98% | 28,696 | 116 | 82,429 |

#### Aberdeenshire

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
|---:|---:|---:|---:|---:|---:|---:|
| 10 | 99.37% | 87.97% | 93.32% | 107,550 | 679 | 14,030 |
| 20 | 99.76% | 79.07% | 88.22% | 96,670 | 228 | 25,361 |
| 30 | 99.86% | 38.09% | 55.14% | 46,564 | 67 | 75,628 |

#### Mid Sussex

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
|---:|---:|---:|---:|---:|---:|---:|
| 10 | 97.85% | 72.39% | 83.21% | 2,679 | 59 | 963 |
| 20 | 99.14% | 52.72% | 68.83% | 1,951 | 17 | 1,733 |
| 30 | 99.68% | 25.48% | 40.59% | 943 | 3 | 2,755 |

#### Gwynedd

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
|---:|---:|---:|---:|---:|---:|---:|
| 10 | 97.88% | 76.10% | 85.63% | 47,493 | 1,029 | 13,886 |
| 20 | 99.11% | 60.10% | 74.83% | 37,509 | 338 | 24,561 |
| 30 | 99.35% | 21.59% | 35.47% | 13,471 | 88 | 48,849 |

## Notes

- The supplied national input was already in ten prepared UKAM-OS parquet chunks, so raw conversion was not rerun. Full UKAM canonical preparation was rerun for 73,058,999 rows with one globally sorted canonical output file.
- All 14 jobs were run in fresh processes: seven supplied-postcode and seven postcode-less runs, including filtered and unfiltered pooled views.
- Postcode-less inputs clear the postcode field and remove postcode text before cleaning. Eligibility, labels, canonical filters and defaults otherwise stay unchanged.
- Matching runtimes exclude accuracy reporting and persistence. RSS and spill are one-second samples; final spill was zero for every job.
- Performance documentation was left unchanged because this run did not establish a controlled comparable historical baseline.
- Pooled matching is about 20 seconds slower than the earlier local experiment. The new prepared inverted index contains 74,218,816 rows, including 11,478,474 district-singleton trigram rows. Most of the increase falls outside the exact, peeled and Splink stage timers while the national index is loaded and materialised.

Link to [manifest.json](manifest.json).
