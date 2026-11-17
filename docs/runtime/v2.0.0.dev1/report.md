# Release Validation: v2.0.0.dev1

**Matching: completed in both postcode modes using `2.0.0.dev1`.** Overall status: completed.

## National Precleaning

71,438,939 canonical rows; E2E runtime **528.211 seconds** (8 min 48 sec).
Full national preparation completed using `2.0.0.dev1`; fresh-connection load and bundle checks passed.

Sampled peak RSS **17.614 GB**; sampled peak allocated spill **42.517 GB**; final spill **0.000 GB**.

### Precleaning Stages

| Stage | Runtime (s) | Sampled peak RSS (GB) | Sampled peak spill (GB) |
| --- | ---: | ---: | ---: |
| Foundational cleaning and IDs | 122.790 | 11.404 | 0.000 |
| Term-frequency aggregation | 2.047 | 11.102 | 0.000 |
| Adjacent distinguishing and TF application | 87.643 | 15.488 | 0.000 |
| Inverted-index derivation | 37.801 | 12.090 | 0.000 |
| Road catalogue and keys | 159.406 | 17.614 | 3.871 |
| Inverted-index write | 6.269 | 7.634 | 1.559 |
| Canonical-address write | 108.308 | 13.894 | 42.517 |
| Manifest finalisation | 1.411 | 9.918 | 38.698 |
| Other preparation work | 0.361 | Unavailable | Unavailable |

## Resource Usage

Pooled councils against the full national canonical.

| Mode | Sampled peak RSS (GB) | Sampled peak spill (GB) |
| --- | ---: | ---: |
| Supplied postcodes | 14.211 | 0.083 |
| Postcode-less | 16.975 | 14.625 |

Both runs finished with zero spill remaining.

## Supplied Postcodes

### Matching Summary At MW 10

| Dataset | Eligible | Runtime (s) | Precision | Recall | F1 | TP | FP | Unmatched |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Pooled councils | 348,284 | 59.242 | 98.91% | 98.07% | 98.49% | 341,561 | 3,772 | 2,951 |
| Hackney | 113,837 | 20.651 | 99.72% | 99.52% | 99.62% | 113,294 | 320 | 223 |
| Rhondda Cynon Taf | 111,247 | 20.558 | 99.13% | 97.73% | 98.42% | 108,721 | 954 | 1,572 |
| Aberdeenshire | 122,301 | 15.235 | 99.17% | 98.43% | 98.80% | 120,385 | 1,009 | 907 |
| Mid Sussex | 3,717 | 7.240 | 96.51% | 87.81% | 91.96% | 3,264 | 118 | 335 |
| Gwynedd | 62,103 | 15.898 | 97.95% | 94.45% | 96.17% | 58,656 | 1,229 | 2,218 |

### Matching Stage Runtimes

| Dataset | Exact (s) | Peeled (s) | Splink (s) | Other pipeline work (s) |
| --- | ---: | ---: | ---: | ---: |
| Pooled councils | 0.782 | 0.698 | 48.246 | 8.114 |
| Hackney | 0.329 | 0.236 | 13.747 | 5.588 |
| Rhondda Cynon Taf | 0.298 | 0.321 | 13.374 | 5.860 |
| Aberdeenshire | 0.267 | 0.196 | 8.753 | 5.347 |
| Mid Sussex | 0.141 | 0.143 | 3.366 | 3.059 |
| Gwynedd | 0.219 | 0.243 | 10.033 | 4.642 |

### MW 8, 10 And 12 Results

#### Pooled councils

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | 98.84% | 98.21% | 98.52% | 342,033 | 3,997 | 2,254 |
| 10 | 98.91% | 98.07% | 98.49% | 341,561 | 3,772 | 2,951 |
| 12 | 98.99% | 97.83% | 98.41% | 340,732 | 3,489 | 4,063 |

#### Hackney

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | 99.70% | 99.60% | 99.65% | 113,378 | 340 | 119 |
| 10 | 99.72% | 99.52% | 99.62% | 113,294 | 320 | 223 |
| 12 | 99.73% | 99.45% | 99.59% | 113,208 | 302 | 327 |

#### Rhondda Cynon Taf

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | 99.09% | 97.87% | 98.48% | 108,883 | 999 | 1,365 |
| 10 | 99.13% | 97.73% | 98.42% | 108,721 | 954 | 1,572 |
| 12 | 99.23% | 97.48% | 98.35% | 108,443 | 838 | 1,966 |

#### Aberdeenshire

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | 99.10% | 98.53% | 98.82% | 120,508 | 1,092 | 701 |
| 10 | 99.17% | 98.43% | 98.80% | 120,385 | 1,009 | 907 |
| 12 | 99.23% | 98.29% | 98.76% | 120,213 | 932 | 1,156 |

#### Mid Sussex

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | 96.27% | 88.78% | 92.37% | 3,300 | 128 | 289 |
| 10 | 96.51% | 87.81% | 91.96% | 3,264 | 118 | 335 |
| 12 | 96.86% | 87.01% | 91.67% | 3,234 | 105 | 378 |

#### Gwynedd

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | 97.84% | 94.68% | 96.23% | 58,798 | 1,297 | 2,008 |
| 10 | 97.95% | 94.45% | 96.17% | 58,656 | 1,229 | 2,218 |
| 12 | 98.06% | 94.14% | 96.06% | 58,461 | 1,158 | 2,484 |

## Postcode-less Inputs

### Matching Summary At MW 10

| Dataset | Eligible | Runtime (s) | Precision | Recall | F1 | TP | FP | Unmatched |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Pooled councils | 348,284 | 78.491 | 96.99% | 79.47% | 87.36% | 276,791 | 8,596 | 62,897 |
| Hackney | 113,837 | 27.749 | 99.58% | 86.41% | 92.53% | 98,362 | 410 | 15,065 |
| Rhondda Cynon Taf | 111,247 | 20.199 | 95.82% | 72.28% | 82.40% | 80,413 | 3,509 | 27,325 |
| Aberdeenshire | 122,301 | 18.070 | 99.18% | 86.77% | 92.56% | 106,122 | 882 | 15,297 |
| Mid Sussex | 3,717 | 6.670 | 97.69% | 74.04% | 84.24% | 2,752 | 65 | 900 |
| Gwynedd | 62,103 | 21.233 | 97.60% | 74.29% | 84.37% | 46,139 | 1,135 | 14,829 |

### Matching Stage Runtimes

| Dataset | Exact (s) | Peeled (s) | Splink (s) | Other pipeline work (s) |
| --- | ---: | ---: | ---: | ---: |
| Pooled councils | 0.253 | 0.255 | 69.358 | 7.279 |
| Hackney | 0.209 | 0.211 | 21.463 | 5.169 |
| Rhondda Cynon Taf | 0.199 | 0.218 | 13.662 | 5.444 |
| Aberdeenshire | 0.184 | 0.203 | 12.062 | 4.970 |
| Mid Sussex | 0.120 | 0.120 | 2.994 | 2.936 |
| Gwynedd | 0.160 | 0.168 | 16.031 | 4.167 |

### MW 8, 10 And 12 Results

#### Pooled councils

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | 96.19% | 80.84% | 87.85% | 281,542 | 11,139 | 55,603 |
| 10 | 96.99% | 79.47% | 87.36% | 276,791 | 8,596 | 62,897 |
| 12 | 97.57% | 77.01% | 86.08% | 268,213 | 6,689 | 73,382 |

#### Hackney

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | 99.45% | 88.38% | 93.59% | 100,611 | 555 | 12,671 |
| 10 | 99.58% | 86.41% | 92.53% | 98,362 | 410 | 15,065 |
| 12 | 99.67% | 83.78% | 91.04% | 95,367 | 311 | 18,159 |

#### Rhondda Cynon Taf

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | 94.33% | 73.10% | 82.37% | 81,320 | 4,886 | 25,041 |
| 10 | 95.82% | 72.28% | 82.40% | 80,413 | 3,509 | 27,325 |
| 12 | 96.98% | 70.74% | 81.81% | 78,697 | 2,447 | 30,103 |

#### Aberdeenshire

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | 98.91% | 87.47% | 92.84% | 106,978 | 1,176 | 14,147 |
| 10 | 99.18% | 86.77% | 92.56% | 106,122 | 882 | 15,297 |
| 12 | 99.33% | 85.58% | 91.94% | 104,665 | 709 | 16,927 |

#### Mid Sussex

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | 97.37% | 75.76% | 85.22% | 2,816 | 76 | 825 |
| 10 | 97.69% | 74.04% | 84.24% | 2,752 | 65 | 900 |
| 12 | 98.02% | 71.78% | 82.87% | 2,668 | 54 | 995 |

#### Gwynedd

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | 96.94% | 75.79% | 85.07% | 47,066 | 1,484 | 13,553 |
| 10 | 97.60% | 74.29% | 84.37% | 46,139 | 1,135 | 14,829 |
| 12 | 98.01% | 72.22% | 83.16% | 44,848 | 909 | 16,346 |

## Notes

- Fresh local raw conversion used ukam-os-builder 0.4.0 with alternate and Welsh address rules, metadata, deduplication and historicaddress exclusion. National preparation and matching used a locally built 2.0.0.dev1 wheel, DuckDB 2.0.0 and Splink 5.0.0. Nothing was published or deployed.
- Preparation used all ten converted national chunks, 10 preparation chunks, one output chunk, 16GB configured DuckDB memory and 14 threads. Manifest, files, indexes and footer counts passed; the bundle loaded in a fresh connection. Bidirectional EXCEPT ALL found no lost or extra identifiers, preserving duplicate multiplicities.
- Supplied-postcode mode retains genuinely missing postcodes. Postcode-less mode removes postcode text and clears the input field before cleaning; extraction cannot recover it. Canonical postcodes, labels, IDs and filters are unchanged. Identical eligible cohort counts and identifier fingerprints were verified across modes.
- Missing labels and labels absent from the selected canonical are excluded; wrong and unmatched observations remain in the denominator. Pooled councils comprise Hackney, Rhondda and Aberdeenshire against the unfiltered national canonical. Regional subsets use the specified council codes and residential classification, except Mid Sussex commercial; national TF and indexes are retained.
- Each dataset and mode has one scored run retaining Splink candidates down to MW 8. MW 8, 10 and 12 use exact threshold overrides with fixed exact and peeled matches; shared runtime is not three separately timed runs. Precision is 1 when no observations match; recall and F1 are 0 when their denominators are zero.
- Workflow runtime excludes accuracy reporting and persistence. Matching E2E includes setup, filtering, scoring, reporting, persistence, threshold evaluation and teardown. Preparation E2E includes setup, cleaning, writes, manifest checks and teardown, but excludes raw conversion and the separate fresh-connection identifier audit.
- Resources use one-second process-tree RSS samples; OS child high-water RSS is recorded separately in the manifest. Sampled peaks are not continuous maxima. GB is 1,000,000,000 bytes. Spill is allocated disk bytes, not logical size. All monitored jobs and verification ended with zero spill remaining. A DuckDB memory setting is not a process-RSS limit.
- Preparation stage peaks are process totals sampled during each stage, not incremental allocations. Stages without a sample have null peaks; benchmark sub-stage RSS/spill was not separately measured. Other work is a timing residual. The three omitted preparation rows remain included in E2E timing.
- Validation required benchmark-helper corrections to retain naturally missing postcodes and replace a removed DuckDB relation.fetchone call in accuracy reporting. All 21 surrounding benchmark and source-loader tests passed. The failed reporting attempt is retained privately and excluded from successful run timings.
- Against the previous local 1.6.0 matching summaries, pooled MW 10 supplied-postcode outcomes change by -7 TP and +4 FP; postcode-less outcomes change by +4 TP and +1 FP. Aberdeenshire supplied-postcode MW 10 has 7 fewer TP and 6 more FP. No regression budget was supplied. The previous summaries used preparation labelled 1.5.3; canonical preparation and dependencies differ, so these observed deltas do not isolate a model effect or establish deterministic repeatability.
- The freshly prepared national folder is active only in the isolated worktree; the original workspace and its configuration are unchanged. Performance documentation is unchanged pending review: this major-version branch changes the model and dependency stack, so older release timings are not a controlled performance baseline. Execution success does not guarantee accuracy or constitute publication approval.

[Manifest](manifest.json)
