# Release validation: `<target-release>`

**Matching completed in supplied-postcode and postcode-less modes.** Overall
status: `<completed|incomplete>`.

Target release: **`<target-release>`**. Installed/source software version:
**`<software-version>`**.

## Preparation

`<canonical-rows>` canonical rows; E2E runtime **`<precleaning-runtime>` seconds**.

Sampled peak RSS **`<precleaning-rss>` GB**; sampled peak allocated spill
**`<precleaning-spill>` GB**; final spill **`<final-spill>` GB**.

### Preparation Stages

| Stage | Runtime (s) |
|---|---:|
| `<stage>` | `<runtime-or-Unavailable>` |
| Overall full-canonical preparation | `<precleaning-runtime>` |

## Resource Usage

| Mode | Dataset | Runtime (s) | Peak RSS (GB) | Peak spill (GB) |
|---|---|---:|---:|---:|
| Supplied postcodes | Pooled councils (filtered) | `<runtime>` | `<rss>` | `<spill>` |
| Supplied postcodes | Pooled councils (unfiltered) | `<runtime>` | `<rss>` | `<spill>` |
| Supplied postcodes | Hackney | `<runtime>` | `<rss>` | `<spill>` |
| Supplied postcodes | Rhondda Cynon Taf | `<runtime>` | `<rss>` | `<spill>` |
| Supplied postcodes | Aberdeenshire | `<runtime>` | `<rss>` | `<spill>` |
| Supplied postcodes | Mid Sussex | `<runtime>` | `<rss>` | `<spill>` |
| Supplied postcodes | Gwynedd | `<runtime>` | `<rss>` | `<spill>` |
| Postcode-less | Pooled councils (filtered) | `<runtime>` | `<rss>` | `<spill>` |
| Postcode-less | Pooled councils (unfiltered) | `<runtime>` | `<rss>` | `<spill>` |
| Postcode-less | Hackney | `<runtime>` | `<rss>` | `<spill>` |
| Postcode-less | Rhondda Cynon Taf | `<runtime>` | `<rss>` | `<spill>` |
| Postcode-less | Aberdeenshire | `<runtime>` | `<rss>` | `<spill>` |
| Postcode-less | Mid Sussex | `<runtime>` | `<rss>` | `<spill>` |
| Postcode-less | Gwynedd | `<runtime>` | `<rss>` | `<spill>` |

All monitored jobs finished with `<zero-or-other>` spill remaining.

## Supplied Postcodes

### Matching Summary At MW 10

| Dataset | Eligible | Runtime (s) | Precision | Recall | F1 | TP | FP | Unmatched |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Pooled councils (filtered) | `<count>` | `<runtime>` | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |
| Pooled councils (unfiltered) | `<count>` | `<runtime>` | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |
| Hackney | `<count>` | `<runtime>` | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |
| Rhondda Cynon Taf | `<count>` | `<runtime>` | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |
| Aberdeenshire | `<count>` | `<runtime>` | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |
| Mid Sussex | `<count>` | `<runtime>` | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |
| Gwynedd | `<count>` | `<runtime>` | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |

### MW 10, 20 And 30 Results

#### Pooled councils (filtered)

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
|---:|---:|---:|---:|---:|---:|---:|
| 10 | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |
| 20 | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |
| 30 | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |

#### Pooled councils (unfiltered)

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
|---:|---:|---:|---:|---:|---:|---:|
| 10 | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |
| 20 | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |
| 30 | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |

#### Hackney

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
|---:|---:|---:|---:|---:|---:|---:|
| 10 | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |
| 20 | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |
| 30 | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |

#### Rhondda Cynon Taf

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
|---:|---:|---:|---:|---:|---:|---:|
| 10 | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |
| 20 | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |
| 30 | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |

#### Aberdeenshire

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
|---:|---:|---:|---:|---:|---:|---:|
| 10 | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |
| 20 | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |
| 30 | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |

#### Mid Sussex

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
|---:|---:|---:|---:|---:|---:|---:|
| 10 | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |
| 20 | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |
| 30 | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |

#### Gwynedd

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
|---:|---:|---:|---:|---:|---:|---:|
| 10 | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |
| 20 | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |
| 30 | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |

## Postcode-less Inputs

### Matching Summary At MW 10

| Dataset | Eligible | Runtime (s) | Precision | Recall | F1 | TP | FP | Unmatched |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Pooled councils (filtered) | `<count>` | `<runtime>` | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |
| Pooled councils (unfiltered) | `<count>` | `<runtime>` | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |
| Hackney | `<count>` | `<runtime>` | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |
| Rhondda Cynon Taf | `<count>` | `<runtime>` | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |
| Aberdeenshire | `<count>` | `<runtime>` | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |
| Mid Sussex | `<count>` | `<runtime>` | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |
| Gwynedd | `<count>` | `<runtime>` | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |

### MW 10, 20 And 30 Results

#### Pooled councils (filtered)

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
|---:|---:|---:|---:|---:|---:|---:|
| 10 | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |
| 20 | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |
| 30 | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |

#### Pooled councils (unfiltered)

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
|---:|---:|---:|---:|---:|---:|---:|
| 10 | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |
| 20 | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |
| 30 | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |

#### Hackney

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
|---:|---:|---:|---:|---:|---:|---:|
| 10 | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |
| 20 | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |
| 30 | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |

#### Rhondda Cynon Taf

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
|---:|---:|---:|---:|---:|---:|---:|
| 10 | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |
| 20 | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |
| 30 | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |

#### Aberdeenshire

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
|---:|---:|---:|---:|---:|---:|---:|
| 10 | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |
| 20 | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |
| 30 | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |

#### Mid Sussex

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
|---:|---:|---:|---:|---:|---:|---:|
| 10 | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |
| 20 | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |
| 30 | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |

#### Gwynedd

| MW | Precision | Recall | F1 | TP | FP | Unmatched |
|---:|---:|---:|---:|---:|---:|---:|
| 10 | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |
| 20 | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |
| 30 | `<percent>` | `<percent>` | `<percent>` | `<count>` | `<count>` | `<count>` |

## Notes

- `<what was prepared and any intentional simplification>`
- `<what benchmark matrix and eligibility rules were used>`
- `<timing and resource-measurement boundaries>`
- `<material limitations or version differences>`

Link to [manifest.json](manifest.json).
