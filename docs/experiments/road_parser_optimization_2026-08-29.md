# Road Parser Performance Optimisation

## Summary

The original fused function parsed all 71,438,939 canonical variants in 214.643
seconds. The optimised canonical path now parses one deterministic preferred row
for each of 39,187,586 UPRNs, stores only `(unique_id, road_1_norm)`, and rejoins
that key to every source variant. At the production 16 GB cap the complete road
key and blocker-cardinality stage takes **77.196 seconds**, or **925,418 output
rows/second**: a **2.78x speed-up**. At 30 GB it takes 62.705 seconds, establishing
the remaining spill ceiling without making extra memory a requirement.
Requiring candidates to exist in the packaged road catalogue reduces the paired
16 GB full run from 78.177 to **66.105 seconds**. This is **15.4% faster** than its
immediate control and **3.25x faster** than the original pooled parser.
At 30 GB the supported path takes **51.205 seconds**, 6.2 seconds above the target.
Further output-preserving SQL and execution changes reduce the current 16 GB path
to **58.121 seconds**. At 30 GB, road-key generation reaches **44.975 seconds**,
but the required 71.4M-row rejoin and blocker cardinalities bring the complete
stage to **51.973 seconds**.
Deduplicating candidate-relevant post-number token tails before expansion reduces
the complete four-chunk 16 GB stage to **26.954 seconds**: 19.946 seconds for
preferred-row selection and road keys plus 7.008 seconds for the final rejoin and
cardinalities. This is a **7.96x speed-up** over the original parser and clears the
45-second target without changing any top-one road key.

The remembered 43.342-second result was valid but covered 33,575,026 preferred
residential/commercial rows and excluded the final 71.4M-row rejoin and blocker
cardinalities. The current production population contains 16.7% more UPRNs plus
all repeated output variants, so 43 seconds is not a like-for-like target.

Fresh exact-label evaluation on a fixed 1/8 hash partition improved from 90.0832%
precision, 83.4453% recall, and 86.6373% F1 with pooled variants to 90.5608%,
83.5589%, and 86.9191% with preferred rows. This exact string comparison is
separate from the frozen safe-core metric of 96.7523%.

## Changes

- Compute `rightmost_numeric_position` once and reuse it to obtain the numeric value.
- Generate road-terminal spans directly from terminal token positions.
- Expand all candidate starts only for addresses without an eligible terminal span.
- Preserve the original three-state terminal logic: terminal, valid non-terminal
  fallback, or no valid candidate.
- Preserve the original distinction between ordinary and facility-cleaned fallback
  windows, including existing truncated-window behaviour.
- Select one canonical row per UPRN: Built Address first, Royal Mail second, then
  deterministic address and row-ID tie-breaks.
- Materialise the preferred parser input once and score it in adaptive, hash-based
  chunks of at most about 10 million UPRNs.
- Materialise only `unique_id` and `road_1_norm` for blocking instead of five road
  diagnostics plus every source column.
- Replace two full-width cardinality windows with narrow grouped counts and joins.
- Rejoin each selected road to every canonical variant before blocker cardinalities
  are calculated, preserving output row cardinality.
- Require production road candidates to have packaged catalogue support and use a
  compact terminal-token/width lookup to reject unsupported windows early.
- For supported fallback candidates, replace the partition validity window with an
  exact catalogue join plus the remaining block predicate.
- Skip redundant `ANALYZE` scans on transient prepared chunks.
- Disable insertion-order preservation while preferred rows and road-key chunks are
  materialised, then restore the caller's DuckDB setting.
- Derive road and road-number cardinalities from one grouped canonical scan.
- Group equivalent post-number token tails within each bounded chunk, score one
  representative, and map its winner back to every UPRN with that signature.
- Preserve facility-specific truncated-window behaviour as part of the signature.
- Define top two as two distinct ranked phrases while retaining nested phrases;
  the previous subset anti-join suppressed valid longer or shorter alternatives.
- Add `benchmarking/road_parser_performance.py` for grouped deterministic timing,
  candidate fingerprints, and persisted-output comparisons.

No Splink comparison, score, threshold, blocker, reranker, or final-match logic was
changed. Serving remains DuckDB-only and uses the same folded JSON scorecard and
phrase catalogue.

## Historical Preferred-Canonical Baseline

These are the previously verified 14-thread results for 33,575,026 prepared
preferred-canonical rows. Raw canonical cleaning is excluded.

| Stage | Rows | Seconds | Throughput |
| --- | ---: | ---: | ---: |
| Candidate generation | 47,676,776 candidates | 30.752 | 1.55M candidates/s |
| Phrase catalogue | 990,119 phrases | 4.844 | Not reported |
| SQL scoring | 47,676,776 candidates | 5.973 | 7.98M candidates/s |
| Winner selection | 30,477,812 winners | 1.773 | 17.19M winners/s |
| **Total** | **33,575,026 inputs** | **43.342** | **774,638 inputs/s** |

Candidate generation represented 71.0% of total wall time.

This older benchmark used 33,575,026 preferred canonical records, one per UPRN.
The new full benchmark uses all 71,438,939 rows in the inferred canonical parquet,
including repeated `unique_id` groups. The populations and execution plans differ,
so their timings must not be treated as a linear A/B comparison.

## Optimised Canonical Production Path

Optimised production-path timings use 14 threads and include preferred-row
selection, road scoring, the rejoin to 71,438,939 variants, and both
blocker-cardinality flags. The historical 214.643-second baseline measured only
pooled parser features, so the reported speed-up is conservative. Source parquet
materialisation is reported separately.

| Configuration | Scored UPRNs | Output rows | Road stage | Output throughput | Speed-up |
| --- | ---: | ---: | ---: | ---: | ---: |
| Original pooled parser, 16 GB | 71,438,939 variants | 71,438,939 | 214.643s | 332,826/s | 1.00x |
| Preferred, four chunks, 16 GB | 39,187,586 | 71,438,939 | **77.196s** | **925,418/s** | **2.78x** |
| Preferred, four chunks, 30 GB | 39,187,586 | 71,438,939 | **62.705s** | **1,139,288/s** | **3.42x** |
| Preferred + catalogue support, four chunks, 16 GB | 39,187,586 | 71,438,939 | **66.105s** | **1,080,694/s** | **3.25x** |
| Preferred + catalogue support, four chunks, 30 GB | 39,187,586 | 71,438,939 | **51.205s** | **1,395,147/s** | **4.19x** |
| Current supported SQL, four chunks, 16 GB | 39,187,586 | 71,438,939 | **58.121s** | **1,229,142/s** | **3.69x** |
| Current supported SQL, four chunks, 30 GB | 39,187,586 | 71,438,939 | **51.973s** | **1,374,527/s** | **4.13x** |
| Tail-signature reuse, four chunks, 16 GB | 39,187,586 | 71,438,939 | **26.954s** | **2,650,387/s** | **7.96x** |

At 16 GB, preferred-row materialisation and compact key generation take 64.435
seconds; the final rejoin and grouped blocker flags take 12.762 seconds. At 30 GB
those phases take 55.438 and 7.267 seconds. The source parquet reads take another
2.040 and 2.131 seconds respectively.

The fixed 1/8 production path completes in 6.452 seconds for 8,929,648 output rows.
The fixed 1/2 path completes in 25.846 seconds for 35,718,708 output rows. The
remaining full-scale nonlinearity is memory pressure, not regex evaluation.

Before signature reuse, the 16 GB split was 50.159 seconds for preferred-row
selection and road keys plus 7.962 seconds for final canonical materialisation.
The current split is 19.946 plus 7.008 seconds. A staged 1/8 profile attributes
4.213 of 5.263 seconds after
preparation to candidate construction, 0.882 seconds to catalogue features and
score arithmetic, and 0.168 seconds to winner selection. Candidate expansion and
memory traffic, not ranking or model arithmetic, now dominate.

Across the full preferred population, 34,580,743 parseable rows contain only
1,774,275 distinct post-number token tails; 34,093,560 rows belong to a reused
signature. On the fixed 1/8 production partition, signature reuse reduces road-key
time from 5.322 to 2.939 seconds and complete time from 5.904 to 3.453 seconds.
A direct fixed-1/20 comparison produced 1,685,419 keys with zero old/new mismatches.
One global signature chunk regressed to 30.123 seconds at 16 GB because its tail
map spilled; four bounded chunks remain faster at 26.954 seconds.

## Catalogue-Supported Candidate Policy

On the fixed 1/20 preferred-row sample, 867,586 of 3,540,742 candidates
(24.50%) were absent from the packaged road catalogue. Only 12,170 of 1,697,589
candidate-bearing addresses (0.717%) had no supported candidate. Terminal token
plus candidate width identified 722,275 unsupported candidates, or 20.40% of all
candidates, before the exact phrase join.

The paired full-scale 16 GB runs used four road chunks and skipped separate truth
and persisted-output validation scans.

| Policy | Road keys | Final rejoin/cardinalities | Complete road stage |
| --- | ---: | ---: | ---: |
| Preferred-row control | 70.715s | 7.462s | 78.177s |
| Catalogue-supported | **58.514s** | 7.591s | **66.105s** |

The support policy therefore saves 12.073 seconds end to end. It does change the
prediction contract: unsupported winners are now rejected rather than retained.
On 1,651,303 exact labels in the fixed 1/20 sample, accepted predictions fell by
3,960 and correct predictions fell by 58. Precision increased by 0.2321 percentage
points, recall decreased by 0.0035 points, and F1 increased by 0.1049 points.

| Preferred-row policy | Accepted | Correct | Precision | Recall | F1 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Allow unsupported candidates | 1,524,217 | 1,380,390 | 90.5639% | 83.5940% | 86.9395% |
| Require catalogue support | 1,520,257 | 1,380,332 | **90.7960%** | 83.5905% | **87.0444%** |

The production canonical path now requires catalogue support by default. The
benchmark keeps an explicit ungated control path for future comparisons.

## Rejected Experiments

- Persisting `rightmost_numeric_position` as `SMALLINT` is supported, but adding it
  to the default cleaning queue was slower because it processes every canonical
  variant while road scoring only consumes preferred UPRNs.
- Pre-filtering terminal positions into lists regressed the 1/8 road stage.
- Replacing preferred-row `row_number()` with `arg_min(struct(...))` preserved the
  output fingerprint but regressed the 1/8 stage.
- Replacing the fallback partition window with a distinct-address join won at 1/8
  but regressed the half-scale stage, so the window remains.
- Existing contiguous bigram/trigram indexes cannot replace the road catalogue:
  their postings do not contain phrase support, numeric diversity, postcode and
  district support, terminal support, or right-context diversity.
- Two 30 GB chunks regressed to 78.413 seconds because each chunk crossed the spill
  threshold. Eight 5M-UPRN chunks at 16 GB reached 65.162 seconds, only 0.943
  seconds faster than four chunks and too close to run variance to change defaults.
- Fusing score calculation and top-one selection regressed the 1/8 road-key phase
  from 5.84 to 9.83 seconds by carrying the candidate graph through the sort.
- Ordered `first(... ORDER BY ...)` aggregation was runtime-neutral versus the
  deterministic `row_number()` winner selection.
- A single exact catalogue join changed terminal/fallback routing, slightly reduced
  F1, and did not improve half-scale runtime.
- Precomputing the numeric anchor after preferred-row selection was exact but
  regressed 1/8 road-key time from 5.81 to 5.91 seconds.
- Caching the 1M-row catalogue in a temp table increased buffer pressure and
  regressed half-scale road-key time from 26.77 to 27.97 seconds.
- Twelve threads tied 14 at half scale; ten threads regressed from 26.39 to 29.01
  seconds, so lower thread-local memory did not offset lost parallelism.
- Dropping fallback candidates raised precision to 94.7212% but reduced recall to
  76.0543% and F1 to 84.3676%; fallback covers 13.3% of candidate-bearing addresses.

## Adjacent Cleaning Audit

- DuckDB already common-subexpression-eliminates the three identical numeric-token
  regex references into one physical projection; no rewrite was made.
- Normal cleaning stages form one CTE graph and do not force repeated source scans
  outside incremental debug mode; queue consolidation would not remove I/O.
- Business-unit type, ID, and presence previously evaluated the same grouped regex
  three times. A single struct-valued extraction is output-identical on 8,929,648
  rows and reduced the isolated pass from 1.37-1.52s to 1.17-1.18s.

## Deterministic A/B Result

The local A/B used 14 DuckDB threads, a 16 GB memory limit, and complete
`unique_id` groups selected by `hash(unique_id) % 100 = 0`. Grouping by
`unique_id` is required because road ranking partitions by that field; sampling by
`ukam_address_id` splits ranking groups and does not reproduce full-file winners.

| Measure | Before | After | Change |
| --- | ---: | ---: | ---: |
| Input rows | 716,496 | 716,496 | 0 |
| Prepared rows | 641,012 | 641,012 | 0 |
| Candidate rows | 1,321,391 | 1,321,391 | 0 |
| Parsed rows | 640,154 | 640,154 | 0 |
| Candidate seconds | 1.475 | 0.664 | **-55.0%** |
| Full parse seconds | 3.028 | 2.228 | **-26.4%** |
| Candidate throughput | 896,086/s | 1,989,884/s | **+122.1%** |
| Input throughput | 236,635/s | 321,573/s | **+35.9%** |
| Successful parses/s | 211,422/s | 287,309/s | **+35.9%** |
| Top-one road mismatches | 0 | 0 | 0 |

## Earlier Sample Statistics

Four optimised runs used complete groups selected by
`hash(unique_id) % 20 = 0`: 3,575,293 inputs, 3,198,076 prepared rows,
6,575,817 candidates, and 3,193,228 successful parses per run.

| Measure | Mean | Median | Range |
| --- | ---: | ---: | ---: |
| Candidate seconds | 2.240 | 2.247 | 2.141-2.325 |
| Full parse seconds | 6.241 | 6.247 | 6.139-6.332 |
| Candidate throughput | 2.938M/s | 2.927M/s | 2.828M-3.072M/s |
| Input throughput | 572,962/s | 572,414/s | 564,664-582,355/s |
| Successful parses/s | 511,734/s | 511,245/s | 504,323-520,123/s |

The final run compared every persisted road feature: phrase, confidence, token
count, margin, and distinctive tokens. All 3,575,293 rows matched.

These results remain useful as output-equivalence tests, but not as full-runtime
estimates. The full run demonstrates substantial scale sensitivity under 16 GB.

## Full 71.4M-Row Phase Breakdown

The phase benchmark used 14 DuckDB threads, a 16 GB memory limit, and all
71,438,939 rows. It produced 63,866,135 prepared rows, 131,165,779 candidates, and
63,769,598 winners. Every selected road agreed with the persisted deployed output.

| Production assignment phase | Rows | Seconds | Throughput | Share |
| --- | ---: | ---: | ---: | ---: |
| Prepare tokens and numeric anchors | 63,866,135 | 10.188 | 7.01M inputs/s | 4.4% |
| Generate terminal-first candidates | 131,165,779 | 178.330 | 0.736M candidates/s | 77.7% |
| Catalogue join and 15 base features | 131,165,779 | 11.171 | 11.74M candidates/s | 4.9% |
| Selected model arithmetic | 131,165,779 | 19.528 | 6.72M candidates/s | 8.5% |
| Rank and select winners | 63,769,598 | 10.193 | 6.26M winners/s | 4.4% |
| **Total assignment, excluding source read** | **71,438,939 inputs** | **229.409** | **311,404 inputs/s** | **100%** |

Materialising the seven required source columns took another 1.864 seconds. The
table is deliberately staged for diagnosis; the actual fused production function
is benchmarked separately because DuckDB can choose a different execution plan.

### Fused Production Result

| Measure | Result |
| --- | ---: |
| Input rows | 71,438,939 |
| Rows with an inferred road | 63,769,598 |
| Source materialisation | 1.686s |
| `add_top_1_road_features()` | **214.643s (3m 34.6s)** |
| Input throughput | **332,826 rows/s** |
| Successful-parse throughput | **297,095 roads/s** |
| Persisted `road_1_norm` mismatches | **0** |

The fused plan is 14.766 seconds faster than the staged diagnostic plan, but it is
still far below the desired one million input rows per second.

### Phrase-Catalogue Precomputation

Rebuilding roadlike recurrence frequencies is a separate operation from normal
canonical road assignment. On the full 71.4M-row source, the measured components
were:

| Catalogue-build phase | Seconds |
| --- | ---: |
| Prepare rows | 10.188 |
| Generate candidates | 178.330 |
| Aggregate 1,889,145 phrase/terminal rows | 21.642 |
| **Total excluding source read and parquet write** | **210.160** |

Normal production parsing does not rebuild this catalogue. It reads the packaged
`roadlike_places.parquet`, preserving its training-only topology provenance.

## End-to-End Public Preparation

The public `prepare_canonical_folder()` workflow was benchmarked separately from
the road-only timings above. The input was the configured 71,438,939-row NGD
parquet. The run used UKAM 1.3.0, DuckDB 1.5.0, 14 threads, a 16 GB DuckDB memory
limit, 10 cleaning chunks, eight canonical output shards, and no road blocking.

| End-to-end measure | Result |
| --- | ---: |
| Total wall time | **933.141s (15m 33.1s)** |
| Process peak RSS | **20,087,422,976 bytes (20.09 GB; 18.71 GiB)** |
| Largest observed DuckDB spill directory | **at least 70 GB** |
| Prepared output size | 1,865,917,369 bytes (1.87 GB) |
| Canonical rows | 71,438,939 |
| Term-frequency rows | 926,832 |
| Inverted-index rows | 61,562,885 |

This run predated benchmark event capture, so the first interval cannot be split
retrospectively into its internal materialised stages. The boundaries below come
from the process start and completed-artefact timestamps. They are practical
wall-clock intervals, not isolated operator microbenchmarks.

| Observed interval | Elapsed | Share |
| --- | ---: | ---: |
| TF derivation, canonical cleaning, index derivation, row count, and TF write | ~724s (12m 04s) | 77.6% |
| Inverted-index parquet serialization | ~28s | 3.0% |
| Eight sorted canonical parquet shards | ~180s (3m 00s) | 19.3% |
| Manifest and finalisation | ~1s | 0.1% |
| **Total** | **933.141s** | **100%** |

The canonical export intervals were 22s, 21s, 22s, 25s, 22s, 21s, 23s, and
24s for shards one through eight. This shows that road-free preparation is not
currently a six-minute operation under the constrained 16 GB configuration. The
dominant cost occurs before parquet export, while DuckDB materialises the cleaning
and indexing tables and spills heavily; output chunking controls artefact size but
does not address that dominant interval.

The benchmark runner now writes an incremental `.events.jsonl` sidecar with
elapsed time and process peak RSS at each existing preparation stage. It survives
an incomplete run and will provide exact internal stage boundaries for subsequent
road-enabled and chunking comparisons.

The matched road-enabled run completed in **1,918.331s (31m 58.3s)** with
20,078,952,448 bytes peak RSS, 80 GB observed spill, and 1,887,504,770 bytes of
output. Roads were assigned to 62,859,358 rows. The file set was only 1.16% larger
than the no-road output, but wall time was 105.6% higher.

| Road-enabled stage | Exact elapsed |
| --- | ---: |
| TF cleaning | 33.950s |
| Foundational cleaning | 63.568s |
| ID plus adjacent-token interval | 159.851s |
| TF application | 222.694s |
| Post-TF plus road interval | 80.062s |
| Bigram index | 342.515s |
| Trigram index | 388.437s |
| Eight output loops | ~548.8s |

This is not a 26.954s scorer regression. Road enrichment was a lazy relation placed
before the index and reused by each output shard, allowing its joins and cardinality
CTEs to be replayed. Index construction now runs from the materialised base clean
rows, road enrichment follows it and is materialised once, and output log counts no
longer issue a second query per shard. These fixes pass focused tests and await a
matched full-scale benchmark.

### Hackney Messy-Address Preparation

The configured Hackney CSV yielded 114,166 usable rows. These fresh-process runs
used the existing prepared canonical term frequencies and inverted index.

| Mode | Setup | Cleaning | Road parser | Total | Peak RSS |
| --- | ---: | ---: | ---: | ---: | ---: |
| Without road parser | 0.386s | 10.015s | - | 10.401s | 5.135 GB |
| With road parser | 0.338s | 7.718s | **0.729s** | 8.785s | 5.178 GB |

The road-enabled run assigned a road to 113,158 of 114,166 rows (99.12%). The
2.297s difference in total cleaning time is run-order/cache variance, not a
road-parser speed-up; the isolated incremental road cost is 0.729s. Peak memory is
dominated by loading and querying the canonical lookup artefacts. The prepared
canonical folder was produced by UKAM 1.2.2 while these runs used 1.3.0.

## Slim Scorer Trial

Each retained model scored exactly the same 131,165,779 materialised candidates.
The shared catalogue join and 15 base features are reported as part of standalone
scoring. `Agreement` is equality with the deployed selected road on the unlabelled
71.4M-row parquet; it is not oracle accuracy.

| Model | Active terms | Arithmetic seconds | Standalone scoring seconds | Arithmetic candidates/s | Deployed-road agreement | Historical frozen safe-core | Historical exact top-one |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Selected tree scorecard | 135 of 150 | 19.528 | 30.698 | 6.72M/s | 100.000% | 96.7523% terminal-first | 93.7707% terminal-first |
| Pairwise logistic | 14 of 15 | 8.244 | 19.415 | 15.91M/s | 93.859% | 93.8627% all-candidate | 90.3968% all-candidate |
| Additive logistic v1 | 37 of 42 | 10.174 | 21.344 | 12.89M/s | 96.952% | 96.3838% all-candidate | 90.0041% all-candidate |
| Balanced additive logistic | 77 of 86 | 13.009 | 24.180 | 10.08M/s | 98.815% | 96.7723% all-candidate | 90.9581% all-candidate |

The selected runtime compiler now omits 15 exactly-zero coefficients, leaving 135
active terms. This is score-preserving. Full-run timing variation was larger than
the expected saving, so no speed-up is claimed for this micro-optimisation yet.

Under the staged full-run plan, substituting the 15-term model would reduce total
time only from about 229.4 to 216.3 seconds, approximately a 5.7% gain, while its
historical safe-core result is 2.89 percentage points below the selected model.
Even free model arithmetic would leave roughly 210 seconds of non-model work in
this materialised plan.

The authoritative candidate-feature database needed to replay every slim model
under terminal-first filtering is absent from this checkout. Their terminal-first
precision and recall therefore remain unmeasured; the table labels the older
all-candidate metrics explicitly.

## Precision and Recall

The reusable oracle-labelled residential/commercial parquet is present. A fixed
`hash(unique_id) % 8 = 0` partition contains 4,121,926 eligible UPRNs with a
non-null preferred Built/Royal Mail street label. Precision is exact correct roads
divided by non-null predictions; recall is exact correct roads divided by all
eligible labels.

| Production input policy | Accepted | Correct | Precision | Recall | F1 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Pool all canonical variants | 3,818,195 | 3,439,554 | 90.0832% | 83.4453% | 86.6373% |
| Preferred row per UPRN | 3,803,229 | 3,444,236 | **90.5608%** | **83.5589%** | **86.9191%** |

The preferred policy changes 285,957 of 8,929,648 persisted variant roads (3.20%),
but improves all three exact-label measures. The previous pooled deployment path
therefore was not a quality oracle; it combined candidates across source variants
although the scorecard was trained and frozen on one preferred row per UPRN.

The separate frozen safe-core figures remain:

| Quality measure | Frozen selected result | Post-change status |
| --- | ---: | --- |
| Safe-core precision | 96.7523% | Scorecard and candidate policy unchanged |
| Safe-core recall | 96.7523% | Scorecard and candidate policy unchanged |
| Safe-core F1 | 96.7523% | Scorecard and candidate policy unchanged |
| Exact top-one recall | 93.7707% | Frozen terminal-first evaluation |
| Correct safe-core winners | 2,856,612 / 2,952,500 | Frozen terminal-first evaluation |

The current catalogue-supported scorer was also measured on the fixed native-ID
1/20 partition. `Precision at K` below is address-level coverage among addresses
with at least one supported prediction; it asks whether any emitted key is correct.
The safe-core label accepts a candidate that is an exact road or a leading road
prefix, matching the ranker's training objective.

| Quality measure | Top 1 | Top 2 distinct | Reachable in any candidate |
| --- | ---: | ---: | ---: |
| Exact precision | 90.7960% | **94.3922%** | 94.9912% |
| Safe-core precision | 93.5730% | **95.5286%** | 95.9611% |
| Exact recall over all truth | 83.5905% | **86.9013%** | 87.4528% |
| Safe-core recall over all truth | 86.1471% | **87.9475%** | 88.3457% |

Of 139,925 incorrect accepted top-one predictions, 104,809 (74.9%) are nested
prefix, suffix, or contained-road errors. The previous top-two subset anti-join
reduced exact precision-at-two to 91.5647%; retaining nested alternatives raises it
to 94.3922%. Exact candidate-slot precision is 66.4515%, so emitting two keys for
every address is not free.

A calibrated cascade is more selective. On one deterministic half, a maximum
top-one/top-two logit margin of 2.2318 was selected to target 95% safe-core
precision. On the untouched half it reached **95.0450%** while emitting a second
key for 50,887 of 759,751 addresses (6.7%); exact precision was 92.8537%. This
policy is suitable for a blocking experiment, but the threshold should be packaged
as a versioned calibrated artefact rather than hard-coded into cleaning.

## Accuracy Architecture

The remaining exact ceiling is partly structural. Candidate generation currently
emits only two- and three-token spans. On the fixed 1/20 truth partition, all
56,196 one-token roads and all 11,601 four-token roads are therefore unreachable;
another 2,924 truth roads contain five or more tokens. Two-token and three-token
truth still contribute 103,411 and 33,061 unreachable rows respectively through
cleaning, anchor, terminal, or catalogue-support failures.

The recommended next design is a two-stage retrieval and reranking cascade:

1. Extend the versioned catalogue to widths one and four. Require authoritative
  phrase plus postcode or district support for one-token roads, where accidental
  building/locality matches are most likely.
2. Cache the top few context-free candidates by post-number tail signature, as the
  production optimisation now does for top one.
3. After mapping those candidates back to UPRNs, rerank only that small set with
  per-address evidence: phrase/postcode support, phrase/district support, house
  number support or range compatibility, preferred-source type, and agreement
  across canonical variants.
4. Train a listwise or pairwise boundary ranker with separate exact-boundary and
  safe-core targets. The current prefix-positive label is useful for blocking but
  cannot by itself optimise exact road strings.
5. Calibrate an adaptive top-two gate on a disjoint split and package its threshold
  with the scorecard. High-margin rows emit one key; ambiguous rows emit two.

This follows the standard high-recall retrieval plus reranking pattern used in
entity-resolution blocking and information retrieval. A CRF-style sequence parser,
as used by libpostal, is a useful challenger or teacher for boundary errors, but it
is not the first production choice here: the authoritative catalogue, DuckDB
execution, and extreme tail reuse make constrained retrieval substantially cheaper.

## Conclusion

- Complete full-scale canonical road preparation is now about 2.65 million output
  rows/second at 16 GB, up from 333,000 rows/second.
- Scoring arithmetic is already in the millions: 6.72M candidates/second for the
  selected model and 15.91M candidates/second for the 15-term challenger.
- Removing duplicated UPRN parsing, wide intermediates, and full-row windows was
  materially more valuable than replacing the scorer.
- A slimmer scorer is not recommended as the primary production optimisation. The
  86-term additive model is the least damaging challenger, but its staged total
  saving is only about 2% and terminal-first oracle quality is not measured.
- Raising the cap from 16 GB to 30 GB saves 14.491 seconds, but the 16 GB path is
  retained as the portable production default.
- Catalogue-supported scoring, execution-plan reductions, and tail-signature reuse
  bring the full stage to 26.954 seconds at 16 GB while preserving every measured
  top-one output.
- The sub-45-second complete-stage target is met with 18.046 seconds of headroom.
- Safe-core top-two accuracy exceeds 95%; a held-out margin cascade retains that
  level while adding a second blocking key to only 6.7% of predicted addresses.

## Reproduction

```bash
uv run python -m benchmarking.road_parser_performance
uv run python -m benchmarking.road_parser_performance --modulus 20
uv run python -m benchmarking.road_parser_phase_benchmark --modulus 1 \
  --output benchmarking/results/road_scoring_experiment/phase_benchmark_full_2026-08-29.json
uv run python -m benchmarking.road_parser_phase_benchmark --modulus 1 \
  --production-only --input-policy canonical --road-chunks 4 \
  --memory-limit 16GB --require-catalogue-support --no-truth --no-validation \
  --output benchmarking/results/road_scoring_experiment/production_benchmark_full_signature_dedup_16gb.json
uv run python -m benchmarking.road_parser_accuracy_audit --modulus 20
uv run pytest tests/test_roadlike_place_stage.py -q
```

The full runtime command skips redundant validation scans. Output and oracle
validation use the fixed 1/8 and 1/2 hash partitions described above.