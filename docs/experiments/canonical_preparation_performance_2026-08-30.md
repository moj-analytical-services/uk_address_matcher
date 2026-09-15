# Canonical preparation performance, 2026-08-30

## Scope

This report measures the public `prepare_canonical_folder()` path on the configured
71,438,939-row NGD parquet, not the road parser in isolation. The benchmark machine
used 14 DuckDB threads, a 16 GB DuckDB memory limit, 10 work chunks, and eight
canonical output shards. Peak RSS is the process high-water mark reported by macOS.

The High-equivalent target is at most 480 seconds without changing canonical row
values, term frequencies, IDs, ordering, or inverted-index postings. Proposed
Medium and Low profiles may omit features only after labelled end-to-end accuracy
measurement.

## Current refactored result

The current spill-aware v2 path completed in **537.531s (8m 57.5s)**. This is the
current production timing: 39.0% faster than optimized v1, 72.0% faster than the
matched road-enabled baseline, and 42.4% faster than the no-road baseline while
retaining road features. It remains 57.531s above the strict 480s High-profile
target.

Three boundaries are useful when discussing how long "cleaning" takes:

| Boundary | Elapsed | Meaning |
| --- | ---: | --- |
| Foundational address cleaning complete | 61.672s (1m 01.7s) | Address normalization, parsing, and pre-TF feature derivation |
| Canonical rows finalized | 328.845s (5m 28.8s) | IDs, adjacent distinguishing, TF features, and all finalized canonical columns |
| Prepared folder complete | 537.531s (8m 57.5s) | Canonical rows plus indexes, roads, Parquet serialization, and manifest |

| Measure | Result |
| --- | ---: |
| Canonical rows | 71,438,939 |
| Term-frequency rows | 926,832 |
| Inverted-index rows | 61,562,885 |
| Roads assigned | 62,859,358 |
| Peak RSS | 18,661,588,992 bytes (17.38 GiB) |
| Maximum observed spill | about 97 GB |
| Prepared output | 1,928,966,850 bytes |
| Size change from road baseline | +2.20% |

The measured v2 boundaries were:

| Stage | Time | Runtime share | Cumulative |
| --- | ---: | ---: | ---: |
| Shared foundational cleaning | 61.672s | 11.47% | 1m 01.7s |
| Deterministic ID assignment | 111.690s | 20.78% | 2m 53.4s |
| TF aggregation from shared clean data | 2.442s | 0.45% | 2m 55.8s |
| Adjacent distinguishing | 71.897s | 13.38% | 4m 07.7s |
| Post-TF processing | 80.876s | 15.05% | 5m 28.6s |
| In-place finalization | 0.268s | 0.05% | 5m 28.8s |
| Bigram and trigram indexes | 65.606s | 12.21% | 6m 34.5s |
| Road enrichment under integrated spill pressure | 64.715s | 12.04% | 7m 39.2s |
| TF/index serialization and canonical exports | 76.720s | 14.27% | 8m 55.9s |
| Manifest finalization | 1.277s | 0.24% | 8m 57.2s |

These boundaries account for 537.163s, or 99.93% of wall time; unlabelled setup and
transitions total only 0.368s. End-to-end throughput is approximately 132,900 input
rows per second, or 7.52 seconds per million rows. Index construction emits about
938,000 index rows per second, road enrichment assigns about 971,000 roads per
second, and the combined artifact serialization phase writes at an effective
25.1 MB/s.

The range path cut post-TF work by 140.758s and in-place finalization saved
49.162s. Preserving the processed table's physical ID locality also reduced artifact
serialization and canonical export from 239.655s to 76.720s. The full v2 audit found
zero TF or index differences and zero physical-order violations in all eight shards.
It found 802 bidirectional canonical differences, representing 401 rows; all become
zero after normalizing only the order of `unusual_tokens_arr` and
`extremely_unusual_tokens_arr`. This is the expected deterministic equal-frequency
tie correction, with every other persisted value unchanged.

## Historical baselines

The completed no-road run produced:

| Measure | Result |
| --- | ---: |
| Rows | 71,438,939 |
| Total | 933.141s (15m 33.1s) |
| Peak RSS | 20,087,422,976 bytes (18.71 GiB) |
| Maximum observed DuckDB spill | at least 70 GB |
| Prepared canonical output | 1,865,917,369 bytes |
| Term-frequency rows | 926,832 |
| Inverted-index rows | 61,562,885 |

Its retained artifact timestamps provide only a coarse split: derivation and TF
serialization took about 724s, index serialization 28s, eight canonical writes
about 180s, and manifest finalization about 1s.

A matched road-enabled diagnostic run retained event boundaries. The exact observed
increments through inverted-index completion were:

| Rank | Stage or interval | Time |
| ---: | --- | ---: |
| 1 | Trigram inverted index | 388.437s |
| 2 | Bigram inverted index | 342.515s |
| 3 | TF application | 222.694s |
| 4 | ID assignment plus adjacent distinguishing | 159.851s |
| 5 | Post-TF finalization plus road enrichment | 80.062s |
| 6 | Foundational canonical cleaning | 63.568s |
| 7 | TF cleaning | 33.950s |
| 8 | Index setup/count | 20.703s |
| 9 | Corpus TF aggregation/materialization | 4.045s |

That run completed in **1,918.331s (31m 58.3s)** with a 20,078,952,448-byte peak
RSS, 80 GB maximum observed spill, and 1,887,504,770 bytes of output. Roads were
assigned to 62,859,358 rows. Output grew by only 1.16% versus no-road, while wall
time grew by 105.6%, proving that file size does not explain the regression.

The eight output loops took approximately 49.0s, 50.3s, 80.3s, 64.4s, 59.9s,
68.1s, 60.3s, and 115.5s, or 548.8s combined. Each interval includes its redundant
post-write recount and any lazy upstream road evaluation. Manifest finalization
took another 1.432s.

## First optimized full run (v1)

The first complete optimized road-enabled run took **881.550s (14m 41.6s)**. This
is 54.0% faster than the matched 1,918.331s road-enabled baseline and 5.5% faster
than the 933.141s no-road baseline, but it does not yet meet the 480s target.

| Measure | Result |
| --- | ---: |
| Canonical rows | 71,438,939 |
| Term-frequency rows | 926,832 |
| Inverted-index rows | 61,562,885 |
| Roads assigned | 62,859,358 |
| Peak RSS | 19,088,146,432 bytes (17.78 GiB) |
| Maximum observed spill | about 74 GB |
| Prepared output | 1,928,971,264 bytes |
| Size change from road baseline | +2.20% |

The measured stage boundaries were:

| Stage | Time |
| --- | ---: |
| Shared foundational cleaning | 62.080s |
| Deterministic ID assignment | 102.916s |
| TF aggregation from shared clean data | 1.595s |
| Adjacent distinguishing | 57.583s |
| Post-TF processing | 221.634s |
| Final full-table projection copy | 49.430s |
| Bigram index | 38.433s |
| Trigram index | 36.513s |
| Road enrichment under integrated spill pressure | 69.552s |
| TF/index serialization and canonical exports | 239.655s |
| Manifest finalization | 1.343s |

The two indexes fell from 730.969s to 74.946s, a 9.75x improvement. Canonical
output loops and associated serialization fell from 548.8s to 239.7s. The full
result instead identifies post-TF processing and its redundant final projection
copy as the next controlling path. The latter copies all 71.4M wide rows only to
remove the private `__ukam_row_id` column.

A bounded per-shard `EXCEPT ALL` audit found zero TF and index differences. It
found 57 canonical rows whose only changed field was the ordering of equal-frequency
tokens inside `unusual_tokens_arr` or `extremely_unusual_tokens_arr`; membership and
all other fields were identical. `list_grade_up` had no explicit tie-breaker. The
pipeline now uses original token position as the deterministic secondary key and
has a focused regression test for that rule.

The final projection copy is also removed. A 5.10M-row low-memory comparison took
0.367s for CTAS versus 0.0018s for `ALTER TABLE DROP COLUMN`, with zero differences
(207x faster). At full scale this removes a measured 49.430s wide-table copy and
reduces downstream spill pressure.

## Where the original slowdown was contained

The 31m 58.3s road-enabled run divides approximately as follows. Percentages use
the full process wall time; the serialization residual is inferred from adjacent
event boundaries rather than an isolated operator timer.

| Area | Time | Share | What dominates |
| --- | ---: | ---: | --- |
| Bigram and trigram index construction | 731.0s | 38.1% | Repeated scans, tokenization, key derivation, aggregation, and lazy road replay |
| Eight canonical output loops | 548.8s | 28.6% | Sorted writes, one redundant recount per shard, and lazy road replay |
| Term-frequency application | 222.7s | 11.6% | Token explosion, lookup join, grouping, sorting, and list reconstruction |
| ID assignment and adjacent distinguishing | 159.9s | 8.3% | Global deterministic sorts and six-neighbour suffix comparisons |
| Post-TF finalization and road enrichment | 80.1s | 4.2% | Final wide materialization plus road keys/cardinalities |
| Foundational canonical cleaning | 63.6s | 3.3% | Normalization, numeric parsing, and tokenization |
| TF/index serialization residual | ~53.1s | 2.8% | Writing the TF and 61.6M-row index parquet files |
| TF cleaning and aggregation | 38.0s | 2.0% | A second cleaning pass followed by corpus token aggregation |
| Index setup/count | 20.7s | 1.1% | Input cardinality and setup queries |
| Manifest/finalization | 1.4s | 0.1% | Hashing and metadata write |

About two thirds of elapsed time is concentrated in index construction and output
loops. Foundational address cleaning is only 3.3%, and the isolated road scorer is
only 1.4% of total wall time. The regression is therefore not primarily expensive
normalization or model arithmetic; it is repeated execution around materialization
boundaries.

## What happens behind the scenes

Canonical preparation does not train or run the final Splink linkage model. It
builds the normalized evidence and lookup structures that later matching stages
consume:

1. The source is minimally cleaned once to derive corpus token frequencies.
2. The source is cleaned again to derive canonical address, flat, business-unit,
   numeric, range, and token features.
3. DuckDB globally sorts those rows to assign deterministic `ukam_address_id`
   values. Adjacent distinguishing then performs another suffix-oriented global
   ordering and examines three neighbours on each side.
4. Token frequencies are attached to every address. In the baseline this explodes
   token arrays to rows, joins 926,832 frequencies, groups by address, sorts token
   positions, and rebuilds each array.
5. The road scorecard selects one preferred source row per UPRN, generates candidate
   road phrases, applies its SQL scorecard, and derives global road/cardinality
   flags. This isolated computation takes 26.954s.
6. The physical blocker builds bigram and trigram posting lists. Ten key-hash
   buckets per strategy keep grouping within 16 GB and preserve global posting
   caps, but the baseline regenerates all keys for every bucket.
7. DuckDB writes the TF table, the 61,562,885-row inverted index, and eight sorted
   canonical parquet shards, then hashes them into the manifest.

The critical DuckDB behavior is laziness: a relation represents a SQL plan, not a
cached result. Reusing `relation.sql_query()` in another query can execute the full
upstream plan again. The baseline placed lazy road cardinality joins before the 20
index bucket queries and reused that relation in every output shard. Each `COPY`
was also followed by a `COUNT(*)` over the same shard query. Consequently a small
1.16% increase in persisted bytes caused a 105.6% increase in wall time.

`CREATE TEMPORARY TABLE ... AS SELECT ...` is the deliberate cache boundary used
by the optimized path. It spends temporary disk once so later index, count, and
export consumers scan materialized columns instead of replaying the model-derived
SQL graph. The permitted 10% output-size increase gives ample room for persisted
sidecars if useful, although the current changes add temporary staging rather than
inflating final artifacts.

## Main finding

The optimized road assignment itself takes 26.954s for all 71.4 million rows. The
large end-to-end road penalty instead comes from orchestration: road enrichment is
a lazy relation placed before inverted-index construction. The index scans that
relation for each strategy and each key-hash bucket even though index SQL uses only
`unique_id` and `clean_full_address`. Output shards can evaluate the same upstream
road joins again.

The first correction is therefore structural:

1. Materialize the base cleaned canonical rows.
2. Build the index from only `unique_id` and `clean_full_address` in that base.
3. Derive road keys after index completion.
4. Explicitly materialize road-enriched rows once.
5. Count and export only from that materialization.

This preserves persisted values and index semantics while removing accidental road
recomputation. A before/after relational equality check remains mandatory.

This correction is now implemented: index construction precedes road enrichment,
the enriched relation is materialized inside the road helper, and shard counts use
contiguous ID boundaries instead of re-running each query. Canonical export now
sorts by the integer ID that already encodes the full deterministic order. The
focused optimization suite passes 101 tests, including a regression test that drops
the road-key staging table before reading the enriched result. A full matched
benchmark remains the performance acceptance gate.

### Reproducible 1M-row findings

A persisted, UPRN-coherent sample (`hash(unique_id) % 71 = 0`) contains 1,004,772
rows. Isolated stages use 14 threads, a 16 GB DuckDB limit, and report zero spill:

| Experiment | Baseline | Candidate | Result |
| --- | ---: | ---: | --- |
| Road enrichment | n/a | 1.466s | Road scoring is not the dominant cost |
| TF attachment SQL | 0.147s exploded join | 17.570s map lookup | Map rejected: 119x slower |
| Six key generators | n/a | 1.089s total | Individual generators take 0.110-0.203s |
| Physical index | 2.882s repeated generation; 0.912s list staging | 0.801s scalar staging | 3.60x vs historical, 1.14x vs list staging |
| Four-chunk post-TF orchestration | 2.075s materialized chunks | 1.189s direct relations | 1.74x faster |
| Deterministic ID assignment | n/a | 0.462s | Moderate sort/materialization cost |
| Adjacent distinguishing | 0.554s repeated tokenization | 0.421s carried token arrays | 24.0% faster |

All TF, index, post-TF, and adjacent comparisons returned zero bidirectional
`EXCEPT ALL` differences. The saved JSON, plans, and profiles are under
`benchmarking/results/preprocessing_1m/`.

A 5,104,699-row pressure test confirmed the index direction: scalar staging took
2.720s, list-array staging 3.230s, and repeated generation 9.373s, again with zero
differences and no spill. Peak RSS reached 12.13 GB while the benchmark retained all
three comparator outputs, so this is evidence for runtime scaling but not a substitute
for the full-run memory gate.

The optimized production path was then exercised end to end with roads enabled:

| Sample | Wall time | Peak RSS | Spill | Output size |
| --- | ---: | ---: | ---: | ---: |
| 1,004,772 rows | 22.078s | 4.78 GB | 0 | 74 MB |
| 5,104,699 rows, optimized v1 | 35.925s | 13.62 GB | 0 | 288 MB |
| 5,104,699 rows, spill-aware v2 | 33.657s | 13.84 GB | 0 | 288 MB |

On 5.10M rows, pre-cleaning took 11.681s, ID assignment 1.016s, adjacent
distinguishing 2.012s, post-TF processing 7.374s, both indexes 2.899s, roads
2.409s, and final artifact writing 8.046s. The manifest reports 5,104,699
canonical rows, 259,646 TF rows, and 9,633,787 index rows.

## Other controlling costs

### Inverted index

Baseline bigram and trigram construction performs 20 passes: two strategies times
10 key-hash buckets. Each pass rescans addresses, tokenizes `clean_full_address`,
derives all keys, and filters the list to one bucket. The production path now
unnests each strategy once into a narrow `(unique_id, key, key_bucket)` table ordered
by bucket, then aggregates the 10 authoritative buckets without repeated list
filtering or `UNNEST`. On 1M rows this takes 0.801s versus 0.912s for list-array
staging and 2.882s for historical repeated generation; at 5.10M rows the respective
times are 2.720s, 3.230s, and 9.373s. Both samples have identical postings.
At full scale both optimized indexes took 65.606s with zero posting differences;
peak process RSS for the complete run was 17.38 GiB.

Bucket ownership must remain `hash(key)`: partitioning source rows by `unique_id`
would split one key's postings across buckets and make global posting caps wrong.

### Term-frequency application

Canonical preparation previously ran foundational cleaning once to derive TF and
again to prepare canonical rows. The production path now materializes that pre-clean
result once and derives TF directly from it. On 1M rows, the standalone raw branch
took 2.449s and reuse took 0.036s (68x); on 5.10M rows they took 3.437s and
0.127s (27x). Both comparisons returned zero TF differences.

Each work chunk filters the full clean table by a hash expression, inserts processed
rows, and deletes those rows from the source. Materializing each filtered chunk before
the post-TF pipeline was redundant; passing the filtered relation directly reduced
the four-chunk 1M stage from 2.075s to 1.189s with identical output. Range partitioning
was slower in memory and was initially rejected. The full run showed that direct
hash filtering still takes 221.634s under spill. At 5.10M rows with a forced 4 GB
limit, direct contiguous-ID ranges without per-chunk deletes took 6.440s versus
7.710s for direct hash filtering, 16.5% faster, with zero exact differences. This
spill-aware range path is now used in production. The final full-table copy has also
been replaced by an in-place private-column drop.

The inner TF operation explodes every token array, joins the frequency table, groups
by row, sorts positions, and reconstructs the array. It takes only 0.147s on 1M rows.
A reusable `MAP(token, rel_freq)` with `list_transform` produced identical values but
took 17.570s, approximately 119x slower, so the existing exploded join remains.

### Adjacent distinguishing

The six-neighbour calculation globally sorts by reversed address. Carrying the
already-tokenized arrays through the same ordered window avoids tokenizing each of
the six lag/lead addresses separately. The 1M stage falls from 0.554s to 0.421s with
identical output; all six neighbours and deterministic tie ordering are preserved.

### Export

Every baseline shard performs a filtered sorted `COPY` and then re-executes the
shard query with `COUNT(*)` only for logging. Road-enabled shard 1 spent about 30s
writing and 19s recounting. Optimized v1 uses the exact contiguous ID-range count
and `ORDER BY ukam_address_id`; tests confirm that physical row order is unchanged.

Compression was a material independent cost. On 5.10M rows, ZSTD levels 15, 9,
and 6 took 8.674s, 2.473s, and 1.992s. Level 6 increased bytes by 4.31%, within
the allowed 10%, and produced zero row differences and zero order violations, so it
is now used for canonical and TF output. Level 3 took 1.149s on 1M rows but increased
bytes by 12.06%, so it was rejected. Building one sorted staging table took 9.139s
at 5.10M versus 8.674s for the existing independent writes and had 19 physical-order
violations; that candidate was also rejected.

Adding `ORDER BY ukam_address_id` back to staged ZSTD 6 writes removed the order
violations but still took 2.043s versus 2.005s for direct ZSTD 6 writes at 5.10M.
The ordered staging variant is therefore also rejected.

## Remaining exact-output opportunities

The accepted row-preserving work reduced the matched road-enabled run from
1,918.331s to 537.531s. The strict High profile still needs 57.531s, but no single
low-risk change is known to recover that amount. The remaining exact-output work
should be a bounded experiment round, not open-ended expression tuning.

| Priority | Candidate | Evidence and ceiling | Exact-output condition |
| ---: | --- | --- | --- |
| 1 | Memory-limit pressure test | The run spilled about 97 GB under a 16 GB DuckDB cap; 24 GB and 32 GB runs could reduce both global-sort and export pressure without changing SQL semantics | IDs, values, postings, shard order, and output-size limit must still pass |
| 2 | Narrow deterministic ID assignment | The 111.690s stage globally sorts 71.4M wide rows and currently appends every source column as a tie-breaker | A primary-key tie audit must prove that a narrower key preserves the exact current ID-to-row mapping |
| 3 | Earlier narrow road-key staging | Integrated road enrichment takes 64.715s versus 26.954s in isolation, giving a strict maximum recoverable gap of 37.761s | Join staged keys after spill-heavy processing and require exact road columns and cardinalities |
| 4 | Work-chunk sweep | Post-TF range processing still takes 80.876s with 10 chunks; a small 5/10/20 full-pressure matrix may improve spill behavior | Exact canonical equality and peak disk/RSS gates remain mandatory |

Adjacent distinguishing is not a low-hanging exact-output target. It already sorts a
narrow keyed relation, carries token arrays through the six-neighbour window, and
joins only the two derived columns back by `__ukam_row_id`. Removing it would save up
to 71.897s and likely cross 480s, but that changes canonical artifacts and remains a
Medium-profile accuracy trade-off.

The recommended stopping rule is to run the memory-limit and narrow-ID experiments
first. Continue to another full 71.4M-row acceptance run only if a pressure test
demonstrates at least 20-30s of plausible end-to-end savings with exact logical
output. Otherwise, 8m57.5s is a reasonable High-profile stopping point: the pipeline
is already 3.57x faster than the matched road-enabled baseline, and reaching eight
minutes would require combining several individually uncertain changes.

## Proposed profiles

| Capability | High accuracy | Medium/default | Low latency |
| --- | --- | --- | --- |
| Corpus TF | Required | Required | Packaged TF candidate |
| Physical indexes | Bigram and trigram | Bigram and trigram | Trigram-only candidate |
| Adjacent distinguishing | Required | Off pending accuracy gate | Off |
| Numeric/signature features | Required | Required | Keep pending timing evidence |
| Road keys | Full selected scorer | Only when road blocking is configured | Off |
| Adaptive second road key | Optional calibrated feature | Off | Off |
| Output policy | Maximum locality | Balanced | Faster compression candidate |

High is the exact-output performance target. Medium and Low are experiment designs,
not claims about accuracy: publish them only after measuring candidate reachability
and labelled precision, recall, and F1 against High.

## Reproducibility

The benchmark harness writes incremental events to
`benchmarking/results/road_scoring_experiment/*.events.jsonl`, so completed stage
timings survive an interrupted run. The detailed implementation and verification
sequence is in `plans/001-cut-canonical-preparation-under-eight-minutes.md`.

The historical roughly five-minute artifact is not an equivalent baseline: it used
UKAM 1.2.2, DuckDB 1.5.2, one output file, and no adjacent-distinguishing columns.
The current run uses UKAM 1.3.0 code, DuckDB 1.5.0, eight files, and adjacent
distinguishing. These variables require a controlled matrix before assigning the
threefold difference to one regression.