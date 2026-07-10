# ART-index blocking for realtime linkage: summary for GitHub

## Summary

The largest small-batch Splink cost is not the match-weight maths. It is the repeated canonical-side query work, especially the full-canonical scans around blocking and candidate rescoring.

The best results for working around this come from two related paths:
1. The fastest path is the **prefiltered ART-scored path**.
   It uses the ART index on the hashed inverted index to get candidate pairs, then prefilters the canonical to just those candidate `unique_id`s before running Splink's comparison and predict SQL.
2. The most faithful path is the **prefiltered full-block path**.
   It reproduces all Splink blocking rules, then prefilters the canonical to the blocked ids before scoring.

A stricter third path is now also validated:
3. The lowest-intrusion path is the **reduced-canonical stock `predict()` path**.
  It reproduces all blocking rules, materialises the blocked canonical subset, rebuilds a fresh linker on that subset, and then calls normal Splink `predict()`.

The most important practical point is this:
- The biggest gains arrive when the ART index is doing the heavy lifting for the inverted-index blocking step, and when canonical rescoring is limited to a tiny candidate-only slice.
- If we add back the other blocking rules, we gain fidelity and keep exact match weights, but some of the speed advantage is given back because we are again visiting Splink blocking SQL and assembling a larger candidate set.

## Headline findings

### Full canonical, 71,438,939 rows

| Row limit | ART candidate-gen | ART-scored prefiltered | Full-block prefiltered | Reduced-canonical stock `predict()` | Stock `predict()` |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 0.0105 s | 0.0239 s | 0.0394 s | 0.8972 s | 11.4543 s |
| 10 | 0.0976 s | 0.0445 s | 0.0767 s | 0.9140 s | 12.2631 s |
| 100 | 1.7169 s | 0.4870 s | 3.3959 s | 4.3498 s | 11.4229 s |

Key interpretation:

- **Prefiltered ART-scored** is the fastest measured end-to-end path for the inverted-index-only experiment.
  It delivered about **445x / 251x / 24x** speed-ups at row limits **1 / 10 / 100**.
- **Prefiltered full-block** is the faithful drop-in path.
  It delivered about **270x / 145x / 3.4x** speed-ups at row limits **1 / 10 / 100**, with **100% coverage** of baseline prediction pairs.
- **Reduced-canonical stock `predict()`** is the lower-intrusion path.
  It delivered about **12.8x / 13.4x / 2.6x** speed-ups at row limits **1 / 10 / 100**, with **100% coverage** of baseline prediction pairs and identical match weights, but it was still materially slower than the prefiltered full-block scorer.
- The earlier **~4 to 5x** result is now clearly understood as the cost of the old, non-prefiltered scoring path. It is not the floor of the ART approach.

### Fidelity

- Prefiltered ART-scored path: `max |Δ match_weight| = 0.00e+00` on shared pairs.
- Prefiltered full-block path: `max |Δ match_weight| = 0.00e+00`, with **100% shared-pair coverage** against baseline predictions.
- Reduced-canonical stock `predict()` path: `max |Δ match_weight| = 0.00e+00`, with **100% shared-pair coverage** against baseline predictions.

## What the strict stock `predict()` path tells us

The reduced-canonical stock `predict()` path answers the implementation question:
what if we do all the containment work up front, but then hand a much smaller
canonical back to normal Splink `predict()`?

The result is: **yes, it works**, and it is **meaningfully faster than stock**.
But it is **not** the best latency path.

Why it remains slower than the prefiltered full-block scorer:

- it still pays about **0.26 s** to rebuild a fresh linker per request
- it still pays about **0.59 to 0.64 s** for stock `predict()` even on a tiny filtered canonical
- at row limit 100, the dominant remaining cost is still full-block pair assembly plus canonical prefilter, not match-weight maths

Rule of thumb:

- if we want the **lowest-risk, closer-to-stock integration**, the reduced-canonical stock `predict()` path is valid
- if we want the **best realtime latency**, the prefiltered full-block scorer remains the better choice

## Follow-up linker and materialisation notes (2026-06-17)

Two follow-up probes were run after the main experiment to answer a narrower
question: is the residual stock-`predict()` cost partly self-inflicted by how we
hand reduced canonicals to Splink?

Findings:

- **The small linker floor is real.** On a 1-row messy batch with canonical
  subsets of **100 / 1,000 / 5,000 / 10,000** rows, `_get_linker()` stayed at
  about **0.39 to 0.43 s**. A manual breakdown put about **0.27 s** of that in
  Splink `Linker(...)` initialisation itself, with only small additional costs
  from our settings prep and `concat_with_tf` registration.
- **There was no evidence of accidental canonical copying inside the reduced
  path.** In this workspace Splink's DuckDB path registers relations/views; it
  does not eagerly copy the reduced canonical into a physical table just because
  `_get_linker()` is called.
- **Materialising the ART-filtered subset can help, but only modestly and not
  uniformly.** On the full-canonical ART database:
  - **row-limit 1**, **7** filtered canonical rows: total
    **0.845 s -> 0.779 s** (**1.08x faster**)
  - **row-limit 10**, **47** filtered canonical rows: total
    **0.810 s -> 0.821 s** (slightly worse once temp-table build cost is
    included)
  - **row-limit 100**, **997** filtered canonical rows: total
    **1.410 s -> 1.190 s** (**1.18x faster**)
  The row-limit-100 gain mostly appeared in `predict()` itself
  (**1.170 s -> 0.555 s**) rather than in linker build time.
- **Materialising the *full* canonical before stock `predict()` is actively
  harmful.** On the full Hackney dataset (**114,166** messy rows) against the
  full **71,438,939**-row canonical:
  - stock prepared relation: **0.466 s** linker build, **14.922 s**
    `predict()`, **18.969 s** total
  - materialised full canonical table: **0.313 s** linker build,
    **34.174 s** `predict()`, plus **14.904 s** one-off materialisation,
    **52.474 s** total
  - reverse-order confirmation still showed **29.839 s** materialised
    `predict()` versus **13.433 s** stock `predict()`, with identical
    prediction row counts (**3,417,643**)

Practical conclusion:

- **Promote materialisation only for the ART-filtered candidate subset, and only
  as a measured realtime optimisation.** The gain is real at row-limits 1 and
  100, but small enough that row-limit 10 was effectively neutral/slightly worse
  after paying the temp-table build cost.
- **Do not materialise the full canonical and then hand that full table back to
  stock Splink `predict()`.**

## Where the gains actually come from

The speed-up does **not** come from changing Splink's scoring maths. It comes from changing how much data the scoring SQL has to touch.

### Old path, about 2.5 seconds

The legacy ART-scored path already had cheap candidate generation, but it still scored against the full canonical view `c_`.
That meant DuckDB still had to revisit the wide canonical during comparison SQL.
On the full canonical, that left a residual cost of about **2.4 to 2.5 seconds**.

### New prefiltered ART path

The prefiltered ART-scored path does one extra thing before scoring:

1. Extract distinct candidate `canonical_uid`s from the ART candidate pairs.
2. Materialise a tiny temporary canonical table:
   `SELECT * FROM c_ WHERE unique_id IN (...)`
3. Build `__splink__blocked_id_pairs` against that tiny canonical slice.
4. Run the same Splink comparison and predict SQL, but point the left input at the filtered canonical table instead of full `c_`.

That is why the old **~2.5 s** path fell to **0.024 s / 0.045 s / 0.487 s**.
The scoring logic stayed the same. The input size changed.

## Why the full-block path is slower than the ART-only path

The full-block path is the correct choice when we need stock-blocking fidelity, but it is not the fastest possible path.

Reason:

- The inverted-index-only path lets the ART index drive blocking almost entirely.
- The full-block path adds back the 8 non-exploding Splink blocking rules.
- That means more blocking work, more pair assembly, and more time inside Splink-side SQL before scoring.

So the rule of thumb is:

- If the goal is the **fastest possible ART-based realtime scoring experiment**, use the **prefiltered ART-scored path**.
- If the goal is **faithful behaviour with stock-blocking recall**, use the **prefiltered full-block path**.
- If the goal is **faithful behaviour while keeping the normal Splink `predict()` call**, use the **reduced-canonical stock `predict()` path**, but expect a latency tradeoff.

## ART lookup planner behaviour

`EXPLAIN ANALYSE` showed that DuckDB changes plan shape as the batch gets larger.

| Row limit | Index scan | Sequential scan | Max reported rows |
| ---: | --- | --- | ---: |
| 1 | Yes | Yes | 9 |
| 10 | No | Yes | 56,887,704 |
| 100 | No | Yes | 61,312,633 |

This matters because it explains why candidate generation is extremely cheap at one row, but not yet at its floor for 10 or 100 rows.

The next major optimisation target is therefore not scoring. It is keeping the inverted-index lookup on ART probes instead of letting DuckDB flip to a very large sequential scan.

## ART-index build process

The build is a one-off cost and should be treated as a persisted deployment artefact, not query-time work.

### Build timings on the full canonical

| Phase | Seconds |
| --- | ---: |
| materialise canonical table | ~100 s |
| build string inverted index | ~4.5 s |
| build hashed inverted index | ~30.8 s |
| build `canonical_ukam_lookup` on existing databases upgraded to the new path | ~10.8 s |

### What is being built

```text
canonical_addresses
  - full prepared canonical rows
  - ART index on unique_id
  - ART indexes on postcode, numeric_token_1, numeric_token_2

inverted_index_string
  - key -> unique_ids[]

inverted_index_hashed
  - hash(key) -> unique_ids[]
  - ART index on key_hash

canonical_ukam_lookup
  - ukam_address_id -> unique_id
  - ART index on ukam_address_id
```

### Critical memory and performance lesson

DuckDB ART indexes over tens of millions of rows cannot be built safely with one large bulk `CREATE INDEX` after a full table CTAS. That caused a `bad_alloc` / SIGTRAP failure on macOS.

The stable build pattern was:

1. Create the table **empty**.
2. Create the ART indexes on the empty table.
3. `INSERT` the data so the index populates incrementally.
4. `CHECKPOINT` after large inserts.

That change was essential. Without it, the full-canonical build crashed. With it, the build completed reliably.

### Full-scale connection settings that mattered

```sql
SET enable_progress_bar = false;
SET threads = 4;
SET memory_limit = '8GB';
SET preserve_insertion_order = false;
SET temp_directory = '.../ukam_art_duckdb_tmp';
SET max_temp_directory_size = '200GB';
```

These settings mattered because ART build allocations and large checkpoints sit outside the spillable buffer pool, so we needed deliberate headroom.

## System flow

```text
                           +------------------------------------+
                           | Persisted ART database             |
                           |------------------------------------|
                           | canonical_addresses                |
                           |   - unique_id ART index            |
                           |   - postcode ART index             |
                           |   - numeric_token_1 ART index      |
                           |   - numeric_token_2 ART index      |
                           | inverted_index_hashed              |
                           |   - key_hash ART index             |
                           | canonical_ukam_lookup              |
                           |   - ukam_address_id ART index      |
                           +-------------------+----------------+
                                               |
                                               v
+----------------+      +-------------------+      +---------------------------+
| messy input    | ---> | clean + TF attach | ---> | derive bigram/trigram     |
| rows           |      |                   |      | keys and hash them        |
+----------------+      +-------------------+      +-------------+-------------+
                                                             |
                                                             v
                                           +-----------------+------------------+
                                           | ART lookup on inverted_index_hashed |
                                           | gives candidate canonical unique_id |
                                           +-----------------+------------------+
                                                             |
                                                             v
                                  +--------------------------+---------------------------+
                                  | Choose scoring mode                                     |
                                  |---------------------------------------------------------|
                                  | Fastest: prefiltered ART-scored                         |
                                  | Faithful: prefiltered full-block                        |
                                  +--------------------------+---------------------------+
                                                             |
                         +-----------------------------------+----------------------------------+
                         |                                                                      |
                         v                                                                      v
        +--------------------------------------+                         +--------------------------------------+
        | Prefilter canonical to candidate ids |                         | Reproduce all Splink blocking rules  |
        | SELECT * FROM c_ WHERE unique_id IN  |                         | then prefilter canonical to blocked  |
        +-------------------+------------------+                         | ids via canonical_ukam_lookup        |
                            |                                            +-------------------+------------------+
                            |                                                                |
                            +-------------------------+--------------------------------------+
                                                      |
                                                      v
                             +------------------------------------------------+
                             | Build __splink__blocked_id_pairs               |
                             +------------------------+-----------------------+
                                                      |
                                                      v
                             +------------------------------------------------+
                             | Run Splink comparison vectors + predict SQL    |
                             | against the filtered canonical slice           |
                             +------------------------+-----------------------+
                                                      |
                                                      v
                                          +----------------------------+
                                          | predictions + match_weight |
                                          +----------------------------+
```

### Most performant ART scoring logic

The fastest measured end-to-end path in this experiment was the prefiltered ART-scored path.
This is the core logic in simplified form.

<details>
<summary>Show the core prefiltered ART scoring logic</summary>

```python
# 1. Candidate pairs already exist from the ART lookup.
candidate_uids = [
    str(int(row[0]))
    for row in con.execute(
        f"SELECT DISTINCT canonical_uid FROM {candidate_pairs_table} ORDER BY canonical_uid"
    ).fetchall()
    if row[0] is not None
]

# 2. Prefilter the canonical to only those candidate ids.
filtered_canonical_table = f"__probe_art_c_filtered_{row_limit}"
con.execute(f'DROP TABLE IF EXISTS "{filtered_canonical_table}"')
if candidate_uids:
    candidate_uid_list = ", ".join(candidate_uids)
    con.execute(
        f'''
        CREATE TEMP TABLE "{filtered_canonical_table}" AS
        SELECT *
        FROM c_
        WHERE unique_id IN ({candidate_uid_list})
        '''
    )
else:
    con.execute(
        f'''
        CREATE TEMP TABLE "{filtered_canonical_table}" AS
        SELECT *
        FROM c_
        LIMIT 0
        '''
    )

# 3. Build blocked id pairs against the filtered canonical.
con.execute('DROP TABLE IF EXISTS "__splink__blocked_id_pairs"')
con.execute(
    f'''
    CREATE TEMP TABLE "__splink__blocked_id_pairs" AS
    SELECT DISTINCT
        'c_-__-' || CAST(c.ukam_address_id AS VARCHAR) AS join_key_l,
        'm_-__-' || CAST(m.ukam_address_id AS VARCHAR) AS join_key_r,
        '0' AS match_key
    FROM {candidate_pairs_table} AS p
    INNER JOIN "{filtered_canonical_table}" AS c
        ON p.canonical_uid = c.unique_id
    INNER JOIN m_ AS m
        ON p.messy_uid = m.unique_id
    '''
)

# 4. Run the usual Splink comparison and predict SQL,
#    but point the left input at the filtered canonical table.
left_input = f"(select *, 'c_' as source_dataset from \"{filtered_canonical_table}\")"
right_input = "(select *, 'm_' as source_dataset from m_)"

comparison_sqls = compute_comparison_vector_values_from_id_pairs_sqls(
    settings._columns_to_select_for_blocking,
    settings._columns_to_select_for_comparison_vector_values,
    input_tablename_l=left_input,
    input_tablename_r=right_input,
    source_dataset_input_column=source_dataset_column,
    unique_id_input_column=unique_id_input_column,
    link_type="two_dataset_link_only",
    sql_dialect_str=linker._sql_dialect_str,
)

pipeline.enqueue_list_of_sqls(comparison_sqls)
pipeline.enqueue_list_of_sqls(
    predict_from_comparison_vectors_sqls_using_settings(
        settings,
        threshold_match_weight=-20,
        sql_infinity_expression=linker._infinity_expression,
    )
)
```

</details>

## Environment and technology used for the experiment

- Machine: Apple Silicon MacBook Pro, M4, 36 GB RAM.
- Operating system: macOS.
- Core libraries: DuckDB, Splink, `uk_address_matcher`, `uv`.
- Prepared canonical dataset:
  - `ukam_canonical_addresses.parquet`
  - `ukam_inverted_index.parquet`
  - `ukam_term_frequencies.parquet`
  - `ukam_manifest.json`
- Full prepared canonical size: 71,438,939 rows.
- Full prepared inverted index size: 61,559,861 keys.
- Persisted auxiliary lookup: `canonical_ukam_lookup` with 71,438,939 rows.
- Validation artefacts:
  - full canonical run summary and results
  - full canonical reduced-canonical stock `predict()` run summary and results
  - Hackney residential run summary and results
- Match-weight equivalence: `max |Δ| = 0.00e+00` in all validated configurations.

## 1. Executive summary

For small real-time batches against the full 71,438,939 row prepared canonical,
stock Splink `predict()` costs about 10.6 to 11.7 seconds per request. The main
reason is not the match-weight maths. It is the canonical-side query shape,
especially the fixed cost around Splink's filtered concat behaviour.

The fastest and most useful optimised paths are now:

| Path | Recall shape | Full canonical timings (row limits 1 / 10 / 100) | Notes |
| --- | --- | --- | --- |
| ART candidate generation only | Partial recall | 0.0105s / 0.0976s / 1.7169s | Fastest possible blocking-only path |
| Prefiltered ART-scored path | Partial recall | 0.0239s / 0.0445s / 0.4870s | Best pure ART runtime path |
| Prefiltered full-block path | Full recall | 0.0394s / 0.0767s / 3.3959s | Best faithful drop-in path |
| Stock Splink `predict()` | Full recall | 10.6406s / 11.1607s / 11.6514s | Current baseline |

Key conclusions:

- The biggest speed-ups arrive when the ART index is used to drive the
  inverted-index blocking path and the canonical is prefiltered before scoring.
- The prefiltered ART-scored path is the fastest end-to-end runtime path, but it
  only reproduces the exploding inverted-index rule, so recall is not complete.
- The prefiltered full-block path is the best faithful path. It reproduces all
  blocking rules, keeps 100% coverage of baseline prediction pairs, preserves
  identical match weights, and is still dramatically faster than stock for small
  batches.
- Once canonical rescoring is prefiltered, the remaining bottleneck moves.
  For the ART-only path it is now the candidate lookup planner fallback.
  For the full-block path it is increasingly the blocking and candidate assembly
  work rather than scoring.

## 2. What changed

The original ART-scored experiment still paid about 2.4 to 2.5 seconds on the
full canonical even after candidate generation had been made cheap. The reason
was that Splink's comparison SQL was still effectively reading from the full
canonical-side input.

The important optimisation was to prefilter the canonical before scoring:

1. Generate candidate canonical `unique_id` values from the ART-backed inverted
   index.
2. Materialise a tiny canonical temp table using `WHERE unique_id IN (...)`.
3. Build `__splink__blocked_id_pairs` against that tiny canonical slice.
4. Run Splink's comparison-vector and predict SQL against the filtered
   canonical slice instead of the full `c_` view.

That change preserved identical match weights on shared pairs and collapsed the
legacy ART-scored cost from about 2.5 seconds to tens of milliseconds for the
smallest full-canonical batches.

## 3. Where the gains come from, and where they do not

### Where the gains come from

The large speed-ups come from using the ART index on the hashed inverted index
for blocking, then avoiding full-canonical rescoring by prefiltering the
canonical to the candidate ids.

That is why the prefiltered ART-scored path is so fast:

- row limit 1: 0.0239s
- row limit 10: 0.0445s
- row limit 100: 0.4870s

### Where the gains do not come from

The gains do not come from making all Splink logic cheap in general.

As soon as we reintroduce the non-exploding blocking rules, we are back inside
Splink's blocking process. That still performs well after canonical prefiltering,
but it is not as cheap as the inverted-index-only path.

That is why the faithful full-block path is slower than the ART-only path:

- row limit 1: 0.0394s
- row limit 10: 0.0767s
- row limit 100: 3.3959s

So the right interpretation is:

- ART-only path = best raw speed, partial recall
- Full-block path = best faithful behaviour, slower because it must still visit
  Splink blocking logic

## 4. Build process and operational lessons

### One-off build timings

For the full unfiltered prepared canonical:

| Phase | Time |
| --- | ---: |
| Materialise canonical table | about 100s |
| Build string inverted index | about 4.5s |
| Build hashed inverted index | about 30.8s |
| Build `canonical_ukam_lookup` on an existing upgraded database | about 10.8s |

### How the ART database is built

The build is not a simple bulk `CREATE INDEX` on a huge table. That approach
failed.

The working process is:

1. Create the table empty.
2. Create ART indexes on the empty table.
3. Insert the data so the ART structures populate incrementally.
4. Checkpoint after large inserts.

This pattern is used for:

- `canonical_addresses`
- `inverted_index_hashed`
- `canonical_ukam_lookup`

### Memory and performance issues

The critical failure mode was DuckDB bulk ART index creation on very large
relations.

Observed behaviour:

- bulk `CREATE INDEX` over the full 71M-row canonical crashed with
  `std::bad_alloc` / `SIGTRAP`
- the failure point was consistent, around database size 11.8 to 12.6 GB
- changing DuckDB `memory_limit` alone did not fix it

The stable fix was incremental ART population:

- create empty table
- create index
- insert rows

On the successful full-canonical build, peak resident memory rose much more
smoothly, to about 8.5 GB, and the build completed cleanly.

Connection settings used for the successful full-canonical runs:

```sql
SET enable_progress_bar = false;
SET threads = 4;
SET memory_limit = '8GB';
SET preserve_insertion_order = false;
SET temp_directory = '.../ukam_art_duckdb_tmp';
SET max_temp_directory_size = '200GB';
```

## 5. System flow

```text
+--------------------+
| messy input batch  |
+--------------------+
          |
          v
+------------------------------+
| clean and apply TF features  |
+------------------------------+
          |
          v
+----------------------------------+
| derive bigram and trigram keys   |
| hash each key                    |
+----------------------------------+
          |
          v
+----------------------------------+
| ART lookup on inverted_index     |
| key_hash -> candidate unique_ids |
+----------------------------------+
          |
          v
+-----------------------------------------------+
| candidate pairs table                          |
| (messy_uid, canonical_uid)                     |
+-----------------------------------------------+
          |
          +--------------------------------------+
          |                                      |
          | fastest ART-only path                | faithful full-block path
          |                                      |
          v                                      v
+----------------------------------+   +----------------------------------+
| prefilter canonical by           |   | run Splink non-exploding         |
| WHERE unique_id IN (...)         |   | blocking rules on raw canonical  |
+----------------------------------+   +----------------------------------+
          |                                      |
          |                                      v
          |                         +----------------------------------+
          |                         | union ART candidates with        |
          |                         | Splink rule-generated pairs      |
          |                         +----------------------------------+
          |                                      |
          |                                      v
          |                         +----------------------------------+
          |                         | prefilter canonical by blocked   |
          |                         | canonical ids                    |
          |                         +----------------------------------+
          |                                      |
          +-------------------+------------------+
                              |
                              v
+------------------------------------------------------+
| build __splink__blocked_id_pairs                     |
+------------------------------------------------------+
                              |
                              v
+------------------------------------------------------+
| Splink comparison vectors and predict SQL            |
| over filtered canonical only                         |
+------------------------------------------------------+
                              |
                              v
+-----------------------------+
| predictions and match score |
+-----------------------------+
```

## 6. Most performant ART logic

Below is the core logic for the fastest end-to-end ART scoring path. It is the
prefiltered ART-scored path, not the faithful full-block path.

<details>
<summary>Show core prefiltered ART scoring logic</summary>

```python
def _materialise_filtered_canonical(
    con: duckdb.DuckDBPyConnection,
    *,
    filtered_canonical_table: str,
    candidate_uids: list[str],
) -> float:
    started = perf_counter()
    con.execute(f'DROP TABLE IF EXISTS "{filtered_canonical_table}"')
    if candidate_uids:
        candidate_uid_list = ", ".join(candidate_uids)
        con.execute(
            f'''
            CREATE TEMP TABLE "{filtered_canonical_table}" AS
            SELECT *
            FROM c_
            WHERE unique_id IN ({candidate_uid_list})
            '''
        )
    else:
        con.execute(
            f'''
            CREATE TEMP TABLE "{filtered_canonical_table}" AS
            SELECT *
            FROM c_
            LIMIT 0
            '''
        )
    return perf_counter() - started


def run_art_scored_prefiltered_path(
    con: duckdb.DuckDBPyConnection,
    *,
    linker: Any,
    candidate_pairs_table: str,
    row_limit: int,
) -> tuple[ArtScoredResult, str]:
    result = ArtScoredResult(row_limit=row_limit)
    filtered_canonical_table = f"__probe_art_c_filtered_{row_limit}"

    candidate_uids = [
        str(int(row[0]))
        for row in con.execute(
            f"SELECT DISTINCT canonical_uid FROM {candidate_pairs_table} ORDER BY canonical_uid"
        ).fetchall()
        if row[0] is not None
    ]
    result.timings["prefilter_canonical"] = _materialise_filtered_canonical(
        con,
        filtered_canonical_table=filtered_canonical_table,
        candidate_uids=candidate_uids,
    )

    started = perf_counter()
    con.execute('DROP TABLE IF EXISTS "__splink__blocked_id_pairs"')
    con.execute(
        f'''
        CREATE TEMP TABLE "__splink__blocked_id_pairs" AS
        SELECT DISTINCT
            'c_-__-' || CAST(c.ukam_address_id AS VARCHAR) AS join_key_l,
            'm_-__-' || CAST(m.ukam_address_id AS VARCHAR) AS join_key_r,
            '0' AS match_key
        FROM {candidate_pairs_table} AS p
        INNER JOIN "{filtered_canonical_table}" AS c
            ON p.canonical_uid = c.unique_id
        INNER JOIN m_ AS m
            ON p.messy_uid = m.unique_id
        '''
    )
    result.timings["build_blocked_id_pairs"] = perf_counter() - started

    predictions_table = f"__probe_art_prefiltered_predictions_{row_limit}"
    score_timings = _score_existing_blocked_pairs(
        con,
        linker=linker,
        predictions_table=predictions_table,
        left_input_table=f'"{filtered_canonical_table}"',
    )
    result.timings.update(score_timings)
    return result, predictions_table
```

</details>

Why this is fast:

- it uses the ART-backed inverted index to get candidate canonical ids
- it restricts the canonical-side input before Splink scoring
- it reuses Splink's own comparison and predict SQL, so the scores stay aligned
  with the stock model
