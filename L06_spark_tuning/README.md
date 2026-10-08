# L06 — Spark performance: shuffles, joins, skew, AQE, partitioning, caching

| | |
|---|---|
| Module | 5 — Spark II (performance) |
| Time | 2 h |
| Stack | Spark **4.1.3** (`spark:4.1.3-python3`), local mode `local[4]`, driver 2 GB |
| RAM | ~2.5 GB; disk: ~1 GB of temporary shuffle files inside the container |
| Prerequisites | L05. Taxi 2024-01 downloaded |

## Learning objectives

Measure — not guess — the effect of the most important Spark tuning knobs:

1. `spark.sql.shuffle.partitions` and **AQE partition coalescing** (E1)
2. **Broadcast hash join vs sort-merge join**, `autoBroadcastJoinThreshold`, the `broadcast()` hint (E2)
3. **Data skew** in a join; fixing it with **salting** and with **AQE skew-join** handling (E3)
4. **repartition vs coalesce** before a write: time, file count, parallelism (E4)
5. **cache / persist**: when it pays off (E5)

…and record every run in a **results table**.

## How the experiments are measured

[`src/l06_experiments.py`](src/l06_experiments.py) runs each variant, forces full execution with the
**noop** sink (`df.write.format("noop")` — computes everything, writes nothing) and then asks the
**Spark REST API** (the data behind the UI) for:

* the **final physical plan** after AQE (e.g. `SortMergeJoin(skew=true)`, `AQEShuffleRead`, `BroadcastHashJoin`)
* the **total number of tasks** of the query
* the **median and max task time of the most skewed stage** — skew shows up as max ≫ median

Results are printed as a Markdown table and appended to `results/results.csv`.

```
E3 data: fact = spark.range(10 M) with key = 0 for 60 % of rows (the "hot key"), uniform otherwise
         dim  = 200 k keys
   skewed shuffle:  partition(hash(0)) gets 6 M rows ──► 1 straggler task while the other cores sit idle
   salting:         (key, salt) with salt ∈ [0, 64) for the hot key ──► hot rows spread over all partitions
   AQE skew join:   at runtime Spark detects the oversized shuffle partition and splits it
```

## Tasks

### 0. Start

```bash
cd labs/L06_spark_tuning
docker compose up -d
docker compose exec spark spark-submit /lab/src/l06_experiments.py --only E1
```

`--only E1 E2 ...` runs selected experiments; no flag runs all (≈ 1 min). Add `--hold 600` to keep
the Spark UI (<http://localhost:4040>) open after the run — use the **SQL / DataFrame** tab to see
the plans and the **Stages** tab → *Event timeline* / *Summary metrics* to see skew.

### E1 — shuffle partitions and AQE coalescing (TODO 1)

The query is `groupBy(PULocationID, DOLocationID).agg(count, avg)` on 3 M trips. The script runs it
with AQE **off** and `shuffle.partitions` = 200, 8, 1. Complete **TODO 1**: run it again with AQE **on**
and 200 partitions.

Expected pattern: 200 partitions → 204 tasks and the slowest run (task scheduling overhead for tiny
tasks); 8 → fast; AQE on + 200 → `AQEShuffleRead` coalesces them into a few partitions → as fast as
hand-tuned.

### E2 — broadcast vs sort-merge join (TODO 2)

`trips ⋈ zones` (265 rows). Variants: threshold 10 MB (broadcast), threshold -1 (sort-merge, both
sides shuffled and sorted). Complete **TODO 2**: keep the threshold at -1 but use the
`F.broadcast(zones_df())` hint.

Look at the plan column: `BroadcastExchange BroadcastHashJoin` vs `Exchange SortMergeJoin`.

### E3 — skewed join (TODO 3, TODO 4)

1. Baseline: sort-merge join with AQE off → look at `task max ms` vs `task median ms`.
2. **TODO 3 — salting**: add a random `salt` (0..63) to the hot key only, replicate the dimension row of the
   hot key 64 times, join on `(key, salt)`. The script checks that the result is identical.
3. **TODO 4 — AQE skew join**: enable AQE and lower the skew thresholds to lab size
   (`skewedPartitionThresholdInBytes=16m`, `skewedPartitionFactor=3`, `advisoryPartitionSizeInBytes=4m`).
   The plan must show `SortMergeJoin(skew=true)`.

### E4 — repartition vs coalesce

Writes the trips to Parquet 4 ways: `repartition(64)`, `repartition(4)`, `coalesce(4)`, `coalesce(1)`.
Compare time, files and tasks. Why does `coalesce(4)` produce only 3 files here? Why is `coalesce(1)`
dangerous on big data even though it avoids a shuffle?

### E5 — cache

A "cleaned" DataFrame (filter + `dropDuplicates` = a full shuffle) is used by three aggregations —
first without cache, then with `persist(MEMORY_AND_DISK)` (materialised by a `count()`, timed
separately). Open the *Storage* tab with `--hold`.

### Record your results

Run everything twice and copy the second table into [`results/RESULTS.md`](results/RESULTS.md) with
your observations. Reference numbers (Apple M-series, Docker 4 GB, `local[4]`):

| experiment | variant | seconds | total tasks | median ms* | max ms* | plan (final, after AQE) |
|---|---|---|---|---|---|---|
| E1 | AQE off, shuffle.partitions=200 | 1.29 | 204 | 4 | 28 | Exchange |
| E1 | AQE off, shuffle.partitions=8 | 0.20 | 12 | 11 | 15 | Exchange |
| E1 | AQE off, shuffle.partitions=1 | 0.23 | 5 | 144 | 146 | Exchange |
| E1 | AQE on, shuffle.partitions=200 | 0.21 | 5 | 98 | 105 | AQEShuffleRead Exchange |
| E2 | AQE off, broadcast (10 MB) | 0.60 | 13 | 31 | 42 | BroadcastExchange BroadcastHashJoin |
| E2 | AQE off, sort-merge (-1) | 0.97 | 21 | 230 | 458 | Exchange SortMergeJoin |
| E2 | AQE on, -1 + broadcast() hint | 0.24 | 6 | 111 | 120 | BroadcastHashJoin |
| E3 | skewed SMJ, AQE off | 2.45 | 40 | 110 | 1263 | Exchange SortMergeJoin |
| E3 | salted SMJ (salt=64), AQE off | 2.97 | 56 | 400 | 935 | Exchange SortMergeJoin |
| E3 | skewed SMJ, AQE skewJoin on | 1.73 | 43 | 113 | 477 | AQEShuffleRead SortMergeJoin(skew=true) |
| E4 | repartition(64) | 5.59 | 68 | | | 64 files |
| E4 | repartition(4) | 3.26 | 8 | | | 4 files |
| E4 | coalesce(4) | 1.85 | 4 | | | 3 files |
| E4 | coalesce(1) | 3.58 | 1 | | | 1 file |
| E5 | no cache (3 queries) | 4.11 | | | | |
| E5 | cache materialisation | 2.34 | | | | |
| E5 | with cache (3 queries) | 0.70 | | | | |

\* median / max task time of the most skewed stage. Timings vary ±20 % between runs; salting was
between 10 % faster and 20 % slower than the baseline in repeated runs (see Q3).

## Checkpoint questions

1. Why is 200 shuffle partitions slow here, and why is `shuffle.partitions=1` a bad idea on real data even though it was fast?
2. What exactly does `AQEShuffleRead coalesced` do, and what information does AQE have that the static planner did not?
3. In E3, which number proves skew? Why did salting cut the straggler but not always the wall-clock time on 4 cores? When does it win clearly?
4. How does AQE decide a partition is skewed, and what does it do with it? What are its limits compared to salting?
5. When would you *not* want a broadcast join even if one side is "small"?
6. `repartition(n)` vs `coalesce(n)`: which one shuffles? Which one can create unbalanced output files? Which one reduces the parallelism of the *previous* stage?
7. In E5, why did caching help so much here, while caching a cheap-to-recompute DataFrame (e.g. a plain, column-pruned Parquet scan) often gives little or no benefit?
8. Give a tuning checklist (5 items) you would apply before throwing more hardware at a slow Spark job.

## Stretch challenges

* Rerun E3 with `L06_FACT_ROWS=25000000` (`docker compose exec -e L06_FACT_ROWS=25000000 spark spark-submit ...`) and after editing `HOT_SHARE = 0.9` in the script — how do the three strategies scale?
* E2 with a *large* dimension: join trips with a 5 M-row synthetic table and compare
  `spark.sql.join.preferSortMergeJoin=false` (shuffled hash join).
* Bucketing in Spark: `df.write.bucketBy(16, "PULocationID").sortBy("PULocationID").saveAsTable(...)`
  twice and join the two tables — is there an `Exchange` in the plan?
* Compare `persist(StorageLevel.MEMORY_ONLY)` vs `DISK_ONLY` vs `MEMORY_AND_DISK_SER`-style storage in the Storage tab.
* Turn on `spark.sql.adaptive.localShuffleReader.enabled=false` and explain the plan difference in E2.

## Cleanup

```bash
docker compose down -v
rm -f results/results.csv
```

## Troubleshooting

| Symptom | Fix |
|---|---|
| Table shows `(REST API unavailable ...)` | The UI is disabled or on another port (another app holds 4040 → 4041). The timings are still valid |
| E3 baseline shows no skew | Broadcast threshold not -1 or AQE left on — check the plan column |
| `SortMergeJoin(skew=true)` never appears | TODO 4 thresholds missing/too high; coalescing must be off for a clean comparison |
| `No space left on device` | Shuffle files: free Docker disk; use a smaller `L06_FACT_ROWS` |
| Very different numbers between runs | Laptop in power-saving mode / other apps; run twice and take the second run |
