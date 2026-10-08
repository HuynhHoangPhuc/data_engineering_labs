# L06 — Solutions

Reference code: [`l06_experiments.py`](l06_experiments.py); a reference results table:
[`reference_results_table.md`](reference_results_table.md) (Apple M-series, Docker 4 GB, `local[4]`).

## TODOs

```python
# TODO 1
set_conf(spark__sql__adaptive__enabled=True, spark__sql__shuffle__partitions=200)
run("E1", "AQE on,  shuffle.partitions=200", q(), "AQE coalesces small shuffle partitions")
# TODO 2
set_conf(spark__sql__adaptive__enabled=True, spark__sql__autoBroadcastJoinThreshold=-1)
run("E2", "AQE on,  threshold -1 + broadcast() hint",
    trips_df().join(F.broadcast(zones_df()), F.col("PULocationID") == F.col("LocationID"))
    .groupBy("Borough").agg(F.count("*").alias("trips")))
# TODO 3
hot = F.col("key") == 0
fact_s = fact.withColumn("salt", F.when(hot, (F.rand(seed=5) * SALT).cast("int")).otherwise(F.lit(0)))
salts = spark.range(SALT).select(F.col("id").cast("int").alias("salt"))
dim_s = dim.filter(~hot).withColumn("salt", F.lit(0)).unionByName(dim.filter(hot).crossJoin(salts))
salted = fact_s.join(dim_s, ["key", "salt"]).groupBy("segment").agg(F.sum("amount").alias("amount"))
# TODO 4
set_conf(spark__sql__adaptive__enabled=True, spark__sql__adaptive__skewJoin__enabled=True,
         spark__sql__adaptive__coalescePartitions__enabled=False,
         spark__sql__adaptive__advisoryPartitionSizeInBytes="4m",
         spark__sql__adaptive__skewJoin__skewedPartitionThresholdInBytes="16m",
         spark__sql__adaptive__skewJoin__skewedPartitionFactor=3)
```

## Checkpoint answers

1. 200 partitions for a result of ~20 k groups (a few MB) means 200 tiny tasks: scheduling,
   serialization and task start-up dominate (median task 4 ms, but 204 tasks ≈ 1.3 s). One partition is
   fast only because the data is tiny; on real data a single reduce task must process the whole shuffle
   — no parallelism, spills to disk, possible OOM. Rule of thumb: 100–200 MB per shuffle partition, or
   let AQE decide.
2. After each shuffle map stage finishes, AQE knows the **actual size of every shuffle partition**
   (map output statistics). `AQEShuffleRead coalesced` makes one reducer read several adjacent small
   partitions until it reaches `advisoryPartitionSizeInBytes` (64 MB default), so 200 → a handful of
   tasks. The static planner only had estimates (file sizes, often wrong after filters/joins).
3. The skewed stage's **max task time ≫ median** (≈ 1.26 s vs 0.11 s): one task processes the 6 M
   hot-key rows while other cores idle. Salting cut the max (≈ 0.9 s) but adds work everywhere: a
   random number per row, an extra join column, and the salted hot key is still spread over only 16
   shuffle partitions/4 cores, so wall time barely changes on a laptop. It wins clearly when there are
   many cores and partitions (the straggler would otherwise hold 199 idle executors) and when the
   per-row join work is heavy (wide rows, many-to-many joins, spills).
4. A shuffle partition is skewed if it is larger than `skewedPartitionFactor` × the median partition
   size **and** larger than `skewedPartitionThresholdInBytes` (defaults 5× and 256 MB — we lowered
   them to 3× / 16 MB). AQE splits the skewed partition into several tasks (by map-output ranges) and
   replicates the matching partition of the other side (`SortMergeJoin(skew=true)`). Limits: only for
   sort-merge/shuffled-hash joins with shuffles on both sides, not for all join types (e.g. the
   non-duplicated side of outer joins), needs the thresholds to fit your data, and cannot fix skew in
   a single key *after* the join (e.g. a skewed `groupBy`) — salting (two-phase aggregation) can.
5. When the "small" side is still large in memory (every executor gets a full copy, the driver
   collects it first → driver OOM, broadcast timeouts), when statistics under-estimate its size
   (e.g. after filters/UDFs), for full outer joins (not supported), or when the join is executed many
   times on changing data (rebuilding the broadcast each time).
6. `repartition(n)` always **shuffles** (round-robin or by expression) → balanced partitions, full
   parallelism before it. `coalesce(n)` merges existing partitions **without a shuffle** → cheap, but
   can create unbalanced files, and because it is a narrow dependency it reduces the parallelism of
   the stage before it (`coalesce(1)` → the whole scan + transformations run in **one task**).
   `coalesce(4)` gave 3 files because the input file has only 3 row groups → one input partition was empty.
7. Here the cached DataFrame is the result of an **expensive** step (a full-shuffle `dropDuplicates`)
   and is reused 3×: without cache the dedup shuffle runs three times (≈ 4.1 s), with cache it runs
   once (2.3 s to materialise) and the 3 queries take 0.7 s. A plain Parquet scan is cheap to
   recompute (column-pruned, vectorised Parquet scan with pushed filters), so caching mostly adds
   memory use. Cache what is expensive *and* reused, and `unpersist()` afterwards.
8. Checklist (any 5): read the plan and the UI first (find the slowest stage, spills, skew);
   filter and prune columns early (predicate/partition pushdown, partitioned data); right-size
   shuffle partitions or rely on AQE; broadcast small dimensions; fix skew (AQE skew join, salting,
   isolate hot keys); avoid Python UDFs (use built-ins / pandas UDFs); cache only reused expensive
   results; avoid small files (repartition before write, compaction); use columnar formats with
   good compression; check executor memory/cores for spills and GC.
