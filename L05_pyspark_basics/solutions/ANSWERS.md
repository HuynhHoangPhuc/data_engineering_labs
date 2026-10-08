# L05 — Solutions

Reference code: [`l05_basics.py`](l05_basics.py) — runs end-to-end in ~30 s:
`docker compose exec spark spark-submit /lab/solutions/l05_basics.py`

## TODOs

```python
# TODO 1
null_counts = trips.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in trips.columns])
# TODO 2
"negative total": F.col("total_amount") < 0,
"distance <= 0 or > 100 miles": (F.col("trip_distance") <= 0) | (F.col("trip_distance") > 100),
"duration <= 0 or > 6 h": (F.col("duration_min") <= 0) | (F.col("duration_min") > 360),
# TODO 3
pu = zones.select(F.col("LocationID").alias("PULocationID"), F.col("Borough").alias("pu_borough"), F.col("Zone").alias("pu_zone"))
do = zones.select(F.col("LocationID").alias("DOLocationID"), F.col("Borough").alias("do_borough"), F.col("Zone").alias("do_zone"))
enriched = clean.join(pu, "PULocationID", "left").join(do, "DOLocationID", "left")
# TODO 4
by_borough = (enriched.groupBy("pu_borough")
              .agg(F.count("*").alias("trips"), F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
                   F.round(F.avg("tip_pct"), 1).alias("avg_tip_pct"), F.round(F.avg("trip_distance"), 2).alias("avg_miles"))
              .orderBy(F.desc("trips")))
# TODO 5
w_rank = Window.partitionBy("pu_borough").orderBy(F.desc("trips"))
top3 = zone_counts.withColumn("rank", F.dense_rank().over(w_rank)).filter("rank <= 3").orderBy("pu_borough", "rank")
# TODO 6
w_day = Window.orderBy("pickup_date")
daily = (daily.withColumn("prev_day_trips", F.lag("trips").over(w_day))
         .withColumn("dod_change_pct", F.round((F.col("trips") / F.col("prev_day_trips") - 1) * 100, 1))
         .withColumn("running_revenue", F.sum("revenue").over(w_day.rowsBetween(Window.unboundedPreceding, 0))))
# TODO 7
(enriched.drop("pickup_ts").repartition("pickup_date")
 .write.mode("overwrite").partitionBy("pickup_date").parquet(OUTPUT))
```

## Reference results (2024-01, Apple M-series, Docker 4 GB)

rows 2,964,624 · 4 input partitions · long trips (>10 mi) 228,254 · kept after cleaning 2,867,786 (96.73 %) ·
Manhattan 2,571,810 trips (avg fare 14.92) · Queens 257,573 (avg fare 52.77, avg 12.76 mi — airports) ·
busiest hour 18:00 · 31 date partitions, ~72 MB · 2024-01-15: 74,743 trips.

## Checkpoint answers

1. Spark splits files into input partitions of at most `spark.sql.files.maxPartitionBytes` (128 MB) but
   also aims for at least `defaultParallelism` (= 4 cores) splits; Parquet can only be split at
   **row-group** boundaries (this file has 3 row groups), so one of the 4 partitions may be empty.
2. `filter`, `withColumn`, `select` are transformations: they only add nodes to a logical plan
   (no data is touched — milliseconds). `count()` is an action: Catalyst optimises the plan, the
   scheduler builds stages and tasks, and data is read. `show`, `collect`, `take`, `write` are also actions.
3. With AQE there are several small jobs (one per query stage) — typically: stage 1 scan the cached
   data + broadcast joins + partial aggregate (4 tasks) → shuffle → stage 2 final aggregate → shuffle
   (range partitioning for `orderBy`) → stage 3 sort/take, plus the jobs that build the two broadcasts.
   **Stage boundaries are the `Exchange` operators** (shuffles); broadcasts run as separate jobs.
4. They are the same 140,162 rows: trips whose record lacks the "trip record metadata" (typically from
   one vendor / street-hail submissions). Options: keep and impute defaults (we set passenger_count 1,
   RatecodeID 1, surcharges 0), keep NULLs and exclude them only from metrics that need them, or
   quarantine them — decide with the data owner and document it.
5. Partial aggregation: each task pre-aggregates its own rows per key (`partial_count`, `partial_avg`
   keeps sum+count) before the shuffle, so only one row per key per task crosses the network; the
   final `HashAggregate` merges them. It is exactly the **combiner** of L02 (and `avg` is split into
   sum/count like the average-fare stretch challenge).
6. Its estimated size (≈ 12 KB file, ~1 MB in memory) is far below `spark.sql.autoBroadcastJoinThreshold`
   (10 MB), so each task gets a full copy and no shuffle of the 2.9 M trips is needed. With both sides
   large (or the threshold set to -1) Spark shuffles both sides by key and uses a **sort-merge join**
   (L06 measures the difference).
7. The plan reads from `InMemoryTableScan`/`InMemoryRelation` instead of re-scanning and re-filtering
   Parquet; the UI *Storage* tab shows the cached size and fraction cached. It is a bad idea when the
   data is used only once, when it does not fit in memory (spills, evictions, GC pressure), or when the
   source read is already cheap (column-pruned Parquet may be faster than a wide cached row set) —
   and forgetting `unpersist()` wastes memory.
8. `partitionBy` writes one file **per task per partition value**. Without the repartition every one
   of the N tasks may contain rows of all 31 days → up to N × 31 small files. `repartition("pickup_date")`
   first sends all rows of one date to the same task → 1 file per date (watch for skew if one value is huge).
