# L05 — PySpark basics: DataFrames, Spark SQL and the Spark UI

| | |
|---|---|
| Module | 4 — Spark I (PySpark) |
| Time | 2 h |
| Stack | Spark **4.1.3** (official `spark:4.1.3-python3` image, Java 17, Python 3.10), local mode `local[4]`, driver 2 GB |
| RAM | ~2.5 GB while the job runs |
| Prerequisites | Python + SQL basics; `labs/datasets/data/taxi/yellow_tripdata_2024-01.parquet` + `taxi_zone_lookup.csv` |

## Learning objectives

1. Create a `SparkSession`, read Parquet and CSV, inspect schemas and partitions.
2. Explain **lazy evaluation**: transformations build a plan, **actions** run jobs.
3. Clean real data (NULLs, negative fares, impossible distances/durations, out-of-range dates) —
   and handle Spark 4's **ANSI mode**.
4. Join, aggregate (`groupBy/agg`), and use **window functions** (`dense_rank`, `lag`, running sums).
5. Express the same logic in **Spark SQL** and compare plans.
6. Read a physical plan with `explain("formatted")` and find its stages in the **Spark UI** (:4040).
7. Write **partitioned Parquet** and prove partition pruning on read.

## Architecture

```
  laptop ── docker compose exec spark spark-submit /lab/src/l05_basics.py
                                   │
            ┌──────────────────────▼───────────────────────────┐
            │ container "spark"  (local[4] = driver + 4 task    │   UI → http://localhost:4040
            │ threads in ONE JVM, 2 GB)                         │   (only while the app runs)
            │   job ─► stages (split at shuffles) ─► tasks      │
            │          (one task per partition)                 │
            └───────────┬───────────────────────────┬──────────┘
            /datasets (read-only: taxi Parquet + zones)   /lab/output (your results, on the laptop)
```

On a cluster the same code runs with a driver and many executors (YARN, Kubernetes, standalone);
`local[4]` is the smallest honest version of that model.

## Tasks

### 0. Start

```bash
cd labs/L05_pyspark_basics
docker compose up -d
docker compose exec spark spark-submit --version       # Spark 4.1.3, Scala 2.13, Java 17
```

Two ways to work — use both:

* **Interactive**: `docker compose exec spark pyspark` (a Python REPL with `spark` ready). Paste
  code step by step and watch <http://localhost:4040> after every line.
* **Script**: complete the TODOs in [`src/l05_basics.py`](src/l05_basics.py) and run
  `docker compose exec spark spark-submit /lab/src/l05_basics.py`
  (add `--hold 600` at the end to keep the UI alive for 10 minutes).

Configuration: [`conf/spark-defaults.conf`](conf/spark-defaults.conf) (`local[4]`, 2 GB driver,
`spark.sql.shuffle.partitions=8`) and a quiet `conf/log4j2.properties`.

### 1. Read the data (section 1 of the script)

```python
trips = spark.read.parquet("/datasets/data/taxi/yellow_tripdata_2024-01.parquet")
trips.printSchema(); trips.count(); trips.rdd.getNumPartitions()
zones = spark.read.option("header", True).option("inferSchema", True).csv("/datasets/data/taxi/taxi_zone_lookup.csv")
```

Expected: 19 columns, pickup/dropoff are `timestamp_ntz`; **2,964,624 rows**; 4 partitions.

### 2. Transformations vs actions

In the `pyspark` shell, type the three transformations of section 2 (filter, withColumn, select)
and look at the UI *Jobs* tab: **nothing**. Then run `.count()`: a job with 2 stages appears.
Expected: building the plan takes a few ms; `count() = 228254 long trips`.

### 3. Cleaning — TODO 1 and TODO 2

* **TODO 1**: one row with the number of NULLs per column. Expected: 140,162 NULLs in
  `passenger_count`, `RatecodeID`, `store_and_fwd_flag`, `congestion_surcharge`, `Airport_fee`
  (the same rows — trips reported without these fields).
* **TODO 2**: complete the rule dictionary. Expected counts:

| Rule | Rows |
|---|---|
| pickup outside 2024-01 | 18 |
| fare ≤ 0 | 38,341 |
| total < 0 | 35,504 |
| distance ≤ 0 or > 100 miles | 60,430 |
| duration ≤ 0 or > 6 h | 2,642 |
| **kept** | **2,867,786 of 2,964,624 (96.73 %)** |

> **ANSI mode (Spark 4 default).** `fare / 0` now raises `[DIVIDE_BY_ZERO]` instead of returning
> NULL, and invalid casts raise errors. Use `F.try_divide`, `try_cast`, or filter first. Try it:
> `spark.range(1).select(F.lit(1) / F.lit(0)).show()`.

### 4. Joins — TODO 3

Join the cleaned trips with the zone lookup **twice** (pickup and dropoff). Rename columns before each
join, otherwise you get `AMBIGUOUS_REFERENCE` errors. The zone table (265 rows) is automatically
**broadcast** — you will see `BroadcastHashJoin` in the plan.

### 5. Aggregations — TODO 4

Per pickup borough: trips, average fare, average tip %, average miles. Expected:

```
+-------------+-------+--------+-----------+---------+
|   pu_borough|  trips|avg_fare|avg_tip_pct|avg_miles|
|    Manhattan|2571810|   14.92|       21.6|     2.31|
|       Queens| 257573|   52.77|       21.8|    12.76|
|     Brooklyn|  22257|   28.77|        6.9|     6.15|
...
```

Busiest pickup hour: 18:00 (206,163 trips).

### 6. Window functions — TODO 5 and TODO 6

* Top-3 pickup zones per borough (`dense_rank` over `partitionBy("pu_borough")`): Manhattan →
  Midtown Center 140,065, Upper East Side South 140,059, Upper East Side North 133,891; Queens → JFK
  138,296, LaGuardia 87,657, East Elmhurst 11,861.
* Daily trips with `lag` (day-over-day change) and a running revenue total. Note the WARN
  *"No Partition Defined for Window operation! Moving all data to a single partition"* — why is it
  harmless here (31 rows) and dangerous on the raw trips?

### 7. Spark SQL

`enriched.createOrReplaceTempView("trips")` then the same aggregation in SQL. The script prints
`SQL result == DataFrame result: True`. Compare `sql_df.explain()` with `by_borough.explain()` —
identical plans: both APIs go through the same Catalyst optimizer.

### 8. Read the plan

`by_borough.explain("formatted")` — find, bottom-up:

* `Scan parquet` with `PushedFilters` (the cleaning filters are pushed into the Parquet reader),
* `InMemoryTableScan` (we cached `clean`),
* `BroadcastExchange` + `BroadcastHashJoin LeftOuter BuildRight` (twice),
* `HashAggregate` (partial: `partial_count`, `partial_avg`) → `Exchange hashpartitioning(pu_borough, 8)` →
  `HashAggregate` (final) — the **combiner idea from L02**,
* `AQEShuffleRead coalesced` — Adaptive Query Execution shrank the 8 shuffle partitions,
* `Exchange rangepartitioning` + `Sort` for `orderBy`.

### 9. The Spark UI

Run the script with `--hold 600` and open <http://localhost:4040>:

| Tab | Look for |
|---|---|
| Jobs | one job per action (`count`, `show`, `collect`, `save`) |
| Stages | stage boundaries = `Exchange` nodes; *Shuffle Read/Write* sizes; task time distribution |
| Storage | the cached `clean` DataFrame (size in memory) |
| SQL / DataFrame | the plan graph of each query with row counts per operator |
| Environment | `spark.sql.shuffle.partitions = 8`, `spark.sql.ansi.enabled`… |

### 10. Write partitioned Parquet — TODO 7

Write `enriched` partitioned by `pickup_date` to `/lab/output/trips_clean` (= `labs/L05_pyspark_basics/output/`
on your laptop). First **without** `repartition("pickup_date")`, count the files, then **with** it:

```bash
find output/trips_clean -name "*.parquet" | wc -l          # with repartition: 31 files (1 per day)
ls output/trips_clean | head -3                            # _SUCCESS  pickup_date=2024-01-01 ...
du -sh output/trips_clean                                   # ~72 MB
```

Reading back with `filter(pickup_date == '2024-01-15')` returns 74,743 trips and the plan shows
`PartitionFilters: [isnotnull(pickup_date), (pickup_date = 2024-01-15)]` — only one directory is read.

## Checkpoint questions

1. Why did `trips.rdd.getNumPartitions()` return 4 for a single 48 MB file?
2. Which lines of section 2 are transformations and which are actions? Why did building the plan take milliseconds?
3. How many jobs and stages did the `by_borough.show()` query produce, and where are the stage boundaries?
4. Why do the NULL counts of five columns match exactly (140,162)? What would you do with those rows?
5. In the formatted plan, what do `partial_count` / `HashAggregate` before the `Exchange` do, and which
   MapReduce concept from L02 is this?
6. Why is the zone table broadcast and not shuffled? When would Spark switch to a sort-merge join?
7. What does `cache()` change in the plan and in the UI? When is caching a bad idea?
8. Why write with `repartition("pickup_date")` before `partitionBy("pickup_date")`? What happens without it?

## Stretch challenges

* Add a `pickup_month` partition level and load a second month (`download_taxi.sh 2024-02`).
* Find the 10 most common (pickup zone → dropoff zone) routes and their median fare
  (`percentile_approx`).
* Rewrite the cleaning rules as a single SQL `CASE WHEN` that labels each row with its *first* failing rule,
  and write rejected rows to `output/rejected/` for auditing.
* Start the History Server to inspect finished apps: event logs are in `/tmp/spark-events` inside the
  container: `docker compose exec spark /opt/spark/sbin/start-history-server.sh` (UI on port 18080 —
  add `"18080:18080"` to `docker-compose.yml`).
* Use a Python UDF to compute tip % and compare its plan (`BatchEvalPython`) and speed with the
  built-in `try_divide`. Then try a vectorised pandas UDF (requires `pip install pandas pyarrow` in the container).

## Cleanup

```bash
docker compose down -v
rm -rf output/trips_clean
```

## Troubleshooting

| Symptom | Fix |
|---|---|
| `[DIVIDE_BY_ZERO]`, `[CAST_INVALID_INPUT]` | ANSI mode — use `try_divide` / `try_cast`, or filter bad rows first |
| `[AMBIGUOUS_REFERENCE] Reference 'LocationID' is ambiguous` | Rename columns of the zone table before joining it twice |
| `PATH_NOT_FOUND ... yellow_tripdata_*.parquet` | Run `bash labs/datasets/download_taxi.sh` on the laptop |
| `localhost:4040` does not load | The UI only exists while an application runs — use `--hold 600` or the `pyspark` shell. A second app uses port 4041 |
| `java.lang.OutOfMemoryError` / exit 137 | Don't `collect()` big DataFrames; keep Docker ≥ 4 GB; `unpersist()` caches you no longer need |
| `Permission denied: /lab/output/...` (Linux hosts) | The container runs as uid 185: `chmod -R a+w labs/L05_pyspark_basics/output` |
| `spark-submit: not found` | Use `docker compose exec spark ...` from this folder (the compose file puts `/opt/spark/bin` on the PATH) |
