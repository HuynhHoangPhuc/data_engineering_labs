# L04 — Solutions

* Sqoop, before changes: [`sqoop_all.sh`](sqoop_all.sh) · after `sql/simulate_changes.sql`:
  [`sqoop_after_changes.sh`](sqoop_after_changes.sh) (run inside the `sqoop` container)
* Spark: [`spark_jdbc_ingest.py`](spark_jdbc_ingest.py) (`--step full`, `--step incremental`)

Full reference sequence (≈ 6 min, from `labs/L04_ingestion`):

```bash
docker compose up -d --build
docker compose exec -T sqoop bash /lab/solutions/sqoop_all.sh
docker compose exec -T postgres psql -U olist -d olist -f /lab/sql/simulate_changes.sql
docker compose exec -T sqoop bash /lab/solutions/sqoop_after_changes.sh
docker compose exec -T spark spark-submit --jars /jars/postgresql.jar /lab/solutions/spark_jdbc_ingest.py --step full
docker compose exec -T spark spark-submit --jars /jars/postgresql.jar /lab/solutions/spark_jdbc_ingest.py --step incremental
```

## TODOs

```bash
# TODO 1.2
sqoop import $CONN --table customers --split-by customer_zip_code_prefix -m 4 \
  --null-string '\\N' --null-non-string '\\N' --target-dir /data/olist/sqoop/customers_full --delete-target-dir
# -> 2450 / 1146 / 427 / 977 rows per file
```

```python
# TODO 1
orders = jdbc(spark, dbtable="orders", partitionColumn="order_seq", lowerBound=str(b.lo),
              upperBound=str(b.hi), numPartitions="4", fetchsize="1000")
# TODO 2
by_ts = jdbc(spark, dbtable="orders", partitionColumn="order_purchase_timestamp",
             lowerBound="2017-01-01 00:00:00", upperBound="2018-09-01 00:00:00", numPartitions="4")
# TODO 3
out = orders.withColumn("purchase_month", F.date_format("order_purchase_timestamp", "yyyy-MM"))
out.write.mode("overwrite").partitionBy("purchase_month").parquet(f"{BASE}/orders")
# TODO 4
changes = jdbc(spark, dbtable=(f"(SELECT * FROM orders WHERE updated_at > timestamp '{wm_old}' "
                               f"AND updated_at <= timestamp '{wm_new}') AS changed_orders")) \
          .withColumn("_ingested_at", F.lit(wm_new).cast("timestamp"))
# TODO 5
w = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc(), F.col("_ingested_at").desc())
current = log.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
```

## Reference results (Apple M-series, Docker 4 GB, synthetic seed)

| Step | Result |
|---|---|
| Sqoop full import `orders`, `-m 4` | 4 splits × 1,250 rows, 1.0 MB, ~20–24 s (mostly YARN container start-up) |
| Sqoop `customers` split on zip prefix | 2450 / 1146 / 427 / 977 rows |
| Saved job after `simulate_changes.sql` | `Lower bound 5000, Upper bound 5003`, `Retrieved 3 records`, last value → 5003 |
| lastmodified + merge | `Retrieved 5 records` (3 new + 2 updated), merge job → `part-r-00000` with 5,003 rows |
| export | `Exported 5000 records` into `orders_copy` |
| Spark parallel read | 4 partitions of ≈ 1,250 rows each (bounds 1..5003) |
| Spark split on timestamp | 782 / 1116 / 1419 / 1689 (business grows over time) |
| Spark Parquet | 5,003 rows, 21 `purchase_month=` directories, write ≈ 2.5 s |
| Spark incremental | run 1: 5,003 rows; after changes: 5 rows; run 3: 0 rows; snapshot = 1 row per order |

## Checkpoint answers

1. `SELECT MIN(split_col), MAX(split_col) FROM table` (the *BoundingValsQuery*, or your
   `--boundary-query`). The integer splitter divides [min, max] into `-m` equal-width ranges and
   each mapper runs `SELECT cols FROM table WHERE split_col >= lo AND split_col < hi` (last range
   inclusive) — equal *width*, not equal *row count*.
2. Split ranges are equal-width over the zip-code range 1000…99999, but customers are concentrated
   in São Paulo (zip 01000–19999 → first range). Equal width ≠ equal rows → skewed mappers: the job
   takes as long as the biggest one. `customer_id` is a text hash: Sqoop refuses to split on text
   unless `-Dorg.apache.sqoop.splitter.allow_text_splitter=true`, and then splits on string prefixes
   (fragile, collation-dependent). Hence `order_seq`.
3. Yes — bounds are **not filters**. Spark builds `WHERE order_seq < b1 OR order_seq IS NULL` for the
   first partition and `WHERE order_seq >= b3` for the last one, so values below `lowerBound` land in
   the first partition and values above `upperBound` (e.g. 9,999) in the last one. Wrong bounds do not
   lose data, but they create skew.
4. The upper bound must be captured **before** the read so rows committed *during* the read are not
   half-included and are picked up next time (the next run starts from exactly this value, with
   `>` / `<=` making ranges disjoint and contiguous). Saving the watermark only after a successful
   write makes a failed run **re-process** the same window (at-least-once) instead of skipping it;
   the dedup step makes re-processing harmless (idempotent).
5. `append` only sees rows with a *new* key above the last value → inserts only. `lastmodified`
   (or a watermark on `updated_at`) sees inserts **and** updates (if the application/trigger bumps
   `updated_at`). **Neither** sees `DELETE`s — a deleted row simply stops appearing.
6. (a) Deletes are invisible (need soft deletes or full-snapshot diffs). (b) Several updates of a
   row between two runs collapse into one — intermediate states are lost. (c) Rows whose
   `updated_at` is set by the app clock or by long transactions that commit later with an older
   timestamp can be missed. (d) Every run scans/queries the source (load) and latency = schedule
   interval. **CDC** (Debezium, L08) reads the database's transaction log (Postgres WAL): every insert,
   update and delete, in commit order, with before/after images, low latency, and almost no query load.
7. 50 parallel connections each running a range scan: connection-pool exhaustion
   (`max_connections`), heavy I/O and locks on a production OLTP database, and little speed-up if the
   DB is the bottleneck. Choose parallelism with the DBA, read from a replica, and run off-peak.
