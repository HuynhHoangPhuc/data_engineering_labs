# L04 — Ingestion from an OLTP database: Sqoop (legacy) → Spark JDBC

| | |
|---|---|
| Module | 3 — Ingestion |
| Time | 2–2.5 h (Part 1 ≈ 60 min, Part 2 ≈ 60 min, comparison 15 min) |
| Stack | PostgreSQL 18.6 (Olist OLTP, seeded) · HDFS 3.4.3 (1 NN + 1 DN) + small YARN (RM, NM, JobHistory) · Sqoop 1.4.7 · Spark 4.1.3 (local mode) |
| RAM | ~3 GB while a Sqoop job runs |
| Prerequisites | L01 (HDFS), L03 helpful (Hive-style partitions). Basic SQL. |

## Learning objectives

1. Explain **batch ingestion** from a relational database: full vs incremental loads, split/partition
   columns, parallel connections, watermarks.
2. Run **Sqoop** full imports (`--split-by`, `-m`), incremental `append` and `lastmodified` imports,
   a saved job and an export — and read the SQL it sends to the database.
3. Do the same with **Spark JDBC** (`partitionColumn`, `lowerBound`, `upperBound`, `numPartitions`,
   watermark on `updated_at`) and write **Parquet** partitioned by month.
4. Compare both tools and explain why Sqoop is retired and where **CDC** (L08) fits.

> **Honest status of Sqoop.** Apache Sqoop was **retired to the Apache Attic in June 2021**; the last
> release (1.4.7) dates from 2017, was built for Hadoop 2.6 and gets no security fixes. You will still
> meet it in older Hadoop platforms, which is why we run it once. The course image
> (`labs/images/sqoop`) makes it work on Hadoop 3.4.3 by adding commons-lang 2.6 and org.json and
> downgrading commons-cli to 1.5.0. Sqoop jobs run on a small YARN cluster because Sqoop silently
> forces a single mapper when MapReduce runs in local mode. **Part 2 (Spark JDBC) is the modern, hands-on core of this lab.**

## Source data: the Olist OLTP database

Postgres is seeded from `labs/datasets/olist/init` (synthetic but realistic Olist data: 5,000 orders,
5,000 customers, 5,852 order items, …). Column names are those of the Kaggle dataset, plus:

* `updated_at` on every table, bumped automatically by a trigger on every `UPDATE`;
* **`orders.order_seq`** — a numeric surrogate key (identity, increasing with purchase time).

**Why `order_seq`?** Olist ids (`order_id`, `customer_id` …) are 32-character **text** hashes.
Range-splitting needs an ordered, preferably evenly distributed **numeric or date/time** column:
Sqoop's text splitter is disabled by default (and unreliable), and Spark's `partitionColumn` must be
numeric, date or timestamp. In this lab we split on `order_seq` (even) and, for comparison, on
`customer_zip_code_prefix` (numeric but **skewed**) and `order_purchase_timestamp` (time-based, grows
over time). `order_id` remains the primary key and the `--merge-key`.

## Architecture

```
            ┌───────────── postgres:5432 (olist) ──────────────┐   log_statement=all:
            │ customers  orders(order_seq, updated_at) ...      │   every SQL statement is logged
            └──────▲─────────────────────────▲─────────────────┘   → docker compose logs postgres
       JDBC (1 conn per mapper)       JDBC (1 conn per partition)
            │                                 │
   ┌────────┴───────────┐           ┌─────────┴───────────┐
   │ sqoop client ──────►│ YARN      │ spark (local[*],     │  UI :4040
   │ map-only MR job on  │ :8088     │ driver 1 GB)         │
   │ resourcemanager/NM  │ :19888    │                      │
   └────────┬───────────┘           └─────────┬───────────┘
            ▼ text files                       ▼ Parquet
   hdfs://namenode:8020/data/olist/sqoop/...   hdfs://namenode:8020/data/olist/spark/...
```

## Part 0 — Start the stack

```bash
cd labs/L04_ingestion
docker compose up -d --build         # builds de-labs/sqoop:1.4.7 on top of de-labs/hadoop:3.4.3
docker compose ps                    # jdbc-driver shows "Exited (0)" — it only downloads the JDBC jar
docker compose exec postgres psql -U olist -d olist -c "\dt"
docker compose exec postgres psql -U olist -d olist -c \
  "SELECT count(*), min(order_seq), max(order_seq), max(updated_at) FROM orders;"
```

Expected: 6 tables; `5000 | 1 | 5000 | 2018-09-08 09:48:29`.

Keep a **second terminal** open that shows the SQL the tools send:

```bash
docker compose logs -f postgres | grep -E "(execute|statement).*(SELECT|INSERT)"
```

## Part 1 — Sqoop (legacy)

```bash
docker compose exec sqoop bash
# inside the container (working dir /work; this lab folder is mounted at /lab):
mkdir -p /lab/work && echo -n olist > /lab/work/.pg_password && chmod 400 /lab/work/.pg_password
export JDBC=jdbc:postgresql://postgres:5432/olist
export CONN="--connect $JDBC --username olist --password-file file:///lab/work/.pg_password"
```

`--password-file` keeps the password out of `ps` output and shell history (`--password` works but
is insecure; `-P` prompts interactively).

### 1.1 Explore

```bash
sqoop list-tables $CONN
sqoop eval $CONN --query "SELECT order_status, count(*) FROM orders GROUP BY 1 ORDER BY 2 DESC"
```

### 1.2 Full import with 4 mappers

```bash
sqoop import $CONN --table orders \
  --split-by order_seq -m 4 \
  --null-string '\\N' --null-non-string '\\N' \
  --target-dir /data/olist/sqoop/orders_full --delete-target-dir
```

Watch the log for:

* `BoundingValsQuery: SELECT MIN("order_seq"), MAX("order_seq") FROM "orders"`
* `number of splits:4` and `Transferred 1.009 MB in ~20 seconds` / `Retrieved 5000 records.`
* in the Postgres terminal: four queries `SELECT "order_id", ... FROM "orders" AS "orders" WHERE ( "order_seq" >= 1 ) AND ( "order_seq" < 1250 )` …
* the job in the YARN UI <http://localhost:8088> (one map task per split, no reducers)

Inspect the result:

```bash
hdfs dfs -ls /data/olist/sqoop/orders_full           # _SUCCESS + part-m-00000 .. 00003
hdfs dfs -cat /data/olist/sqoop/orders_full/part-m-00000 | head -2
for f in $(hdfs dfs -ls -C /data/olist/sqoop/orders_full/part-m-*); do echo "$f $(hdfs dfs -cat $f | wc -l)"; done
```

Expected: exactly 1,250 rows in each of the 4 files (even split).

The values are written as text: timestamps like `2017-01-03 01:29:02.0`, NULL as `\N`.

**TODO 1.2** — import `customers` with `--split-by customer_zip_code_prefix -m 4` into
`/data/olist/sqoop/customers_full` and count rows per file. Why are they so uneven?
(Reference: 2450 / 1146 / 427 / 977.)

### 1.3 Incremental append (new rows only)

Initial load — "everything with `order_seq` > 0":

```bash
sqoop import $CONN --table orders --split-by order_seq -m 2 \
  --null-string '\\N' --null-non-string '\\N' \
  --target-dir /data/olist/sqoop/orders_append \
  --incremental append --check-column order_seq --last-value 0
```

At the end Sqoop prints what to use next time:

```
INFO tool.ImportTool:  --incremental append
INFO tool.ImportTool:   --check-column order_seq
INFO tool.ImportTool:   --last-value 5000
INFO tool.ImportTool: (Consider saving this with 'sqoop job --create')
```

### 1.4 Saved job (Sqoop remembers the last value)

```bash
sqoop job --create orders_incr -- import $CONN --table orders --split-by order_seq -m 1 \
  --null-string '\\N' --null-non-string '\\N' \
  --target-dir /data/olist/sqoop/orders_append \
  --incremental append --check-column order_seq --last-value 5000
sqoop job --list
sqoop job --show orders_incr | grep incremental
```

Now simulate a business day in Postgres (**second terminal on your laptop**):

```bash
docker compose exec postgres psql -U olist -d olist -f /lab/sql/simulate_changes.sql
```

It inserts **3 new orders** (order_seq 5001–5003) and changes the status of **2 existing** orders
(their `updated_at` becomes *now*). Back in the Sqoop container:

```bash
sqoop job --exec orders_incr
sqoop job --show orders_incr | grep incremental.last.value     # now 5003
hdfs dfs -ls /data/olist/sqoop/orders_append                    # one NEW small part file (3 rows)
hdfs dfs -cat /data/olist/sqoop/orders_append/part-m-* | wc -l  # 5003

The saved-job metastore lives in `~/.sqoop` **inside the sqoop container**: it survives
`docker compose stop/start` but not `docker compose down`.
```

The 2 **updated** orders were *not* re-imported: append mode only sees new keys.

### 1.5 Incremental lastmodified + merge (new AND changed rows)

`lastmodified` selects rows whose timestamp column is newer than `--last-value`; `--merge-key`
then runs a second MapReduce job that **replaces** old versions of the same key. The merge job
parses the existing files again, so it must know how NULL was written: add
`--input-null-string '\\N' --input-null-non-string '\\N'` (otherwise: `Can't parse input data: '\N'`).

Run the baseline import, then `simulate_changes.sql`, then the second import:

```bash
export NULLS="--null-string \\N --null-non-string \\N --input-null-string \\N --input-null-non-string \\N"
sqoop import $CONN --table orders -m 1 $NULLS \
  --target-dir /data/olist/sqoop/orders_lastmod \
  --incremental lastmodified --check-column updated_at --last-value '1970-01-01 00:00:00'
# ... note the printed --last-value (the DB time at import), run sql/simulate_changes.sql again, then:
sqoop import $CONN --table orders -m 1 $NULLS \
  --target-dir /data/olist/sqoop/orders_lastmod \
  --incremental lastmodified --check-column updated_at --last-value '<value printed above>' \
  --merge-key order_id
hdfs dfs -ls /data/olist/sqoop/orders_lastmod          # part-r-00000 = merged snapshot
hdfs dfs -cat /data/olist/sqoop/orders_lastmod/part-r-00000 | wc -l
```

Expected: second run `Retrieved 5 records` (3 new + 2 updated); the merged snapshot holds one row per
order (total = previous count + 3), and the 2 updated orders show their new status.

### 1.6 Export (HDFS → database)

```bash
sqoop eval $CONN --query "CREATE TABLE orders_copy (LIKE orders INCLUDING DEFAULTS)"
sqoop export $CONN --table orders_copy --export-dir /data/olist/sqoop/orders_full -m 2 \
  --input-null-string '\\N' --input-null-non-string '\\N'
sqoop eval $CONN --query "SELECT count(*) FROM orders_copy"                       # 5000
```

In the Postgres log you will see batched multi-row `INSERT INTO orders_copy ...` statements.

Reference scripts: `solutions/sqoop_all.sh` (1.1–1.4 + lastmodified baseline) and
`solutions/sqoop_after_changes.sh` (after `simulate_changes.sql`).

## Part 2 — Spark JDBC (modern)

Open [`src/spark_jdbc_ingest.py`](src/spark_jdbc_ingest.py) and complete **TODO 1–3** (step `full`).
Run it in the Spark container:

```bash
docker compose exec spark spark-submit --jars /jars/postgresql.jar /lab/src/spark_jdbc_ingest.py --step full
```

While it runs open <http://localhost:4040> (Jobs → Stages: the parallel read stage has **4 tasks**).
Expected output (times vary):

```
naive partitions: 1
order_seq bounds: 1 .. 5003
parallel partitions: 4
+---------+-----+
|partition|count|
|        0| 1251|  ...  (≈ 1,250 per partition)
rows per partition when splitting on order_purchase_timestamp (volume grows over time): ... uneven
rows written: 5003 | months: 21        (20 months of history + the new orders of today)
== Physical Plan == ... PartitionFilters: [isnotnull(purchase_month#..), (purchase_month#.. = 2018-08)]
```

In the Postgres terminal you see Spark's 4 range queries, e.g.
`SELECT 1 FROM orders WHERE "order_seq" < 1252 or "order_seq" is null`,
`... WHERE "order_seq" >= 1252 AND "order_seq" < 2503`, … (for `count()` Spark only asks for `SELECT 1` —
column pruning pushed into the SQL!).

Check the files on HDFS:

```bash
docker compose exec namenode hdfs dfs -ls /data/olist/spark/orders | head
docker compose exec namenode hdfs dfs -du -s -h /data/olist/spark/orders /data/olist/sqoop/orders_full
```

### 2.2 Incremental load with a watermark

Complete **TODO 4–5** (step `incremental`), then:

```bash
docker compose exec spark spark-submit --jars /jars/postgresql.jar /lab/src/spark_jdbc_ingest.py --step incremental
# 1st run: no watermark yet -> reads all 5003 rows, saves watermark = DB now()
docker compose exec postgres psql -U olist -d olist -f /lab/sql/simulate_changes.sql
docker compose exec spark spark-submit --jars /jars/postgresql.jar /lab/src/spark_jdbc_ingest.py --step incremental
# 2nd run: "changed rows since last run: 5"  (3 new + 2 updated)
docker compose exec spark spark-submit --jars /jars/postgresql.jar /lab/src/spark_jdbc_ingest.py --step incremental
# 3rd run without changes: 0 rows
```

The job keeps an append-only **change log** (`orders_changes`) and rebuilds a deduplicated
**current snapshot** (`orders_current`: one row per `order_id`, latest `updated_at`) — the same
idea as Sqoop's `--merge-key`, and the basis of the bronze/silver layers later in the course.

## Part 3 — Compare

Fill in:

| | Sqoop 1.4.7 | Spark JDBC |
|---|---|---|
| Parallelism knob | `-m` + `--split-by` | `numPartitions` + `partitionColumn` |
| How ranges are computed | `SELECT MIN, MAX` (BoundingValsQuery) | you pass `lowerBound`/`upperBound` (stride only) |
| Incremental | `--incremental append/lastmodified`, saved job stores last value | your code: watermark query + state file |
| Handling updates | `--merge-key` (extra MR job) | window/`row_number` dedup, or MERGE into a table format (L11) |
| Output formats | text, SequenceFile, Avro (Parquet via Kite is broken on Hadoop 3) | anything Spark writes (Parquet, ORC, Delta/Iceberg) |
| Transformations during load | none (SQL `--query` only) | full DataFrame API |
| Time for full `orders` import (your run) | | |
| Status | Apache Attic (retired 2021) | actively developed |

## Checkpoint questions

1. What SQL does Sqoop run *before* the import, and how does it turn the result into 4 splits?
2. Why were the `customers` part files uneven with `--split-by customer_zip_code_prefix`? What would
   happen with `--split-by customer_id`?
3. `lowerBound=1, upperBound=5003, numPartitions=4`: does Spark read rows with `order_seq` = 9,999?
   Where do they end up?
4. Why must the watermark's upper bound be fixed **before** reading (`now()` captured first), and why is
   the watermark saved only **after** the write succeeded?
5. Append vs lastmodified: which one captures `UPDATE`s? Which one captures `DELETE`s?
6. Name two load problems a watermark on `updated_at` cannot solve, and what CDC (L08) does differently.
7. What happens to the source database if you set `-m 50` / `numPartitions=50`?

## Stretch challenges

* `sqoop import --query 'SELECT o.*, c.customer_state FROM orders o JOIN customers c USING (customer_id) WHERE $CONDITIONS' --split-by o.order_seq ...` — what is `$CONDITIONS` for?
* Sqoop into Avro: `--as-avrodatafile`; try `--as-parquetfile` and explain the error (Kite SDK vs Hadoop 3).
* Spark: add `pushDownPredicate`/`.filter("order_status = 'delivered'")` and check the Postgres log — is the filter pushed into the SQL?
* Spark: read with `predicates=[...]` (a list of WHERE clauses, one per partition) to split on `order_status`.
* Load the **real Kaggle Olist data** (`labs/datasets/olist/README.md`) and compare import times.
* Register the Spark output as a Hive external table (L03 stack) pointing at the Parquet directory.

## Cleanup

```bash
docker compose down -v
rm -rf work/            # password file, lastmod value
```

## Troubleshooting

| Symptom | Fix |
|---|---|
| `ERROR cli.SqoopParser: Could not load required method of Parser` | You are not using the course Sqoop image (commons-cli too new). `docker compose build sqoop` |
| `NoClassDefFoundError: org/apache/commons/lang/StringUtils` | Same — the image adds commons-lang 2.6 |
| `Generating splits for a textual index column allowed only in case of "-Dorg.apache.sqoop.splitter.allow_text_splitter=true"` | You tried `--split-by order_id`: text column. Use `order_seq` (or pass the -D flag and see how bad the splits are) |
| `Output directory ... already exists` | Add `--delete-target-dir` (full imports) or use a new `--target-dir` |
| Sqoop warnings about HBase/HCatalog/Accumulo | Harmless (set to dummy dirs in the image) |
| `number of splits:1` although you passed `-m 4` | MapReduce ran in local mode (Sqoop then forces 1 mapper). Run Sqoop in the `sqoop` service of this stack, which submits to YARN |
| `NoClassDefFoundError: org/json/JSONObject` on `sqoop job --show/--exec` | Old image without org.json: `docker compose build sqoop` |
| Merge job: `Can't parse input data: '\N'` | Add `--input-null-string '\\N' --input-null-non-string '\\N'` |
| Job hangs in `ACCEPTED` | YARN has no free memory — another job still running? `yarn application -list` |
| `java.lang.ClassNotFoundException: org.postgresql.Driver` in Spark | Pass `--jars /jars/postgresql.jar`; check `docker compose logs jdbc-driver` |
| Spark: `Name node is in safe mode` / `Connection refused namenode:8020` | HDFS still starting — wait 10–20 s |
| `incremental` run keeps reading everything | The watermark was not saved (job failed before the last step) or you deleted `/data/olist/spark/_state` |
| Re-seed Postgres | `docker compose down -v && docker compose up -d` (init scripts run only on an empty volume) |
