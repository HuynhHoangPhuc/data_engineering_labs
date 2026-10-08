# L03 — Hive: SQL on HDFS, partitioning, file formats, bucketing

| | |
|---|---|
| Module | 2 — Hive |
| Time | 2 h |
| Stack | Hive **4.2.1** (HiveServer2 + standalone Metastore, Tez in local mode) · Metastore DB on PostgreSQL 18 · HDFS 3.4.3 (1 NameNode + 1 DataNode) |
| RAM | ~2.3 GB under load (HiveServer2 ≈ 1.3 GB) |
| Disk | `apache/hive:4.2.1` image is large (~4 GB unpacked) — pull it before class |
| Prerequisites | L01 (HDFS basics). `labs/datasets/data/taxi/yellow_tripdata_2024-01.parquet` downloaded |

## Learning objectives

1. Explain the Hive architecture: **HiveServer2** (SQL endpoint) → **Metastore** (schemas,
   partitions, locations — stored in an RDBMS) → **execution engine** (Tez) → **HDFS** (data files).
2. Create **external** tables over existing files (CSV with `LazySimpleSerDe` and `OpenCSVSerde`).
3. Create **managed**, **partitioned** ORC and Parquet tables and fill them with **dynamic partitioning**.
4. Measure the effect of the file format (CSV vs ORC vs Parquet) on size and query time.
5. Prove **partition pruning** with `EXPLAIN DEPENDENCY` / `EXPLAIN EXTENDED`.
6. Create a **bucketed** table and use bucket sampling.

## Architecture

```
  laptop ── ./beeline.sh ──► hiveserver2:10000 (JDBC/Thrift)   UI → http://localhost:10002
                               │  parse → plan (Calcite CBO) → Tez DAG (runs inside HS2: local mode)
                               │ Thrift :9083
                               ▼
                           metastore ──JDBC──► postgres (database "metastore": TBLS, SDS, PARTITIONS …)
                               │
       reads/writes data files │ (hdfs://namenode:8020)
                               ▼
              namenode :9870 ── datanode      /data/taxi_csv            (external, CSV)
                                              /user/hive/warehouse/taxi.db/trips_orc/pickup_month=2024-01/…
```

In production, Tez tasks run as YARN containers on many nodes (or in LLAP daemons). To fit a
laptop we run Tez in **local mode** inside HiveServer2 — the SQL, plans and files are identical.

## Tasks

### 1. Start the stack

```bash
cd labs/L03_hive
docker compose up -d --build        # builds de-labs/hadoop if L01 was never run
docker compose ps                   # jdbc-driver exits (0) after downloading the Postgres JDBC jar - expected
docker compose logs -f hiveserver2  # wait for "Hive Session ID = ...", then Ctrl-C (~40-60 s)
```

Open the HiveServer2 web UI: <http://localhost:10002> and the NameNode UI: <http://localhost:9870>.

Connect with Beeline (Hive's JDBC client):

```bash
./beeline.sh
0: jdbc:hive2://localhost:10000/> SHOW DATABASES;
0: jdbc:hive2://localhost:10000/> !quit
```

Two helpers are provided: `./beeline.sh` (interactive, same as
`docker compose exec hiveserver2 beeline -u jdbc:hive2://localhost:10000/ -n hive`) and
`./run_sql.sh <file.sql>` (runs a whole file and prints the time of every statement).

### 2. Put raw CSV data on HDFS

Hive reads files that are already in HDFS. Convert the January Parquet file to headerless CSV
(~2.96 M rows, ~275 MB) and upload it together with the zone lookup:

```bash
docker compose exec namenode bash -c '
  python3 /datasets/parquet_to_csv.py /datasets/data/taxi/yellow_tripdata_2024-01.parquet \
          /datasets/data/taxi/csv/yellow_2024-01.csv
  hdfs dfs -mkdir -p /data/taxi_csv /data/zones
  hdfs dfs -put -f /datasets/data/taxi/csv/yellow_2024-01.csv /data/taxi_csv/
  hdfs dfs -put -f /datasets/data/taxi/taxi_zone_lookup.csv  /data/zones/
  hdfs dfs -ls -h /data/taxi_csv /data/zones'
```

Short on time or disk? add `--limit 1000000` to the `parquet_to_csv.py` command (all numbers below
in brackets come from a 1 M-row run). Downloaded more months? Convert and upload them the same way —
they will become extra partitions in step 3.

### 3. External tables (`sql/01_external_tables.sql`)

Open [`sql/01_external_tables.sql`](sql/01_external_tables.sql). `trips_csv` is complete; complete
**TODO 1** (`zones` uses `OpenCSVSerde` because the file has a header and `"quoted"` values). Run it:

```bash
./run_sql.sh sql/01_external_tables.sql
```

Expected: 3 sample trips, 3 zones (`1 | EWR | Newark Airport | EWR` …) and `csv_rows` =
2,964,624 (1,000,000).

An EXTERNAL table is only metadata. Look at what the Metastore stored — it is just a relational DB:

```bash
docker compose exec postgres psql -U hive -d metastore -c \
 'SELECT t."TBL_NAME", t."TBL_TYPE", s."LOCATION", s."INPUT_FORMAT"
  FROM "TBLS" t JOIN "SDS" s ON t."SD_ID" = s."SD_ID";'
```

### 4. Partitioned ORC and Parquet tables (`sql/02_partitioned_tables.sql`)

Complete **TODO 2–4**: enable non-strict dynamic partitioning, compute `pickup_month`
(`'yyyy-MM'`) from the pickup timestamp and create/fill the Parquet twin. Run:

```bash
./run_sql.sh sql/02_partitioned_tables.sql
docker compose exec namenode hdfs dfs -ls -R /user/hive/warehouse/taxi.db/trips_orc
```

Expected `SHOW PARTITIONS`: `pickup_month=2002-12`, `2009-01`, `2023-12`, `2024-01` (plus more if you
loaded more months). **Surprise!** The January file contains a handful of trips with wrong clock
values — dynamic partitioning faithfully creates a partition for each. (Data-quality discussion:
where should such rows go?)

Compare storage:

```bash
docker compose exec namenode hdfs dfs -du -h /data/taxi_csv /user/hive/warehouse/taxi.db
```

| Format | Size (1 M rows) | Size (full month) |
|---|---|---|
| CSV (text) | (88.5 M) | |
| ORC + ZLIB | (17.1 M) | |
| Parquet + Snappy | (20.9 M) | |

> **Hive 4 note:** run `DESCRIBE FORMATTED trips_orc;` — *Table Type* is `EXTERNAL_TABLE` with
> `TRANSLATED_TO_EXTERNAL=TRUE` and `external.table.purge=TRUE`. In Hive 4, a "managed" table must be
> **transactional (ACID)**; a plain `CREATE TABLE ... STORED AS ORC` is translated by the Metastore into
> an external table that still lives in the warehouse and whose files are deleted on `DROP TABLE`
> (purge). Add `TBLPROPERTIES ('transactional'='true')` to get a real ACID managed table (stretch).

### 5. CSV vs ORC vs Parquet (`sql/03_compare_formats.sql`)

```bash
./run_sql.sh sql/03_compare_formats.sql
```

Each query reports `N rows selected (x.xxx seconds)`. Run the file **twice** (the first run warms up
the JVM) and record the second run:

| Query | CSV | ORC | Parquet |
|---|---|---|---|
| Q1 top pickup zones Jan 2024 | (≈ 4.6 s) | (≈ 1.2 s) | (≈ 1.1–1.8 s) |

Q1 top row: zone `132` (JFK) — 59,777 trips in the 1 M sample, avg fare ≈ 59.71. Q2 shows trips per
borough (Manhattan ≈ 88 %).

### 6. Partition pruning (`sql/04_partition_pruning.sql`)

Complete **TODO 5** and run:

```bash
./run_sql.sh sql/04_partition_pruning.sql
```

* `EXPLAIN DEPENDENCY` lists the **input partitions**: with `pickup_month = '2024-01'` → 1 partition;
  with a filter on `tpep_pickup_datetime` → all 4.
* In `EXPLAIN EXTENDED` find `Path -> Alias:` — only `.../trips_orc/pickup_month=2024-01` is scanned,
  and `filterExpr: (pickup_month = '2024-01')`.

### 7. Bucketing (`sql/05_bucketing.sql`)

Complete **TODO 6** (`CLUSTERED BY ... SORTED BY ... INTO 4 BUCKETS`) and run:

```bash
./run_sql.sh sql/05_bucketing.sql
docker compose exec namenode hdfs dfs -ls /user/hive/warehouse/taxi.db/trips_bucketed
```

Expected: exactly **4 files** `000000_0 … 000003_0`; `TABLESAMPLE(BUCKET 1 OUT OF 4 ...)` reads one
of them; all 59,777 JFK rows (1 M sample) are in **one** file (`INPUT__FILE__NAME`).

## Checkpoint questions

1. What exactly is stored in the Metastore, and what is stored in HDFS? What happens to each on
   `DROP TABLE trips_csv` (external) vs `DROP TABLE trips_orc` (purge) ?
2. Why does `zones` need `OpenCSVSerde` while `trips_csv` works with `ROW FORMAT DELIMITED`?
   What is the downside of `OpenCSVSerde`?
3. Why are ORC/Parquet 4–5× smaller than CSV for the same rows? Name two reasons.
4. Why is Q1 faster on ORC/Parquet? Relate your answer to *column pruning*, *predicate pushdown /
   min-max statistics* and *partition pruning*.
5. What is the difference between static and dynamic partitioning? Why is `strict` mode the default?
6. Our partition column has ~4 values. What goes wrong if you partition by `pulocationid` **and**
   day? By `tpep_pickup_datetime`?
7. Partitioning vs bucketing: when do you use which? Why does bucketing give a fixed number of files?
8. Which rows went to the `2002-12`/`2009-01` partitions, and how would you handle them in a real pipeline?

## Stretch challenges

* **ACID**: create `zone_notes (locationid INT, note STRING) STORED AS ORC TBLPROPERTIES ('transactional'='true')`,
  `INSERT`, `UPDATE`, `DELETE`, then look at the `delta_*`/`delete_delta_*` directories in HDFS.
* **Statistics**: `ANALYZE TABLE trips_orc PARTITION (pickup_month) COMPUTE STATISTICS FOR COLUMNS;`
  then `SET hive.compute.query.using.stats=true; SELECT count(*) FROM trips_orc;` — how long does it take now, and why?
* **Load a second month** (`bash labs/datasets/download_taxi.sh 2024-02`) and use a **static** partition insert:
  `INSERT OVERWRITE TABLE trips_orc PARTITION (pickup_month='2024-02') SELECT ... WHERE ...`.
* **Vectorization**: `SET hive.vectorized.execution.enabled=false;` and rerun Q1 on ORC. Look for `Execution mode: vectorized` in `EXPLAIN`.
* **Schema-on-read**: `ALTER TABLE trips_csv SET LOCATION '/data/zones';` then `SELECT * LIMIT 3` — what do you get? (Set it back afterwards.)

## Cleanup

```bash
docker compose down -v
rm -f ../datasets/data/taxi/csv/*.csv      # the CSV copies (Parquet originals stay)
```

## Troubleshooting

| Symptom | Fix |
|---|---|
| `Could not open client transport ... Connection refused` | HiveServer2 is still starting (40–90 s). `docker compose logs -f hiveserver2` |
| `metastore` exits: `schema initialization failed` / `ClassNotFoundException: org.postgresql.Driver` | The `jdbc-driver` helper could not download the JDBC jar (offline?). `docker compose logs jdbc-driver`; rerun `docker compose up -d` when online |
| `Unable to create a terminal` from beeline | You ran beeline without a TTY (e.g. `exec -T` or piping). Use `./run_sql.sh file.sql`, or `docker compose exec -e JAVA_TOOL_OPTIONS=-Dorg.jline.terminal.provider=dumb hiveserver2 beeline ... -e "..."` |
| `beeline -f` stops half-way or echoes garbage | Known Hive 4.2 beeline quirk without a real terminal — use `./run_sql.sh` |
| `Name node is in safe mode` | HDFS just started — wait 10 s |
| A partition named `pickup_month=__HIVE_DEFAULT_PARTITION__` | Your partition expression returned NULL (TODO 3 not done). `ALTER TABLE trips_orc DROP PARTITION (pickup_month='__HIVE_DEFAULT_PARTITION__');` |
| HiveServer2 exits with code 137 | Out of memory: close other stacks, give Docker ≥ 4 GB |
| Everything is slow the first time | JVM + Tez warm-up; the second run of the same query is 2–3× faster |
