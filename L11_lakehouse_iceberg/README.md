# L11 - Lakehouse with Apache Iceberg: Spark writes, Trino reads, on an S3-compatible object store

| | |
|---|---|
| Module | 9 - Lakehouse |
| Time | 3 hours |
| Stack | `chrislusf/seaweedfs:4.44` (S3 API), `apache/iceberg-rest-fixture:1.10.1` (REST catalog), `spark:4.1.3-python3` + Iceberg **1.11.0**, `trinodb/trino:483` |
| RAM | ~0.7 GB idle; + Spark driver ~1.3 GB while a job runs; + Trino ~1.3 GB (profile `trino`) |

## Learning objectives

1. Explain the three layers of a lakehouse table: **data files** (Parquet) -> **metadata** (manifests, snapshots, `metadata.json`) -> **catalog** (pointer to the current `metadata.json`).
2. Create a **hidden-partitioned** Iceberg table (`days(pickup_ts)`) with Spark and query the *same* table from **Trino**.
3. Perform **schema evolution** and **partition evolution** without rewriting data.
4. Do **upserts with `MERGE INTO`** and row-level `DELETE`, and understand copy-on-write.
5. Use **time travel** and **metadata tables** (`snapshots`, `history`, `files`, `partitions`).
6. Run **maintenance**: `rewrite_data_files` (compaction), `expire_snapshots`, `rewrite_manifests`.

## Why SeaweedFS and not MinIO?

MinIO was the classic "S3 on a laptop". In 2025 MinIO removed features from the community edition, stopped publishing
binaries and Docker images (Oct 2025), put the repository in maintenance mode (Dec 2025) and archived it (Feb 2026).
Old images still exist in some registries but receive no security fixes. We use **SeaweedFS** (Apache-2.0, arm64 images,
~150-300 MB RAM) with its built-in S3 gateway. Everything in this lab only uses the S3 API, so Garage, Ceph RGW, or real
AWS S3 work the same way - only the endpoint and keys change.

The **Iceberg REST catalog** is Apache's reference implementation (`iceberg-rest-fixture`), backed here by a SQLite file on
a volume. It is meant for tests/labs; production options are Apache Polaris, Lakekeeper, Nessie, Unity Catalog, AWS Glue,
etc. - all speak the same REST protocol, so Spark/Trino configs barely change.

## Architecture

```
   spark-sql / pyspark (container "spark")                trino CLI (container "trino", profile "trino")
          |  Iceberg SparkCatalog "lake"                          |  Iceberg connector, catalog "lake"
          +-------------------+-------------------------------+---+
                              | REST (table -> current metadata.json)
                              v
                 iceberg-rest :8181  (SQLite file on a volume)
                              |
          data + metadata files via S3 API (path-style)
                              v
       seaweedfs  :8333 S3  | :8888 filer UI  -> bucket "warehouse"
            s3://warehouse/nyc/trips/data/pickup_ts_day=2024-01-15/xxx.parquet
            s3://warehouse/nyc/trips/metadata/00003-....metadata.json, snap-....avro, ...-m0.avro
```

## Files

| Path | Purpose |
|---|---|
| `docker-compose.yml` | seaweedfs, create-bucket (one-shot), iceberg-rest, spark (idle), trino (profile) |
| `conf/spark-defaults.conf` | Iceberg runtime + AWS bundle packages, catalog `lake` (REST + S3FileIO) |
| `trino/` | Trino single-node config (1 GB heap) + `catalog/lake.properties` |
| `seaweedfs/s3.json` | S3 access key/secret (`lakeadmin` / `lakepassword`) |
| `sql/worksheet.sql` | **Starter**: the TODOs of every step |
| `solutions/01_*.sql ... 06_*.sql` | Complete SQL per step; `solutions/CHECKPOINT_ANSWERS.md` |

The taxi file comes from `labs/datasets/data/taxi/yellow_tripdata_2024-01.parquet` (mounted read-only at `/data/taxi`).

---

## Step 1 - Start the object store, catalog and Spark

```bash
cd labs/L11_lakehouse_iceberg
docker compose up -d
docker compose logs create-bucket          # "created bucket warehouse"
curl -s localhost:8181/v1/config           # REST catalog answers with its endpoint list
```

Open the SeaweedFS filer UI at <http://localhost:8888/buckets/warehouse/> - empty for now.

Start the Spark SQL shell (first start downloads the Iceberg jars, ~50 MB):

```bash
docker compose exec spark /opt/spark/bin/spark-sql
```

`spark.sql.defaultCatalog=lake`, so `SHOW NAMESPACES;` lists the REST catalog's namespaces.

## Step 2 - Create a partitioned Iceberg table and load a month (Spark)

Work through `sql/worksheet.sql` step 2 (solution: `solutions/01_create_and_load.sql`, or run it non-interactively:
`docker compose exec spark /opt/spark/bin/spark-sql -f /opt/lab/solutions/01_create_and_load.sql`).

Key DDL:

```sql
CREATE TABLE lake.nyc.trips (... pickup_ts TIMESTAMP_NTZ, ...)
USING iceberg
PARTITIONED BY (days(pickup_ts))        -- a partition *transform*: no extra column, users just filter on pickup_ts
TBLPROPERTIES ('format-version' = '2');
```

Expected (the load of ~3 M rows takes ~6 s):

```
2964624
{"pickup_ts_day":2002-12-31}    2       1
{"pickup_ts_day":2009-01-01}    3       1
{"pickup_ts_day":2023-12-31}    10      1
{"pickup_ts_day":2024-01-01}    81013   1
{"pickup_ts_day":2024-01-02}    75519   1
```

Yes - the raw TLC data contains trips "from 2002". We delete them in step 6.

Now look at the files: <http://localhost:8888/buckets/warehouse/nyc/trips/> -> `data/pickup_ts_day=.../*.parquet` and
`metadata/*.metadata.json`, `snap-*.avro` (manifest list), `*-m0.avro` (manifests).

## Step 3 - Query the same table from Trino

```bash
docker compose --profile trino up -d trino       # ~30 s to start
docker compose exec trino trino --catalog lake --schema nyc
```

```sql
SELECT count(*) FROM trips;
SELECT CAST(pickup_ts AS date) AS day, count(*) AS trips, round(sum(total_amount)) AS revenue
FROM trips WHERE pickup_ts >= TIMESTAMP '2024-01-01' AND pickup_ts < TIMESTAMP '2024-02-01'
GROUP BY 1 ORDER BY trips DESC LIMIT 5;
EXPLAIN ANALYZE SELECT count(*) FROM trips
WHERE pickup_ts >= TIMESTAMP '2024-01-15' AND pickup_ts < TIMESTAMP '2024-01-16';
SELECT snapshot_id, operation, committed_at FROM "trips$snapshots";
```

Expected: the same count as Spark; `EXPLAIN ANALYZE` shows the table scan read **one** partition's data
(`Input: 77,033 rows`), not 3 M - partition pruning from the hidden `days()` transform. Two engines, one table, no copies.

> Memory tip (4 GB Docker): stop Trino (`docker compose stop trino`) before heavy Spark jobs, and vice versa.

## Step 4 - Schema evolution (Spark, then look from Trino)

```sql
ALTER TABLE lake.nyc.trips ADD COLUMN congestion_surcharge DOUBLE COMMENT 'added in step 4';
ALTER TABLE lake.nyc.trips RENAME COLUMN trip_distance TO trip_distance_mi;
ALTER TABLE lake.nyc.trips ALTER COLUMN vendor_id TYPE BIGINT;
DESCRIBE TABLE lake.nyc.trips;
```

Each statement takes milliseconds: Iceberg tracks columns by **ID**, not by name or position, so renames and additions
are metadata-only. Old files simply return NULL for `congestion_surcharge`. In Trino, `DESCRIBE trips;` shows the new
schema immediately.

## Step 5 - Partition evolution

```sql
ALTER TABLE lake.nyc.trips ADD PARTITION FIELD vendor_id;
-- insert the trips of 2024-01-31 23:00-24:00 again (see solutions/03_schema_partition_evolution.sql)
SELECT spec_id, count(*) AS files, sum(record_count) AS rows FROM lake.nyc.trips.files GROUP BY spec_id;
```

Expected:

```
1       2       3061          <- new files: partitioned by day AND vendor
0       35      2964624       <- old files keep the old layout (day only) - nothing was rewritten
```

Queries work across both layouts; Iceberg plans each spec separately.

## Step 6 - MERGE INTO (upserts) and DELETE

Run `solutions/04_merge_upsert.sql` step by step. It creates `lake.nyc.zone_daily` (trips/revenue per day and pickup zone)
for Jan 1-15, then MERGEs a recomputation of Jan 10-20:

```
rows_before_merge: 3351  max_day 2024-01-15
rows_after_merge:  4484  max_day 2024-01-20        <- 10-15 matched (unchanged), 16-20 inserted
snapshots:  append (added 3351) | overwrite (added 4484, deleted 3351)
```

The `overwrite` snapshot shows **copy-on-write**: the data file of the January partition was rewritten with the merged rows.
Run the MERGE again: no new data - idempotent upserts are what make pipelines safely re-runnable.

Finally `DELETE FROM lake.nyc.trips WHERE pickup_ts < '2024-01-01' OR pickup_ts >= '2024-02-01'` removes the 18 bogus trips.

## Step 7 - Metadata tables and time travel

```sql
SELECT committed_at, snapshot_id, parent_id, operation, summary['total-records'] FROM lake.nyc.trips.snapshots;
SELECT * FROM lake.nyc.trips.history;
SELECT file_path, partition, record_count FROM lake.nyc.trips.files LIMIT 5;
```

Expected snapshots (ids differ):

```
2026-09-23 17:31:36   7624722956560461405  NULL                 append  2964624
2026-09-23 17:31:52   8656908829285089402  7624722956560461405  append  2967685
2026-09-23 17:32:04   1191717774087485731  8656908829285089402  delete  2967667
```

Time travel (paste *your* first snapshot id - Spark requires a literal):

```sql
SELECT count(*) FROM lake.nyc.trips;                                   -- 2967667
SELECT count(*) FROM lake.nyc.trips VERSION AS OF 7624722956560461405; -- 2964624  (before re-insert + delete)
```

In Trino: `SELECT count(*) FROM trips FOR VERSION AS OF 7624722956560461405;`
Roll back (metadata-only): `CALL lake.system.rollback_to_snapshot('lake.nyc.trips', <id>);`

## Step 8 - Maintenance

Run `solutions/06_maintenance.sql`:

```
partition {"trip_date_month":649}  file_count 8  ...     <- 8 tiny INSERTs = 8 tiny files ("small files problem")
CALL rewrite_data_files -> rewritten_data_files_count 8, added_data_files_count 1
partition {"trip_date_month":649}  file_count 1
CALL expire_snapshots(retain_last => 1) -> deleted_data_files_count ..., deleted_manifest_lists_count ...
```

After `expire_snapshots`, time travel to an expired snapshot fails - history is a cost (storage) you manage. In Trino the
equivalents are `ALTER TABLE trips EXECUTE optimize` and `ALTER TABLE trips EXECUTE expire_snapshots(retention_threshold => '7d')`.

---

## Checkpoint questions

1. What exactly does the REST catalog store for `lake.nyc.trips`? What happens on a commit when two writers race?
2. Why can the user filter on `pickup_ts` and still get partition pruning, although there is no `pickup_date` column? What goes wrong with Hive-style `PARTITIONED BY (pickup_date)` when a user filters on `pickup_ts`?
3. Why is renaming a column safe in Iceberg but dangerous with plain Parquet files + Hive metastore?
4. After partition evolution, some files are partitioned by day and others by day+vendor. How does a query for one day and vendor 2 get planned?
5. The MERGE produced an `overwrite` snapshot that deleted 3,351 and added 4,484 records although only 1,133 rows were new. Why? What would `write.merge.mode=merge-on-read` change?
6. What is the difference between `expire_snapshots` and `remove_orphan_files`?
7. Spark and Trino both wrote/read the same table. What guarantees that Trino never sees a half-written Spark commit?

Answers: `solutions/CHECKPOINT_ANSWERS.md`.

## Stretch challenges

1. **Merge-on-read**: `ALTER TABLE lake.nyc.zone_daily SET TBLPROPERTIES ('write.merge.mode'='merge-on-read', 'write.delete.mode'='merge-on-read')`, repeat the MERGE and look for `delete` files in `lake.nyc.zone_daily.files` (`content = 1`).
2. **Branches and tags** (Iceberg WAP): `ALTER TABLE lake.nyc.trips CREATE BRANCH audit`, write to the branch (`spark.wap.branch`), validate, then `CALL lake.system.fast_forward('lake.nyc.trips', 'main', 'audit')`.
3. **Sort order / Z-order**: `CALL lake.system.rewrite_data_files(table => 'lake.nyc.trips', strategy => 'sort', sort_order => 'zorder(pu_location_id, do_location_id)')` and compare files read by a query filtering on `pu_location_id` (Trino `EXPLAIN ANALYZE`).
4. **PyIceberg**: `pip install "pyiceberg[s3fs,pyarrow]"`, connect to `http://localhost:8181` with the S3 endpoint `http://localhost:8333` and read the table into pandas without any JVM.
5. **Another engine**: DuckDB's `iceberg` extension can attach a REST catalog - query the table from DuckDB.

## Cleanup

```bash
docker compose --profile trino down -v
```

## Troubleshooting

| Symptom | Fix |
|---|---|
| `create-bucket` fails / `NoSuchBucket` | SeaweedFS not ready yet: `docker compose up -d` again (create-bucket is idempotent). |
| Spark: `ClassNotFoundException: org.apache.iceberg.spark.SparkCatalog` | Jars not downloaded (no internet in the container) - check `spark.jars.packages` output at startup. |
| Spark: `Unable to execute HTTP request: seaweedfs` / 403 | Endpoint/keys in `conf/spark-defaults.conf` must match `seaweedfs/s3.json`; path-style access must be `true`. |
| Trino: `Query exceeded per-node memory limit` | Laptop config (`query.max-memory-per-node=512MB`): add filters/LIMIT or raise it in `trino/config.properties` and `-Xmx` in `trino/jvm.config`. |
| Trino exits with code 137 | Docker ran out of memory - stop the Spark container first. |
| `[PARSE_SYNTAX_ERROR]` on `VERSION AS OF (SELECT ...)` | Spark requires a literal snapshot id/timestamp; copy it from the `snapshots` table. |
| `CATALOG_NOT_FOUND ... nyc` warning in CALL output | Use catalog-qualified names in procedures: `table => 'lake.nyc.trips'`. |
| Tables gone after restart | You used `down -v` (volumes deleted) - that's the cleanup command. Use `docker compose stop` to pause. |
