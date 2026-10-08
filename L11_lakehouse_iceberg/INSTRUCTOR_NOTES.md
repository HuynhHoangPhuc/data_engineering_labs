# L11 - Instructor notes

## Timing (≈ 3 h)

| Block | Minutes |
|---|---|
| Table formats: why "a folder of Parquet files" is not a table (atomicity, schema, listing cost); Iceberg metadata tree on the whiteboard | 25 |
| Step 1-2: stack, create + load, browse the files in the filer UI | 25 |
| Step 3: Trino on the same table, EXPLAIN ANALYZE | 20 |
| Step 4-5: schema + partition evolution | 20 |
| Step 6: MERGE / DELETE, copy-on-write discussion | 25 |
| Step 7-8: metadata tables, time travel, maintenance | 30 |
| Checkpoint questions | 15 |

## Before class

- Disk: SeaweedFS 0.35 GB + REST fixture ~0.7 GB + Spark 2.2 GB (shared with L09) + **Trino ~2.4 GB**. Pull at home:
  `docker compose --profile trino pull`.
- Warm the Ivy cache once (`docker compose exec spark /opt/spark/bin/spark-sql -e "SHOW NAMESPACES"`); `down` without `-v` keeps it.
- The taxi file `labs/datasets/data/taxi/yellow_tripdata_2024-01.parquet` must exist.

## Tested (Sep 2026, Apple Silicon, Docker VM 3.8 GB)

- create-bucket, REST catalog `/v1/config`, Spark `01`-`06` solution scripts (load of 2,964,624 rows in ~6 s;
  partition evolution spec 0/1 file counts; MERGE 3,351 -> 4,484 rows; DELETE of 18 bogus trips; snapshots/history/files;
  time travel by literal id = 2,964,624; rewrite_data_files 8 -> 1 file; expire_snapshots; rewrite_manifests).
- Memory while Spark runs: seaweedfs ~320 MB (limit raised to 768 MB), rest ~240 MB, Spark ~1.3 GB.
- Found during testing: `VERSION AS OF (subquery)` is a parse error and `DECLARE ... DEFAULT (subquery)` is rejected ->
  README uses literal ids. Procedures with `table => 'nyc.x'` print a harmless `CATALOG_NOT_FOUND nyc` stack trace ->
  use `'lake.nyc.x'`.
- Trino 483 (~1.05 GB RSS): `02_trino_queries.sql` (count, top days, `EXPLAIN ANALYZE` reads 77,033 rows = one partition, `$snapshots`), `FOR VERSION AS OF`, `EXECUTE optimize`. Spark and Trino were run one at a time to fit a ~4 GB Docker VM.

## Common student errors

- Forgetting `TIMESTAMP_NTZ`: the TLC Parquet timestamps have no time zone; declaring `TIMESTAMP` (with local time zone)
  makes Spark shift values by the session time zone (we pin `UTC`, but mention it).
- Positional `INSERT` after schema evolution puts values in the wrong columns - use explicit column order (or `INSERT ... BY NAME`).
- Running Trino and a Spark job at the same time on a 4 GB Docker VM -> OOM kills.
- Confusing `expire_snapshots` with deleting data: current data is never touched.
- Expecting MinIO: explain the 2025/2026 licensing/distribution story and that S3 is an API, not a product.

## Grading hints

| Item | Points |
|---|---|
| Table created with hidden partitioning, loaded, counts match in Spark and Trino | 20 |
| Schema + partition evolution with `files` table evidence (spec_id) | 20 |
| MERGE idempotency shown (second run adds nothing), DELETE | 20 |
| Time travel + metadata table queries | 15 |
| Maintenance before/after file counts | 10 |
| Checkpoint answers | 15 |
