# L03 — Instructor notes

## Timing (≈ 2 h)

| Block | Min |
|---|---|
| Architecture recap (HS2 / Metastore / Tez / HDFS), start stack | 15 |
| 2. CSV to HDFS | 10 |
| 3. External tables + peek into the Metastore DB | 15 |
| 4. Partitioned ORC/Parquet (dynamic partitions, the "surprise" partitions) | 25 |
| 5. Format comparison | 15 |
| 6. Partition pruning / EXPLAIN | 15 |
| 7. Bucketing | 10 |
| Checkpoint discussion | 15 |

## Before class

* `docker pull apache/hive:4.2.1` (≈ 1.6 GB download, ≈ 4 GB on disk) and `docker pull postgres:18.6`.
  The stack also builds/uses `de-labs/hadoop:3.4.3` from L01.
* The `jdbc-driver` one-shot service downloads `postgresql-42.7.8.jar` from Maven Central on first
  start (stored in the `ext-jars` volume). Offline classrooms: run once while online and keep the volume.

## Design decisions (so you can explain them)

* **Official `apache/hive:4.2.1`** image (multi-arch, arm64 native). Metastore runs as its own
  service backed by **Postgres** (not embedded Derby) so students can query `TBLS`/`SDS`/`PARTITIONS`
  and see that the metastore is "just a database" — the same metastore idea reappears with Spark,
  Trino and Iceberg catalogs.
* **Tez local mode** inside HiveServer2 (image default) — no YARN needed, keeps RAM ≈ 2.3 GB.
* HDFS has a single DataNode (replication 1) to save memory; the L01 3-node cluster is not needed here.
* Hive 4.2's Beeline cannot run without a TTY (`Unable to create a terminal`, jline 3.25 FFM provider),
  and `beeline -f` is unreliable without a real terminal; `run_sql.sh` passes file contents with
  `-e` and `-Dorg.jline.terminal.provider=dumb`. Interactive `./beeline.sh` works normally.

## Common student errors

| Error | Fix |
|---|---|
| Forget `nonstrict` → `Dynamic partition strict mode requires at least one static partition column` | TODO 2 |
| Partition expression returns NULL → `__HIVE_DEFAULT_PARTITION__` | TODO 3 |
| Column order in dynamic-partition INSERT: partition column not last | The partition value must be the **last** SELECT column |
| Querying `zones.locationid` as number without CAST | OpenCSVSerde → strings; join with `CAST(z.locationid AS INT)` |
| Timing the first run | Always compare the 2nd run (JVM/Tez warm-up) |
| Confusion that `trips_orc` is "EXTERNAL" | Explain Hive 4 managed = ACID; translated external + purge |

## Grading hints

Collect the completed SQL files, the size table, the timing table and answers to Q1, Q3, Q4, Q6, Q7.
Q4 must mention column pruning *and* partition pruning; Q6 must mention small files / metastore pressure.

## Talking points

* Hive today: the SQL-on-Hadoop pioneer; its **Metastore** outlived it and is the catalog behind
  Spark SQL, Trino, Impala — and (as HMS catalog) Iceberg tables (Module 9).
* Hive-style partitioning (`col=value/` directories) is exactly what Spark writes with `partitionBy` (L05).
* Modern table formats (Iceberg) replace directory-listing partitions with metadata files, hidden
  partitioning and partition evolution — contrast with the "surprise partitions" here.
