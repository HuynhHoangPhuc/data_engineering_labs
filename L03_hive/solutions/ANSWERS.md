# L03 — Solutions

Complete SQL: [`01_external_tables.sql`](01_external_tables.sql) · [`02_partitioned_tables.sql`](02_partitioned_tables.sql) ·
[`03_compare_formats.sql`](03_compare_formats.sql) · [`04_partition_pruning.sql`](04_partition_pruning.sql) ·
[`05_bucketing.sql`](05_bucketing.sql). End-to-end: `CSV_LIMIT=1000000 bash solutions/l03_run_all.sh`
(~2.5 min on a fresh stack).

## TODOs

```sql
-- TODO 1
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
LOCATION '/data/zones'
TBLPROPERTIES ('skip.header.line.count' = '1');
-- TODO 2
SET hive.exec.dynamic.partition.mode = nonstrict;
-- TODO 3
date_format(tpep_pickup_datetime, 'yyyy-MM') AS pickup_month
-- TODO 4: see 02_partitioned_tables.sql (CREATE TABLE trips_parquet ... STORED AS PARQUET + INSERT OVERWRITE)
-- TODO 5
EXPLAIN DEPENDENCY SELECT count(*) FROM trips_orc
WHERE tpep_pickup_datetime >= '2024-01-01' AND tpep_pickup_datetime < '2024-02-01';
-- TODO 6
CLUSTERED BY (pulocationid) SORTED BY (pulocationid) INTO 4 BUCKETS
```

## Reference results (1,000,000-row sample of 2024-01, Apple M-series, Docker 4 GB)

| Item | Result |
|---|---|
| `SHOW PARTITIONS trips_orc` | 2002-12 (2 rows), 2009-01 (1), 2023-12 (10), 2024-01 (999,987) |
| INSERT OVERWRITE into ORC / Parquet | 14.5 s / 13.8 s |
| Size CSV / ORC / Parquet / bucketed ORC (5 cols) | 88.5 MB / 17.1 MB / 20.9 MB / 8.0 MB |
| Q1 CSV / ORC / Parquet | 3.5–4.6 s / 1.2–1.5 s / 1.1–1.8 s |
| Q1 top rows | 132 → 59,777 trips (avg fare 59.71); 161 → 48,749; 237 → 47,260 |
| Q2 | Manhattan 879,988 trips; Queens 106,101; Brooklyn 7,298 … |
| EXPLAIN DEPENDENCY | 1 input partition vs 4 input partitions |
| Bucketing | 4 files; sample bucket 1/4 = 199,497 rows (hash buckets are not equal-sized); JFK rows all in `000002_0` |

## Checkpoint answers

1. **Metastore**: database/table names, columns & types, SerDe + input/output formats, table type,
   **location** of every table and partition, partition values, statistics, ACID/transaction state.
   **HDFS**: only the data files. `DROP TABLE trips_csv` (external) removes metadata only — files in
   `/data/taxi_csv` stay. `DROP TABLE trips_orc` (external + `external.table.purge=true`, or a
   true managed table) removes metadata **and** the warehouse directory.
2. `ROW FORMAT DELIMITED` (LazySimpleSerDe) splits on the delimiter only — it cannot remove quotes
   or honour commas inside quotes, and our trips CSV has neither. The zone file has `"quoted"`
   fields ("Allerton/Pelham Gardens") → OpenCSVSerde. Downsides: every column is a STRING (cast
   yourself), slower parsing, no special NULL handling.
3. (a) **Columnar layout** puts similar values together → encodings (dictionary, RLE, delta,
   bit-packing) work very well (e.g. `vendorid`, `payment_type` have 2–6 distinct values).
   (b) **Binary types + compression** (ZLIB/Snappy) instead of text digits and delimiters. Also
   timestamps become 8-byte numbers instead of 19 characters.
4. Q1 needs 2–3 of 19 columns: ORC/Parquet read only those column chunks (**column pruning**);
   CSV must read and parse every byte of every line. Row-group/stripe **min/max statistics** let
   the reader skip data that cannot match a predicate (predicate pushdown), and the query on
   `pickup_month` scans only one directory (**partition pruning**). ORC/Parquet readers are also
   **vectorized** (1,024 rows per batch).
5. Static: the partition value is written in the statement (`PARTITION (pickup_month='2024-01')`),
   every row goes there. Dynamic: the value comes from the data (last SELECT column). Strict mode
   requires at least one static value so a buggy query cannot create thousands of partitions (and
   files/metastore objects) by accident — exactly what happened with the 2002/2009 rows at small scale.
6. `pulocationid` × day ≈ 265 × 31 ≈ 8,000 partitions per month, each with a tiny file → the
   *small-files problem* (NameNode heap, metastore load, slow planning, many tiny tasks). Partitioning
   by the raw timestamp creates ~one partition per second — disastrous. Rule of thumb: partitions
   should hold ≥ ~1 GB (or at least hundreds of MB), low-cardinality columns that queries filter on.
7. Partitioning = directories by value, for coarse filtering on low-cardinality columns
   (date, country). Bucketing = `hash(col) mod N` files, for high-cardinality columns (ids): enables
   sampling, bucket map joins / sort-merge-bucket joins and avoids a shuffle when two tables are
   bucketed the same way. N is fixed at DDL time, hence a fixed number of files (per partition).
8. A few trips whose `tpep_pickup_datetime` is in 2002, 2009 or December 2023 — device clock errors
   in the source. In a pipeline: validate the event time against the file's month, route
   out-of-range rows to a quarantine/"rejected" table (with a reason) and alert/count them; do not
   silently create partitions for them. (Covered again in L05 cleaning and the data-quality module.)
