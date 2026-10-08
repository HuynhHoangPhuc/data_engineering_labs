# L00 exercises - solutions and checkpoint answers

Reference runs: Apple Silicon Mac (8 GB), Python 3.12 venv (pandas 3.0.6, polars 2.0.0, DuckDB 1.5.6, pyarrow 25.0.1),
`postgres:18.6` with the synthetic Olist seed, taxi file `yellow_tripdata_2024-01.parquet`. Timings vary by machine;
counts and sizes do not.

## Solution files

| Part | Run (from `labs/L00_setup/exercises`) |
|---|---|
| (a) | `bash solutions/a_linux.sh` (+ the commands of README a.2-a.4) |
| (b) | `docker compose exec -T postgres psql -U olist < solutions/b_sql.sql` and `... < b_sql/oltp_vs_olap.sql` |
| (c) | `python solutions/c_python/data_handling.py` |
| (d) | the commands in README (d) - there is no script; the point is typing them |
| (e) | `python solutions/e_formats/formats.py` |

Expected outputs are listed in the README next to each task.

## Checkpoint answers

**1. `sort` before `uniq -c`.** `uniq` only collapses *adjacent* identical lines. Unsorted input gives one count per
run of equal lines (e.g. `1 1`, `1 2`, `3 1`, ...), so the same value appears many times with partial counts.

**2. `NR` / `FNR`.** `NR` is the record number across all input files, `FNR` the record number within the current file.
They are equal only while awk reads the first file, so `NR == FNR { ...; next }` builds the lookup table from the
zone file and the second block processes the trips. It is a hash join done by hand (the small side in memory), the same
idea as Spark's broadcast join.

**3. Permissions.** `600` = `rw-------`: only the owner can read/write (right for secrets; ssh refuses keys that are
more open). `644` = `rw-r--r--`: everyone on the machine can read. A script needs the execute bit `x` to be run as
`./script.sh` (otherwise `Permission denied`, exit 126); `bash script.sh` only needs read permission.

**4. Shell vs environment variable.** `TAXI_FILE=a.csv` creates a shell variable visible only in the current shell;
`export` puts it in the environment, which child processes (scripts, python, docker) inherit. `VAR=x cmd` sets it for one
command. `docker compose` reads `.env` in the project folder to substitute `${VAR}` in `docker-compose.yml`
(and `env_file:` passes variables into containers) - which is why `.env` files with secrets must not be committed.

**5. Aggregate before joining (B5).** `order_payments` can have several rows per order (vouchers + card). Joining
orders to raw payments and then to anything else at item level multiplies rows (fan-out): an order with 2 items and
2 payments produces 4 rows, so revenue is counted twice. Summing payments per order first gives exactly one row per
order (the grain of the join). Same lesson as L12's `fact_orders`.

**6. `RANK`, `DENSE_RANK`, `ROW_NUMBER`.** Revenues 100, 90, 90, 80 -> `ROW_NUMBER` 1, 2, 3, 4 (ties broken arbitrarily);
`RANK` 1, 2, 2, 4 (gap after a tie); `DENSE_RANK` 1, 2, 2, 3 (no gap). "Top 2 per state" with `RANK` may return more
than 2 rows when there are ties; `ROW_NUMBER` returns exactly 2.

**7. OLTP vs OLAP plans.** The point lookup uses an **Index Scan** on the primary-key B-tree: it descends ~3-4 index
pages and reads one heap page - ~10 buffers and ~0.2 ms, and this barely grows with table size (O(log n)). The
aggregation needs every row: **Parallel Seq Scan** of all ~25,000 pages (195 MB) + Hash Join + HashAggregate, ~160 ms,
growing linearly with the data. Row stores with indexes are optimised for many small reads/writes by key (OLTP);
analytics wants scans of few columns over many rows (OLAP) -> column stores, compression, parallelism, and a
separate system so dashboards do not slow down the shop.

**8. Parquet vs CSV in DuckDB.** CSV must be read entirely and parsed as text (find delimiters, convert strings to
numbers) for every query, even if only 2 columns are needed. Parquet is columnar and typed: DuckDB reads only the
`payment_type` and `fare_amount` column chunks (a few hundred KB here), already in binary form, compressed and
dictionary-encoded, and can skip row groups using min/max statistics. Result: ~10-20x faster on 1 M rows, and the
gap grows with the number of columns.

**9. Bind mount vs named volume.** A bind mount maps a host path into the container (`./www:/www`) - you see/edit the
files on the laptop; Docker never deletes them. A named volume (`ticks:/data`) is storage managed by Docker (inside
the Docker VM on macOS/Windows); it survives `docker compose down` and container re-creation and is deleted by
`docker compose down -v` or `docker volume rm`. Use bind mounts for code/config/datasets, volumes for database files.

**10. Format choice.** (a) Partner export: CSV (or CSV.gz) - universal and human-readable; agree on a schema,
encoding and delimiter, or Parquet if the partner has modern tooling. (b) Data lake table: Parquet (zstd/snappy),
usually managed by a table format such as Iceberg/Delta (Module 9) for schema evolution and ACID. (c) Kafka event: a
row-oriented, schema'd format - Avro or Protobuf with a schema registry (Module 6), or JSON for simplicity;
columnar formats make no sense for single events.
