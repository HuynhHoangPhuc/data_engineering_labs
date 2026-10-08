# L04 — Instructor notes

## Timing (≈ 2 h 15 min)

| Block | Min |
|---|---|
| Intro: full vs incremental, split columns, why `order_seq` exists | 15 |
| Part 1 Sqoop 1.1–1.2 (+ Postgres log watching) | 25 |
| Part 1 Sqoop 1.3–1.6 (append, saved job, lastmodified/merge, export) | 35 |
| Part 2 Spark JDBC full | 25 |
| Part 2 incremental watermark | 20 |
| Comparison table + checkpoint discussion | 15 |

If short on time: demo Part 1 from the front (run `solutions/sqoop_all.sh` + `sqoop_after_changes.sh`,
~6 min) and let students do Part 2 hands-on.

## Setup facts

* `de-labs/sqoop:1.4.7` = course Hadoop image + Sqoop 1.4.7 tarball (archive.apache.org) +
  commons-lang 2.6 + commons-cli 1.5.0 (replaces Hadoop's 1.9.0, whose removed
  `Option.addValueForProcessing` makes Sqoop fail with "Could not load required method of Parser")
  + PostgreSQL JDBC 42.7.8. Sqoop MapReduce runs with the **LocalJobRunner** (no YARN in this stack) —
  mention that on a cluster each mapper would be a YARN container on a different node.
* Postgres runs with `log_statement=all` so students see the exact range queries of Sqoop and Spark.
  This is the single most useful teaching device of the lab.
* Each Sqoop command takes 10–25 s (JVM + codegen + javac of the generated record class) — use the
  waiting time to discuss the log.

## Common student errors

| Error | Fix |
|---|---|
| `--split-by order_id` | text split column → error; this is the "why order_seq" discussion |
| Forgetting `--null-string '\\N'` then exporting | export fails on `"null"` strings for timestamps; use the same null tokens on import and export |
| `--last-value` typed by hand with the wrong format | copy the value Sqoop prints; for timestamps quote it |
| `simulate_changes.sql` run twice → 6 new orders | fine — explain that counts differ from the README |
| Spark: `lowerBound/upperBound` thought to be filters | ask Q3; show that rows outside land in the first/last partition |
| Spark incremental run reads everything every time | watermark file not written (exception before), or `_state` deleted |

## Grading hints

Collect: filled comparison table (with their own timings), per-file row counts for orders vs customers
(TODO 1.2), completed `spark_jdbc_ingest.py`, answers Q1, Q3, Q4, Q6. Q6 must mention deletes and
intermediate updates between two runs (and/or long-running transactions committing "in the past").

## Talking points

* Sqoop's legacy lives on: the same split-by/bounding-query design is in Spark JDBC, Airbyte,
  Debezium's initial snapshot, AWS DMS full load.
* Query-based incremental loads vs log-based CDC (L08): deletes, every intermediate version, load on
  the source, latency.
* In the capstone the OLTP source is read with CDC; watermark loads remain common for SaaS APIs
  and warehouses that do not expose a change log.
