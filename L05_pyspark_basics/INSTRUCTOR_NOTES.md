# L05 — Instructor notes

## Timing (≈ 2 h)

| Block | Min |
|---|---|
| Spark architecture recap (driver, executors, partitions, jobs/stages/tasks) + start | 15 |
| 1–2 read, lazy evaluation (watch the UI while stepping through the pyspark shell) | 20 |
| 3 cleaning (+ ANSI mode discussion) | 20 |
| 4–5 joins and aggregations | 20 |
| 6 windows | 15 |
| 7–8 Spark SQL + `explain("formatted")` | 15 |
| 9–10 Spark UI tour + partitioned write | 15 |

## Setup facts

* Image `spark:4.1.3-python3` (Docker Official Image; multi-arch; Java 17; **Python 3.10** inside the
  image — PySpark 4.1 supports 3.10+). Local mode `local[4]`, driver 2 GB, `spark.sql.shuffle.partitions=8`
  (see `conf/spark-defaults.conf`) — explain why 200 is the default and why it is wrong here (L06 measures it).
* Spark 4 has **ANSI mode on by default**: `fare / 0` raises `DIVIDE_BY_ZERO`, bad casts raise errors
  instead of returning NULL. Students coming from Spark 3 tutorials hit this — the solution uses `try_divide`.
* The Spark UI (:4040) exists only while an application runs. Use `--hold 600` or the `pyspark` shell.
* Output is written to `labs/L05_pyspark_basics/output/` (bind mount). On **Linux** hosts the container
  user (uid 185) may lack write permission: `chmod -R a+w labs/L05_pyspark_basics/output` first.

## Common student errors

| Error | Fix |
|---|---|
| `[DIVIDE_BY_ZERO]` / `[CAST_INVALID_INPUT]` | ANSI mode — use `try_divide` / `try_cast` or filter first |
| `AnalysisException: [AMBIGUOUS_REFERENCE]` after joining zones twice | rename zone columns before each join (TODO 3) |
| Thousands of tiny files after `partitionBy` | missing `repartition("pickup_date")` (TODO 7 discussion) |
| Calling `count()` everywhere while debugging | every action = a job; show it in the UI |
| `collect()` on the full DataFrame | driver OOM; use `show()`, `limit()`, `take()` |

## Grading hints

Collect `src/l05_basics.py` and answers. Q3 (lazy evaluation / DAG) and Q5 (reading the
formatted plan: Exchange = shuffle, partial/final HashAggregate, BroadcastHashJoin) are the key ones.
