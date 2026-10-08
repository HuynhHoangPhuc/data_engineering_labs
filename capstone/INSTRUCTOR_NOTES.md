# Capstone - Instructor notes

## Positioning

The capstone re-uses the exact building blocks of L08 (Debezium), L09 (Structured Streaming), L11 (Iceberg on SeaweedFS +
REST catalog + Trino), L10 (Airflow 3) and L12 (dimensional modeling / dbt). Students who finished those labs recognise every
container. The new ideas are *integration* ones: medallion layers on Iceberg, CDC -> current-state MERGE, data-quality gates
between layers, and running a multi-stage platform on limited hardware.

## Why Spark Connect in the reference?

Airflow must trigger Spark jobs. Options on an 8 GB laptop:

| Option | Verdict |
|---|---|
| PySpark + JVM inside the Airflow image | +700 MB image, a second JVM per task -> OOM on 8 GB |
| `docker exec` via the Docker socket mounted into Airflow | works but insecure and not portable |
| SparkSubmitOperator to a standalone cluster | needs Spark binaries in the Airflow image |
| **Spark Connect** (`pyspark-client`, 1.6 MB, no JVM) | chosen: one long-running Spark server holds the Iceberg/Kafka packages; Airflow tasks and laptops are thin gRPC clients |

## Suggested team roles

Ingestion (Debezium, Kafka, bronze), Modeling (silver, gold, dbt), Platform (Airflow, DQ, dashboard, runbook). Rotate for the report.

## Typical pitfalls to watch at milestone reviews

- Silver keeps the **first** event per key instead of the latest, or orders by `ingested_at` (all rows of one micro-batch share it) instead of LSN/offset.
- Deletes: `op='d'` has `after = null` - the key must come from `before` (the reference uses `coalesce(after_json, before_json)`).
- Snapshot events (`op='r'`) and later updates for the same key in one bronze batch: again, ordering by LSN.
- Gold fan-out (joining items and payments directly) -> revenue doubled. Ask for a reconciliation check.
- DQ checks that only `print` and never fail the task.
- Checkpoint deleted "to fix an error" -> bronze re-ingests everything from Kafka -> duplicates in bronze. Good discussion: bronze is append-only, so dedup by (topic, partition, offset) is the safety net (the reference DQ check `bronze_no_duplicate_offsets`).
- Replication slot growth when Connect is stopped for days (`max_slot_wal_keep_size=2GB` in the compose file limits the damage; ask for a monitoring query in the runbook).
- Metabase: the Starburst driver (for Trino) ships with Metabase; if a student's version doesn't list "Starburst", add the driver jar to `/plugins`.

## Hardware guidance

Tell teams on 8 GB laptops to follow the stage loop in README section 3 and to do the final demo on a 16 GB lab machine or
a cloud VM. Disk: all images together need ~15 GB (Debezium Connect 2.3 GB, Spark 2.2 GB, Trino ~2.5 GB, Airflow ~1.7 GB,
Metabase ~1 GB, Kafka 0.7 GB, Postgres 0.7 GB, SeaweedFS 0.35 GB, REST catalog ~0.7 GB).

## What was tested in the reference skeleton

All stages were run end-to-end on an 8 GB Apple Silicon Mac (Docker VM ~3.8 GB), one stage at a time:
- **Ingest:** Debezium (6 tables) → Kafka → `bronze_ingest.py --available-now` via Spark Connect → `lake.bronze.cdc_events`.
- **Batch:** `silver_merge.py` row counts equal Postgres for all 6 tables (incl. deletes); `gold_build.py` builds dim_date, dim_customers, fact_orders, daily_sales; DQ checks pass on normal data and fail after `simulate_shop.py --chaos`; the Airflow `capstone_pipeline` DAG succeeds, and an injected bad silver row fails `dq_silver` (downstream `upstream_failed`, failure callback logs `ALERT`).
- **Serve:** Trino 483 + Metabase v0.63 (~2.5 GB). Metabase's bundled Starburst (Trino) driver connects to `trino:8080`, catalog `lake`, schema `gold`; dashboard numbers match Trino and Spark (5,096 orders, revenue 696,236.03). `scripts/metabase_bootstrap.py` reproduces the connection + an example dashboard.
