# Capstone milestones

| Milestone | Week | Demo / hand-in | Weight in rubric |
|---|---|---|---|
| M1 - Platform up + CDC | end of week 1 | Core + ingest stages start from README; connector RUNNING; `simulate_shop.py` changes visible in Kafka; design doc v1 (architecture diagram, table list, grain of each gold table) | R1 |
| M2 - Bronze + Silver | end of week 2 | Bronze streaming with restart evidence; silver tables for all 6 sources with deletes applied; 3 DQ checks | R2, R3, part of R5 |
| M3 - Gold + orchestration | end of week 3 | Star schema; Airflow DAG running bronze -> silver -> gold with DQ gates, retries, failure callback; backfill of one day | R4, R5, R6 |
| M4 - Serving + final | week 4 | Dashboard (4+ charts) on Trino; maintenance job; runbook; report; 10-min presentation + live or recorded demo | R7, R8, P |

## Week-by-week plan (suggested)

### Week 1
- Read L08, L09, L11 again. Clone the skeleton, run the smoke test in `README.md` section 6.
- Decide team roles: *ingestion* (Debezium/Kafka/bronze), *modeling* (silver/gold/dbt), *platform* (Airflow, DQ, dashboard).
- Write the design doc v1: which Postgres columns become which gold columns; grain of `fact_orders` (order) vs `fact_order_items` (line).
- Exercise the CDC path: `simulate_shop.py`, inspect topics, choose `REPLICA IDENTITY` per table (justify).

### Week 2
- Bronze: finish `bronze_ingest.py` settings (trigger, partitioning, checkpoint location), run the kill/restart experiment, record evidence.
- Silver: complete `silver_merge.py` for all tables (primary keys from `01_schema.sql`, composite keys for items/payments).
- Handle ordering: several events for the same key in one batch -> keep the one with the highest `lsn` (window function).
- First DQ checks (bronze not empty, silver PK unique, silver row count == Postgres row count right after a sync).

### Week 3
- Gold: dimensions, facts, `daily_sales`; avoid fan-out; `dim_date`.
- Airflow DAG: complete `capstone_pipeline.py`; add failure callback; test a failing DQ check (inject bad data with `simulate_shop.py --chaos`).
- Optional: re-implement gold in dbt (`dbt-trino` on the `serve` stage or `dbt-spark` via Spark Connect/Thrift).

### Week 4
- Dashboard in Metabase (Trino driver) or Superset.
- Maintenance DAG (weekly compaction + expire snapshots).
- Runbook, report, presentation. Rehearse the demo on a clean machine (`down -v`, then follow your README).

## Checkpoint meeting agenda (15 min per team)

1. Live demo of the milestone (5 min) - from a clean start if possible.
2. Walk through one design decision and its alternative (5 min).
3. Risks and next steps; instructor feedback (5 min).
