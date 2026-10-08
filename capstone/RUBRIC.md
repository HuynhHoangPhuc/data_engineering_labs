# Capstone grading rubric (100 points + 10 bonus)

Each criterion is graded on 4 levels. Points shown are the maximum for the level.

| # | Criterion | Excellent | Good | Fair | Missing / broken |
|---|---|---|---|---|---|
| R1 | **CDC ingestion** (10) | All 6 tables captured; snapshot + streaming shown; connector config explained (slot, publication, replica identity choice); slot monitoring query in runbook (10) | All tables captured, config partly explained (7) | Some tables / only snapshot (4) | No CDC (0) |
| R2 | **Bronze streaming** (15) | Checkpointed Structured Streaming into Iceberg; kill/restart demo with **evidence** (Kafka end offsets == bronze counts per partition, no duplicates); partitioning justified (15) | Works, restart shown without evidence (10) | Batch job instead of streaming, or no checkpoint (5) | Missing (0) |
| R3 | **Silver** (15) | All tables; incremental MERGE; deletes and out-of-order events handled (latest LSN wins); idempotent re-runs proven (15) | All tables, full rebuild each run, deletes handled (10) | Some tables; deletes ignored (5) | Missing (0) |
| R4 | **Gold / modeling** (15) | Clear grain statements; fact + >=3 dims + date dim + aggregate; conformed keys; documented star-schema diagram; SQL reproducible (15) | Star schema present, some grain issues (e.g. fan-out double counting) (10) | Flat tables only (5) | Missing (0) |
| R5 | **Data quality** (10) | >=6 meaningful checks on all layers incl. reconciliation; failing check demonstrably stops downstream and is visible in Airflow (10) | >=4 checks, gate works (7) | Checks exist but don't block (3) | None (0) |
| R6 | **Orchestration** (10) | Airflow 3 DAG(s) with TaskFlow, retries, templated dates, Asset-based trigger or clear dependencies, failure callback, backfill demonstrated (10) | DAG runs end-to-end with retries (7) | Manual scripts + a partial DAG (3) | Missing (0) |
| R7 | **Serving / dashboard** (10) | >=4 charts answering the business questions, numbers reconciled with SQL, freshness shown on the dashboard (10) | 4 charts, not reconciled (7) | 1-3 charts (3) | Missing (0) |
| R8 | **Operations & engineering quality** (10) | Iceberg maintenance scheduled; runbook (replay, backfill, slot growth, schema change); clean repo, README to reproduce in < 15 min; resource-aware setup (profiles) (10) | Most of it (7) | Hard to reproduce (3) | - (0) |
| P | **Report & presentation** (5) | Architecture diagram, design decisions and trade-offs (why Iceberg, why availableNow vs continuous, cost of REPLICA IDENTITY FULL, ...), honest limitations, clear demo (5) | Good but superficial trade-offs (3) | Demo only (1) | - (0) |

## Bonus (max +10)

| Bonus | Points |
|---|---|
| Gold layer in **dbt** with tests + docs (dbt-trino or dbt-spark) | +4 |
| SCD type 2 dimension (customers) with correct as-of joins in the fact | +3 |
| Avro + schema registry with a demonstrated compatible schema change | +3 |
| CI pipeline (lint, unit tests for transformations, `dbt build` on a sample) | +3 |
| Alerting to a real channel (Slack/Discord/e-mail webhook) | +2 |
| Real Kaggle dataset end-to-end with performance notes | +2 |

## Deductions

- Secrets committed to git (real passwords/tokens): -5
- Pipeline cannot be started from the README by the grader: -10 (one chance to fix within 48 h)
- Numbers on the dashboard contradict the report without explanation: -5

## Individual contribution

Each student submits a 1-paragraph contribution statement; git history is checked. Individual grade = team grade ± up to 10%.
