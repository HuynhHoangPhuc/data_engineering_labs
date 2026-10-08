# L10 - Orchestration with Apache Airflow 3

| | |
|---|---|
| Module | 8 - Orchestration |
| Time | 2.5 - 3 hours |
| Stack | Airflow **3.3.2** (`apache/airflow:slim-3.3.2-python3.12` + DuckDB, built locally), LocalExecutor, `postgres:18.6` metadata DB |
| RAM | ~0.9 GB idle, ~1.3 GB while a task runs |

## Learning objectives

1. Run a minimal Airflow 3 deployment and name its components: **API server** (UI + REST + Execution API), **scheduler**, **DAG processor**, metadata DB, executor.
2. Write a DAG with the **TaskFlow API** from the Airflow 3 Task SDK (`from airflow.sdk import dag, task, Asset`).
3. Make runs deterministic with the **logical date** and Jinja templating; understand why you never use `datetime.now()` inside a pipeline.
4. Configure **retries**, and a **data-quality gate** that fails fast (no retries) and blocks publishing.
5. Load history with **`airflow backfill create`**.
6. Chain DAGs with **data-aware scheduling (Assets)**: producer DAG -> Asset event -> consumer DAG.

## Prerequisites

- L05 (Spark basics) and the taxi dataset: `labs/datasets/download_taxi.sh` (January 2024 is enough - other months are downloaded by the DAG itself).
- Slides: Module 8.
- ~2 GB free disk for the image build.

## Architecture

```
                         +-------------------- docker compose (LocalExecutor) ---------------------+
 browser :8080  ------>  | airflow-apiserver   UI, REST API, Execution API (tasks report here)    |
                         | airflow-scheduler   creates DAG runs, queues tasks, runs them as        |
                         |                     sub-processes (LocalExecutor, parallelism 4)        |
                         | airflow-dag-processor  parses ./dags every 30 s                          |
                         | postgres            metadata DB (runs, task instances, XComs, assets)   |
                         +-------------------------------------------------------------------------+
 ./dags  ./logs  ./data (bind mounts)      ../datasets/data/taxi -> /opt/airflow/datasets/taxi (read-only)

 taxi_monthly (@monthly):  download --> aggregate (DuckDB) --> quality_check --> publish ==Asset==> taxi_report
```

**Why DuckDB instead of Spark inside the tasks?** Running Spark from an Airflow worker needs a JVM + PySpark in the
Airflow image (+~700 MB) and ~1 GB of RAM per task on top of Airflow's ~1 GB - too much for an 8 GB laptop running
everything in Docker. DuckDB is an in-process columnar engine: it aggregates a 3-million-row month in ~2 s with <512 MB.
**Airflow's job is orchestration, not computation**: in production the `aggregate` task would submit the work to a Spark
cluster (`SparkSubmitOperator`, Spark Connect, EMR/Dataproc/Databricks operators) and only wait for it - the DAG shape stays
the same. The capstone does exactly that through Spark Connect.

**Trimmed compose.** The official `docker-compose.yaml` runs CeleryExecutor + Redis + worker + triggerer + flower (~3-4 GB).
Ours uses LocalExecutor (tasks run inside the scheduler container), 1 API-server worker, no Redis/Celery, and the triggerer
only under the `triggerer` profile. Authentication uses the SimpleAuthManager with `all_admins=true` (no login) - **lab only**.

## Files

| File | Purpose |
|---|---|
| `Dockerfile`, `requirements.txt` | Airflow slim + `[postgres]` extra + DuckDB |
| `docker-compose.yml` | postgres, airflow-init, api-server, scheduler, dag-processor (+ triggerer profile) |
| `dags/taxi_monthly.py` | **Starter** producer DAG (TODO 1-5) |
| `dags/taxi_report.py` | **Starter** Asset consumer DAG (TODO 6) |
| `solutions/dags/` | Complete DAGs; `solutions/CHECKPOINT_ANSWERS.md` |

---

## Step 1 - Build and start

```bash
cd labs/L10_airflow
echo "AIRFLOW_UID=$(id -u)" > .env          # files in logs/ and data/ belong to you (Linux); harmless on macOS
docker compose build                         # ~2-3 min the first time
docker compose up -d
docker compose ps                            # airflow-init exits 0; the others become healthy (~1 min)
```

Open <http://localhost:8080> (no login in this lab). Check health from the CLI:

```bash
curl -s localhost:8080/api/v2/monitor/health | python3 -m json.tool
```

Expected: `metadatabase`, `scheduler` and `dag_processor` are `healthy`; `triggerer` is `null` (not started).

Useful alias - the Airflow CLI runs inside a container:

```bash
alias af='docker compose exec airflow-scheduler airflow'
af version            # 3.3.2
af dags list          # taxi_monthly, taxi_report (paused)
af dags list-import-errors
```

## Step 2 - Read the DAG, complete the TODOs

Open `dags/taxi_monthly.py`. Tasks:

| Task | Type | What it does |
|---|---|---|
| `download` | `@task.bash` | Uses the shared copy from `labs/datasets` if present, else downloads `yellow_tripdata_<month>.parquet` from the TLC CDN. If the month is not published yet it exits with code **99 -> task skipped**. The last stdout line (the path) becomes the XCom return value. |
| `aggregate` | `@task` | DuckDB: filter bad trips, daily aggregates -> `data/staging/month=YYYY-MM/daily.parquet`; returns stats |
| `quality_check` | `@task` | Raises `AirflowFailException` if the stats look wrong -> no retry, `publish` becomes `upstream_failed` |
| `publish` | `@task(outlets=[Asset])` | Atomic rename into `data/published/...`, writes `_SUCCESS`, emits an Asset event |

Complete:

- **TODO 1** `default_args`: 2 retries, 20 s apart.
- **TODO 2** the four quality rules.
- **TODO 3** `publish` must declare `outlets=[TAXI_DAILY_AGG]`.
- **TODO 4** `month = "{{ logical_date.strftime('%Y-%m') }}"` - a Jinja template rendered per run (TaskFlow arguments are templated).
- **TODO 5** wire `download -> aggregate -> quality_check -> publish`.
- **TODO 6** in `dags/taxi_report.py`: `schedule=[TAXI_DAILY_AGG]`.

The DAG processor re-parses files every 30 s; check `af dags list-import-errors` after saving.

## Step 3 - Test one task, then one run, without the scheduler

```bash
af tasks test taxi_monthly download 2024-01-01      # runs only that task, nothing is recorded in the DB
af dags test taxi_monthly 2024-01-01                # whole DAG in one process - great for debugging
```

Expected at the end of `aggregate`'s log:

```
{'month': '2024-01', ..., 'raw_rows': 2964624, 'kept_rows': 2870091, 'rejected_pct': 3.19, 'days': 31, 'expected_days': 31, ...}
```

## Step 4 - Real runs through the scheduler

```bash
af dags unpause taxi_report
af dags unpause taxi_monthly
```

Unpausing `taxi_monthly` immediately creates **one** scheduled run for the latest completed interval (e.g. logical date
2026-09-01, because `catchup=False`). That month is not published by the TLC yet, so `download` is **skipped** and the
run ends green with all tasks skipped - check it in the Grid view. That's our exit-code-99 design, not an error.

Trigger January 2024 manually:

```bash
af dags trigger taxi_monthly --logical-date 2024-01-01T00:00:00+00:00
af dags list-runs taxi_monthly
```

Expected: the run is `success` after ~10 s, **and** a new `taxi_report` run appears with run id `asset_triggered__...`.

```bash
cat data/reports/monthly_summary.csv
```

```
month,trips,revenue_musd,revenue_per_trip,busiest_day
2024-01,2870091,78.48,27.35,2024-01-27
```

In the UI: *Assets* page -> `file:///opt/airflow/data/published/taxi_daily_agg` shows the producer task and the consumer DAG.

## Step 5 - Backfill three months

```bash
af backfill create --dag-id taxi_monthly --from-date 2024-01-01 --to-date 2024-03-31 --reprocess-behavior completed
af dags list-runs taxi_monthly
```

Expected: runs `backfill__2024-02-01...` and `backfill__2024-03-01...` are created and executed one after the other
(`max_active_runs=1`); the existing January run is cleared and re-run because of `--reprocess-behavior completed`
(default `none` would skip dates that already succeeded). Each run downloads its own month (~50 MB) - logical date in, data out.
In the UI the backfill shows up as a banner on the DAG page; `taxi_report` re-runs and the CSV now has 3 lines.

> Airflow 3 changed backfills: they are created by the CLI/REST/UI but **executed by the scheduler** (in Airflow 2 the CLI ran them itself).

## Step 6 - Watch the quality gate stop bad data

```bash
af dags trigger taxi_monthly --logical-date 2024-02-15T00:00:00+00:00 --run-id bad_data_test \
   --conf '{"simulate_bad_data": true}'
```

(A different time on the same month because `(dag_id, logical_date)` must be unique; `month` is still `2024-02`.)

Expected in the Grid view: `download` ✅, `aggregate` ✅, `quality_check` ❌ (**try 1 only**, no retry), `publish` = `upstream_failed`.
Log of `quality_check`:

```
AirflowFailException: Data quality check failed: rejected 2x.xx% of rows (> 10%)
```

`data/published/` was **not** touched, and `taxi_report` did not run. That's the point of a gate.

## Step 7 - Retries

```bash
af dags trigger taxi_monthly --logical-date 2024-01-20T00:00:00+00:00 --run-id retry_test \
   --conf '{"simulate_transient_error": true}'
```

Expected in the Grid view: `aggregate` shows **try 1 failed** (`RuntimeError: simulated transient error`), the state goes
`up_for_retry` for 20 s (`retry_delay`), then **try 2 succeeds** and the run is green. Open the task's *Logs* tab and switch
between attempts. If TODO 1 is missing (no retries), the whole run fails instead - try it.

Now compare with step 6: the DQ gate raises `AirflowFailException`, which **skips the retries** - retrying cannot fix bad data,
but it can fix a network blip. `download` has its own `retries=3` with `retry_exponential_backoff=True` (20 s, 40 s, 80 s ...).

---

## Checkpoint questions

1. Which component parses DAG files in Airflow 3, and which one runs task code with the LocalExecutor?
2. Why is `month` derived from `logical_date` and not from `datetime.now()`? What would a backfill produce with `now()`?
3. With `schedule="@monthly"` in Airflow 3, what is the logical date of the run that processes March 2024? How does this differ from Airflow 2's data-interval semantics?
4. Why does `quality_check` raise `AirflowFailException` instead of a normal exception?
5. What is the difference between a task that is `skipped` and one that is `upstream_failed`?
6. Three `taxi_monthly` runs finished within a minute during the backfill, but `taxi_report` ran fewer than three times. Why?
7. Why must `publish` write to a temp file and rename it?
8. Name two things you would change before running this DAG in production.

Answers: `solutions/CHECKPOINT_ANSWERS.md`.

## Stretch challenges

1. **Sensor instead of skip**: replace the HEAD check with a deferrable `HttpSensor`/`@task.sensor` that waits (reschedule mode) until the file appears. Start the triggerer: `docker compose --profile triggerer up -d`.
2. **Dynamic task mapping**: `aggregate.expand(zone_group=[...])` to compute per-borough aggregates in parallel.
3. **Spark instead of DuckDB**: write the aggregation as a PySpark script and submit it to the L05 Spark cluster (or to a Spark Connect server as in the capstone). Keep `quality_check` unchanged.
4. **Callbacks**: add `on_failure_callback` that writes a line to `data/alerts.log` (or posts to a Discord/Slack webhook).
5. **Asset with metadata**: yield `Metadata(TAXI_DAILY_AGG, {"rows": ...})` from `publish` and read it in `taxi_report` via `triggering_asset_events`.

## Cleanup

```bash
docker compose down -v            # removes containers + metadata DB volume
rm -rf logs/* data/*              # optional: task logs and produced files
```

## Troubleshooting

| Symptom | Fix |
|---|---|
| `airflow-init` exits 1, `ModuleNotFoundError: asyncpg` | You built from an old Dockerfile: Airflow 3 needs `apache-airflow[postgres]` (psycopg2 **and** asyncpg). `docker compose build --no-cache`. |
| UI loads but DAG list empty | Wait 30 s (DAG processor interval); `af dags list-import-errors`. |
| Run stays `queued` forever | DAG is paused (`af dags unpause taxi_monthly`) or another run is active (`max_active_runs=1`). |
| `UniqueViolation ... dag_run_dag_id_logical_date_key` on trigger | A run for that logical date already exists; use another time (see step 6) or clear the existing run. |
| `download` skipped for a month you expected to exist | The TLC CDN answered non-200 to the HEAD request (no internet? proxy?). Try `curl -I https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet`. |
| `PermissionError` writing `data/` or `logs/` (Linux) | Create `.env` with `AIRFLOW_UID=$(id -u)` and `docker compose up -d` again. |
| Scheduler container restarts (exit 137) | Out of memory: stop other stacks; LocalExecutor runs tasks inside the scheduler container (limit 1.5 GB). |
| Port 8080 busy | L07/L08 Kafka UI or another Airflow is running. |
