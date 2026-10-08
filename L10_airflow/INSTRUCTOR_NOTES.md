# L10 - Instructor notes

## Timing (≈ 2 h 45 min)

| Block | Minutes |
|---|---|
| Airflow 3 architecture (API server, DAG processor, Task SDK / Execution API, what changed from 2.x) | 20 |
| Step 1: build + start (start the build at the beginning of the lecture!) | 15 |
| Step 2: TODOs | 35 |
| Step 3-4: tests, first runs, Assets | 25 |
| Step 5: backfill | 15 |
| Step 6-7: DQ gate and retries | 20 |
| Checkpoint discussion | 15 |

## Before class

- `docker compose build` takes 2-3 minutes and ~1.2 GB disk (slim base 264 MB compressed + DuckDB + postgres extra).
  Ask students to build at home.
- Make sure `labs/datasets/data/taxi/yellow_tripdata_2024-01.parquet` exists (shared copy -> no download for January).
  February/March are downloaded by the DAG (~50 MB each) during the backfill.

## Tested (Sep 2026, Apple Silicon, Docker VM 3.8 GB)

- Stack memory: api-server 270 MB, scheduler 400 MB (while idle; +DuckDB during `aggregate`), dag-processor 160 MB, postgres 60 MB.
- Unpause -> scheduled run for 2026-09-01 -> `download` skipped (exit 99) -> all skipped, run success.
- Manual run 2024-01: success in ~4 s; `taxi_report` asset-triggered; CSV as in the README.
- Backfill 2024-01..2024-03 with `--reprocess-behavior completed`: two `backfill__` runs + the January run re-run.
  (For the test, February/March were small synthetic files mounted in place of the shared folder to respect a
  one-month download limit; the numbers in your class will be the real ones, ~2.9-3.0 M trips/month.)
- `simulate_bad_data`: `quality_check` failed on try 1 with `AirflowFailException`, `publish` upstream_failed.
- Found during testing: Airflow 3 needs **asyncpg** for the Postgres metadata DB (async engine) - the slim image lacks it;
  fixed with `apache-airflow[postgres]` in the Dockerfile.
- Found during testing: triggering a second run with the same logical date fails with a unique-constraint error -> README step 6
  uses `2024-02-15`.

## Common student errors

- Importing from Airflow 2 paths (`from airflow.decorators import dag, task`, `from airflow.datasets import Dataset`):
  still partially shimmed but deprecated - insist on `airflow.sdk`.
- Calling `datetime.now()` / `date.today()` at the top of the DAG file (evaluated at *parse* time, every 30 s!).
- Heavy code at module level (imports of pandas/duckdb) slows the DAG processor - keep imports inside tasks.
- Forgetting to unpause -> runs stay queued.
- Expecting `airflow dags backfill` (Airflow 2 command) - in Airflow 3 it's `airflow backfill create`.
- Using `{{ ds }}` math with Python instead of templates; or templating inside a *non-templated* place (plain Python code in the DAG body is not rendered).

## Grading hints

| Item | Points |
|---|---|
| TODO 1-6 correct, no import errors | 30 |
| Grid screenshot: successful run + asset-triggered report | 15 |
| Backfill of 3 months with the resulting CSV | 15 |
| DQ gate failure screenshot + 2 sentences on why no retry | 15 |
| Retry demo (try 1 failed, try 2 success) | 10 |
| Checkpoint answers | 15 |
