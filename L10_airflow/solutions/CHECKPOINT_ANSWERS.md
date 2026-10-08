# L10 - Checkpoint answers

1. The **DAG processor** (`airflow dag-processor`, a separate service since Airflow 3) parses the files in `dags/` and
   stores serialized DAGs in the metadata DB. With the **LocalExecutor** the **scheduler** process forks a sub-process per
   task instance; the task code talks to the **API server's Execution API** (Task SDK) instead of the database directly.

2. The logical date identifies *which slice of data* a run is responsible for. Re-running or backfilling the run for
   2024-03 must always process March 2024. With `datetime.now()` every backfill run would process the *current* month
   (three identical runs, and history never loaded) and results would depend on when you happen to run them.

3. `logical_date = 2024-03-01T00:00Z`. Airflow 3 uses `CronTriggerTimetable` semantics by default
   (`[scheduler] create_cron_data_intervals = False`): the run is identified by the moment the cron fires, and
   `data_interval_start == data_interval_end == logical_date`. In Airflow 2 (`CronDataIntervalTimetable`) the run for
   March had logical date 2024-03-01 but was only *started* after the interval ended (2024-04-01) with
   `data_interval_end = 2024-04-01`. Our DAG simply treats "the month of the logical date" as the data to process.

4. `AirflowFailException` marks the task failed **immediately, ignoring the remaining retries**. Bad data won't become good by
   retrying, so retries would only delay the alert and waste resources. A normal exception is retried (`retries=2`).

5. `skipped`: the task decided (or was told) not to run - e.g. exit code 99 in `@task.bash`, `AirflowSkipException`,
   a short-circuit/branch; downstream tasks with the default trigger rule are skipped too and the run can still be
   successful. `upstream_failed`: the task could not run because an upstream task **failed**; the run fails.

6. Asset events are queued; when several arrive before the scheduler creates the consumer run, they are **collapsed into
   one** asset-triggered run (its `triggering_asset_events` contains all of them). Data-aware scheduling is "run when
   there is new data", not "run once per event".

7. So a reader (the report DAG, a BI tool, a person) never sees a half-written file: the rename is atomic on the same
   filesystem, and `_SUCCESS` is written only after the data file is complete. Object stores emulate this with
   write-to-temp + commit protocols or table formats like Iceberg (next lab).

8. Examples: a proper auth manager (FAB/Keycloak) and secrets in a secrets backend instead of compose env vars;
   CeleryExecutor/KubernetesExecutor so heavy tasks don't run inside the scheduler; remote logging (S3); pinned image
   with DAG bundles from git; alerting callbacks / SLAs; the heavy compute pushed to Spark; data stored in an object
   store/lakehouse instead of a local volume; tests in CI (`dag.test()`, import-error check).
