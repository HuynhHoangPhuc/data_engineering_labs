"""L10 - Monthly NYC taxi pipeline (STARTER - complete TODO 1-5; solution in solutions/dags/).

download -> aggregate (DuckDB) -> quality_check -> publish  (publish emits the Asset `taxi_daily_agg`)

Key ideas shown here
* TaskFlow API from the Airflow 3 Task SDK (`airflow.sdk`)
* the month to process comes from the run's *logical date* via Jinja templating
  ({{ logical_date.strftime('%Y-%m') }}) - never from datetime.now(), so re-runs and
  backfills are deterministic
* retries with exponential backoff on the network-bound task
* a data-quality gate that FAILS the run (publish never happens) when the data is bad
* data-aware scheduling: `publish` has an Asset outlet that triggers the `taxi_report` DAG
"""
from __future__ import annotations

import os
import shutil
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import Asset, Param, dag, task

DATA = Path(os.environ.get("LAB_DATA_DIR", "/opt/airflow/data"))
SHARED_TAXI = Path(os.environ.get("SHARED_TAXI_DIR", "/opt/airflow/datasets/taxi"))  # labs/datasets/data/taxi (read-only)
TLC_BASE_URL = os.environ.get("TLC_BASE_URL", "https://d37ci6vzurychx.cloudfront.net")

TAXI_DAILY_AGG = Asset("file:///opt/airflow/data/published/taxi_daily_agg")


@dag(
    dag_id="taxi_monthly",
    description="Download a month of NYC yellow taxi trips, aggregate per day, check quality, publish",
    schedule="@monthly",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,                  # history is loaded explicitly with `airflow backfill create`
    max_active_runs=1,
    default_args={
        "owner": "de-course",
        # TODO 1: every task should retry 2 times, waiting 20 seconds between tries
    },
    params={
        # Trigger with {"simulate_bad_data": true} to watch the quality gate stop the pipeline
        "simulate_bad_data": Param(False, type="boolean", description="Corrupt 20% of the fares"),
        # Trigger with {"simulate_transient_error": true} to watch a retry: the 1st try of aggregate fails
        "simulate_transient_error": Param(False, type="boolean", description="Fail the first try of aggregate"),
    },
    tags=["L10", "taxi"],
)
def taxi_monthly():

    @task.bash(retries=3, retry_exponential_backoff=True)
    def download() -> str:
        # Returned string is a bash script, rendered with Jinja before it runs.
        # The last line printed to stdout becomes the task's XCom return value (the file path).
        return """
        set -euo pipefail
        MONTH="{{ logical_date.strftime('%Y-%m') }}"
        FILE="yellow_tripdata_${MONTH}.parquet"
        mkdir -p "$LAB_DATA_DIR/raw"
        if [ -s "$SHARED_TAXI_DIR/$FILE" ]; then
            echo "using shared copy $SHARED_TAXI_DIR/$FILE" >&2
            echo "$SHARED_TAXI_DIR/$FILE"
        elif [ -s "$LAB_DATA_DIR/raw/$FILE" ]; then
            echo "already downloaded" >&2
            echo "$LAB_DATA_DIR/raw/$FILE"
        else
            # TLC publishes each month with a ~2 month delay. Not there yet -> exit 99 = task SKIPPED
            # (downstream tasks are skipped too) instead of failing and retrying for nothing.
            if ! curl -sfI "$TLC_BASE_URL/trip-data/$FILE" >/dev/null; then
                echo "$FILE is not published yet -> skipping this run" >&2
                exit 99
            fi
            curl -fL --retry 2 -o "$LAB_DATA_DIR/raw/$FILE.part" "$TLC_BASE_URL/trip-data/$FILE" >&2
            mv "$LAB_DATA_DIR/raw/$FILE.part" "$LAB_DATA_DIR/raw/$FILE"
            echo "$LAB_DATA_DIR/raw/$FILE"
        fi
        """

    @task
    def aggregate(raw_path: str, month: str, params=None, ti=None) -> dict:
        """Clean + aggregate one month with DuckDB (in-process, ~200 MB RAM). Returns stats for the DQ gate."""
        import duckdb

        if params and params.get("simulate_transient_error") and ti.try_number == 1:
            raise RuntimeError("simulated transient error (e.g. network blip) - Airflow will retry")

        start = pendulum.parse(f"{month}-01")
        end = start.add(months=1)
        out_dir = DATA / "staging" / f"month={month}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / "daily.parquet"

        con = duckdb.connect()
        con.execute("SET memory_limit='512MB'; SET threads=2;")
        fare_expr = "fare_amount"
        if params and params.get("simulate_bad_data"):
            fare_expr = "CASE WHEN random() < 0.2 THEN -abs(fare_amount) ELSE fare_amount END"
        con.execute(f"""
            CREATE TEMP VIEW trips AS
            SELECT tpep_pickup_datetime AS pickup_ts, trip_distance, passenger_count,
                   {fare_expr} AS fare_amount, tip_amount, total_amount
            FROM read_parquet('{raw_path}')
        """)
        con.execute(f"""
            CREATE TEMP VIEW clean AS
            SELECT * FROM trips
            WHERE pickup_ts >= TIMESTAMP '{start:%Y-%m-%d}' AND pickup_ts < TIMESTAMP '{end:%Y-%m-%d}'
              AND trip_distance > 0 AND fare_amount >= 0 AND total_amount >= 0
        """)
        con.execute(f"""
            COPY (
                SELECT CAST(pickup_ts AS DATE)          AS trip_date,
                       count(*)                          AS trips,
                       round(sum(total_amount), 2)       AS revenue,
                       round(avg(fare_amount), 2)        AS avg_fare,
                       round(avg(trip_distance), 2)      AS avg_distance_mi,
                       round(avg(tip_amount), 2)         AS avg_tip
                FROM clean GROUP BY 1 ORDER BY 1
            ) TO '{out_file}' (FORMAT parquet)
        """)
        raw_rows = con.execute("SELECT count(*) FROM trips").fetchone()[0]
        kept_rows, days, min_fare, max_fare = con.execute(
            f"SELECT sum(trips), count(*), min(avg_fare), max(avg_fare) FROM read_parquet('{out_file}')"
        ).fetchone()
        stats = {
            "month": month,
            "staging_file": str(out_file),
            "raw_rows": raw_rows,
            "kept_rows": int(kept_rows or 0),
            "rejected_pct": round(100 * (raw_rows - (kept_rows or 0)) / max(raw_rows, 1), 2),
            "days": days,
            "expected_days": (end - start).days,
            "min_daily_avg_fare": min_fare,
            "max_daily_avg_fare": max_fare,
        }
        print(stats)
        return stats

    @task
    def quality_check(stats: dict) -> dict:
        """Data-quality gate: raise -> task fails (no retries) -> publish is skipped (upstream_failed)."""
        from airflow.sdk.exceptions import AirflowFailException

        failures = []
        # TODO 2: append a message to `failures` when
        #   - raw_rows < 100_000                      (file truncated / wrong month)
        #   - rejected_pct > 10                       (too many invalid trips)
        #   - days != expected_days                   (missing days)
        #   - min/max daily avg fare outside [5, 100] (unit or parsing bug)
        if failures:
            # AirflowFailException = fail immediately, do NOT retry (retrying bad data is pointless)
            raise AirflowFailException("Data quality check failed: " + "; ".join(failures))
        print(f"DQ passed for {stats['month']}: {stats['kept_rows']} trips over {stats['days']} days")
        return stats

    # TODO 3: declare that publish() updates the Asset TAXI_DAILY_AGG (so taxi_report is triggered)
    @task
    def publish(stats: dict) -> str:
        """Atomically promote the checked file; emitting the Asset event triggers downstream DAGs."""
        src = Path(stats["staging_file"])
        dst_dir = DATA / "published" / "taxi_daily_agg" / f"month={stats['month']}"
        dst_dir.mkdir(parents=True, exist_ok=True)
        tmp = dst_dir / "daily.parquet.tmp"
        shutil.copyfile(src, tmp)
        tmp.replace(dst_dir / "daily.parquet")      # rename = atomic on the same filesystem
        (dst_dir / "_SUCCESS").write_text(str(stats))
        print(f"published {dst_dir}")
        return str(dst_dir)

    # TODO 4: the month must come from the run's logical date (Jinja template), e.g. "2024-01".
    #         Hint: look at how the bash script in download() builds MONTH.
    month = "2024-01"
    # TODO 5: wire the tasks: download -> aggregate -> quality_check -> publish
    #         (TaskFlow: passing one task's return value into another creates the dependency)
    raw_path = download()
    stats = aggregate(raw_path, month)


taxi_monthly()
