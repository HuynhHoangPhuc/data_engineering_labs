"""L10 - Asset consumer DAG (STARTER - complete TODO 6).

No time schedule: it runs whenever the `taxi_daily_agg` Asset is updated by
`taxi_monthly.publish` (data-aware scheduling, formerly "Datasets" in Airflow 2).
"""
from __future__ import annotations

import os
from pathlib import Path

import pendulum
from airflow.sdk import Asset, dag, task

DATA = Path(os.environ.get("LAB_DATA_DIR", "/opt/airflow/data"))
TAXI_DAILY_AGG = Asset("file:///opt/airflow/data/published/taxi_daily_agg")


@dag(
    dag_id="taxi_report",
    schedule=None,                      # TODO 6: run whenever TAXI_DAILY_AGG is updated (data-aware scheduling)
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["L10", "taxi", "asset-consumer"],
)
def taxi_report():

    @task
    def build_report(triggering_asset_events=None) -> str:
        import duckdb

        # Which producer run(s) caused this run? (useful for lineage/debugging)
        for asset, events in (triggering_asset_events or {}).items():
            for ev in events:
                print(f"triggered by {asset.uri} from dag={ev.source_dag_id} run={ev.source_run_id}")

        pub = DATA / "published" / "taxi_daily_agg"
        out = DATA / "reports"
        out.mkdir(parents=True, exist_ok=True)
        report = out / "monthly_summary.csv"
        duckdb.sql(f"""
            COPY (
              SELECT strftime(trip_date, '%Y-%m')            AS month,
                     sum(trips)                              AS trips,
                     round(sum(revenue) / 1e6, 2)            AS revenue_musd,
                     round(sum(revenue) / sum(trips), 2)     AS revenue_per_trip,
                     arg_max(trip_date, trips)               AS busiest_day
              FROM read_parquet('{pub}/month=*/daily.parquet')
              GROUP BY 1 ORDER BY 1
            ) TO '{report}' (HEADER, DELIMITER ',')
        """)
        print(report.read_text())
        return str(report)

    build_report()


taxi_report()
