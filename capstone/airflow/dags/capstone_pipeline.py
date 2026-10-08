"""Capstone - reference orchestration (Airflow 3).

bronze (Structured Streaming, availableNow) -> DQ bronze -> silver MERGE -> DQ silver -> gold -> DQ gold -> publish Asset
+ a weekly Iceberg maintenance DAG.

Tasks run the job modules in /opt/airflow/jobs (= spark/jobs, mounted read-only) through Spark Connect
(SPARK_REMOTE=sc://spark:15002) - the Airflow image only contains the thin `pyspark-client`.
"""
from __future__ import annotations

import os
import sys
from datetime import timedelta

import pendulum
from airflow.sdk import Asset, dag, task

JOBS_DIR = os.environ.get("CAPSTONE_JOBS_DIR", "/opt/airflow/jobs")
GOLD = Asset("iceberg://lake/gold")


def _spark(app):
    sys.path.insert(0, JOBS_DIR)
    from common import get_spark
    return get_spark(app)


def _dq(layer: str):
    from airflow.sdk.exceptions import AirflowFailException
    sys.path.insert(0, JOBS_DIR)
    import dq_checks
    if not dq_checks.run(_spark(f"dq-{layer}"), layer):
        raise AirflowFailException(f"data quality gate failed for layer {layer}")


def notify_failure(context):
    """Failure callback: replace the print with a Slack/Discord webhook for the bonus."""
    ti = context["ti"]
    print(f"ALERT: {ti.dag_id}.{ti.task_id} failed for run {context['run_id']}")


@dag(
    dag_id="capstone_pipeline",
    schedule="*/30 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1), "on_failure_callback": notify_failure},
    tags=["capstone"],
)
def capstone_pipeline():

    @task
    def bronze_ingest():
        sys.path.insert(0, JOBS_DIR)
        import bronze_ingest as b
        return b.run(_spark("bronze"), available_now=True)

    @task(retries=0)
    def dq_bronze(_):
        _dq("bronze")

    @task
    def silver_merge(_):
        sys.path.insert(0, JOBS_DIR)
        import silver_merge as s
        return s.run(_spark("silver"))

    @task(retries=0)
    def dq_silver(_):
        _dq("silver")

    @task
    def gold_build(_):
        sys.path.insert(0, JOBS_DIR)
        import gold_build as g
        return g.run(_spark("gold"))

    @task(retries=0)
    def dq_gold(_):
        _dq("gold")

    @task(outlets=[GOLD])
    def publish(_):
        print("gold layer published")

    publish(dq_gold(gold_build(dq_silver(silver_merge(dq_bronze(bronze_ingest()))))))


capstone_pipeline()


@dag(
    dag_id="capstone_maintenance",
    schedule="@weekly",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["capstone", "maintenance"],
)
def capstone_maintenance():

    @task
    def compact_and_expire():
        spark = _spark("maintenance")
        for t in ["lake.bronze.cdc_events", "lake.silver.orders", "lake.silver.order_items", "lake.silver.order_payments"]:
            spark.sql(f"CALL lake.system.rewrite_data_files(table => '{t}')").show()
            spark.sql(f"CALL lake.system.expire_snapshots(table => '{t}', "
                      f"older_than => current_timestamp() - INTERVAL 7 DAYS, retain_last => 5)").show()

    compact_and_expire()


capstone_maintenance()
