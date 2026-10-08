"""Capstone - tiny data-quality framework: every check is a SQL query that returns the number of BAD rows.

    python spark/jobs/dq_checks.py --layer bronze|silver|gold
Exit code 1 if any check with severity "error" fails -> Airflow task fails -> downstream tasks don't run.

TODO (students): add >= 3 checks of your own (freshness of bronze, reconciliation gold revenue vs silver payments,
                 referential integrity order_items -> products, ...).
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import get_spark  # noqa: E402

ACCEPTED_STATUS = "('created','approved','invoiced','processing','shipped','delivered','canceled','unavailable')"

CHECKS = {
    "bronze": [
        ("bronze_not_empty", "error",
         "SELECT CASE WHEN count(*) = 0 THEN 1 ELSE 0 END FROM lake.bronze.cdc_events"),
        ("bronze_known_ops", "error",
         "SELECT count(*) FROM lake.bronze.cdc_events WHERE op NOT IN ('r','c','u','d')"),
        ("bronze_no_duplicate_offsets", "error",
         "SELECT count(*) FROM (SELECT kafka_topic, kafka_partition, kafka_offset FROM lake.bronze.cdc_events "
         "GROUP BY 1, 2, 3 HAVING count(*) > 1)"),
    ],
    "silver": [
        ("orders_pk_unique", "error",
         "SELECT count(*) FROM (SELECT order_id FROM lake.silver.orders GROUP BY 1 HAVING count(*) > 1)"),
        ("orders_status_accepted", "error",
         f"SELECT count(*) FROM lake.silver.orders WHERE order_status NOT IN {ACCEPTED_STATUS}"),
        ("items_price_positive", "error",
         "SELECT count(*) FROM lake.silver.order_items WHERE price < 0"),
        ("orders_customer_fk", "warn",
         "SELECT count(*) FROM lake.silver.orders o LEFT ANTI JOIN lake.silver.customers c ON o.customer_id = c.customer_id"),
    ],
    "gold": [
        ("fact_orders_pk_unique", "error",
         "SELECT count(*) - count(DISTINCT order_id) FROM lake.gold.fact_orders"),
        ("fact_orders_date_fk", "error",
         "SELECT count(*) FROM lake.gold.fact_orders f LEFT ANTI JOIN lake.gold.dim_date d ON f.order_date_key = d.date_key"),
        ("daily_sales_revenue_non_negative", "error",
         "SELECT count(*) FROM lake.gold.daily_sales WHERE revenue < 0"),
    ],
}


def run(spark, layer: str) -> bool:
    ok = True
    for name, severity, sql in CHECKS[layer]:
        bad = spark.sql(sql).collect()[0][0]
        status = "PASS" if bad == 0 else ("FAIL" if severity == "error" else "WARN")
        print(f"[{status}] {layer}.{name}: {bad} bad rows")
        if bad and severity == "error":
            ok = False
    return ok


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--layer", choices=list(CHECKS), required=True)
    a = ap.parse_args()
    sys.exit(0 if run(get_spark("capstone-dq"), a.layer) else 1)
