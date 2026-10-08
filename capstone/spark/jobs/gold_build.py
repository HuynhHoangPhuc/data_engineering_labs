"""Capstone - GOLD: silver -> star schema + aggregates (Spark SQL on Iceberg).

Reference skeleton: dim_date, dim_customers, fact_orders, daily_sales are complete.
TODO (students): dim_products (with English category names), dim_sellers, fact_order_items,
                 late-delivery rate by state, and (bonus) re-implement this layer in dbt.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import get_spark  # noqa: E402

GOLD_SQL = {
    "dim_date": """
        SELECT CAST(date_format(d, 'yyyyMMdd') AS INT) AS date_key, d AS date_day,
               year(d) AS year, month(d) AS month, date_format(d, 'yyyy-MM') AS year_month,
               dayofweek(d) AS day_of_week, dayofweek(d) IN (1, 7) AS is_weekend
        FROM (SELECT explode(sequence(DATE '2016-01-01', DATE '2027-12-31', INTERVAL 1 DAY)) AS d)
    """,
    "dim_customers": """
        SELECT customer_id AS customer_key, customer_unique_id, customer_zip_code_prefix,
               initcap(customer_city) AS customer_city, upper(customer_state) AS customer_state
        FROM lake.silver.customers
    """,
    # Grain: one row per order. Items and payments are aggregated to the order grain BEFORE joining (no fan-out).
    "fact_orders": """
        WITH items AS (
            SELECT order_id, count(*) AS item_count, sum(price) AS items_value, sum(freight_value) AS freight_value
            FROM lake.silver.order_items GROUP BY order_id
        ), pays AS (
            SELECT order_id, sum(payment_value) AS payment_value, max_by(payment_type, payment_value) AS main_payment_type
            FROM lake.silver.order_payments GROUP BY order_id
        )
        SELECT o.order_id, o.customer_id AS customer_key,
               CAST(date_format(o.order_purchase_timestamp, 'yyyyMMdd') AS INT) AS order_date_key,
               o.order_status, o.order_purchase_timestamp AS purchased_at, o.order_delivered_customer_date AS delivered_at,
               o.order_estimated_delivery_date AS estimated_delivery_at,
               coalesce(i.item_count, 0) AS item_count, coalesce(i.items_value, 0) AS items_value,
               coalesce(i.freight_value, 0) AS freight_value, coalesce(p.payment_value, 0) AS payment_value,
               p.main_payment_type,
               CASE WHEN o.order_delivered_customer_date IS NULL THEN NULL
                    ELSE to_date(o.order_delivered_customer_date) > to_date(o.order_estimated_delivery_date) END AS is_late
        FROM lake.silver.orders o
        LEFT JOIN items i ON i.order_id = o.order_id
        LEFT JOIN pays  p ON p.order_id = o.order_id
    """,
    "daily_sales": """
        SELECT d.date_day, d.year_month, c.customer_state,
               count(*) AS orders,
               sum(CASE WHEN f.order_status = 'canceled' THEN 1 ELSE 0 END) AS canceled_orders,
               round(sum(CASE WHEN f.order_status <> 'canceled' THEN f.payment_value ELSE 0 END), 2) AS revenue,
               round(avg(CASE WHEN f.is_late THEN 1.0 WHEN f.is_late = false THEN 0.0 END), 4) AS late_rate
        FROM lake.gold.fact_orders f
        JOIN lake.gold.dim_date d      ON d.date_key = f.order_date_key
        JOIN lake.gold.dim_customers c ON c.customer_key = f.customer_key
        GROUP BY 1, 2, 3
    """,
    # TODO: "dim_products": ..., "dim_sellers": ..., "fact_order_items": ...
}


def run(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.gold")
    counts = {}
    for name, sql in GOLD_SQL.items():         # dict order = dependency order
        spark.sql(f"CREATE OR REPLACE TABLE lake.gold.{name} USING iceberg AS {sql}")
        counts[name] = spark.sql(f"SELECT count(*) FROM lake.gold.{name}").collect()[0][0]
        print(f"gold.{name}: {counts[name]} rows")
    return counts


if __name__ == "__main__":
    run(get_spark("capstone-gold"))
