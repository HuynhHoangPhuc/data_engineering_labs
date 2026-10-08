"""Shared helpers for the capstone Spark jobs (executed through Spark Connect)."""
import os

from pyspark.sql import SparkSession


def get_spark(app: str) -> SparkSession:
    """Connect to the long-running Spark Connect server (container `spark`).

    Laptop:  SPARK_REMOTE defaults to sc://localhost:15002
    Airflow: SPARK_REMOTE=sc://spark:15002 (set in docker-compose.yml)
    """
    remote = os.environ.get("SPARK_REMOTE", "sc://localhost:15002")
    spark = SparkSession.builder.remote(remote).appName(app).getOrCreate()
    return spark


# Source tables: primary key columns + the JSON schema of Debezium's before/after images.
# Timestamps arrive as epoch MILLIS (time.precision.mode=connect), numerics as strings (decimal.handling.mode=string).
TABLES = {
    "customers": {
        "pk": ["customer_id"],
        "schema": "customer_id STRING, customer_unique_id STRING, customer_zip_code_prefix INT, "
                  "customer_city STRING, customer_state STRING, updated_at BIGINT",
    },
    "sellers": {
        "pk": ["seller_id"],
        "schema": "seller_id STRING, seller_zip_code_prefix INT, seller_city STRING, seller_state STRING, updated_at BIGINT",
    },
    "products": {
        "pk": ["product_id"],
        "schema": "product_id STRING, product_category_name STRING, product_name_lenght INT, "
                  "product_description_lenght INT, product_photos_qty INT, product_weight_g INT, "
                  "product_length_cm INT, product_height_cm INT, product_width_cm INT, updated_at BIGINT",
    },
    "orders": {
        "pk": ["order_id"],
        "schema": "order_id STRING, customer_id STRING, order_status STRING, order_purchase_timestamp BIGINT, "
                  "order_approved_at BIGINT, order_delivered_carrier_date BIGINT, "
                  "order_delivered_customer_date BIGINT, order_estimated_delivery_date BIGINT, updated_at BIGINT",
    },
    "order_items": {
        "pk": ["order_id", "order_item_id"],
        "schema": "order_id STRING, order_item_id INT, product_id STRING, seller_id STRING, "
                  "shipping_limit_date BIGINT, price STRING, freight_value STRING, updated_at BIGINT",
    },
    "order_payments": {
        "pk": ["order_id", "payment_sequential"],
        "schema": "order_id STRING, payment_sequential INT, payment_type STRING, payment_installments INT, "
                  "payment_value STRING, updated_at BIGINT",
    },
}

# How each silver column is derived from the parsed JSON struct `r`
TIMESTAMP_COLS = {"updated_at", "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
                  "order_delivered_customer_date", "order_estimated_delivery_date", "shipping_limit_date"}
DECIMAL_COLS = {"price", "freight_value", "payment_value"}


def silver_select_list(table: str) -> str:
    cols = []
    for part in TABLES[table]["schema"].split(","):
        name = part.split()[0]
        if name in TIMESTAMP_COLS:
            cols.append(f"timestamp_millis(r.{name}) AS {name}")
        elif name in DECIMAL_COLS:
            cols.append(f"CAST(r.{name} AS DECIMAL(12,2)) AS {name}")
        else:
            cols.append(f"r.{name} AS {name}")
    return ", ".join(cols)


def column_names(table: str) -> list:
    return [p.split()[0] for p in TABLES[table]["schema"].split(",")]
