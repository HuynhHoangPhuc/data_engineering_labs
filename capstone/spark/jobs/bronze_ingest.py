"""Capstone - BRONZE: Kafka (Debezium topics) -> Iceberg lake.bronze.cdc_events with Structured Streaming.

    python spark/jobs/bronze_ingest.py --available-now   # process everything new, then stop (batch-like, cheap)
    python spark/jobs/bronze_ingest.py                   # run continuously, 30 s micro-batches (Ctrl+C to stop)

Exactly-once into Iceberg: Kafka offsets are tracked in the checkpoint (server side: /checkpoints/bronze_cdc),
and each micro-batch is one atomic Iceberg commit.
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import get_spark  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "kafka:19092")   # resolved by the Spark server, not by the client
CHECKPOINT = "/checkpoints/bronze_cdc"


def run(spark, available_now: bool = True):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.bronze")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lake.bronze.cdc_events (
            source_table     STRING,
            op               STRING,       -- r (snapshot) | c | u | d
            pk               STRING,       -- Kafka key = primary key as JSON
            lsn              BIGINT,
            tx_id            BIGINT,
            source_ts        TIMESTAMP,    -- when the change was committed in Postgres
            before_json      STRING,
            after_json       STRING,
            kafka_topic      STRING,
            kafka_partition  INT,
            kafka_offset     BIGINT,
            ingested_at      TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (source_table, days(ingested_at))
    """)

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA)
           .option("subscribePattern", r"olist\.public\..*")
           .option("startingOffsets", "earliest")
           .option("maxOffsetsPerTrigger", 20000)
           .load())
    v = F.col("value").cast("string")
    events = raw.where(F.col("value").isNotNull()).select(
        F.get_json_object(v, "$.source.table").alias("source_table"),
        F.get_json_object(v, "$.op").alias("op"),
        F.col("key").cast("string").alias("pk"),
        F.get_json_object(v, "$.source.lsn").cast("bigint").alias("lsn"),
        F.get_json_object(v, "$.source.txId").cast("bigint").alias("tx_id"),
        F.timestamp_millis(F.get_json_object(v, "$.source.ts_ms").cast("bigint")).alias("source_ts"),
        F.get_json_object(v, "$.before").alias("before_json"),
        F.get_json_object(v, "$.after").alias("after_json"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.current_timestamp().alias("ingested_at"),
    )
    writer = (events.writeStream.format("iceberg").outputMode("append")
              .option("checkpointLocation", CHECKPOINT)
              .queryName("bronze_cdc"))
    writer = writer.trigger(availableNow=True) if available_now else writer.trigger(processingTime="30 seconds")
    q = writer.toTable("lake.bronze.cdc_events")
    try:
        q.awaitTermination()
    except KeyboardInterrupt:
        q.stop()
    n = spark.sql("SELECT count(*) FROM lake.bronze.cdc_events").collect()[0][0]
    print(f"bronze.cdc_events rows: {n}")
    return n


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--available-now", action="store_true")
    a = ap.parse_args()
    run(get_spark("capstone-bronze"), available_now=a.available_now)
