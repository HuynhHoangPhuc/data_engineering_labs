"""L09 - Spark Structured Streaming: Kafka -> windowed counts per wiki -> Parquet + console (SOLUTION).

Run inside the spark container (see README):
    docker compose exec spark /opt/spark/bin/spark-submit /opt/lab/solutions/stream_job.py
(the Kafka connector package is configured in conf/spark-defaults.conf -> spark.jars.packages)

What it does
  1. readStream from Kafka topic `wikimedia.recentchange`
  2. parse the JSON value with an explicit schema
  3. event time = the event's own `timestamp` field (epoch seconds), NOT the Kafka arrival time
  4. 1-minute tumbling windows per wiki, watermark 2 minutes
  5. sink A: Parquet files, outputMode=append   (a window is written ONCE, after the watermark passes it)
     sink B: console,       outputMode=update   (changed windows printed every trigger)
  Each sink has its OWN checkpoint directory -> exactly-once file output + restart recovery.
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, LongType, StringType, StructField,
                               StructType)

KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "kafka:19092")
TOPIC = os.environ.get("TOPIC", "wikimedia.recentchange")
BASE = os.environ.get("LAB_DIR", "/opt/lab")
OUT = f"{BASE}/output/wiki_counts"
CKPT = f"{BASE}/checkpoints"

# Only the fields we need. Unknown fields are ignored; missing ones become null.
schema = StructType([
    StructField("id", LongType()),
    StructField("type", StringType()),          # edit | new | log | categorize
    StructField("wiki", StringType()),          # enwiki, wikidatawiki, commonswiki, ...
    StructField("server_name", StringType()),
    StructField("title", StringType()),
    StructField("user", StringType()),
    StructField("bot", BooleanType()),
    StructField("timestamp", LongType()),       # event time, epoch seconds
    StructField("length", StructType([StructField("old", LongType()), StructField("new", LongType())])),
])

spark = (SparkSession.builder
         .appName("L09-wiki-windowed-counts")
         .config("spark.sql.shuffle.partitions", "4")       # tiny laptop cluster: 200 is overkill
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", KAFKA)
       .option("subscribe", TOPIC)
       .option("startingOffsets", "earliest")      # only used on the FIRST run; afterwards the checkpoint wins
       .option("maxOffsetsPerTrigger", 5000)       # back-pressure: at most 5k records per micro-batch
       .option("failOnDataLoss", "false")
       .load())

events = (raw
          .select(F.from_json(F.col("value").cast("string"), schema).alias("e"),
                  F.col("timestamp").alias("kafka_ts"))
          .select("e.*", "kafka_ts")
          .where(F.col("wiki").isNotNull() & F.col("timestamp").isNotNull())
          .withColumn("event_time", F.timestamp_seconds("timestamp"))
          .withColumn("bytes_changed", F.coalesce(F.col("length.new"), F.lit(0)) - F.coalesce(F.col("length.old"), F.lit(0))))

counts = (events
          .withWatermark("event_time", "2 minutes")                      # tolerate 2 min of lateness
          .groupBy(F.window("event_time", "1 minute").alias("w"), "wiki")
          .agg(F.count("*").alias("edits"),
               F.sum(F.when(F.col("bot"), 1).otherwise(0)).alias("bot_edits"),
               F.sum("bytes_changed").alias("bytes_changed"))
          .select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"),
                  "wiki", "edits", "bot_edits", "bytes_changed"))

# Sink A - Parquet, append mode: a (window, wiki) row is written exactly once, when the
# watermark (max event_time seen - 2 min) passes window_end. Expect the first files after ~3-4 minutes.
parquet_q = (counts
             .withColumn("dt", F.to_date("window_start"))
             .writeStream
             .queryName("wiki_counts_parquet")
             .format("parquet")
             .option("path", OUT)
             .option("checkpointLocation", f"{CKPT}/wiki_counts_parquet")
             .partitionBy("dt")
             .outputMode("append")
             .trigger(processingTime="30 seconds")
             .start())

# Sink B - console, update mode: every trigger prints the windows whose counts changed.
console_q = (counts
             .where(F.col("edits") >= 20)                  # keep the console readable: busy wikis only
             .writeStream
             .queryName("wiki_counts_console")
             .format("console")
             .option("truncate", "false")
             .option("numRows", 15)
             .option("checkpointLocation", f"{CKPT}/wiki_counts_console")
             .outputMode("update")
             .trigger(processingTime="30 seconds")
             .start())

print(f"Streaming from {KAFKA}/{TOPIC} -> {OUT}  (Spark UI: http://localhost:4040/StreamingQuery/)")
spark.streams.awaitAnyTermination()
