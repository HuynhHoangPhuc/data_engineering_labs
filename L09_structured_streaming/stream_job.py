"""L09 - Spark Structured Streaming: Kafka -> windowed counts per wiki -> Parquet + console (STARTER - complete TODO 1-5; solution: solutions/stream_job.py).

Run inside the spark container (see README):
    docker compose exec spark /opt/spark/bin/spark-submit /opt/lab/stream_job.py
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

# TODO 1: parse the Kafka `value` (bytes) as JSON with `schema`:
#   F.from_json(F.col("value").cast("string"), schema).alias("e")  then select("e.*")
# TODO 2: keep rows with non-null wiki/timestamp and add
#   event_time    = F.timestamp_seconds("timestamp")
#   bytes_changed = length.new - length.old (treat nulls as 0)
events = None  # <- your code

# TODO 3: event-time aggregation
#   - watermark of 2 minutes on event_time
#   - group by a 1-minute tumbling window on event_time AND wiki
#   - aggregate: edits = count(*), bot_edits = number of rows where bot is true, bytes_changed = sum
#   - select window_start, window_end, wiki, edits, bot_edits, bytes_changed
counts = None  # <- your code

# TODO 4: Parquet sink -> path OUT, checkpoint f"{CKPT}/wiki_counts_parquet",
#         outputMode "append", trigger every 30 seconds, partitioned by dt = to_date(window_start)
parquet_q = None  # <- your code

# TODO 5: console sink -> outputMode "update", checkpoint f"{CKPT}/wiki_counts_console",
#         only rows with edits >= 20, truncate=false, trigger every 30 seconds
console_q = None  # <- your code

print(f"Streaming from {KAFKA}/{TOPIC} -> {OUT}  (Spark UI: http://localhost:4040/StreamingQuery/)")
spark.streams.awaitAnyTermination()
