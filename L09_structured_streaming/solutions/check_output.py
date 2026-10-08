"""L09 - Batch-read the streaming Parquet output and verify exactly-once (SOLUTION, also used in step 6).

    docker compose exec spark /opt/spark/bin/spark-submit /opt/lab/solutions/check_output.py
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE = os.environ.get("LAB_DIR", "/opt/lab")
spark = SparkSession.builder.appName("L09-check-output").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet(f"{BASE}/output/wiki_counts")
print(f"rows: {df.count()}   files: {len(df.inputFiles())}")

print("Windows written so far (latest 5):")
(df.groupBy("window_start").agg(F.sum("edits").alias("edits"), F.countDistinct("wiki").alias("wikis"))
   .orderBy(F.desc("window_start")).show(5, truncate=False))

print("Top 10 wikis by edits:")
(df.groupBy("wiki").agg(F.sum("edits").alias("edits"), F.sum("bot_edits").alias("bot_edits"))
   .withColumn("bot_pct", F.round(100 * F.col("bot_edits") / F.col("edits"), 1))
   .orderBy(F.desc("edits")).show(10, truncate=False))

dups = df.groupBy("window_start", "wiki").count().where("count > 1").count()
print(f"duplicate (window_start, wiki) rows: {dups}   <- must be 0 (exactly-once file sink)")
spark.stop()
