"""L13 Part 3 (STARTER - complete TODO 1) - a two-step PySpark pipeline whose lineage is captured by OpenLineage.

    docker compose exec spark spark-submit /lab/part3_lineage/taxi_lineage_job.py

The OpenLineage listener (configured in conf/spark-defaults.conf) watches every write and emits
START/COMPLETE events with input datasets, output datasets, schemas and column-level lineage:

  /datasets/.../yellow_tripdata_2024-01.parquet ──(clean)──> /lab/output/lineage/trips_clean
  /lab/output/lineage/trips_clean ──┐
  /datasets/.../taxi_zone_lookup.csv ┴─(join + aggregate)──> /lab/output/lineage/daily_borough_revenue

Nothing in this file mentions OpenLineage: lineage is collected from Spark's logical plans,
so existing jobs get lineage without code changes.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RAW = "/datasets/data/taxi/yellow_tripdata_2024-01.parquet"
ZONES = "/datasets/data/taxi/taxi_zone_lookup.csv"
OUT = "/lab/output/lineage"

spark = SparkSession.builder.appName("taxi_lineage").getOrCreate()

# ---- step 1: raw -> cleaned (bronze -> silver) ------------------------------------------------
raw = spark.read.parquet(RAW)
clean = (
    raw.filter(
        (F.col("tpep_pickup_datetime") >= F.lit("2024-01-01").cast("timestamp"))
        & (F.col("tpep_pickup_datetime") < F.lit("2024-02-01").cast("timestamp"))
        & (F.col("fare_amount") > 0)
        & (F.col("trip_distance").between(0.01, 100))
        & (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
    )
    .select(
        F.to_date("tpep_pickup_datetime").alias("pickup_date"),
        "tpep_pickup_datetime",
        "PULocationID",
        "DOLocationID",
        "trip_distance",
        "fare_amount",
        "tip_amount",
        "total_amount",
    )
)
clean.write.mode("overwrite").parquet(f"{OUT}/trips_clean")

# ---- step 2: cleaned + zone lookup -> daily revenue per borough (silver -> gold) -------------
trips = spark.read.parquet(f"{OUT}/trips_clean")
# explicit schema: no extra Spark job to infer it (every Spark job shows up in the lineage backend)
zones = (spark.read.option("header", True)
         .schema("LocationID INT, Borough STRING, Zone STRING, service_zone STRING").csv(ZONES)
         .select(F.col("LocationID").alias("zone_id"), F.col("Borough").alias("borough")))
# TODO 1: left-join `trips` (PULocationID) with `zones` (zone_id), group by pickup_date and borough and compute
#   trips   = count(*)
#   revenue = round(sum(total_amount), 2)
#   avg_tip = round(avg(tip_amount), 2)
# then write it with mode("overwrite") as Parquet to f"{OUT}/daily_borough_revenue".
# After the run, the lineage graph in Marquez should show TWO jobs chained through trips_clean.
daily = None  # replace
if daily is None:
    raise SystemExit("TODO 1 not done yet: build `daily` and write it to f'{OUT}/daily_borough_revenue'")

result = spark.read.parquet(f"{OUT}/daily_borough_revenue")
print(f"daily_borough_revenue: {result.count()} rows")
result.orderBy(F.desc("revenue")).show(5, truncate=False)
spark.stop()
