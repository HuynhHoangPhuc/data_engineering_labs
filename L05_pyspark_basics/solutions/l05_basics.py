"""L05 -- PySpark basics on NYC taxi data (REFERENCE SOLUTION).

    docker compose exec spark spark-submit /lab/solutions/l05_basics.py
    docker compose exec spark spark-submit /lab/solutions/l05_basics.py --hold 600   # keep UI :4040 open

Sections match the README tasks (1..10).
"""
import argparse
import glob
import time

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

TAXI_GLOB = "/datasets/data/taxi/yellow_tripdata_*.parquet"
ZONES_CSV = "/datasets/data/taxi/taxi_zone_lookup.csv"
OUTPUT = "/lab/output/trips_clean"


def banner(title):
    print(f"\n{'=' * 80}\n{title}\n{'=' * 80}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hold", type=int, default=0, help="seconds to keep the app (and UI :4040) alive at the end")
    args = ap.parse_args()

    # ------------------------------------------------------------------ 1. SparkSession + read
    banner("1. SparkSession and reading Parquet")
    spark = SparkSession.builder.appName("L05-pyspark-basics").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print("Spark", spark.version, "| master", spark.sparkContext.master,
          "| ANSI mode:", spark.conf.get("spark.sql.ansi.enabled"))

    files = sorted(glob.glob(TAXI_GLOB))
    print("input files:", files)
    trips = spark.read.parquet(*files)
    trips.printSchema()
    print("rows:", trips.count(), "| partitions:", trips.rdd.getNumPartitions())
    trips.show(3, truncate=False)

    zones = (spark.read.option("header", True).option("inferSchema", True).csv(ZONES_CSV))
    zones.printSchema()

    # ------------------------------------------------------------------ 2. transformations vs actions
    banner("2. Transformations are lazy, actions run jobs")
    t0 = time.time()
    lazy = (trips.filter(F.col("trip_distance") > 10)
                 .withColumn("fare_per_mile", F.col("fare_amount") / F.col("trip_distance"))
                 .select("PULocationID", "trip_distance", "fare_per_mile"))
    print(f"building 3 transformations took {time.time() - t0:.3f} s  (no Spark job yet -- check the UI)")
    t0 = time.time()
    n_long = lazy.count()
    print(f"count() = {n_long} long trips, took {time.time() - t0:.2f} s  (an ACTION: a job ran)")

    # ------------------------------------------------------------------ 3. cleaning
    banner("3. Data quality and cleaning")
    null_counts = trips.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in trips.columns])
    null_counts.show(vertical=True)

    trips = (trips
             .withColumn("pickup_ts", F.col("tpep_pickup_datetime"))
             .withColumn("duration_min",
                         (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60.0))
    months = [f.rsplit("_", 1)[1][:7] for f in files]          # ['2024-01', ...]
    rules = {
        "pickup outside the file's month(s)": ~F.date_format("pickup_ts", "yyyy-MM").isin(months),
        "negative or zero fare": F.col("fare_amount") <= 0,
        "negative total": F.col("total_amount") < 0,
        "distance <= 0 or > 100 miles": (F.col("trip_distance") <= 0) | (F.col("trip_distance") > 100),
        "duration <= 0 or > 6 h": (F.col("duration_min") <= 0) | (F.col("duration_min") > 360),
    }
    # one pass to count every rule (conditional aggregation)
    trips.select([F.sum(cond.cast("int")).alias(name) for name, cond in rules.items()]).show(vertical=True, truncate=False)

    bad = None
    for cond in rules.values():
        bad = cond if bad is None else (bad | cond)
    clean = (trips.filter(~bad)
             .fillna({"passenger_count": 1, "RatecodeID": 1, "congestion_surcharge": 0.0, "Airport_fee": 0.0})
             .withColumn("pickup_date", F.to_date("pickup_ts"))
             .withColumn("pickup_hour", F.hour("pickup_ts"))
             # ANSI mode: plain '/' raises DIVIDE_BY_ZERO -> try_divide returns NULL instead
             .withColumn("tip_pct", F.round(F.try_divide(F.col("tip_amount"), F.col("fare_amount")) * 100, 1)))
    clean = clean.cache()
    total, kept = trips.count(), clean.count()
    print(f"kept {kept:,} of {total:,} rows ({100 * kept / total:.2f} %)")

    # ------------------------------------------------------------------ 4. joins
    banner("4. Join with the zone lookup (pickup and dropoff)")
    pu = zones.select(F.col("LocationID").alias("PULocationID"), F.col("Borough").alias("pu_borough"),
                      F.col("Zone").alias("pu_zone"))
    do = zones.select(F.col("LocationID").alias("DOLocationID"), F.col("Borough").alias("do_borough"),
                      F.col("Zone").alias("do_zone"))
    enriched = clean.join(pu, "PULocationID", "left").join(do, "DOLocationID", "left")
    enriched.select("pickup_ts", "pu_borough", "pu_zone", "do_borough", "do_zone", "total_amount").show(5, False)

    # ------------------------------------------------------------------ 5. groupBy / agg
    banner("5. groupBy / agg")
    by_borough = (enriched.groupBy("pu_borough")
                  .agg(F.count("*").alias("trips"),
                       F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
                       F.round(F.avg("tip_pct"), 1).alias("avg_tip_pct"),
                       F.round(F.avg("trip_distance"), 2).alias("avg_miles"))
                  .orderBy(F.desc("trips")))
    by_borough.show()
    by_hour = enriched.groupBy("pickup_hour").agg(F.count("*").alias("trips")).orderBy("pickup_hour")
    by_hour.show(24)

    # ------------------------------------------------------------------ 6. window functions
    banner("6. Window functions")
    zone_counts = enriched.groupBy("pu_borough", "pu_zone").agg(F.count("*").alias("trips"))
    w_rank = Window.partitionBy("pu_borough").orderBy(F.desc("trips"))
    top3 = (zone_counts.withColumn("rank", F.dense_rank().over(w_rank))
            .filter("rank <= 3").orderBy("pu_borough", "rank"))
    top3.show(30, truncate=False)

    daily = enriched.groupBy("pickup_date").agg(F.count("*").alias("trips"), F.round(F.sum("total_amount")).cast("long").alias("revenue"))
    w_day = Window.orderBy("pickup_date")
    daily = (daily.withColumn("prev_day_trips", F.lag("trips").over(w_day))
             .withColumn("dod_change_pct", F.round((F.col("trips") / F.col("prev_day_trips") - 1) * 100, 1))
             .withColumn("running_revenue", F.sum("revenue").over(w_day.rowsBetween(Window.unboundedPreceding, 0)))
             .orderBy("pickup_date"))
    daily.show(10)

    # ------------------------------------------------------------------ 7. Spark SQL
    banner("7. Spark SQL on a temp view")
    enriched.createOrReplaceTempView("trips")
    sql_df = spark.sql("""
        SELECT pu_borough, count(*) AS trips, round(avg(fare_amount), 2) AS avg_fare,
               round(avg(tip_pct), 1) AS avg_tip_pct, round(avg(trip_distance), 2) AS avg_miles
        FROM trips
        GROUP BY pu_borough
        ORDER BY trips DESC
    """)
    sql_df.show()
    same = sql_df.collect() == by_borough.collect()
    print("SQL result == DataFrame result:", same)

    # ------------------------------------------------------------------ 8. explain
    banner("8. explain('formatted')")
    by_borough.explain("formatted")

    # ------------------------------------------------------------------ 9 + 10. write partitioned parquet
    banner("10. Write partitioned Parquet and read it back")
    (enriched.drop("pickup_ts")
     .repartition("pickup_date")            # one task (-> one file) per date instead of N files per date
     .write.mode("overwrite").partitionBy("pickup_date").parquet(OUTPUT))
    back = spark.read.parquet(OUTPUT)
    print("partitions (dates) written:", back.select("pickup_date").distinct().count())
    one_day = back.filter(F.col("pickup_date") == "2024-01-15")
    print("trips on 2024-01-15:", one_day.count())
    one_day.explain("formatted")             # look for PartitionFilters in the Scan node

    if args.hold:
        print(f"\nHolding for {args.hold} s -- open http://localhost:4040 (Ctrl-C to stop)")
        time.sleep(args.hold)
    spark.stop()


if __name__ == "__main__":
    main()
