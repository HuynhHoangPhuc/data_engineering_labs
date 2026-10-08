"""L05 -- PySpark basics on NYC taxi data (STARTER -- complete the TODOs).

    docker compose exec spark spark-submit /lab/src/l05_basics.py
    docker compose exec spark spark-submit /lab/src/l05_basics.py --hold 600   # keep UI :4040 open

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
    # TODO 1: build ONE row with the number of NULLs per column
    #         hint: [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in trips.columns]
    null_counts = trips.limit(0)
    null_counts.show(vertical=True)

    trips = (trips
             .withColumn("pickup_ts", F.col("tpep_pickup_datetime"))
             .withColumn("duration_min",
                         (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60.0))
    months = [f.rsplit("_", 1)[1][:7] for f in files]          # ['2024-01', ...]
    rules = {
        "pickup outside the file's month(s)": ~F.date_format("pickup_ts", "yyyy-MM").isin(months),
        "negative or zero fare": F.col("fare_amount") <= 0,
        # TODO 2: add rules for: negative total_amount, trip_distance <= 0 or > 100,
        #         duration_min <= 0 or > 360
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
    # TODO 3: join clean with zones TWICE: once on PULocationID (-> pu_borough, pu_zone) and once on
    #         DOLocationID (-> do_borough, do_zone). Rename the zone columns before each join.
    enriched = clean.withColumn("pu_borough", F.lit(None)).withColumn("pu_zone", F.lit(None)) \
                    .withColumn("do_borough", F.lit(None)).withColumn("do_zone", F.lit(None))
    enriched.select("pickup_ts", "pu_borough", "pu_zone", "do_borough", "do_zone", "total_amount").show(5, False)

    # ------------------------------------------------------------------ 5. groupBy / agg
    banner("5. groupBy / agg")
    # TODO 4: per pu_borough: trips (count), avg_fare, avg_tip_pct, avg_miles -- ordered by trips desc
    by_borough = enriched.groupBy("pu_borough").count()
    by_borough.show()
    by_hour = enriched.groupBy("pickup_hour").agg(F.count("*").alias("trips")).orderBy("pickup_hour")
    by_hour.show(24)

    # ------------------------------------------------------------------ 6. window functions
    banner("6. Window functions")
    zone_counts = enriched.groupBy("pu_borough", "pu_zone").agg(F.count("*").alias("trips"))
    # TODO 5: top-3 pickup zones per borough with dense_rank() over a window partitioned by pu_borough
    top3 = zone_counts
    top3.show(30, truncate=False)

    daily = enriched.groupBy("pickup_date").agg(F.count("*").alias("trips"), F.round(F.sum("total_amount")).cast("long").alias("revenue"))
    w_day = Window.orderBy("pickup_date")
    # TODO 6: add prev_day_trips (lag), dod_change_pct and running_revenue (sum over rows unbounded preceding)
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
    # TODO 7: write enriched (without pickup_ts) as Parquet to OUTPUT, partitioned by pickup_date,
    #         mode overwrite. Try it first WITHOUT and then WITH .repartition("pickup_date") -- count the files.
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
