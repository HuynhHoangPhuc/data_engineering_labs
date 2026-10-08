"""L04 Part 2 -- Spark JDBC ingestion from Postgres to Parquet on HDFS (REFERENCE SOLUTION).

Run inside the spark container:
    docker compose exec spark spark-submit --jars /jars/postgresql.jar \
        /lab/solutions/spark_jdbc_ingest.py --step full
    docker compose exec spark spark-submit --jars /jars/postgresql.jar \
        /lab/solutions/spark_jdbc_ingest.py --step incremental

--step full         single-partition read vs. parallel read (partitionColumn/lowerBound/upperBound/
                    numPartitions), rows per partition, Parquet partitioned by purchase month.
--step incremental  watermark on updated_at: read only rows changed since the last run, append them
                    to a change log, rebuild the "current" snapshot, save the new watermark.
"""
import argparse
import time

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

JDBC_URL = "jdbc:postgresql://postgres:5432/olist"
JDBC_OPTS = {"url": JDBC_URL, "user": "olist", "password": "olist", "driver": "org.postgresql.Driver"}
BASE = "hdfs://namenode:8020/data/olist/spark"


def jdbc(spark, **options):
    """spark.read.format('jdbc') with the connection options pre-filled."""
    reader = spark.read.format("jdbc").options(**JDBC_OPTS)
    for k, v in options.items():
        reader = reader.option(k, v)
    return reader.load()


def timed(label, fn):
    t0 = time.time()
    result = fn()
    print(f"[time] {label}: {time.time() - t0:.2f} s")
    return result


def step_full(spark):
    # (1) naive read: ONE partition -> one JDBC connection, one task does all the work
    naive = jdbc(spark, dbtable="orders")
    print("naive partitions:", naive.rdd.getNumPartitions())
    timed("naive count", naive.count)

    # (2) the bounds, like Sqoop's BoundingValsQuery
    b = jdbc(spark, query="SELECT min(order_seq) AS lo, max(order_seq) AS hi FROM orders").first()
    print(f"order_seq bounds: {b.lo} .. {b.hi}")

    # (3) parallel read: Spark generates numPartitions range queries on partitionColumn.
    #     lowerBound/upperBound only decide the STRIDE -- they do NOT filter rows.
    orders = jdbc(
        spark,
        dbtable="orders",
        partitionColumn="order_seq",
        lowerBound=str(b.lo),
        upperBound=str(b.hi),
        numPartitions="4",
        fetchsize="1000",
    )
    print("parallel partitions:", orders.rdd.getNumPartitions())
    timed("parallel count", orders.count)
    orders.groupBy(F.spark_partition_id().alias("partition")).count().orderBy("partition").show()

    # (4) same with a timestamp partition column (also allowed: numeric, date, timestamp)
    by_ts = jdbc(
        spark,
        dbtable="orders",
        partitionColumn="order_purchase_timestamp",
        lowerBound="2017-01-01 00:00:00",
        upperBound="2018-09-01 00:00:00",
        numPartitions="4",
    )
    print("rows per partition when splitting on order_purchase_timestamp (volume grows over time):")
    by_ts.groupBy(F.spark_partition_id().alias("partition")).count().orderBy("partition").show()

    # (5) write Parquet, partitioned by purchase month (Hive-style directories)
    out = orders.withColumn("purchase_month", F.date_format("order_purchase_timestamp", "yyyy-MM"))
    timed("write parquet", lambda: out.write.mode("overwrite").partitionBy("purchase_month")
          .parquet(f"{BASE}/orders"))
    back = spark.read.parquet(f"{BASE}/orders")
    print("rows written:", back.count(), "| months:", back.select("purchase_month").distinct().count())
    back.filter("purchase_month = '2018-08'").explain()   # look for PartitionFilters

    # (6) the other tables, one call each (small tables: 1 partition is fine)
    for table in ["customers", "order_items", "order_payments", "products", "sellers"]:
        df = jdbc(spark, dbtable=table)
        df.write.mode("overwrite").parquet(f"{BASE}/{table}")
        print(f"{table}: {df.count()} rows")


def path_exists(spark, path):
    """Ask the Hadoop FileSystem API (through py4j) whether an HDFS path exists."""
    jpath = spark._jvm.org.apache.hadoop.fs.Path(path)
    return jpath.getFileSystem(spark._jsc.hadoopConfiguration()).exists(jpath)


def read_watermark(spark):
    state = f"{BASE}/_state/orders_watermark"
    if not path_exists(spark, state):          # first run: no state yet -> load everything
        return "1970-01-01 00:00:00"
    return spark.read.json(state).first()["watermark"]


def step_incremental(spark):
    wm_old = read_watermark(spark)
    # upper bound = DB clock NOW, fixed before reading, so the next run starts exactly here
    wm_new = jdbc(spark, query="SELECT to_char(now(), 'YYYY-MM-DD HH24:MI:SS.US') AS ts").first().ts
    print(f"watermark: ({wm_old}, {wm_new}]")

    changes = jdbc(
        spark,
        dbtable=(f"(SELECT * FROM orders WHERE updated_at > timestamp '{wm_old}' "
                 f"AND updated_at <= timestamp '{wm_new}') AS changed_orders"),
    ).withColumn("_ingested_at", F.lit(wm_new).cast("timestamp"))
    changes = changes.cache()
    n = changes.count()
    print(f"changed rows since last run: {n}")
    changes.select("order_seq", "order_id", "order_status", "updated_at").orderBy("updated_at").show(10, False)

    # append-only change log (bronze-style), one directory per ingestion
    changes.write.mode("append").parquet(f"{BASE}/orders_changes")

    # current snapshot = latest version of every order_id across the whole change log
    log = spark.read.parquet(f"{BASE}/orders_changes")
    w = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc(), F.col("_ingested_at").desc())
    current = log.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    current.write.mode("overwrite").parquet(f"{BASE}/orders_current")
    print("change-log rows:", log.count(), "| current snapshot rows:", spark.read.parquet(f"{BASE}/orders_current").count())

    # persist the new watermark ONLY after the data was written successfully
    spark.createDataFrame([(wm_new,)], "watermark string").coalesce(1) \
        .write.mode("overwrite").json(f"{BASE}/_state/orders_watermark")
    print("saved watermark", wm_new)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--step", choices=["full", "incremental"], required=True)
    args = ap.parse_args()
    spark = (SparkSession.builder.appName(f"L04-jdbc-{args.step}")
             .config("spark.hadoop.dfs.replication", "1")      # single-DataNode lab HDFS
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    {"full": step_full, "incremental": step_incremental}[args.step](spark)
    spark.stop()


if __name__ == "__main__":
    main()
