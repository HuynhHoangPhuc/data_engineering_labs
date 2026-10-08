"""L06 -- Spark performance experiments (STARTER -- complete the TODOs).

    docker compose exec spark spark-submit /lab/src/l06_experiments.py            # all experiments
    docker compose exec spark spark-submit /lab/src/l06_experiments.py --only E3  # one experiment

Every run is timed and summarised from the Spark REST API (final physical plan nodes, number of
tasks, median vs max task time of the slowest stage). Results are appended to
/lab/results/results.csv and printed as a table.

Experiments
  E1  spark.sql.shuffle.partitions 200 vs 8 (AQE off) and 200 with AQE partition coalescing
  E2  broadcast hash join vs sort-merge join (autoBroadcastJoinThreshold) + AQE join demotion
  E3  skewed join: baseline (AQE off) vs salting vs AQE skew-join handling
  E4  repartition vs coalesce before a write (time, #files, parallelism)
  E5  cache/persist: 3 queries on the same cleaned DataFrame, with and without cache
"""
import argparse
import csv
import glob
import json
import os
import time
import urllib.request

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

TAXI = sorted(glob.glob("/datasets/data/taxi/yellow_tripdata_*.parquet"))
ZONES = "/datasets/data/taxi/taxi_zone_lookup.csv"
RESULTS = "/lab/results/results.csv"
N_FACT = int(os.environ.get("L06_FACT_ROWS", 10_000_000))   # synthetic skew fact rows
N_KEYS = 200_000                                           # distinct keys in the synthetic dimension
HOT_SHARE = 0.6                                            # 60 % of fact rows have key 0

spark = None
rows = []


# --------------------------------------------------------------------------- helpers
def api(path):
    ui = spark.sparkContext.uiWebUrl or "http://localhost:4040"
    with urllib.request.urlopen(f"{ui}/api/v1/applications/{spark.sparkContext.applicationId}{path}") as r:
        return json.load(r)


def last_sql_summary():
    """Final (post-AQE) plan nodes + task statistics of the last SQL execution.

    Returns (nodes, (total_tasks, median_ms, max_ms)) where median/max are taken from the stage with
    the largest gap between its slowest and its median task -- i.e. the most skewed stage.
    """
    try:
        execs = api("/sql?details=false&offset=0&length=100000")
        last = max(execs, key=lambda e: e["id"])
        detail = api(f"/sql/{last['id']}?details=true&planDescription=false")
        names = [n["nodeName"] for n in detail.get("nodes", [])]
        keep = ("BroadcastHashJoin", "SortMergeJoin", "ShuffledHashJoin", "AQEShuffleRead", "Exchange",
                "BroadcastExchange", "InMemoryTableScan", "Coalesce")
        nodes = sorted({n for n in names if n.startswith(keep)})
        total_tasks, worst = 0, None
        for job_id in last.get("successJobIds", []):
            for sid in api(f"/jobs/{job_id}")["stageIds"]:
                for st in api(f"/stages/{sid}"):
                    if st["status"] != "COMPLETE":
                        continue
                    total_tasks += st["numTasks"]
                    q = api(f"/stages/{sid}/{st['attemptId']}/taskSummary?quantiles=0.5,1.0")
                    med, mx = q["executorRunTime"]
                    if worst is None or (mx - med) > (worst[1] - worst[0]):
                        worst = (med, mx)
        return nodes, (total_tasks, *worst) if worst else None
    except Exception as exc:  # the REST API is a nice-to-have; never fail the experiment
        return [f"(REST API unavailable: {exc.__class__.__name__})"], None


def run(exp, variant, df, note=""):
    """Execute df fully (noop sink = no output files) and record timing + plan facts."""
    t0 = time.time()
    df.write.format("noop").mode("overwrite").save()
    secs = time.time() - t0
    nodes, worst = last_sql_summary()
    tasks, med, mx = worst if worst else ("?", "?", "?")
    row = dict(experiment=exp, variant=variant, seconds=round(secs, 2), total_tasks=tasks,
               task_median_ms=med, task_max_ms=mx, plan=" ".join(nodes), note=note)
    rows.append(row)
    print(f"[{exp}] {variant:<40} {secs:6.2f} s | tasks={tasks} skewiest stage: median={med} ms max={mx} ms | {' '.join(nodes)}")
    return secs


def set_conf(**kv):
    for k, v in kv.items():
        spark.conf.set(k.replace("__", "."), str(v))


def reset_conf():
    set_conf(spark__sql__adaptive__enabled=True,
             spark__sql__adaptive__coalescePartitions__enabled=True,
             spark__sql__adaptive__skewJoin__enabled=True,
             spark__sql__shuffle__partitions=8,
             spark__sql__autoBroadcastJoinThreshold=10 * 1024 * 1024)


def trips_df():
    return spark.read.parquet(*TAXI)


def zones_df():
    return spark.read.option("header", True).option("inferSchema", True).csv(ZONES)


# --------------------------------------------------------------------------- experiments
def e1_shuffle_partitions():
    q = lambda: (trips_df().groupBy("PULocationID", "DOLocationID")
                 .agg(F.count("*").alias("trips"), F.avg("total_amount").alias("avg_total")))
    reset_conf()
    set_conf(spark__sql__adaptive__enabled=False, spark__sql__shuffle__partitions=200)
    run("E1", "AQE off, shuffle.partitions=200", q())
    set_conf(spark__sql__shuffle__partitions=8)
    run("E1", "AQE off, shuffle.partitions=8", q())
    set_conf(spark__sql__shuffle__partitions=1)
    run("E1", "AQE off, shuffle.partitions=1", q())
    # TODO 1: turn AQE back ON (keep shuffle.partitions=200) and run the same query as
    #         variant "AQE on,  shuffle.partitions=200". What does AQEShuffleRead do?


def e2_broadcast_vs_smj():
    q = lambda: (trips_df().join(zones_df(), F.col("PULocationID") == F.col("LocationID"))
                 .groupBy("Borough").agg(F.count("*").alias("trips")))
    reset_conf()
    set_conf(spark__sql__adaptive__enabled=False)
    run("E2", "AQE off, broadcast (threshold 10MB)", q())
    set_conf(spark__sql__autoBroadcastJoinThreshold=-1)
    run("E2", "AQE off, sort-merge (threshold -1)", q())
    set_conf(spark__sql__adaptive__enabled=True, spark__sql__autoBroadcastJoinThreshold=-1)
    # TODO 2: with AQE on and the threshold still -1, force a broadcast join with the F.broadcast()
    #         hint on zones_df() and record it as variant "AQE on,  threshold -1 + broadcast() hint".


def skew_inputs():
    fact = (spark.range(N_FACT)
            .withColumn("key", F.when(F.rand(seed=7) < HOT_SHARE, F.lit(0))
                        .otherwise((F.rand(seed=11) * N_KEYS).cast("long")))
            .withColumn("amount", (F.rand(seed=3) * 100).cast("double")))
    dim = (spark.range(N_KEYS).withColumnRenamed("id", "key")
           .withColumn("segment", F.concat(F.lit("seg_"), (F.col("key") % 50).cast("string"))))
    return fact, dim


def e3_skew():
    reset_conf()
    fact, dim = skew_inputs()
    fact.groupBy((F.col("key") == 0).alias("is_hot_key")).count().show()
    # force a shuffle join (dim is 200k rows -> could be broadcast, which would hide the skew)
    set_conf(spark__sql__autoBroadcastJoinThreshold=-1, spark__sql__adaptive__enabled=False,
             spark__sql__shuffle__partitions=16)
    joined = fact.join(dim, "key").groupBy("segment").agg(F.sum("amount").alias("amount"))
    base = run("E3", "skewed SMJ, AQE off", joined)

    # Fix 1: SALTING -- spread the hot key over SALT sub-keys. Only the HOT key is salted (random
    # salt 0..SALT-1); every other key keeps salt 0. The dimension row of the hot key is replicated
    # SALT times, all other dimension rows once -> almost no extra data.
    SALT = 64   # >> shuffle partitions, so hash collisions of (key, salt) even out
    # TODO 3: SALTING of the hot key only.
    #   fact_s: new int column "salt" = random 0..SALT-1 when key == 0, else 0
    #   dim_s : dim rows with key != 0 get salt 0; the dim row with key == 0 is cross-joined with
    #           spark.range(SALT) (as int column "salt") -> SALT copies
    #   salted: fact_s.join(dim_s, ["key", "salt"]) then the same groupBy/agg as `joined`
    salted = joined
    run("E3", f"salted SMJ (salt={SALT}), AQE off", salted)

    # Fix 2: AQE skew join -- split the skewed shuffle partition at runtime
    # TODO 4: enable AQE + skew-join handling. Our data is small, so lower the thresholds:
    #         advisoryPartitionSizeInBytes=4m, skewJoin.skewedPartitionThresholdInBytes=16m,
    #         skewJoin.skewedPartitionFactor=3, and disable coalescePartitions for a clean comparison.
    run("E3", "skewed SMJ, AQE skewJoin on", fact.join(dim, "key").groupBy("segment").agg(F.sum("amount")),
        "look for AQEShuffleRead (skewed) in the SQL tab")
    # sanity: all variants compute the same answer
    a = joined.agg(F.round(F.sum("amount"), 0)).first()[0]
    b = salted.agg(F.round(F.sum("amount"), 0)).first()[0]
    print(f"same result with and without salting: {a == b} ({a})")
    return base


def e4_repartition_vs_coalesce():
    reset_conf()
    out = "/tmp/l06_out"
    for label, df in [("repartition(64)", trips_df().repartition(64)),
                      ("repartition(4)", trips_df().repartition(4)),
                      ("coalesce(4)", trips_df().coalesce(4)),
                      ("coalesce(1)", trips_df().coalesce(1))]:
        t0 = time.time()
        df.write.mode("overwrite").parquet(out)
        secs = time.time() - t0
        files = [f for f in os.listdir(out) if f.endswith(".parquet")]
        nodes, worst = last_sql_summary()
        tasks, med, mx = worst if worst else ("?", "?", "?")
        rows.append(dict(experiment="E4", variant=label, seconds=round(secs, 2), total_tasks=tasks,
                         task_median_ms=med, task_max_ms=mx, plan=" ".join(nodes), note=f"{len(files)} files"))
        print(f"[E4] {label:<38} {secs:6.2f} s | files={len(files)} | tasks={tasks} | {' '.join(nodes)}")


def e5_cache():
    reset_conf()

    def cleaned():
        # dedup is a full shuffle of ~3 M rows -> an "expensive" DataFrame worth caching
        return (trips_df().filter("fare_amount > 0 AND trip_distance > 0")
                .dropDuplicates(["tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID",
                                 "DOLocationID", "total_amount"])
                .select("tpep_pickup_datetime", "PULocationID", "payment_type", "tip_amount",
                        "fare_amount", "total_amount")
                .withColumn("tip_pct", F.try_divide(F.col("tip_amount"), F.col("fare_amount")))
                .withColumn("hour", F.hour("tpep_pickup_datetime")))

    def three_queries(df, label):
        t0 = time.time()
        df.groupBy("hour").count().collect()
        df.groupBy("PULocationID").agg(F.avg("tip_pct")).collect()
        df.groupBy("payment_type").agg(F.sum("total_amount")).collect()
        secs = time.time() - t0
        rows.append(dict(experiment="E5", variant=label, seconds=round(secs, 2), total_tasks="",
                         task_median_ms="", task_max_ms="", plan="", note="3 aggregations"))
        print(f"[E5] {label:<38} {secs:6.2f} s")

    three_queries(cleaned(), "no cache")
    df = cleaned().persist(StorageLevel.MEMORY_AND_DISK)
    t0 = time.time()
    n = df.count()   # materialise the cache
    rows.append(dict(experiment="E5", variant="cache materialisation (count)", seconds=round(time.time() - t0, 2),
                     total_tasks="", task_median_ms="", task_max_ms="", plan="", note=f"{n} rows"))
    three_queries(df, "with cache (MEMORY_AND_DISK)")
    info = [(r["name"], r["memoryUsed"], r["diskUsed"]) for r in api("/storage/rdd")] if spark else []
    print("Storage tab:", info)
    df.unpersist()


EXPERIMENTS = {"E1": e1_shuffle_partitions, "E2": e2_broadcast_vs_smj, "E3": e3_skew,
               "E4": e4_repartition_vs_coalesce, "E5": e5_cache}


def main():
    global spark
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", nargs="*", choices=list(EXPERIMENTS), help="run only these experiments")
    ap.add_argument("--hold", type=int, default=0, help="keep the UI alive N seconds at the end")
    args = ap.parse_args()
    spark = SparkSession.builder.appName("L06-tuning").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print("Spark", spark.version, "| cores", spark.sparkContext.defaultParallelism, "| input", TAXI)
    trips_df().count()   # warm-up: JVM, file listing, OS cache (not recorded)

    for name in (args.only or list(EXPERIMENTS)):
        print(f"\n===== {name}: {EXPERIMENTS[name].__doc__ or EXPERIMENTS[name].__name__}")
        EXPERIMENTS[name]()

    os.makedirs(os.path.dirname(RESULTS), exist_ok=True)
    new_file = not os.path.exists(RESULTS)
    with open(RESULTS, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        if new_file:
            w.writeheader()
        w.writerows(rows)
    print("\n| experiment | variant | seconds | total tasks | task median ms* | task max ms* | plan (final, after AQE) | note |")
    print("|---|---|---|---|---|---|---|---|")
    for r in rows:
        print(f"| {r['experiment']} | {r['variant']} | {r['seconds']} | {r['total_tasks']} | "
              f"{r['task_median_ms']} | {r['task_max_ms']} | {r['plan']} | {r['note']} |")
    print("* median / max task run time of the most skewed stage of that query")
    print(f"\nappended {len(rows)} rows to {RESULTS}")
    if args.hold:
        time.sleep(args.hold)
    spark.stop()


if __name__ == "__main__":
    main()
