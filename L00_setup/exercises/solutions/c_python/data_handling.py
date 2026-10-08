#!/usr/bin/env python3
"""L00 exercise (c) SOLUTION - Python data handling: CSV vs Parquet, pandas vs polars vs DuckDB, generators.

    python c_python/data_handling.py

Uses data/taxi_sample.csv (100,000 rows, made in step 0 of the README) and the full January 2024
Parquet file (2,964,624 rows) from labs/datasets/data/taxi/.
"""
import csv
import time
import tracemalloc
from pathlib import Path

import duckdb
import pandas as pd
import polars as pl
import pyarrow.parquet as pq

EX = Path(__file__).resolve().parents[2] if Path(__file__).resolve().parent.parent.name == "solutions" \
    else Path(__file__).resolve().parents[1]
CSV = EX / "data" / "taxi_sample.csv"
PARQUET = EX.parent.parent / "datasets" / "data" / "taxi" / "yellow_tripdata_2024-01.parquet"


def timed(label: str, fn):
    t = time.perf_counter()
    out = fn()
    print(f"  {label:<44} {time.perf_counter() - t:7.3f} s")
    return out


# ---------------------------------------------------------------- 1. reading files with pandas
print("1. pandas: CSV vs Parquet")
df_csv = timed("pd.read_csv(sample, 100k rows)", lambda: pd.read_csv(CSV, parse_dates=["tpep_pickup_datetime",
                                                                                        "tpep_dropoff_datetime"]))
print(f"  dtypes inferred from text, e.g. passenger_count -> {df_csv['passenger_count'].dtype}, "
      f"store_and_fwd_flag -> {df_csv['store_and_fwd_flag'].dtype}")
print(f"  memory: {df_csv.memory_usage(deep=True).sum() / 1e6:.1f} MB for {len(df_csv):,} rows")
df_pq = timed("pd.read_parquet(full month, 2 columns)", lambda: pd.read_parquet(PARQUET,
                                                                               columns=["payment_type", "fare_amount"]))
print(f"  {len(df_pq):,} rows; the types come from the Parquet schema (no guessing)")

# ---------------------------------------------------------------- 2. the same aggregation, three engines
print("\n2. avg fare and trip count per payment_type on the full month (3 M rows)")
r_pd = timed("pandas  read_parquet + groupby", lambda: (
    pd.read_parquet(PARQUET, columns=["payment_type", "fare_amount"])
    .groupby("payment_type").agg(trips=("fare_amount", "size"), avg_fare=("fare_amount", "mean"))))
r_pl = timed("polars  scan_parquet (lazy) + group_by", lambda: (
    pl.scan_parquet(PARQUET).group_by("payment_type")
    .agg(pl.len().alias("trips"), pl.col("fare_amount").mean().alias("avg_fare"))
    .sort("payment_type").collect()))
r_dd = timed("DuckDB  SQL directly on the Parquet file", lambda: duckdb.sql(
    f"SELECT payment_type, count(*) AS trips, avg(fare_amount) AS avg_fare "
    f"FROM read_parquet('{PARQUET}') GROUP BY 1 ORDER BY 1").df())
print(r_dd.round(2).to_string(index=False))
assert r_pd["trips"].tolist() == r_dd["trips"].tolist() == r_pl["trips"].to_list(), "engines disagree!"
print("  -> all three engines return the same numbers")


# ---------------------------------------------------------------- 3. streaming with generators
def read_trips(path: Path):
    """Generator: yields one trip (dict) at a time - memory stays flat whatever the file size."""
    with path.open(newline="") as f:
        for row in csv.DictReader(f):
            yield row


def big_fares(rows, threshold: float):
    """A generator that consumes another generator: a lazy filter (like a pipeline stage)."""
    for r in rows:
        if float(r["fare_amount"]) > threshold:
            yield r


print("\n3. generators: process the CSV one row at a time")
tracemalloc.start()
t = time.perf_counter()
n = total = 0
for trip in big_fares(read_trips(CSV), 50.0):
    n += 1
    total += float(trip["total_amount"])
peak = tracemalloc.get_traced_memory()[1]
tracemalloc.stop()
print(f"  {n:,} trips with fare > 50, revenue {total:,.2f} - {time.perf_counter() - t:.2f} s, "
      f"peak Python memory {peak / 1e6:.2f} MB")

tracemalloc.start()
df_all = pd.read_csv(CSV)
peak_df = tracemalloc.get_traced_memory()[1]
tracemalloc.stop()
print(f"  the same file loaded at once with pandas: peak {peak_df / 1e6:.1f} MB")

print("  batches instead of rows: pyarrow iter_batches over the 3 M-row Parquet file")
n = 0
for batch in pq.ParquetFile(PARQUET).iter_batches(batch_size=500_000, columns=["fare_amount"]):
    n += 1
    print(f"    batch {n}: {batch.num_rows:,} rows")
