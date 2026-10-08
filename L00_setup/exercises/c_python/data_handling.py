#!/usr/bin/env python3
"""L00 exercise (c) STARTER - Python data handling: CSV vs Parquet, pandas vs polars vs DuckDB, generators.

    python c_python/data_handling.py

Complete TODO 1-5 (solution: solutions/c_python/data_handling.py).
"""
import csv
import time
from pathlib import Path

import duckdb
import pandas as pd
import polars as pl

EX = Path(__file__).resolve().parents[1]                          # labs/L00_setup/exercises
CSV = EX / "data" / "taxi_sample.csv"
PARQUET = EX.parent.parent / "datasets" / "data" / "taxi" / "yellow_tripdata_2024-01.parquet"


def timed(label: str, fn):
    t = time.perf_counter()
    out = fn()
    print(f"  {label:<44} {time.perf_counter() - t:7.3f} s")
    return out


print("1. pandas: CSV vs Parquet")
df_csv = timed("pd.read_csv(sample, 100k rows)", lambda: pd.read_csv(CSV))
print(df_csv.dtypes.head(6))
# TODO 1: re-read the CSV with parse_dates=[...] for the two timestamp columns. What was the dtype before?
#         Print the DataFrame's memory with df.memory_usage(deep=True).sum()
# TODO 2: read ONLY the columns payment_type and fare_amount of the full Parquet month with
#         pd.read_parquet(PARQUET, columns=[...]) and time it. Why can't read_csv skip columns as cheaply?

print("\n2. avg fare and trip count per payment_type on the full month (3 M rows)")
r_pd = timed("pandas  read_parquet + groupby", lambda: (
    pd.read_parquet(PARQUET, columns=["payment_type", "fare_amount"])
    .groupby("payment_type").agg(trips=("fare_amount", "size"), avg_fare=("fare_amount", "mean"))))
print(r_pd.round(2))
# TODO 3: the same with polars (pl.scan_parquet(...).group_by(...).agg(pl.len(), pl.col(...).mean()).collect())
#         and with DuckDB (duckdb.sql("SELECT ... FROM read_parquet('...') GROUP BY 1").df()). Time both.
_ = (pl, duckdb)


def read_trips(path: Path):
    """Generator: yields one trip (dict) at a time."""
    with path.open(newline="") as f:
        for row in csv.DictReader(f):
            yield row


print("\n3. generators")
# TODO 4: write a generator big_fares(rows, threshold) that yields only rows with fare_amount > threshold,
#         chain it after read_trips(CSV) and compute the count and the sum of total_amount for fares > 50.
#         Measure peak memory with tracemalloc and compare with pd.read_csv(CSV).
# TODO 5: iterate over the full Parquet month in batches of 500,000 rows with
#         pyarrow.parquet.ParquetFile(PARQUET).iter_batches(batch_size=..., columns=["fare_amount"])
first = next(read_trips(CSV))
print(f"  first row as dict: VendorID={first['VendorID']} fare={first['fare_amount']}")
