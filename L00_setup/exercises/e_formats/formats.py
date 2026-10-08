#!/usr/bin/env python3
"""L00 exercise (e) STARTER - same data, different file formats: size and query speed.

    python e_formats/formats.py                 # 1,000,000 taxi rows (default)

Complete TODO 1-3, run, and fill the results table in the README.
Solution: solutions/e_formats/formats.py
"""
import argparse
import time
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow.parquet as pq

EX = Path(__file__).resolve().parents[1]                         # labs/L00_setup/exercises
TAXI = EX.parent.parent / "datasets" / "data" / "taxi" / "yellow_tripdata_2024-01.parquet"
OUT = EX / "data" / "formats"

QUERY = "SELECT payment_type, count(*) AS trips, round(avg(fare_amount), 2) AS avg_fare FROM {src} GROUP BY 1 ORDER BY 1"

FORMATS = {
    # name: (file name, DuckDB COPY options, DuckDB reader expression)
    "csv":    ("trips.csv",    "(FORMAT csv, HEADER)",                   "read_csv('{p}')"),
    "csv.gz": ("trips.csv.gz", "(FORMAT csv, HEADER, COMPRESSION gzip)", "read_csv('{p}')"),
    # TODO 1: add four more formats (see https://duckdb.org/docs/sql/statements/copy):
    #   "json (lines)"   -> trips.jsonl,            (FORMAT json),                              read_json('{p}')
    #   "parquet (none)" -> trips_none.parquet,     (FORMAT parquet, COMPRESSION uncompressed), read_parquet('{p}')
    #   "parquet snappy" -> trips_snappy.parquet,   ... COMPRESSION snappy
    #   "parquet zstd"   -> trips_zstd.parquet,     ... COMPRESSION zstd
}


def best_of(n: int, fn) -> float:
    times = []
    for _ in range(n):
        t = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t)
    return min(times)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=1_000_000)
    args = ap.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"CREATE TABLE trips AS SELECT * FROM read_parquet('{TAXI}') LIMIT {args.rows}")

    rows = []
    for name, (fname, opts, reader) in FORMATS.items():
        path = OUT / fname
        t_write = best_of(1, lambda: con.execute(f"COPY trips TO '{path}' {opts}"))
        src = reader.format(p=path)
        # TODO 2: time QUERY on `src` with a FRESH duckdb.connect() each time, best of 3
        t_query = float("nan")
        cols = ["payment_type", "fare_amount"]
        if name.startswith("csv"):
            t_pd = best_of(1, lambda: pd.read_csv(path, usecols=cols).groupby("payment_type")["fare_amount"].mean())
        elif name.startswith("parquet"):
            t_pd = best_of(1, lambda: pd.read_parquet(path, columns=cols).groupby("payment_type")["fare_amount"].mean())
        else:
            t_pd = float("nan")
        rows.append((name, path.stat().st_size / 1e6, t_write, t_query, t_pd))

    csv_size = rows[0][1]
    print(f"{'format':<16}{'size MB':>9}{'vs CSV':>8}{'write s':>9}{'DuckDB s':>10}{'pandas s':>10}")
    for name, size, tw, tq, tp in rows:
        print(f"{name:<16}{size:>9.1f}{size / csv_size:>8.0%}{tw:>9.2f}{tq:>10.3f}{tp:>10.3f}")

    # TODO 3: open one of your Parquet files with pq.ParquetFile(...).metadata and print
    #   num_rows, num_row_groups, and for row group 0 the compressed size + statistics (min/max)
    #   of the columns payment_type and tpep_pickup_datetime. What does the min pickup date tell you?
    _ = pq


if __name__ == "__main__":
    main()
