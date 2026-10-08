#!/usr/bin/env python3
"""L00 exercise (e) SOLUTION - same data, different file formats: size and query speed.

    python e_formats/formats.py                 # 1,000,000 taxi rows (default)
    python e_formats/formats.py --rows 3000000  # the whole month

Writes the same rows as CSV, CSV+gzip, JSON lines, Parquet (uncompressed, snappy, zstd) into
data/formats/, then runs the same column-subset aggregation on each file with DuckDB:

    SELECT payment_type, count(*), avg(fare_amount) FROM <file> GROUP BY payment_type

and prints a size / time table. Finally it shows WHY Parquet wins: per-column chunks + min/max statistics.
"""
import argparse
import time
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow.parquet as pq

EX = Path(__file__).resolve().parents[2] if Path(__file__).resolve().parent.parent.name == "solutions" \
    else Path(__file__).resolve().parents[1]
TAXI = EX.parent.parent / "datasets" / "data" / "taxi" / "yellow_tripdata_2024-01.parquet"
OUT = EX / "data" / "formats"

QUERY = "SELECT payment_type, count(*) AS trips, round(avg(fare_amount), 2) AS avg_fare FROM {src} GROUP BY 1 ORDER BY 1"

FORMATS = {
    # name: (file name, COPY options, DuckDB reader expression)
    "csv":             ("trips.csv",            "(FORMAT csv, HEADER)",                        "read_csv('{p}')"),
    "csv.gz":          ("trips.csv.gz",         "(FORMAT csv, HEADER, COMPRESSION gzip)",      "read_csv('{p}')"),
    "json (lines)":    ("trips.jsonl",          "(FORMAT json)",                               "read_json('{p}')"),
    "parquet (none)":  ("trips_none.parquet",   "(FORMAT parquet, COMPRESSION uncompressed)",  "read_parquet('{p}')"),
    "parquet snappy":  ("trips_snappy.parquet", "(FORMAT parquet, COMPRESSION snappy)",        "read_parquet('{p}')"),
    "parquet zstd":    ("trips_zstd.parquet",   "(FORMAT parquet, COMPRESSION zstd)",          "read_parquet('{p}')"),
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
    print(f"{args.rows:,} rows, 19 columns\n")

    rows = []
    for name, (fname, opts, reader) in FORMATS.items():
        path = OUT / fname
        t_write = best_of(1, lambda: con.execute(f"COPY trips TO '{path}' {opts}"))
        src = reader.format(p=path)
        # fresh connection per measurement: no cached data, same engine for every format
        t_query = best_of(3, lambda: duckdb.connect().execute(QUERY.format(src=src)).fetchall())
        # the same 2 columns with pandas (single-threaded, materialises a DataFrame)
        cols = ["payment_type", "fare_amount"]
        if name.startswith("csv"):
            t_pd = best_of(1, lambda: pd.read_csv(path, usecols=cols).groupby("payment_type")["fare_amount"].mean())
        elif name.startswith("parquet"):
            t_pd = best_of(1, lambda: pd.read_parquet(path, columns=cols).groupby("payment_type")["fare_amount"].mean())
        else:
            t_pd = float("nan")   # pandas.read_json has no column projection: it must parse every field
        rows.append((name, path.stat().st_size / 1e6, t_write, t_query, t_pd))

    csv_size = rows[0][1]
    print(f"{'format':<16}{'size MB':>9}{'vs CSV':>8}{'write s':>9}{'DuckDB s':>10}{'pandas s':>10}")
    for name, size, tw, tq, tp in rows:
        print(f"{name:<16}{size:>9.1f}{size / csv_size:>8.0%}{tw:>9.2f}{tq:>10.3f}{tp:>10.3f}")

    print("\nquery result (identical for every format):")
    print(duckdb.sql(QUERY.format(src=f"read_parquet('{OUT / 'trips_zstd.parquet'}')")))

    # ---- why Parquet is fast: columnar layout + statistics ----
    md = pq.ParquetFile(OUT / "trips_zstd.parquet").metadata
    print(f"Parquet metadata: {md.num_rows:,} rows, {md.num_row_groups} row group(s), {md.num_columns} column chunks each")
    rg = md.row_group(0)
    for i in range(rg.num_columns):
        col = rg.column(i)
        if col.path_in_schema in ("payment_type", "fare_amount", "tpep_pickup_datetime", "store_and_fwd_flag"):
            st = col.statistics
            print(f"  {col.path_in_schema:<22} {col.compression:<6} {col.total_compressed_size / 1e6:6.2f} MB "
                  f"(uncompressed {col.total_uncompressed_size / 1e6:6.2f} MB)  min={st.min if st else '?'}  max={st.max if st else '?'}")
    print("The query above reads only the payment_type and fare_amount chunks - the other 17 columns are skipped.")


if __name__ == "__main__":
    main()
