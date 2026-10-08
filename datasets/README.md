# Shared datasets

Mounted into lab containers as `/datasets` (read-only in most labs). Downloaded data lives in
`datasets/data/` which is **git-ignored**.

| Dataset | How to get it | Used in |
|---|---|---|
| NYC TLC Yellow Taxi trips (Parquet, monthly, ~48 MB / ~3 M rows per month) + zone lookup CSV | `bash download_taxi.sh [YYYY-MM ...]` (default `2024-01`) → `data/taxi/yellow_tripdata_YYYY-MM.parquet`, `data/taxi/taxi_zone_lookup.csv` | L01, L02, L03, L05, L06, L10, L11 |
| Olist Brazilian e-commerce (OLTP source in Postgres) | nothing to download — `olist/init/01_schema.sql` + `02_seed.sql` (synthetic, same schema); real Kaggle data: see [`olist/README.md`](olist/README.md) | L04, L08, L12, capstone |
| Wikimedia EventStreams (live SSE) | streamed at runtime from `https://stream.wikimedia.org/v2/stream/recentchange` | L09 |

## Tools

* `download_taxi.sh` — idempotent (skips existing files), downloads to `*.part` first, validates the month format.
  Source: TLC CloudFront distribution `https://d37ci6vzurychx.cloudfront.net/trip-data/…` (verified Sep 2026).
  Override with `TLC_BASE_URL=...` if you mirror the files locally.
* `parquet_to_csv.py SRC DST [--limit N] [--header]` — Parquet → CSV with Hive-friendly timestamps
  (`YYYY-MM-DD HH:MM:SS`). Needs `pyarrow` (pre-installed in `de-labs/hadoop`); `--show-schema` prints the schema.
* `olist/generate_seed.py` — regenerates the synthetic Olist seed (stdlib only, deterministic).

## Taxi schema (2024 files)

`VendorID int32, tpep_pickup_datetime timestamp, tpep_dropoff_datetime timestamp, passenger_count int64,
trip_distance double, RatecodeID int64, store_and_fwd_flag string, PULocationID int32, DOLocationID int32,
payment_type int64, fare_amount double, extra double, mta_tax double, tip_amount double, tolls_amount double,
improvement_surcharge double, total_amount double, congestion_surcharge double, Airport_fee double`

January 2024: 2,964,624 rows. It contains a few rows with pickup dates outside January (2002, 2009,
2023-12) and negative amounts — used on purpose in the cleaning/partitioning exercises.
