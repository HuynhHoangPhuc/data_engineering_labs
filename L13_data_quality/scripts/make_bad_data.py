#!/usr/bin/env python3
"""Create a BROKEN "next month" file from a good month - the input for "watch the checks fail".

    python scripts/make_bad_data.py                    # 2024-01 -> data/landing/yellow_tripdata_2024-02.parquet
    python scripts/make_bad_data.py --src 2024-01 --as 2024-02

The story: "February just landed in the landing zone. Is it good?" It is January shifted by 31 days,
with six realistic defects injected (deterministic, seed 42):

  1. partial load        - only trips picked up before the 14th of the month (the export job died)
  2. schema drift        - column `Airport_fee` renamed to `airport_fee` (this really happened in TLC files)
  3. NULL timestamps     - 2 % of `tpep_pickup_datetime` set to NULL
  4. negative fares      - 5 % of `fare_amount` multiplied by -1
  5. unknown code        - 1,000 rows with `payment_type = 9` (valid codes are 0-6)
  6. duplicate rows      - 1 % of rows appended a second time (an at-least-once loader retried)
"""
import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

LAB = next(p for p in Path(__file__).resolve().parents if (p / "docker-compose.yml").exists())
SHARED = LAB.parent / "datasets" / "data" / "taxi"
LANDING = LAB / "data" / "landing"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--src", default="2024-01", help="good month to start from (default 2024-01)")
    ap.add_argument("--as", dest="as_month", default="2024-02", help="month name of the broken file (default 2024-02)")
    ap.add_argument("--shift-days", type=int, default=31, help="shift timestamps by N days (default 31)")
    args = ap.parse_args()

    src = SHARED / f"yellow_tripdata_{args.src}.parquet"
    if not src.exists():
        print(f"[FAIL] {src} not found - run: bash labs/datasets/download_taxi.sh {args.src}", file=sys.stderr)
        return 1
    rng = np.random.default_rng(42)
    df = pq.read_table(src).to_pandas()
    n0 = len(df)
    shift = np.timedelta64(args.shift_days, "D")
    for c in ("tpep_pickup_datetime", "tpep_dropoff_datetime"):
        df[c] = df[c] + shift

    # 1. partial load: keep trips before the 14th of the (new) month
    year, month = map(int, args.as_month.split("-"))
    cutoff = np.datetime64(f"{year:04d}-{month:02d}-14")
    df = df[df["tpep_pickup_datetime"] < cutoff].reset_index(drop=True)
    print(f"1. partial load       : {n0:,} -> {len(df):,} rows (pickups before {cutoff})")

    # 2. schema drift
    df = df.rename(columns={"Airport_fee": "airport_fee"})
    print("2. schema drift       : Airport_fee -> airport_fee")

    # 3. NULL pickup timestamps
    idx = rng.choice(len(df), size=int(0.02 * len(df)), replace=False)
    df.loc[idx, "tpep_pickup_datetime"] = None
    print(f"3. NULL pickup times  : {len(idx):,} rows")

    # 4. negative fares
    idx = rng.choice(len(df), size=int(0.05 * len(df)), replace=False)
    df.loc[idx, "fare_amount"] = -df.loc[idx, "fare_amount"].abs()
    print(f"4. negative fares     : {len(idx):,} rows")

    # 5. unknown payment_type
    idx = rng.choice(len(df), size=1000, replace=False)
    df.loc[idx, "payment_type"] = 9
    print(f"5. payment_type = 9   : {len(idx):,} rows")

    # 6. duplicates
    idx = rng.choice(len(df), size=int(0.01 * len(df)), replace=False)
    dups = df.iloc[idx]
    df = pd.concat([df, dups], ignore_index=True)
    print(f"6. duplicate rows     : {len(dups):,} rows appended")

    LANDING.mkdir(parents=True, exist_ok=True)
    dst = LANDING / f"yellow_tripdata_{args.as_month}.parquet"
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), dst)
    print(f"[ ok ] wrote {dst.relative_to(LAB)}: {len(df):,} rows ({dst.stat().st_size / 1e6:.1f} MB)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
