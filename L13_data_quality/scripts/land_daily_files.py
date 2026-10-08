#!/usr/bin/env python3
"""Part 2 - simulate a DAILY feed: split a taxi month into one file per pickup day.

    python scripts/land_daily_files.py --half-day 2024-01-23 --skip-day 2024-01-25
    python scripts/land_daily_files.py --only 2024-01-25          # the late file finally arrives

Writes data/daily/yellow_trips/ds=YYYY-MM-DD/trips.parquet (one partition per day) from
labs/datasets/data/taxi/yellow_tripdata_<month>.parquet, with two classic incidents:

  --half-day DAY   the loader crashed at noon: only trips picked up before 12:00 are delivered
  --skip-day DAY   the file never arrives (no partition at all)
"""
import argparse
import shutil
import sys
from datetime import date, timedelta
from pathlib import Path

import duckdb

LAB = next(p for p in Path(__file__).resolve().parents if (p / "docker-compose.yml").exists())
SHARED = LAB.parent / "datasets" / "data" / "taxi"
DAILY = LAB / "data" / "daily" / "yellow_trips"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--month", default="2024-01")
    ap.add_argument("--half-day", action="append", default=[], metavar="YYYY-MM-DD")
    ap.add_argument("--skip-day", action="append", default=[], metavar="YYYY-MM-DD")
    ap.add_argument("--only", action="append", default=[], metavar="YYYY-MM-DD", help="(re)land only these days")
    args = ap.parse_args()

    src = SHARED / f"yellow_tripdata_{args.month}.parquet"
    if not src.exists():
        print(f"[FAIL] {src} not found - run: bash labs/datasets/download_taxi.sh {args.month}", file=sys.stderr)
        return 1
    y, m = map(int, args.month.split("-"))
    first = date(y, m, 1)
    days = []
    d = first
    while d.month == m:
        days.append(d)
        d += timedelta(days=1)
    if args.only:
        days = [date.fromisoformat(x) for x in args.only]

    con = duckdb.connect()
    con.execute(f"CREATE TEMP TABLE month AS SELECT * FROM read_parquet('{src}')")
    for day in days:
        out = DAILY / f"ds={day}"
        if out.exists():
            shutil.rmtree(out)
        if str(day) in args.skip_day:
            print(f"{day}  SKIPPED (file never arrives)")
            continue
        end = f"{day} 12:00:00" if str(day) in args.half_day else f"{day + timedelta(days=1)} 00:00:00"
        out.mkdir(parents=True)
        n = con.execute(
            f"COPY (SELECT * FROM month WHERE tpep_pickup_datetime >= TIMESTAMP '{day} 00:00:00' "
            f"AND tpep_pickup_datetime < TIMESTAMP '{end}') TO '{out / 'trips.parquet'}' (FORMAT parquet)"
        ).fetchone()[0]
        note = "  <- HALF DAY (loader died at 12:00)" if str(day) in args.half_day else ""
        print(f"{day}  {n:>7,} rows{note}")
    print(f"[ ok ] partitions in {DAILY.relative_to(LAB)}/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
