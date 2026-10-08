#!/usr/bin/env python3
"""Land a month of NYC taxi data into the lab's landing zone (data/landing/).

    python scripts/land_month.py 2024-01

Copies labs/datasets/data/taxi/yellow_tripdata_YYYY-MM.parquet (downloaded in L00 with
labs/datasets/download_taxi.sh) to labs/L13_data_quality/data/landing/. We copy instead of reading the
shared folder directly so this lab can drop *broken* files next to the good one without touching
the dataset every other lab uses.
"""
import shutil
import sys
from pathlib import Path

LAB = next(p for p in Path(__file__).resolve().parents if (p / "docker-compose.yml").exists())
SHARED = LAB.parent / "datasets" / "data" / "taxi"
LANDING = LAB / "data" / "landing"


def main() -> int:
    months = sys.argv[1:] or ["2024-01"]
    LANDING.mkdir(parents=True, exist_ok=True)
    for month in months:
        src = SHARED / f"yellow_tripdata_{month}.parquet"
        if not src.exists():
            print(f"[FAIL] {src} not found - run: bash labs/datasets/download_taxi.sh {month}", file=sys.stderr)
            return 1
        dst = LANDING / src.name
        shutil.copyfile(src, dst)
        print(f"[ ok ] landed {dst.relative_to(LAB)} ({dst.stat().st_size / 1e6:.1f} MB)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
