#!/usr/bin/env python3
"""L13 Part 4 (SOLUTION) - a batch pipeline with two data-quality GATES that block publishing.

    python part4_gate/pipeline.py --month 2024-01                     # good data   -> PUBLISHED (exit 0)
    python part4_gate/pipeline.py --month 2024-02                     # broken file -> BLOCKED at the input gate (exit 1)
    python part4_gate/pipeline.py --month 2024-01 --buggy-transform   # wrong grain -> BLOCKED at the output gate (exit 2)

  data/landing/yellow_tripdata_<month>.parquet
     |  [gate 1] GX checkpoint yellow_trips_raw_cp (Part 1)  -- FAIL -> move file to data/quarantine/, alert, exit 1
     v
  transform (DuckDB): daily trips/revenue per pickup zone   -> data/staging/daily_zone_<month>.parquet
     |  [gate 2] GX suite daily_zone_gate on the DataFrame   -- FAIL -> keep staging for debugging, alert, exit 2
     v
  publish: atomic rename staging -> data/published/ + _manifest.json   (consumers only ever read published/)

"Write-audit-publish" (WAP): never let consumers see data that has not passed its checks.
"""
import argparse
import calendar
import json
import os
import shutil
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import great_expectations as gx
import great_expectations.expectations as gxe

LAB = next(p for p in Path(__file__).resolve().parents if (p / "docker-compose.yml").exists())
sys.path.insert(0, str(LAB / "part1_gx"))
from run_checkpoint import run as run_raw_checkpoint  # noqa: E402  (Part 1 gate, reused as-is)

DATA = LAB / "data"
LANDING, STAGING, PUBLISHED, QUARANTINE = (DATA / d for d in ("landing", "staging", "published", "quarantine"))
WEBHOOK = os.environ.get("ALERT_WEBHOOK_URL", "http://localhost:9099/alert")
OUTPUT_COLUMNS = ["pickup_date", "zone_id", "trips", "revenue", "avg_fare"]


def alert(month: str, check: str, message: str) -> None:
    body = {"severity": "critical", "check": check, "dataset": "daily_zone", "partition": month, "message": message}
    print(f"  -> ALERT: {check} - {message}")
    req = urllib.request.Request(WEBHOOK, data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"}, method="POST")
    try:
        urllib.request.urlopen(req, timeout=3).read()
    except OSError as exc:
        print(f"  !! alert not delivered ({exc}); is `docker compose --profile monitor up -d` running?", file=sys.stderr)


def transform(month: str, buggy: bool) -> Path:
    """Daily trips and revenue per pickup zone (grain: pickup_date x zone_id)."""
    src = LANDING / f"yellow_tripdata_{month}.parquet"
    y, m = map(int, month.split("-"))
    start, end = f"{y:04d}-{m:02d}-01", (f"{y + 1:04d}-01-01" if m == 12 else f"{y:04d}-{m + 1:02d}-01")
    # The bug: grouping by an extra column that is not selected silently changes the grain -> duplicate keys
    group_by = "pickup_date, zone_id, payment_type" if buggy else "pickup_date, zone_id"
    STAGING.mkdir(parents=True, exist_ok=True)
    out = STAGING / f"daily_zone_{month}.parquet"
    duckdb.execute(f"""
        COPY (
            SELECT CAST(tpep_pickup_datetime AS DATE) AS pickup_date,
                   PULocationID                       AS zone_id,
                   count(*)                           AS trips,
                   round(sum(total_amount), 2)        AS revenue,
                   round(avg(fare_amount), 2)         AS avg_fare
            FROM read_parquet('{src}')
            WHERE tpep_pickup_datetime >= TIMESTAMP '{start}' AND tpep_pickup_datetime < TIMESTAMP '{end}'
              AND fare_amount > 0 AND trip_distance > 0 AND total_amount >= 0
            GROUP BY {group_by}
            ORDER BY pickup_date, zone_id
        ) TO '{out}' (FORMAT parquet)""")
    return out


def output_gate(context, month: str, path: Path):
    """GX on an in-memory DataFrame: pandas data source + dataframe asset + 'whole dataframe' batch definition."""
    ds = context.data_sources.add_or_update_pandas(name="pipeline_frames")
    asset = ds.get_asset("daily_zone") if "daily_zone" in ds.get_asset_names() else ds.add_dataframe_asset("daily_zone")
    bd = asset.get_batch_definition("whole") if "whole" in [b.name for b in asset.batch_definitions] \
        else asset.add_batch_definition_whole_dataframe("whole")

    suite = gx.ExpectationSuite(name="daily_zone_gate")
    for e in [
        gxe.ExpectTableColumnsToMatchOrderedList(column_list=OUTPUT_COLUMNS),
        gxe.ExpectCompoundColumnsToBeUnique(column_list=["pickup_date", "zone_id"]),   # the grain
        gxe.ExpectColumnValuesToNotBeNull(column="pickup_date"),
        gxe.ExpectColumnValuesToNotBeNull(column="zone_id"),
        gxe.ExpectColumnValuesToBeBetween(column="trips", min_value=1),
        gxe.ExpectColumnValuesToBeBetween(column="revenue", min_value=0),
        # every day of the month must be present - the value comes from a SUITE PARAMETER at run time
        gxe.ExpectColumnUniqueValueCountToBeBetween(column="pickup_date",
                                                    min_value={"$PARAMETER": "days_in_month"},
                                                    max_value={"$PARAMETER": "days_in_month"}),
        gxe.ExpectColumnSumToBeBetween(column="trips", min_value=2_000_000, max_value=4_500_000),
    ]:
        suite.add_expectation(e)
    suite = context.suites.add_or_update(suite)
    vd = context.validation_definitions.add_or_update(
        gx.ValidationDefinition(name="daily_zone_gate_vd", data=bd, suite=suite))

    df = duckdb.sql(f"SELECT * FROM read_parquet('{path}')").df()
    y, m = map(int, month.split("-"))
    result = vd.run(batch_parameters={"dataframe": df},
                    expectation_parameters={"days_in_month": calendar.monthrange(y, m)[1]})
    failed = [r for r in result.results if not r.success]
    for r in failed:
        cfg = r.expectation_config
        print(f"  FAIL {cfg.type} {cfg.kwargs.get('column') or cfg.kwargs.get('column_list') or ''} "
              f"{ {k: v for k, v in (r.result or {}).items() if k in ('observed_value', 'unexpected_count')} }")
    return result.success, len(df), failed


def publish(month: str, staged: Path, rows: int) -> Path:
    PUBLISHED.mkdir(parents=True, exist_ok=True)
    final = PUBLISHED / staged.name
    os.replace(staged, final)                       # atomic on the same file system
    manifest = {"month": month, "file": final.name, "rows": rows,
                "published_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "checks": ["yellow_trips_raw_cp", "daily_zone_gate"]}
    (PUBLISHED / f"_manifest_{month}.json").write_text(json.dumps(manifest, indent=2))
    return final


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--month", default="2024-01")
    ap.add_argument("--buggy-transform", action="store_true", help="introduce a grain bug in the transform")
    args = ap.parse_args()
    month = args.month
    src = LANDING / f"yellow_tripdata_{month}.parquet"
    if not src.exists():
        print(f"[FAIL] {src} missing (python scripts/land_month.py {month})", file=sys.stderr)
        return 3

    print(f"== [1/4] gate 1: validate the raw input ({src.name})")
    if not run_raw_checkpoint(month, only_failures=True):
        QUARANTINE.mkdir(parents=True, exist_ok=True)
        shutil.move(src, QUARANTINE / src.name)
        alert(month, "input_gate", f"raw file failed yellow_trips_raw_cp -> moved to data/quarantine/{src.name}")
        print("PIPELINE BLOCKED at gate 1 - nothing was transformed or published.")
        return 1

    print("== [2/4] transform (DuckDB)")
    staged = transform(month, args.buggy_transform)
    print(f"  wrote {staged.relative_to(LAB)}")

    print("== [3/4] gate 2: validate the output (suite daily_zone_gate)")
    context = gx.get_context(mode="file", project_root_dir=str(LAB))
    ok, rows, failed = output_gate(context, month, staged)
    if not ok:
        alert(month, "output_gate", f"{len(failed)} expectation(s) failed on {staged.name}; kept in data/staging/")
        print("PIPELINE BLOCKED at gate 2 - data/published/ is unchanged.")
        return 2
    print(f"  all output expectations passed ({rows:,} rows)")

    print("== [4/4] publish")
    final = publish(month, staged, rows)
    print(f"PUBLISHED {final.relative_to(LAB)} ({rows:,} rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
