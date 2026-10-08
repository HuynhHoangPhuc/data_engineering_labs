#!/usr/bin/env python3
"""L13 Part 2 (SOLUTION) - freshness & volume monitor for a daily feed, with webhook alerts.

    python part2_monitor/monitor.py --from 2024-01-01 --to 2024-01-31     # replay a month, day by day
    python part2_monitor/monitor.py --ds 2024-01-25                        # check one day
    python part2_monitor/monitor.py --report                               # print the metrics table

For every day `ds` the monitor behaves as if it ran at  as_of = ds + 1 day 06:00  (the morning after):

 1. METRICS   row_count and max(tpep_pickup_datetime) of data/daily/yellow_trips/ds=<ds>/ (DuckDB),
              upserted into the table `volume_metrics` of data/monitor.duckdb (the "metrics store").
 2. VOLUME    z = (row_count - mean) / std over the last --window healthy days (status OK).
              |z| > --k  -> VOLUME_ANOMALY. Missing partition -> MISSING. < --min-history days -> WARMUP.
 3. FRESHNESS lag = as_of - newest pickup seen so far. lag > --sla-hours -> STALE.
 4. ALERT     every failed check is POSTed as JSON to the webhook (--webhook / $ALERT_WEBHOOK_URL).
"""
import argparse
import json
import math
import os
import sys
import urllib.request
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb

LAB = next(p for p in Path(__file__).resolve().parents if (p / "docker-compose.yml").exists())
DAILY = LAB / "data" / "daily" / "yellow_trips"
DEFAULT_DB = LAB / "data" / "monitor.duckdb"
DEFAULT_WEBHOOK = os.environ.get("ALERT_WEBHOOK_URL", "http://localhost:9099/alert")

DDL = """
CREATE TABLE IF NOT EXISTS volume_metrics (
    ds               DATE PRIMARY KEY,
    row_count        BIGINT,
    max_pickup       TIMESTAMP,
    checked_as_of    TIMESTAMP,
    z_score          DOUBLE,
    volume_status    VARCHAR,     -- OK | WARMUP | VOLUME_ANOMALY | MISSING
    freshness_lag_h  DOUBLE,
    freshness_status VARCHAR      -- FRESH | STALE
)"""


def collect_metrics(con, ds: date) -> tuple[int, datetime | None]:
    """Row count and newest pickup timestamp of one daily partition (0, None if it does not exist)."""
    part = DAILY / f"ds={ds}"
    files = sorted(part.glob("*.parquet")) if part.exists() else []
    if not files:
        return 0, None
    return con.execute(
        "SELECT count(*), max(tpep_pickup_datetime) FROM read_parquet(?)", [[str(f) for f in files]]
    ).fetchone()


def volume_check(con, ds: date, row_count: int, window: int, min_history: int, k: float):
    """z-score of today's count against the last `window` HEALTHY days (anomalies are excluded so
    that one bad day does not poison the baseline)."""
    if row_count == 0:
        return None, "MISSING"
    hist = [r[0] for r in con.execute(
        "SELECT row_count FROM volume_metrics WHERE ds < ? AND volume_status IN ('OK', 'WARMUP') "
        "ORDER BY ds DESC LIMIT ?", [ds, window]).fetchall()]
    if len(hist) < min_history:
        return None, "WARMUP"
    mean = sum(hist) / len(hist)
    std = math.sqrt(sum((x - mean) ** 2 for x in hist) / (len(hist) - 1))
    z = (row_count - mean) / std if std > 0 else 0.0
    return z, ("VOLUME_ANOMALY" if abs(z) > k else "OK")


def freshness_check(con, as_of: datetime, sla_hours: float):
    """Lag between 'now' and the newest event we have received so far (any partition up to as_of)."""
    newest = con.execute("SELECT max(max_pickup) FROM volume_metrics WHERE max_pickup <= ?", [as_of]).fetchone()[0]
    if newest is None:
        return None, "STALE"
    lag_h = (as_of - newest).total_seconds() / 3600
    return lag_h, ("STALE" if lag_h > sla_hours else "FRESH")


def send_alert(url: str, alert: dict) -> None:
    req = urllib.request.Request(url, data=json.dumps(alert).encode(),
                                 headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=3) as resp:
            resp.read()
    except OSError as exc:  # never let alerting crash the monitor - but say so loudly
        print(f"    !! could not deliver alert to {url}: {exc}", file=sys.stderr)


def check_day(con, ds: date, args) -> list[dict]:
    as_of = datetime.combine(ds + timedelta(days=1), datetime.min.time()) + timedelta(hours=6)
    row_count, max_pickup = collect_metrics(con, ds)
    z, vstatus = volume_check(con, ds, row_count, args.window, args.min_history, args.k)
    con.execute("INSERT OR REPLACE INTO volume_metrics (ds, row_count, max_pickup, checked_as_of, z_score, "
                "volume_status) VALUES (?, ?, ?, ?, ?, ?)", [ds, row_count, max_pickup, as_of, z, vstatus])
    lag_h, fstatus = freshness_check(con, as_of, args.sla_hours)
    con.execute("UPDATE volume_metrics SET freshness_lag_h = ?, freshness_status = ? WHERE ds = ?",
                [lag_h, fstatus, ds])

    zs = f"{z:+6.2f}" if z is not None else "   n/a"
    lags = f"{lag_h:5.1f}h" if lag_h is not None else "   n/a"
    print(f"{ds}  rows={row_count:>7,}  z={zs}  {vstatus:<14}  lag={lags} {fstatus}")

    alerts = []
    base = {"dataset": "yellow_trips", "partition": str(ds), "as_of": as_of.isoformat()}
    if vstatus == "MISSING":
        alerts.append({**base, "severity": "critical", "check": "volume",
                       "message": "partition missing (0 rows)", "value": 0})
    elif vstatus == "VOLUME_ANOMALY":
        alerts.append({**base, "severity": "warning", "check": "volume",
                       "message": f"row_count {row_count:,} is {z:+.1f} std from the {args.window}-day mean "
                                  f"(threshold +/-{args.k})", "value": row_count, "z_score": round(z, 2)})
    if fstatus == "STALE":
        alerts.append({**base, "severity": "critical", "check": "freshness",
                       "message": f"newest data is {lag_h:.1f} h old (SLA {args.sla_hours:g} h)"
                       if lag_h is not None else "no data at all", "value": lag_h})
    for a in alerts:
        print(f"    -> ALERT {a['severity']}: {a['check']} - {a['message']}")
        if not args.no_alerts:
            send_alert(args.webhook, a)
    return alerts


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--ds", help="check one day YYYY-MM-DD")
    ap.add_argument("--from", dest="start", help="first day of a replay")
    ap.add_argument("--to", dest="end", help="last day of a replay")
    ap.add_argument("--report", action="store_true", help="print the metrics table and exit")
    ap.add_argument("--window", type=int, default=7, help="days of history for mean/std (default 7)")
    ap.add_argument("--min-history", type=int, default=7, help="days needed before z-scores are trusted")
    ap.add_argument("--k", type=float, default=3.0, help="anomaly threshold in standard deviations (default 3)")
    ap.add_argument("--sla-hours", type=float, default=24.0, help="freshness SLA in hours (default 24)")
    ap.add_argument("--webhook", default=DEFAULT_WEBHOOK, help=f"alert URL (default {DEFAULT_WEBHOOK})")
    ap.add_argument("--no-alerts", action="store_true", help="print alerts but do not POST them")
    ap.add_argument("--db", default=str(DEFAULT_DB), help="DuckDB metrics store")
    ap.add_argument("--reset", action="store_true", help="drop the metrics table first")
    args = ap.parse_args()

    Path(args.db).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(args.db)
    if args.reset:
        con.execute("DROP TABLE IF EXISTS volume_metrics")
    con.execute(DDL)

    if args.report:
        print(con.sql("SELECT ds, row_count, max_pickup, round(z_score, 2) AS z, volume_status, "
                      "round(freshness_lag_h, 1) AS lag_h, freshness_status FROM volume_metrics ORDER BY ds"))
        return 0
    if args.ds:
        days = [date.fromisoformat(args.ds)]
    elif args.start and args.end:
        d, end, days = date.fromisoformat(args.start), date.fromisoformat(args.end), []
        while d <= end:
            days.append(d)
            d += timedelta(days=1)
    else:
        ap.error("give --ds DAY, or --from DAY --to DAY, or --report")

    n_alerts = sum(len(check_day(con, d, args)) for d in days)
    print(f"\n{len(days)} day(s) checked, {n_alerts} alert(s)")
    return 1 if n_alerts else 0


if __name__ == "__main__":
    sys.exit(main())
