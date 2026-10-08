#!/usr/bin/env python3
"""L13 Part 1 - run the checkpoint for one month and print a readable summary.

    python part1_gx/run_checkpoint.py --month 2024-01            # good month  -> exit code 0
    python part1_gx/run_checkpoint.py --month 2024-02            # broken file -> exit code 1
    python part1_gx/run_checkpoint.py --month 2024-01 --open     # + open Data Docs in the browser

The exit code is what a pipeline (Part 4, Airflow, CI) uses as the quality gate.
"""
import argparse
import sys
from pathlib import Path

import great_expectations as gx
from great_expectations.core.run_identifier import RunIdentifier

LAB = next(p for p in Path(__file__).resolve().parents if (p / "docker-compose.yml").exists())


def fmt_observed(res) -> str:
    r = res.result or {}
    if "unexpected_percent" in r and r.get("unexpected_count") is not None:
        return f"{r['unexpected_count']:,} unexpected ({r['unexpected_percent']:.3f} %)"
    if "observed_value" in r:
        v = r["observed_value"]
        if isinstance(v, list):  # column set: show only the mismatch
            d = r.get("details", {}).get("mismatched", {})
            return f"mismatched {d}" if d else f"{len(v)} columns"
        if isinstance(v, float):
            return f"observed {v:.4f}"
        return f"observed {v:,}" if isinstance(v, int) else f"observed {v}"
    if res.exception_info and res.exception_info.get("raised_exception"):
        return "ERROR " + str(res.exception_info.get("exception_message"))[:70]
    return ""


def run(month: str, checkpoint_name: str = "yellow_trips_raw_cp", open_docs: bool = False,
        only_failures: bool = False) -> bool:
    year, mm = month.split("-")
    context = gx.get_context(mode="file", project_root_dir=str(LAB))
    try:
        checkpoint = context.checkpoints.get(checkpoint_name)
    except Exception as exc:  # DataContextError
        sys.exit(f"[FAIL] {exc} - run part1_gx/gx_setup.py first (TODO 7 creates the checkpoint)")
    # run_name groups the result in Data Docs ("taxi_2024-01" instead of "__none__")
    result = checkpoint.run(batch_parameters={"year": year, "month": mm},
                            run_id=RunIdentifier(run_name=f"taxi_{month}"))

    for vr in result.run_results.values():
        stats = vr.statistics
        print(f"\nSuite {vr.suite_name} on {month}: "
              f"{stats['successful_expectations']}/{stats['evaluated_expectations']} expectations passed")
        for res in sorted(vr.results, key=lambda x: (x.success, x.expectation_config.type)):
            cfg = res.expectation_config
            col = cfg.kwargs.get("column") or cfg.kwargs.get("column_A") or ""
            mark = "PASS" if res.success else "FAIL"
            if only_failures and res.success:
                continue
            print(f"  {mark}  {cfg.type:55} {col:22} {fmt_observed(res)}")
    print(f"\nCheckpoint {checkpoint_name}: {'SUCCESS' if result.success else 'FAILED'}")
    docs = LAB / "gx" / "uncommitted" / "data_docs" / "local_site" / "index.html"
    print(f"Data Docs : file://{docs}")
    if open_docs:
        context.open_data_docs()
    return bool(result.success)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--month", default="2024-01", help="YYYY-MM of data/landing/yellow_tripdata_YYYY-MM.parquet")
    ap.add_argument("--checkpoint", default="yellow_trips_raw_cp")
    ap.add_argument("--open", action="store_true", help="open Data Docs in the browser afterwards")
    a = ap.parse_args()
    sys.exit(0 if run(a.month, a.checkpoint, a.open) else 1)
