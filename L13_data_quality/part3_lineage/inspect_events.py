#!/usr/bin/env python3
"""Summarise OpenLineage events written by the FILE transport (Part 3 fallback, no Marquez needed).

    python part3_lineage/inspect_events.py                          # default output/openlineage/events.jsonl
    python part3_lineage/inspect_events.py output/openlineage/events.jsonl --column revenue

Prints one line per START/COMPLETE event (time, eventType, job, inputs -> outputs; --all adds RUNNING) and, for COMPLETE events with
column-level lineage, which input columns feed a chosen output column. Standard library only.
"""
import argparse
import json
from pathlib import Path

LAB = next(p for p in Path(__file__).resolve().parents if (p / "docker-compose.yml").exists())


def short(ds: dict) -> str:
    return f"{ds.get('namespace', '')}:{ds.get('name', '')}"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("file", nargs="?", default=str(LAB / "output" / "openlineage" / "events.jsonl"))
    ap.add_argument("--column", default="revenue", help="output column to trace (column-level lineage)")
    ap.add_argument("--all", action="store_true", help="also show RUNNING events")
    args = ap.parse_args()

    events = [json.loads(line) for line in Path(args.file).read_text().splitlines() if line.strip()]
    print(f"{len(events)} events in {args.file}\n")
    for e in events:
        if e["eventType"] == "RUNNING" and not args.all:
            continue
        ins = ", ".join(short(d) for d in e.get("inputs", [])) or "-"
        outs = ", ".join(short(d) for d in e.get("outputs", [])) or "-"
        print(f"{e['eventTime'][:19]}  {e['eventType']:<8} {e['job']['name']}")
        if e.get("inputs") or e.get("outputs"):
            print(f"{'':30}{ins}\n{'':28}-> {outs}")

    print(f"\nColumn-level lineage of output column '{args.column}':")
    seen = set()
    for e in events:
        for out in e.get("outputs", []):
            fields = out.get("facets", {}).get("columnLineage", {}).get("fields", {})
            if args.column in fields:
                srcs = sorted({f"{f['namespace']}:{f['name']}.{f['field']}"
                               for f in fields[args.column].get("inputFields", [])})
                key = (out["name"], tuple(srcs))
                if key not in seen:
                    seen.add(key)
                    print(f"  {out['name']}.{args.column}  <=  " + ", ".join(srcs))
    if not seen:
        print("  (none found - is the job finished? COMPLETE events carry the columnLineage facet)")


if __name__ == "__main__":
    main()
