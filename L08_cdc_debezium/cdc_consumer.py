"""L08 - Read Debezium change events and keep a live mirror of a table (STARTER - complete TODO 1-3; solution in solutions/cdc_consumer.py).

Every Debezium event value (JSON, schemas disabled) looks like:
    {"before": {...} | null, "after": {...} | null,
     "source": {"lsn": ..., "txId": ..., "table": "orders", "snapshot": "true"|"false"|"last", ...},
     "op": "r" | "c" | "u" | "d", "ts_ms": ...}
A DELETE is followed by a *tombstone*: same key, value = null (lets log compaction drop the key).

Usage:
    python cdc_consumer.py                       # orders table, from the beginning
    python cdc_consumer.py --table order_items
    python cdc_consumer.py --quiet               # only print non-snapshot events + summary
"""
import argparse
import json
import signal
from collections import Counter
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaException

OP_NAMES = {"r": "READ(snapshot)", "c": "CREATE", "u": "UPDATE", "d": "DELETE"}
running = True


def stop(*_):
    global running
    running = False


def micros_to_iso(v):
    """Debezium encodes `timestamp` columns as epoch microseconds (io.debezium.time.MicroTimestamp)."""
    if isinstance(v, int) and v > 10**14:
        return datetime.fromtimestamp(v / 1_000_000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return v


def pretty(row, cols):
    if row is None:
        return "null"
    return "{" + ", ".join(f"{c}={micros_to_iso(row.get(c))}" for c in cols if c in row) + "}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", default="orders")
    ap.add_argument("--group", default=None, help="default: a new group every run (reads from the beginning)")
    ap.add_argument("--quiet", action="store_true", help="don't print snapshot (op=r) events")
    args = ap.parse_args()
    topic = f"olist.public.{args.table}"
    show_cols = {
        "orders": ["order_id", "order_status", "order_purchase_timestamp", "updated_at"],
        "customers": ["customer_id", "customer_city", "customer_state", "updated_at"],
    }.get(args.table, None)

    signal.signal(signal.SIGINT, stop)
    group = args.group or f"l08-mirror-{datetime.now():%H%M%S}"
    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    # TODO 3: subscribe to `topic`

    print(f"consuming {topic} as group {group} (Ctrl+C to stop)")

    mirror = {}          # primary key (as JSON string) -> latest row image
    ops = Counter()
    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            key = msg.key().decode() if msg.key() else None
            # TODO 1: a DELETE is followed by a *tombstone* (msg.value() is None).
            #         Count it in ops["tombstone"], print it, and `continue`
            #         (json.loads(None) would crash).

            event = json.loads(msg.value())
            op = event["op"]
            before, after = event.get("before"), event.get("after")
            ops[op] += 1

            # Apply the change to our local replica
            # TODO 2: apply the change to `mirror` (dict key -> row):
            #   op r/c/u -> store the `after` image;  op d -> remove the key

            if args.quiet and op == "r":
                continue
            cols = show_cols or list((after or before or {}).keys())[:4]
            src = event["source"]
            print(f"[{OP_NAMES[op]:<14}] lsn={src.get('lsn')} txId={src.get('txId')} key={key}")
            if op in ("u", "d"):
                print(f"    before: {pretty(before, cols)}")
            if op in ("r", "c", "u"):
                print(f"    after : {pretty(after, cols)}")
    finally:
        consumer.close()
        print("\n=== summary ===")
        print("events by op:", dict(ops))
        print(f"rows in local mirror of {args.table}: {len(mirror)}")
        print(f"compare with: docker compose exec postgres psql -U olist -c 'select count(*) from {args.table}'")


if __name__ == "__main__":
    main()
