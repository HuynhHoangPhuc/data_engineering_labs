"""L09 - OFFLINE FALLBACK: replay a recorded sample of Wikimedia events into Kafka.

Use this when https://stream.wikimedia.org is blocked/unreachable (campus firewall,
no internet) - the Spark job cannot tell the difference.

By default the events are *re-timed*: the `timestamp` (epoch seconds, used as event
time by the Spark job) and `meta.dt` are shifted so the first event happens "now" and
the original spacing is kept (divided by --speed). That way windows and watermarks
behave exactly like with the live stream.

Usage:
    python producer/replay_producer.py                          # data/sample_recentchange.jsonl.gz, 1x speed, loops forever
    python producer/replay_producer.py --speed 5                # 5x faster
    python producer/replay_producer.py --no-loop --no-retime    # replay once, original timestamps
"""
import argparse
import gzip
import json
import signal
import time
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer

running = True


def stop(*_):
    global running
    running = False


def load(path):
    opener = gzip.open if str(path).endswith(".gz") else open
    with opener(path, "rt", encoding="utf-8") as f:
        events = [json.loads(line) for line in f if line.strip()]
    events.sort(key=lambda e: e.get("timestamp", 0))
    return events


def main():
    here = Path(__file__).resolve().parent.parent
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", default=str(here / "data" / "sample_recentchange.jsonl.gz"))
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="wikimedia.recentchange")
    ap.add_argument("--speed", type=float, default=1.0, help="replay speed multiplier")
    ap.add_argument("--no-retime", action="store_true", help="keep original timestamps")
    ap.add_argument("--no-loop", action="store_true", help="stop at the end of the file")
    args = ap.parse_args()
    signal.signal(signal.SIGINT, stop)

    events = load(args.file)
    print(f"loaded {len(events)} events from {args.file}")
    producer = Producer({"bootstrap.servers": args.bootstrap, "linger.ms": 50, "acks": "all"})
    first_ts = events[0]["timestamp"]
    span = events[-1]["timestamp"] - first_ts + 1
    sent, rnd = 0, 0
    while running:
        wall_start = time.time()
        for e in events:
            if not running:
                break
            offset = (e["timestamp"] - first_ts) / args.speed
            # wait until it's this event's turn
            delay = wall_start + offset - time.time()
            if delay > 0:
                time.sleep(delay)
            if not args.no_retime:
                new_ts = int(wall_start + offset)
                e = dict(e, timestamp=new_ts)
                e["meta"] = dict(e.get("meta", {}),
                                 dt=datetime.fromtimestamp(new_ts, tz=timezone.utc).isoformat().replace("+00:00", "Z"))
            producer.produce(args.topic, key=e.get("wiki", "unknown").encode(), value=json.dumps(e).encode())
            sent += 1
            producer.poll(0)
            if sent % 200 == 0:
                print(f"{sent} events replayed (round {rnd + 1}, event time {datetime.fromtimestamp(e['timestamp']):%H:%M:%S})")
        rnd += 1
        if args.no_loop or not running:
            break
        print(f"end of file (span {span}s) -> looping")
    producer.flush(10)
    print(f"done: {sent} events")


if __name__ == "__main__":
    main()
