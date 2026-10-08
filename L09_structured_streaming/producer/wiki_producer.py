"""L09 - Wikimedia EventStreams (SSE) -> Kafka producer.

Reads the public `recentchange` stream (every edit on every Wikimedia wiki,
~20-50 events/s) and writes each event to Kafka, keyed by wiki (e.g. "enwiki").

Wikimedia's User-Agent policy requires a descriptive UA with contact info:
    https://foundation.wikimedia.org/wiki/Policy:Wikimedia_Foundation_User-Agent_Policy
Set yours with:  export WIKI_CONTACT="your.name@university.edu"

Usage:
    python producer/wiki_producer.py                         # stream until Ctrl+C
    python producer/wiki_producer.py --max-events 2000       # stop after N events
    python producer/wiki_producer.py --record data/my_sample.jsonl --max-events 3000
                                                             # also save raw events (for offline replay)
    python producer/wiki_producer.py --no-kafka --max-events 20   # connectivity test, prints events only
If the endpoint is unreachable (firewall, no internet) use producer/replay_producer.py instead.
"""
import argparse
import json
import os
import signal
import sys
import time

import requests
from confluent_kafka import Producer

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
CONTACT = os.environ.get("WIKI_CONTACT", "set-WIKI_CONTACT-env-var")
USER_AGENT = f"BigDataEngineeringCourse-L09/1.0 (educational lab; contact: {CONTACT}) python-requests"

running = True


def stop(*_):
    global running
    running = False


def sse_events(url, last_event_id=None):
    """Minimal Server-Sent-Events parser: yields (event_id, data_str)."""
    headers = {"User-Agent": USER_AGENT, "Accept": "text/event-stream"}
    if last_event_id:
        headers["Last-Event-ID"] = last_event_id  # resume where we stopped
    with requests.get(url, headers=headers, stream=True, timeout=(10, 60)) as r:
        r.raise_for_status()
        event_id, data = None, []
        for raw in r.iter_lines(decode_unicode=True):
            if not running:
                return
            if raw is None:
                continue
            if raw == "":                      # blank line = end of one event
                if data:
                    yield event_id, "\n".join(data)
                event_id, data = None, []
            elif raw.startswith("data:"):
                data.append(raw[5:].lstrip())
            elif raw.startswith("id:"):
                event_id = raw[3:].strip()
            # "event:" and ":" (comments / keep-alives) are ignored


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="wikimedia.recentchange")
    ap.add_argument("--max-events", type=int, default=0, help="0 = unlimited")
    ap.add_argument("--record", help="also append raw JSON events to this .jsonl file")
    ap.add_argument("--no-kafka", action="store_true", help="don't send to Kafka (test / record only)")
    args = ap.parse_args()
    signal.signal(signal.SIGINT, stop)

    if CONTACT.startswith("set-"):
        print("WARNING: please `export WIKI_CONTACT=you@example.edu` (Wikimedia UA policy)", file=sys.stderr)

    producer = None if args.no_kafka else Producer(
        {"bootstrap.servers": args.bootstrap, "linger.ms": 50, "compression.type": "lz4", "acks": "all"})
    rec = open(args.record, "a", encoding="utf-8") if args.record else None
    sent, last_id, t0 = 0, None, time.time()
    while running and (args.max_events == 0 or sent < args.max_events):
        try:
            for event_id, data in sse_events(STREAM_URL, last_id):
                last_id = event_id or last_id
                try:
                    event = json.loads(data)
                except json.JSONDecodeError:
                    continue
                if event.get("meta", {}).get("domain") == "canary":
                    continue  # Wikimedia monitoring events, not real edits
                if producer:
                    producer.produce(args.topic, key=event.get("wiki", "unknown").encode(), value=data.encode())
                    producer.poll(0)
                elif args.max_events and args.max_events <= 50:
                    print(f"{event.get('wiki'):<14} {event.get('type'):<10} {event.get('title')}")
                if rec:
                    rec.write(data + "\n")
                sent += 1
                if sent % 100 == 0:
                    print(f"{sent} events sent ({sent / (time.time() - t0):.1f}/s), last wiki={event.get('wiki')}")
                if args.max_events and sent >= args.max_events:
                    break
        except (requests.RequestException, ConnectionError) as e:
            if not running:
                break
            print(f"stream error: {e!r} -> reconnecting in 3 s (resume from id)", file=sys.stderr)
            time.sleep(3)
    if producer:
        producer.flush(10)
    if rec:
        rec.close()
    print(f"done: {sent} events")


if __name__ == "__main__":
    main()
