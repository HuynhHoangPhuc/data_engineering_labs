"""L07 - Kafka consumer (SOLUTION).

Joins a consumer group, prints every rebalance (assign / revoke) and checks that
the per-key sequence numbers produced by producer.py always increase.

Usage (open 2-3 terminals with the same --group to watch rebalancing):
    python solutions/consumer.py --group orders-app
    python solutions/consumer.py --group orders-app --sleep 1.0   # slow consumer -> lag
"""
import argparse
import json
import signal
import time

from confluent_kafka import Consumer, KafkaError, KafkaException

BOOTSTRAP = "localhost:9092,localhost:9093,localhost:9094"
running = True


def stop(*_):
    global running
    running = False


def on_assign(consumer, partitions):
    print(f"** REBALANCE: assigned {[p.partition for p in partitions]}")


def on_revoke(consumer, partitions):
    print(f"** REBALANCE: revoked  {[p.partition for p in partitions]}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", default="orders")
    ap.add_argument("--group", default="orders-app")
    ap.add_argument("--sleep", type=float, default=0.0, help="simulate slow processing (s/msg)")
    ap.add_argument("--from-beginning", action="store_true")
    args = ap.parse_args()

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": args.group,
        "client.id": f"{args.group}-consumer",
        # Only used when the group has NO committed offset yet
        "auto.offset.reset": "earliest" if args.from_beginning else "latest",
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 1000,
        # Cooperative rebalancing: only the moved partitions are revoked
        "partition.assignment.strategy": "cooperative-sticky",
        "session.timeout.ms": 10000,
    })
    consumer.subscribe([args.topic], on_assign=on_assign, on_revoke=on_revoke)

    last_seq = {}           # (producer run, key) -> last sequence number seen
    key_partition = {}      # key -> partition it arrived on
    out_of_order = 0
    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            key = msg.key().decode() if msg.key() else "<none>"
            event = json.loads(msg.value())
            seq = event.get("seq")
            run_key = (event.get("run_id"), key)  # seq restarts at 1 for each producer run

            flag = ""
            if run_key in last_seq and seq is not None and seq <= last_seq[run_key]:
                out_of_order += 1
                flag = "  <-- OUT OF ORDER / DUPLICATE?"
            if key in key_partition and key_partition[key] != msg.partition():
                flag += "  <-- key changed partition!"
            if seq is not None:
                last_seq[run_key] = max(seq, last_seq.get(run_key, 0))
            key_partition[key] = msg.partition()

            print(
                f"p{msg.partition()} off={msg.offset():<5} key={key:<12} seq={str(seq):<4} "
                f"amount={event.get('amount')}{flag}"
            )
            if args.sleep:
                time.sleep(args.sleep)
    finally:
        print(f"closing consumer (out-of-order events seen: {out_of_order})")
        consumer.close()  # leaves the group cleanly -> triggers an immediate rebalance


if __name__ == "__main__":
    main()
