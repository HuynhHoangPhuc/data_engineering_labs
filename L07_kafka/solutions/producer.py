"""L07 - Kafka producer (SOLUTION).

Sends fake "order" events keyed by customer id. Every event carries a per-key
sequence number so the consumer can prove that ordering is preserved *per key*
(not globally).

Usage:
    python solutions/producer.py --count 60 --rate 5
    python solutions/producer.py --count 0 --rate 2        # run forever (Ctrl+C)
    python solutions/producer.py --acks 1 --count 20       # compare acks settings
"""
import argparse
import json
import random
import signal
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import KafkaException, Producer

BOOTSTRAP = "localhost:9092,localhost:9093,localhost:9094"
CUSTOMERS = [f"customer-{i:02d}" for i in range(1, 11)]  # 10 keys
PRODUCTS = ["book", "laptop", "phone", "coffee", "shoes", "bike"]

running = True


def stop(*_):
    global running
    running = False


def delivery_report(err, msg):
    """Called once per message, from producer.poll()/flush(), after the broker acks."""
    if err is not None:
        print(f"  !! delivery FAILED key={msg.key().decode()} err={err}")
        return
    print(
        f"  -> delivered key={msg.key().decode():<12} partition={msg.partition()} "
        f"offset={msg.offset()}"
    )


def build_producer(acks: str) -> Producer:
    conf = {
        "bootstrap.servers": BOOTSTRAP,
        "client.id": "l07-producer",
        # Durability: wait until all in-sync replicas have the record.
        "acks": acks,
        # Idempotence = no duplicates on retry + ordering preserved per partition.
        # It requires acks=all, so we only enable it in that case.
        "enable.idempotence": acks == "all",
        # Use the same hash as the Java client (murmur2) so the key->partition
        # mapping matches kafka-console-producer and Kafka Streams.
        "partitioner": "murmur2_random",
        "linger.ms": 20,               # small batching window
        "compression.type": "lz4",
        "delivery.timeout.ms": 30000,  # give up after 30 s (shows errors quickly in the lab)
    }
    return Producer(conf)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", default="orders")
    ap.add_argument("--count", type=int, default=60, help="0 = run until Ctrl+C")
    ap.add_argument("--rate", type=float, default=5.0, help="messages per second")
    ap.add_argument("--acks", default="all", choices=["0", "1", "all"])
    args = ap.parse_args()

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    producer = build_producer(args.acks)
    seq = {c: 0 for c in CUSTOMERS}
    run_id = uuid.uuid4().hex[:6]  # sequence numbers restart for every producer run
    sent = 0
    while running and (args.count == 0 or sent < args.count):
        customer = random.choice(CUSTOMERS)
        seq[customer] += 1
        event = {
            "run_id": run_id,
            "customer_id": customer,
            "seq": seq[customer],
            "product": random.choice(PRODUCTS),
            "amount": round(random.uniform(5, 500), 2),
            "ts": datetime.now(timezone.utc).isoformat(),
        }
        while True:
            try:
                producer.produce(
                    args.topic,
                    key=customer.encode(),
                    value=json.dumps(event).encode(),
                    on_delivery=delivery_report,
                )
                break
            except BufferError:
                # local queue is full (e.g. all brokers down) -> wait and retry
                producer.poll(0.5)
        sent += 1
        print(f"sent #{sent:<4} key={customer} seq={seq[customer]}")
        producer.poll(0)  # serve delivery callbacks
        time.sleep(1.0 / args.rate)

    print("flushing...")
    remaining = producer.flush(35)
    print(f"done, {sent} produced, {remaining} still undelivered")


if __name__ == "__main__":
    try:
        main()
    except KafkaException as e:
        print("Kafka error:", e)
