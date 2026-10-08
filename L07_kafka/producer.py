"""L07 - Kafka producer (STARTER - fill in the TODOs; compare with solutions/producer.py).

Sends fake "order" events keyed by customer id. Every event carries a per-key
sequence number so the consumer can prove that ordering is preserved *per key*
(not globally).

Usage:
    python producer.py --count 60 --rate 5
    python producer.py --count 0 --rate 2        # run forever (Ctrl+C)
    python producer.py --acks 1 --count 20       # compare acks settings
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
        # TODO 1: set "acks" from the function argument (0, 1 or all)
        # TODO 2: enable idempotence ("enable.idempotence") - only valid with acks=all
        # TODO 3: set "partitioner" to "murmur2_random" so keys map to the same
        #         partitions as the Java client / kafka-console-producer
        "linger.ms": 20,
        "compression.type": "lz4",
        "delivery.timeout.ms": 30000,
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
                # TODO 4: produce the event to args.topic
                #   - key   = customer id (bytes)  -> decides the partition
                #   - value = JSON-encoded event (bytes)
                #   - on_delivery = delivery_report
                raise NotImplementedError("TODO 4: call producer.produce(...)")
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
