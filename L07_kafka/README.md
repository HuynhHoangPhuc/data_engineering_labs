# L07 - Apache Kafka: KRaft cluster, producers, consumer groups, failures

| | |
|---|---|
| Module | 6 - Kafka |
| Time | 2.5 - 3 hours |
| Level | Intermediate |
| Stack | `apache/kafka:4.3.1` x 3 (KRaft combined mode), optional `kafbat/kafka-ui:v1.5.0`, Python `confluent-kafka` |
| RAM | ~1.8 GB for Docker (+0.4 GB with the UI) |

## Learning objectives

By the end of this lab you can:

1. Run a 3-node Kafka 4.x cluster **without ZooKeeper** (KRaft) and read the quorum status.
2. Create a topic with 6 partitions and replication factor 3, and explain *leader*, *replicas*, *ISR*.
3. Produce and consume from the CLI and from Python, with **keys**, and prove that ordering is guaranteed **per key / per partition**, not globally.
4. Run several consumers in one **consumer group** and watch partitions being rebalanced.
5. Explain `acks=all` + `min.insync.replicas=2`, and predict what happens when 1 or 2 brokers die.
6. Measure **consumer lag** and reset offsets with `kafka-consumer-groups.sh`.

## Prerequisites

- L00 done (Docker Desktop running, >= 4 GB RAM assigned to Docker).
- Python 3.10+ on your laptop.
- Slides: Module 6 (topics/partitions, replication, consumer groups, KRaft).

## Architecture

```
            your laptop (Python producer / consumers)
          localhost:9092      localhost:9093      localhost:9094
                |                   |                   |
   +------------v------+ +----------v--------+ +--------v----------+
   | kafka1            | | kafka2            | | kafka3            |
   | broker+controller | | broker+controller | | broker+controller |
   | node.id=1         | | node.id=2         | | node.id=3         |
   +---------+---------+ +---------+---------+ +---------+---------+
             |   INTERNAL :19092 (replication)   |
             +------------ CONTROLLER :29093 (KRaft Raft quorum) --+
                               |
                      kafka-ui :8080 (optional)
```

Each container runs **both** roles (`process.roles=broker,controller`, "combined mode").
That is fine for a laptop; production clusters use dedicated controllers.

## Files

| File | Purpose |
|---|---|
| `docker-compose.yml` | 3 KRaft brokers (+ optional UI under profile `ui`) |
| `producer.py` / `consumer.py` | **Starter code** with TODOs |
| `solutions/` | Complete producer/consumer + checkpoint answers |
| `requirements.txt` | Python dependency (`confluent-kafka`) |

---

## Step 0 - Start the cluster

```bash
cd labs/L07_kafka
docker compose up -d
docker compose ps
```

Expected: three containers `l07-kafka1..3` with status `Up`.

Create a shell helper so the long commands below stay readable (bash/zsh):

```bash
k() { tool=$1; shift; docker compose exec kafka1 /opt/kafka/bin/$tool --bootstrap-server kafka1:19092 "$@"; }
```

> All Kafka CLI tools live in `/opt/kafka/bin` inside the container. We run them from
> `kafka1`, but any broker works.

Check the KRaft quorum:

```bash
docker compose exec kafka1 /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server kafka1:19092 describe --status
```

Expected (leader id may differ):

```
ClusterId:              cQDxtDc8SBys_-Nk_98Zvw
LeaderId:               2
LeaderEpoch:            1
...
CurrentVoters:          [{"id": 1, ...}, {"id": 2, ...}, {"id": 3, ...}]
```

The **LeaderId** is the *active controller* - the node that owns cluster metadata (topics, partition leaders, ISR). There is no ZooKeeper anywhere.

## Step 1 - Topics from the CLI

```bash
k kafka-topics.sh --create --topic orders --partitions 6 --replication-factor 3 \
  --config min.insync.replicas=2
k kafka-topics.sh --create --topic demo --partitions 3 --replication-factor 3
k kafka-topics.sh --list
k kafka-topics.sh --describe --topic orders
```

Expected `--describe` output:

```
Topic: orders  PartitionCount: 6  ReplicationFactor: 3  Configs: min.insync.replicas=2
    Topic: orders  Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3  Elr:   LastKnownElr:
    Topic: orders  Partition: 1  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1  ...
    ...
```

Read one line aloud with your partner: *partition 0 lives on brokers 1,2,3; broker 1 is the leader
(all reads/writes go there); all 3 replicas are in sync.* (`Elr` = Eligible Leader Replicas, a Kafka 4.x
safety feature - ignore it for now.)

## Step 2 - Console producer / consumer with keys

Terminal A - consumer (leave it running):

```bash
docker compose exec kafka1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka1:19092 --topic demo --from-beginning \
  --formatter-property print.key=true --formatter-property print.partition=true \
  --formatter-property print.offset=true
```

Terminal B - producer, type `key:value` lines, e.g. `user1:hello`, `user2:world`, `user1:again`:

```bash
docker compose exec kafka1 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka1:19092 --topic demo \
  --reader-property parse.key=true --reader-property key.separator=:
```

Expected in terminal A:

```
Partition:0  Offset:0  user1  hello
Partition:2  Offset:0  user2  world
Partition:0  Offset:1  user1  again
```

`user1` always lands on the same partition: **partition = hash(key) % numPartitions**. Stop both with `Ctrl+C`.

## Step 3 - Python producer with keys

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

Open `producer.py` and complete **TODO 1-4** (acks, idempotence, partitioner, `produce()`).
Then run it (it produces 60 events keyed by `customer-01..10`, each with a per-key sequence number `seq`):

```bash
python producer.py --count 60 --rate 5
```

Expected:

```
sent #1    key=customer-07 seq=1
  -> delivered key=customer-07  partition=4 offset=0
...
done, 60 produced, 0 still undelivered
```

Stuck? Compare with `solutions/producer.py`.

## Step 4 - Python consumer and per-key ordering

Complete **TODO 1-3** in `consumer.py`, then:

```bash
python consumer.py --group orders-app --from-beginning
```

Expected: lines like `p4 off=0 key=customer-07 seq=1 ...`. For every key, `seq` is strictly increasing and the key
never changes partition. Messages of *different* keys are interleaved - Kafka does **not** guarantee a global order.
When you stop it (`Ctrl+C`) it prints `out-of-order events seen: 0`.

## Step 5 - Consumer group rebalancing

1. Terminal A: `python consumer.py --group orders-app` -> prints `** REBALANCE: assigned [0, 1, 2, 3, 4, 5]`.
2. Terminal B: start a **second** consumer with the same group. Watch both terminals:
   A prints `revoked [0, 1, 2]`, B prints `assigned [0, 1, 2]` (exact numbers vary).
3. Terminal C: start a **third** one -> each consumer owns 2 partitions.
4. Terminal D: `python producer.py --count 0 --rate 5` (runs until Ctrl+C).
5. Stop consumer B with `Ctrl+C` -> its partitions move to A and C within a few seconds.
6. Predict (don't run): what would a 7th consumer in this group do on a 6-partition topic?

We use `partition.assignment.strategy=cooperative-sticky`: only the partitions that move are revoked
(incremental rebalance) instead of stopping the whole group.

Inspect the group from the CLI:

```bash
k kafka-consumer-groups.sh --describe --group orders-app
k kafka-consumer-groups.sh --describe --group orders-app --members
```

## Step 6 - Durability: `acks=all`, `min.insync.replicas`, killing brokers

Keep the producer from step 5 running (`--count 0`) and one consumer running.

1. **Kill one broker**:
   ```bash
   docker compose stop kafka2
   k kafka-topics.sh --describe --topic orders
   k kafka-topics.sh --describe --under-replicated-partitions --topic orders
   ```
   Expected: partitions whose leader was 2 got a new leader (1 or 3) and every ISR shrank to 2 brokers,
   e.g. `Leader: 3  Replicas: 2,3,1  Isr: 3,1`. **The producer keeps working** because ISR (2) >= `min.insync.replicas` (2).
   The consumer may print a short pause but keeps consuming.

2. **Kill a second broker**:
   ```bash
   docker compose stop kafka3
   ```
   After ~30 s the producer prints `!! delivery FAILED ... Message timed out`.
   Two things broke at once: (a) ISR = 1 < `min.insync.replicas` = 2, so the leader rejects `acks=all`
   writes with `NotEnoughReplicasException` (see `docker compose logs kafka1 | grep NotEnoughReplicas`),
   and (b) in combined mode 1 of 3 controllers has no Raft majority, so metadata cannot change either.
   The client retries until `delivery.timeout.ms` (30 s) and then gives up.

3. **Bring them back**:
   ```bash
   docker compose start kafka2 kafka3
   k kafka-topics.sh --describe --topic orders
   ```
   After ~10-20 s the ISR is `1,2,3` again, but most leaders are now on broker 1 (unbalanced).
   Trigger a preferred-leader election (Kafka also does this automatically every 5 minutes):
   ```bash
   k kafka-leader-election.sh --election-type preferred --all-topic-partitions
   ```

4. **Isolate the `min.insync.replicas` effect** (quorum stays healthy):
   ```bash
   k kafka-configs.sh --alter --entity-type topics --entity-name orders --add-config min.insync.replicas=3
   docker compose stop kafka2
   python producer.py --count 3              # acks=all -> FAILED after 30 s
   python producer.py --count 3 --acks 1     # acks=1   -> delivered!
   k kafka-configs.sh --alter --entity-type topics --entity-name orders --add-config min.insync.replicas=2
   docker compose start kafka2
   ```
   `acks=1` only waits for the leader - fast, but you can lose data if that leader dies before followers copy it.

## Step 7 - Consumer lag and offset management

```bash
# Slow consumer: 0.5 s per message
python consumer.py --group slow-app --sleep 0.5
# Another terminal: burst of 100 messages
python producer.py --count 100 --rate 50
# Third terminal: watch the LAG column grow, then shrink
k kafka-consumer-groups.sh --describe --group slow-app
```

Expected:

```
GROUP     TOPIC   PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID ...
slow-app  orders  0          180             228             48   slow-app-consumer-...
```

`LAG = LOG-END-OFFSET - CURRENT-OFFSET` = messages written but not yet processed by this group.

Replay history: stop the consumer (offsets can only be reset for an **inactive** group), then

```bash
k kafka-consumer-groups.sh --group slow-app --topic orders --reset-offsets --to-earliest --dry-run
k kafka-consumer-groups.sh --group slow-app --topic orders --reset-offsets --to-earliest --execute
python consumer.py --group slow-app       # re-reads everything
```

## Step 8 (optional) - Web UI

```bash
docker compose --profile ui up -d
open http://localhost:8080
```

Browse brokers, topics, partitions, messages and consumer groups (Kafbat UI, the maintained fork of "UI for Apache Kafka").

---

## Checkpoint questions

1. What replaced ZooKeeper in Kafka 4.x, and which node is currently the active controller in your cluster?
2. With 6 partitions and 10 customer keys, is it possible that some partition receives *no* data? Check with `kafka-consumer-groups.sh --describe` (LOG-END-OFFSET). Why?
3. You start 8 consumers in the same group on the 6-partition `orders` topic. What happens?
4. Two different groups (`orders-app`, `slow-app`) read the same topic. Do they interfere? Why/why not?
5. With RF=3, `min.insync.replicas=2`, `acks=all`: how many broker failures can the topic tolerate **for writes**? For reads?
6. Why does the Python client need `partitioner=murmur2_random` to agree with the Java console producer?
7. What does `enable.idempotence=true` protect you from? What does it *not* protect you from?
8. What is the difference between `auto.offset.reset=earliest` and `--reset-offsets --to-earliest`?

Answers: `solutions/CHECKPOINT_ANSWERS.md` (try first!).

## Stretch challenges

1. **New rebalance protocol (KIP-848).** Kafka 4.x ships the server-side "consumer" group protocol. Add
   `"group.protocol": "consumer"` to the consumer config (and remove `partition.assignment.strategy`, which is not allowed
   with it). Repeat step 5 - what changes in the logs and in `kafka-consumer-groups.sh --describe --members`?
2. **Add partitions** to `orders` (`kafka-topics.sh --alter --topic orders --partitions 8`). Produce again. Do keys still
   land on the same partition as before? What does that mean for ordering?
3. **Exactly-once-ish pipeline**: write a consumer that reads `orders`, filters `amount > 100` and produces to
   `big-orders` using a transactional producer (`transactional.id`, `send_offsets_to_transaction`).
4. **Compaction**: create a topic with `cleanup.policy=compact`, produce several values per key, and observe that only the latest
   value per key survives (set `segment.ms=10000` and `min.cleanable.dirty.ratio=0.01` to see it quickly).

## Cleanup

```bash
docker compose --profile ui down -v
deactivate   # leave the Python venv
```

## Troubleshooting

| Symptom | Fix |
|---|---|
| `Failed to resolve 'kafka2:19092'` from Python | Your laptop must use `localhost:9092,9093,9094` (EXTERNAL listener). `kafkaN:19092` only works inside Docker. |
| Port 9092/9093/9094/8080 already in use | Another Kafka/lab is running: `docker ps`, stop it, or change the host port in `docker-compose.yml` **and** `KAFKA_ADVERTISED_LISTENERS`. |
| Broker exits with `OutOfMemoryError` / exit code 137 | Docker Desktop RAM too low; give Docker >= 4 GB, close other stacks. |
| `ValueError: ... group.id must be set` | TODO 1 in `consumer.py` not done. |
| `NotImplementedError: TODO 4` | Finish `producer.produce(...)` in `producer.py`. |
| `pip install confluent-kafka` builds from source and fails | Use Python 3.10-3.14 on arm64 (wheels exist); upgrade pip: `pip install -U pip`. |
| Consumer prints nothing | New group + `auto.offset.reset=latest`: start the producer *after* the consumer, or use `--from-beginning`. |
| `--reset-offsets` says group is active | Stop all consumers in that group first (`Ctrl+C`), wait ~10 s. |
| Cluster ID error after editing compose | Old volumes: `docker compose down -v` and start again. |
