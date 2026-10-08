# L07 - Instructor notes

## Timing (≈ 2 h 45 min)

| Block | Minutes |
|---|---|
| Step 0-1: start cluster, quorum, topics (live demo, students follow) | 25 |
| Step 2: console producer/consumer | 15 |
| Step 3-4: Python producer/consumer TODOs | 40 |
| Step 5: rebalancing (needs 3-4 terminals - pair students) | 25 |
| Step 6: kill brokers | 30 |
| Step 7: lag + reset offsets | 15 |
| Checkpoint discussion | 15 |

First `docker compose up` pulls ~400 MB (`apache/kafka:4.3.1`). Ask students to pull before class:
`docker compose pull` (and `docker compose --profile ui pull`).

## Tested on

Apple Silicon, Docker Desktop with 3.8 GB VM memory: 3 brokers ≈ 1.35 GB RSS total, UI ≈ 0.3 GB.
Python 3.12 + `confluent-kafka` 2.15.1 (arm64 wheel).

## Common student errors

- Using `kafka1:19092` from the laptop -> DNS failure. Explain INTERNAL vs EXTERNAL listeners (good 5-min whiteboard).
- Forgetting `--bootstrap-server` (very old tutorials still show `--zookeeper`, which no longer exists).
- `--property` on console tools prints a deprecation warning in Kafka 4.x; the README uses `--reader-property` /
  `--formatter-property`.
- Starting the consumer after the producer with a new group and `latest` -> "nothing happens".
- Resetting offsets while a consumer in that group still runs -> error "group is not empty".
- Expecting `NOT_ENOUGH_REPLICAS` in the Python output: librdkafka retries retriable errors until
  `delivery.timeout.ms`, so students see `_MSG_TIMED_OUT`. Point them at the broker log (`grep NotEnoughReplicas`).
- Surprise that stopping 2 of 3 nodes also freezes metadata: combined mode = controllers die with brokers. Great
  discussion point about dedicated controllers.

## Demo tips

- Keep `k kafka-topics.sh --describe --topic orders` in a `watch -n 2` loop on the projector during step 6
  (`brew install watch`).
- In step 5 ask students to predict assignments before starting consumer 2 and 3.
- Partition skew: with only 10 keys, often 1 partition is empty (partition 1 in our test run). Use it to
  motivate key design.

## Grading hints (if submitted)

| Item | Points |
|---|---|
| Producer TODOs correct (acks, idempotence, murmur2, keyed produce with callback) | 25 |
| Consumer TODOs correct (group.id, subscribe with callbacks, ordering check) | 25 |
| Screenshot/log of a rebalance with 3 consumers | 15 |
| `describe` output before/after stopping a broker, with 2-3 sentence explanation | 20 |
| Checkpoint answers | 15 |
