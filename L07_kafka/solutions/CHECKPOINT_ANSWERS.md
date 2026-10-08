# L07 - Checkpoint answers

1. **KRaft** (Kafka Raft metadata mode). A quorum of controllers stores cluster metadata in the internal
   `__cluster_metadata` log and elects an active controller with Raft. Kafka 4.0 removed ZooKeeper completely.
   The active controller is `LeaderId` in `kafka-metadata-quorum.sh describe --status`.

2. **Yes.** The partition is `murmur2(key) % 6`; 10 keys hashed into 6 buckets rarely spread perfectly - in our test
   run partition 1 received no records at all. With few keys (low cardinality) partitions become skewed; that is why
   you pick keys with many distinct values and roughly uniform volume.

3. Only 6 consumers get one partition each; the other **2 sit idle** (they are hot standbys that take over on the next
   rebalance). Parallelism inside a group is capped by the number of partitions.

4. They do **not** interfere. Each group has its own committed offsets in `__consumer_offsets`; every group receives every
   message (pub/sub between groups, queue semantics within a group).

5. Writes need `ISR >= min.insync.replicas = 2`, so the topic tolerates **1** broker failure for `acks=all` writes.
   Reads keep working as long as a leader can be elected from the ISR - with 2 brokers down the last replica can still
   serve reads *if* the controller quorum is healthy. In our combined-mode lab, losing 2 of 3 nodes also loses the KRaft
   majority, so no metadata changes (leader elections) are possible - another reason production uses dedicated controllers.

6. librdkafka's default partitioner is `consistent_random` (CRC32), Java uses **murmur2**. Different hash -> the same key
   lands on different partitions depending on the client language, which breaks per-key ordering across producers.

7. Idempotence gives each producer a PID + per-partition sequence numbers, so broker-side **duplicates caused by retries**
   are dropped and ordering is kept even with retries in flight. It does **not** protect against duplicates created by
   your application (e.g. re-running a job, a crash before commit on the consumer side) and it does not give atomic
   writes to several partitions - that needs transactions.

8. `auto.offset.reset` is a **fallback** used only when a group has no committed offset (or it is out of range).
   `--reset-offsets --to-earliest` **overwrites** the committed offsets of an existing (inactive) group, forcing a replay.
