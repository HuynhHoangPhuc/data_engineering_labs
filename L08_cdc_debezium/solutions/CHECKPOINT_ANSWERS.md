# L08 - Checkpoint answers

1. (a) **Hard deletes** - a deleted row simply disappears from a `WHERE updated_at > ?` query; CDC emits a `d` event.
   (b) **Every intermediate state** - if a row changes 3 times between two polls, polling sees only the last version.
   (c) **Changes that don't touch `updated_at`** (a bulk UPDATE, a trigger disabled, a buggy app).
   Also: exact commit order + transaction ids, near-real-time latency, and no heavy range scans on the OLTP database.

2. `r` = read (snapshot of existing rows; only during the snapshot phase), `c` = create (INSERT), `u` = update, `d` = delete.
   (`t` = truncate exists too but is off by default.)

3. With the default `REPLICA IDENTITY DEFAULT`, Postgres writes only the **primary key** of the old row to the WAL (and for an
   UPDATE that doesn't change the key, nothing at all), so Debezium has no old values -> `before: null`. `REPLICA IDENTITY FULL`
   logs the entire old row for every UPDATE/DELETE: more WAL volume, more I/O and network, and slower on wide tables - enable it
   only where consumers truly need the before image.

4. A tombstone is a record with the same key and a **null value**. With `cleanup.policy=compact`, Kafka eventually removes all
   records for a key whose latest value is a tombstone, so deleted rows really disappear from a compacted "table topic".
   Consumers must handle `value == None`.

5. Kafka Connect stores source offsets (the last LSN/txId processed) in the Kafka topic `_connect_offsets`
   (`OFFSET_STORAGE_TOPIC`). Postgres tracks the consumer's position in the **replication slot** (`confirmed_flush_lsn`,
   `restart_lsn` in `pg_replication_slots`) and retains WAL after that point.

6. The slot keeps `restart_lsn` pinned, so Postgres can never recycle WAL: `pg_wal` grows until the disk is full and the database
   stops accepting writes (or, with `max_slot_wal_keep_size`, the slot is invalidated and CDC must re-snapshot).
   Always `SELECT pg_drop_replication_slot('...')` when you retire a connector, and monitor retained WAL.

7. One topic per table keeps a single schema per topic, lets consumers subscribe to only what they need and lets each table
   scale/retain independently. Keying by primary key sends all changes of one row to **one partition**, so they are consumed
   **in order** (per-key ordering from L07) and enables log compaction per row.

8. Postgres logical decoding emits one change per affected row, including cascaded ones: 1 `d` event in `olist.public.orders`,
   plus one `d` per order line in `olist.public.order_items` and one per payment in `olist.public.order_payments`
   (each followed by a tombstone). All share the same `txId`.
