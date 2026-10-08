# L08 - Change Data Capture with Debezium: Postgres -> Kafka

| | |
|---|---|
| Module | 3 (Ingestion) / 6 (Kafka Connect) |
| Time | 2 - 2.5 hours |
| Stack | `postgres:18.6` (Olist OLTP, `wal_level=logical`), `apache/kafka:4.3.1` (1 node, KRaft), `quay.io/debezium/connect:3.6.3.Final`; optional `kafbat/kafka-ui:v1.5.0`, `apicurio/apicurio-registry:3.3.3` |
| RAM | ~1.3 GB (+0.3 GB UI, +0.4 GB registry) |

## Learning objectives

1. Explain *why* log-based CDC beats "SELECT ... WHERE updated_at > ?" polling (L04): deletes, every intermediate change, low load on the source, ordering.
2. Configure Postgres for logical decoding and understand **replication slots** and **publications**.
3. Deploy a Debezium source connector through the **Kafka Connect REST API**.
4. Read Debezium change events: `before` / `after` / `source` / `op` (`r`, `c`, `u`, `d`), **tombstones**, and the effect of `REPLICA IDENTITY`.
5. Show that CDC is resumable: stop the connector, change data, restart -> nothing is lost.

## Prerequisites

- L07 (Kafka basics, consumer groups). L04 helps (same Olist database).
- The shared Olist init scripts exist: `labs/datasets/olist/init/01_schema.sql`, `02_seed.sql` (5,000 synthetic orders; see `labs/datasets/olist/README.md` to load the real Kaggle data).
- Python venv with `pip install -r requirements.txt`.

## Architecture

```
 +---------------------+   WAL (logical decoding,     +-----------------------------+      +-------------------+
 | Postgres 18 (olist) |   pgoutput plugin)           | Kafka Connect (Debezium)    |      | Kafka (KRaft)     |
 |  orders, customers, |=============================>|  PostgresConnector task     |----->| olist.public.<tbl>|
 |  ... wal_level=     |   replication slot           |  REST API :8083             |      | one topic / table |
 |  logical            |   "debezium_olist"           |  offsets in _connect_offsets|      +---------+---------+
 +---------------------+                              +-----------------------------+                |
        ^ psql: INSERT/UPDATE/DELETE                                                     cdc_consumer.py (laptop)
```

Topic naming: `<topic.prefix>.<schema>.<table>` -> `olist.public.orders`.

## Files

| File | Purpose |
|---|---|
| `docker-compose.yml` | Postgres + Kafka + Connect (+ profiles `ui`, `avro`) |
| `connectors/olist-postgres.json` | Connector config (JSON, schemas disabled) |
| `connectors/olist-postgres-avro.json` | Stretch: Avro + Apicurio schema registry |
| `sql/changes.sql` | The DML used in step 4 |
| `cdc_consumer.py` | **Starter** Python consumer (TODOs) |
| `solutions/` | Complete consumer + checkpoint answers |

---

## Step 1 - Start the stack

```bash
cd labs/L08_cdc_debezium
docker compose up -d
docker compose ps          # postgres + kafka "healthy", connect "starting" -> "healthy" (~40 s)
```

Verify Postgres is ready for logical decoding and seeded:

```bash
docker compose exec postgres psql -U olist -c "SHOW wal_level" -c "SELECT count(*) FROM orders"
```

Expected: `logical` and `5000`.

## Step 2 - Explore Kafka Connect

```bash
curl -s localhost:8083/ | python3 -m json.tool             # Connect worker version (Kafka 4.x)
curl -s localhost:8083/connector-plugins | python3 -m json.tool | grep class
curl -s localhost:8083/connectors                           # [] - nothing deployed yet
```

You should see `io.debezium.connector.postgresql.PostgresConnector` (plus MySQL, MongoDB, SQL Server, Oracle, ... and the JDBC sink).

## Step 3 - Register the connector and watch the snapshot

Read `connectors/olist-postgres.json` first. Key settings:

| Setting | Meaning |
|---|---|
| `plugin.name=pgoutput` | Postgres' built-in logical decoding output plugin (no extension needed) |
| `slot.name` | Replication slot: Postgres keeps WAL until Debezium confirms it (resumability!) |
| `publication.autocreate.mode=filtered` | Debezium creates a publication for only the included tables |
| `snapshot.mode=initial` | First read the existing rows (op `r`), then stream changes |
| `topic.prefix=olist` | Topic names start with `olist.` |
| `*.converter.schemas.enable=false` | Plain JSON without the embedded schema (easier to read) |
| `decimal.handling.mode=string` | `numeric` columns as `"12.50"` instead of base64-encoded bytes |

```bash
curl -s -X POST -H "Content-Type: application/json" \
     --data @connectors/olist-postgres.json localhost:8083/connectors | python3 -m json.tool
curl -s localhost:8083/connectors/olist-postgres/status | python3 -m json.tool
```

Expected: `"state": "RUNNING"` for the connector and task 0.

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --list
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:19092 --topic olist.public.orders
```

Expected: six `olist.public.*` topics; `olist.public.orders:0:5000` (one snapshot event per row).

Look at one snapshot event:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 \
  --topic olist.public.orders --from-beginning --max-messages 1 --formatter-property print.key=true
```

Expected (shortened):

```
{"order_id":"183e77f7..."}   {"before":null,"after":{"order_id":"183e77f7...","order_status":"delivered",
 "order_purchase_timestamp":1493929206000000, ...},"source":{"connector":"postgresql","snapshot":"first_in_data_collection",
 "table":"orders","txId":829,"lsn":40271424,...},"op":"r","ts_ms":...}
```

Notice: key = primary key; `op":"r"` (read = snapshot); timestamps are **epoch microseconds** (`io.debezium.time.MicroTimestamp`).

## Step 4 - Capture INSERT / UPDATE / DELETE

1. Complete TODO 1-3 in `cdc_consumer.py`, then start it (terminal A):
   ```bash
   python cdc_consumer.py --quiet        # --quiet hides the 5,000 snapshot events
   ```
2. Terminal B - open psql and run the statements of `sql/changes.sql` **one by one**, watching terminal A after each:
   ```bash
   docker compose exec postgres psql -U olist
   ```

Expected output in terminal A:

```
[CREATE        ] lsn=44054472 txId=837 key={"order_id":"4332..."}
    after : {order_id=4332..., order_status=created, ...}
[UPDATE        ] lsn=44072536 txId=838 key={"order_id":"4332..."}
    before: null                                     <-- default REPLICA IDENTITY: no old values!
    after : {..., order_status=approved, ...}
[UPDATE        ] ...                                   (after ALTER TABLE ... REPLICA IDENTITY FULL)
    before: {..., order_status=approved, ...}        <-- now the full old row is in the WAL
    after : {..., order_status=invoiced, ...}
[DELETE        ] ...
    before: {..., order_status=invoiced, ...}
  TOMBSTONE key={"order_id":"4332..."}                <-- value = null, for log compaction
```

3. Stop the consumer (`Ctrl+C`): the summary shows the local mirror has as many rows as `SELECT count(*) FROM orders`.
   You just built a replica of a table from a stream of changes.

## Step 5 - Resumability: stop the connector, keep changing data

```bash
docker compose stop connect
docker compose exec postgres psql -U olist -c \
  "UPDATE orders SET order_status='canceled' WHERE order_id IN (SELECT order_id FROM orders WHERE order_status='shipped' LIMIT 3)"
docker compose exec postgres psql -U olist -c \
  "SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal FROM pg_replication_slots"
```

Expected: slot `debezium_olist`, `active = f`, some kB of retained WAL - Postgres **keeps** the WAL for the absent consumer.

```bash
docker compose start connect
# wait ~30 s, then
python cdc_consumer.py --quiet          # new group -> reads everything, prints the 3 updates too
```

The 3 updates made while Connect was down are delivered. The connector stored its last LSN in the Kafka topic `_connect_offsets`
and restarted from there. **Danger**: a slot that is never consumed makes WAL grow forever and can fill the database disk
(we cap it with `max_slot_wal_keep_size=1GB` in `docker-compose.yml`). Always drop slots of retired connectors.

## Step 6 - Connector management via REST

```bash
curl -s localhost:8083/connectors?expand=status | python3 -m json.tool
curl -s -X PUT localhost:8083/connectors/olist-postgres/pause
curl -s -X PUT localhost:8083/connectors/olist-postgres/resume
curl -s localhost:8083/connectors/olist-postgres/config | python3 -m json.tool
curl -s localhost:8083/connectors/olist-postgres/offsets | python3 -m json.tool   # the stored LSN
```

---

## Checkpoint questions

1. Name three things log-based CDC captures that `updated_at`-based incremental import (L04 Sqoop/Spark JDBC) cannot.
2. What are `op` values `r`, `c`, `u`, `d`? Which one appears only during the snapshot?
3. Why was `before` null in the first UPDATE but filled in the second? What is the cost of `REPLICA IDENTITY FULL`?
4. What is a tombstone and why does Debezium emit one after a delete?
5. Where are the connector's offsets stored, and where does Postgres keep track of what Debezium has read?
6. The connector is deleted but the replication slot is not dropped. What happens to the database over the next weeks?
7. Why is every table a separate topic, and why is the message key the primary key?
8. Deleting an order cascades to its `order_items` and `order_payments` (FK `ON DELETE CASCADE`). How many change events does one `DELETE FROM orders ...` produce, and in which topics?

Answers: `solutions/CHECKPOINT_ANSWERS.md`.

## Stretch challenges

1. **Avro + schema registry.** Start the registry and register a second connector that writes Avro:
   ```bash
   docker compose --profile avro up -d registry
   curl -s -X POST -H "Content-Type: application/json" --data @connectors/olist-postgres-avro.json localhost:8083/connectors
   curl -s localhost:8081/apis/registry/v3/search/artifacts | python3 -m json.tool      # key + value schemas registered
   docker compose exec postgres psql -U olist -c "ALTER TABLE customers ADD COLUMN loyalty_tier text" \
       -c "UPDATE customers SET loyalty_tier='gold' WHERE customer_state='SP' AND customer_city='campinas'"
   curl -s localhost:8081/apis/registry/v3/groups/default/artifacts/olistavro.public.customers-value/versions | python3 -m json.tool
   ```
   Expected: version `1` and `2` of the value schema - schema evolution, captured automatically. Compare the message size of
   `olistavro.public.customers` with `olist.public.customers` (`kafka-log-dirs.sh` or the UI). The registry used is
   Apicurio (bundled converters in the Debezium image, `ENABLE_APICURIO_CONVERTERS=true`); Confluent Schema Registry works the same way
   but its converter jars must be added to the image.
2. **Least privilege.** Create a dedicated `debezium` role with `REPLICATION` and `SELECT` only, create the publication yourself
   (`CREATE PUBLICATION dbz_olist FOR TABLE ...`), set `publication.autocreate.mode=disabled`, and redeploy.
3. **Single Message Transform.** Add `"transforms": "unwrap"`, `"transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState"`,
   `"transforms.unwrap.delete.tombstone.handling.mode": "rewrite"` to get flat rows (only the `after` image + `__deleted`). When would you want this?
4. **Sink it**: use the Debezium JDBC sink connector (already in the image) to replicate `olist.public.orders` into a second Postgres database.
5. **UI**: `docker compose --profile ui up -d` and browse topics, messages and the connector in Kafbat UI (<http://localhost:8080>).

## Cleanup

```bash
docker compose --profile avro --profile ui down -v
```

`-v` also deletes the Postgres volume, so the next `up` re-seeds the database from `labs/datasets/olist/init`.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `curl: (7) Failed to connect to localhost port 8083` | Connect needs ~30-60 s to start; `docker compose logs -f connect` until you see `Finished starting connectors and tasks`. |
| Connector `FAILED`, trace mentions `wal_level` | Postgres was started without `-c wal_level=logical` (check `SHOW wal_level`); recreate: `docker compose down -v && docker compose up -d`. |
| `replication slot "debezium_olist" already exists` / is active | A previous connector still uses it. Delete the old connector (`curl -X DELETE localhost:8083/connectors/<name>`) or drop the slot: `SELECT pg_drop_replication_slot('debezium_olist');` |
| Tables empty / `relation "orders" does not exist` | The init scripts only run on an **empty** volume: `docker compose down -v` then `up -d`. Check the mount `../datasets/olist/init`. |
| Port 5432 or 9092 in use | Another lab (L04, L07, L09, L12) is running: `docker compose down` there first. |
| `numeric` values look like `"AfQ="` | You removed `decimal.handling.mode=string` - that is base64 of the unscaled bytes. |
| Consumer prints nothing | It's in `--quiet` mode and there were no changes yet; or you reused a `--group` that already committed offsets. |
