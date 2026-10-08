# L08 - Instructor notes

## Timing (≈ 2 h 15 min)

| Block | Minutes |
|---|---|
| Recap polling vs log-based CDC (whiteboard: WAL, slot, publication) | 15 |
| Steps 1-3: stack, REST API, snapshot | 30 |
| Step 4: DML + consumer TODOs | 40 |
| Step 5: resumability + WAL retention danger | 20 |
| Step 6 + checkpoint discussion | 25 |

Pre-pull before class (the Debezium Connect image is ~2.2 GB on disk!):
`docker compose pull && docker compose --profile avro pull`.

## Tested

Apple Silicon, Docker Desktop 3.8 GB VM: postgres 126 MB, kafka 460 MB, connect 520 MB RSS. Snapshot of 5,000 orders
(+ other tables) completes in < 5 s. The Avro stretch (Apicurio 3.3.3 + bundled Apicurio 3.2.5 converters) was tested,
including a schema change creating version 2.

## Common errors

- Registering the connector before Connect is ready -> `curl: (52) Empty reply`. Wait for the healthcheck.
- Posting the JSON twice -> `409 Conflict ... already exists`. Use `PUT /connectors/<name>/config` (body = only the `config` object) to update.
- Students who ran L04 or L12 first still have another Postgres on 5432 -> port conflict.
- Confusion about microsecond timestamps: show `to_timestamp(1493929206000000/1e6)` in psql.
- Forgetting that psql autocommits each statement -> each UPDATE is a separate txId; use the explicit `BEGIN/COMMIT` in `sql/changes.sql` step 5 to show a multi-row transaction.
- Dropping and recreating the connector with the same `slot.name` while the old task is still running -> "slot is active".

## Discussion prompts

- What happens to downstream consumers if someone runs `ALTER TABLE orders DROP COLUMN ...`? (Schema evolution; registry compatibility rules in the stretch.)
- Outbox pattern: why do microservices write an `outbox` table and let Debezium publish it, instead of writing to Kafka directly (dual-write problem)?

## Grading hints

| Item | Points |
|---|---|
| Connector running, screenshot/log of status | 15 |
| Consumer TODOs (tombstone handling, mirror logic, subscribe) | 30 |
| Captured c/u/d events with correct before/after explanation (REPLICA IDENTITY) | 25 |
| Resumability experiment explained (slot + offsets) | 15 |
| Checkpoint answers | 15 |
