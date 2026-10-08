# L09 - Instructor notes

## Timing (≈ 2 h 45 min)

| Block | Minutes |
|---|---|
| Event time vs processing time, watermarks (whiteboard timeline!) | 20 |
| Steps 1-2: Kafka + producer (live or replay) | 20 |
| Step 3: TODOs in stream_job.py | 45 |
| Step 4-5: append semantics, checkpoint anatomy | 25 |
| Step 6: crash & recovery, exactly-once verification | 25 |
| Checkpoint questions | 15 |

## Before class

- `docker compose pull` (Spark image ~2 GB on disk - shared with L05/L06/L11 if they use the same `spark:4.1.3-python3` tag).
- Warm the Ivy cache once: run the solution job for 1 minute so `spark-sql-kafka` jars are in the `ivy-cache` volume
  (`docker compose down` without `-v` keeps it).
- Test `curl -sI https://stream.wikimedia.org/v2/stream/recentchange` from the classroom network. If blocked, the whole
  class uses `replay_producer.py` - the lab works identically.
- Wikimedia asks clients to identify themselves: make every student set `WIKI_CONTACT`. 30 students x 1 connection is fine.

## Tested (Sep 2026, Apple Silicon, Docker VM 3.8 GB)

- Live stream: ~26-41 events/s. Spark driver RSS ≈ 1.2 GB, Kafka ≈ 0.5 GB.
- First Parquet files after ~3.5 min, as predicted by the watermark arithmetic.
- `docker compose restart spark` + resubmit -> resumed at `Batch: 6`; `check_output.py` showed 0 duplicate (window, wiki) rows.
- `pkill -f stream_job.py` inside `docker compose exec ... bash -c` also kills the exec'ing shell (its command line matches);
  that's why the README uses `docker compose restart spark` as the "crash".

## Common errors

- Using `F.col("timestamp")` from Kafka (ingestion time) instead of the event's field - windows look right live but break on replay.
- Watermark applied *after* `groupBy` -> append mode rejected.
- Same checkpoint for both sinks.
- Students get impatient waiting for Parquet: ask them to predict the first file time from the watermark before they look.
- On restart after changing the aggregation: state schema incompatibility errors. Teach "new logic = new checkpoint".
- Running `check_output.py` while the stream runs inside a 2 GB container -> OOM-kill. Stop the stream first.

## Grading hints

| Item | Points |
|---|---|
| TODO 1-2 parsing + event time | 20 |
| TODO 3 watermark + window aggregation | 25 |
| TODO 4-5 sinks with separate checkpoints and correct output modes | 20 |
| Recovery experiment: log showing resumed batch id + `duplicate rows: 0` | 20 |
| Checkpoint answers | 15 |
