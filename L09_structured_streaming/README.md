# L09 - Spark Structured Streaming: Wikimedia edits -> Kafka -> windowed counts -> Parquet

| | |
|---|---|
| Module | 7 - Stream processing |
| Time | 2.5 - 3 hours |
| Stack | `apache/kafka:4.3.1` (1 node, KRaft), `spark:4.1.3-python3` (local mode) + `spark-sql-kafka-0-10_2.13:4.1.3`, Python producer on the laptop |
| RAM | ~1.8 GB (Kafka 0.5 GB + Spark driver 1.2 GB) |

## Learning objectives

1. Ingest a real public event stream (Server-Sent Events) into Kafka with a Python producer.
2. Read Kafka from Spark Structured Streaming and parse JSON with an explicit schema.
3. Distinguish **event time** vs **processing time**, and use a **watermark** to bound state and handle late data.
4. Compute **tumbling-window** aggregations and understand output modes (`append` vs `update`).
5. Write an exactly-once **file sink** (Parquet) and prove **checkpoint recovery** by killing and restarting the job.

## Prerequisites

- L05 (DataFrames), L07 (Kafka). Slides: Module 7.
- Python venv: `pip install -r requirements.txt` (`confluent-kafka`, `requests`).
- Internet access to `https://stream.wikimedia.org` - **or** use the offline fallback (step 2b), which replays the recorded
  sample `data/sample_recentchange.jsonl.gz` (23,881 real events, ~10 minutes, recorded Sep 2026).

## Architecture

```
 stream.wikimedia.org (SSE, ~40 events/s)                                   laptop: ./output/wiki_counts/dt=.../*.parquet
          |  wiki_producer.py  (or replay_producer.py - offline)                  ^
          v                                                                      | append (exactly-once)
 +------------------+        +-------------------------------------------------+-+--------------+
 | Kafka            | -----> | Spark Structured Streaming (spark container, local[2])           |
 | wikimedia.       |        |  from_json -> event_time -> withWatermark(2 min)                 |
 | recentchange (3p)|        |  -> groupBy(window(1 min), wiki).count()                          |
 +------------------+        |  checkpoints/  (offsets, commits, state)   Spark UI :4040        |
                             +---------------------------------------------+--------------------+
                                                                           | update
                                                                           v console
```

## Files

| File | Purpose |
|---|---|
| `docker-compose.yml`, `conf/spark-defaults.conf` | Kafka + an idle Spark container (we `spark-submit` into it) |
| `producer/wiki_producer.py` | Live SSE -> Kafka producer (complete, read it!) |
| `producer/replay_producer.py` | Offline fallback: replays the recorded sample, re-timed to "now" |
| `data/sample_recentchange.jsonl.gz` | Recorded sample (trimmed fields) |
| `stream_job.py` | **Starter** streaming job (TODO 1-5) |
| `solutions/stream_job.py`, `solutions/check_output.py` | Solution + batch verification script |

---

## Step 1 - Start Kafka and Spark

```bash
cd labs/L09_structured_streaming
docker compose up -d
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 \
  --create --topic wikimedia.recentchange --partitions 3 --replication-factor 1
```

## Step 2a - Produce the live stream

Wikimedia requires a descriptive User-Agent with contact info - put **your** e-mail:

```bash
python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt
export WIKI_CONTACT="your.name@university.edu"
python producer/wiki_producer.py --no-kafka --max-events 10     # connectivity test, prints 10 edits
python producer/wiki_producer.py                                # leave running (Ctrl+C to stop)
```

Expected: `100 events sent (26.7/s), last wiki=enwiki` ... roughly 25-45 events/s.

Read the code: it's a 20-line SSE parser (`data:` lines, blank line = end of event), it keys each message by `wiki`, and
on disconnect it reconnects with `Last-Event-ID` so no events are lost.

## Step 2b - OFFLINE fallback (no internet / firewall)

```bash
python producer/replay_producer.py              # 1x speed, loops, timestamps shifted to "now"
python producer/replay_producer.py --speed 5    # faster
```

The Spark job cannot tell the difference: `timestamp` (event time) is rewritten so the first event happens now and the
original spacing is preserved. Record your own sample at home with
`python producer/wiki_producer.py --no-kafka --max-events 20000 --record data/my_sample.jsonl`.

Peek at the topic:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 \
  --topic wikimedia.recentchange --max-messages 1 | python3 -m json.tool | head -30
```

Fields we use: `wiki`, `type`, `bot`, `user`, `title`, `timestamp` (**epoch seconds = event time**), `length.old/new`.

## Step 3 - Write the streaming job

Open `stream_job.py` and complete:

| TODO | What |
|---|---|
| 1 | `from_json(value, schema)` - Kafka gives you `key`, `value` (binary), `topic`, `partition`, `offset`, `timestamp` |
| 2 | `event_time = timestamp_seconds("timestamp")`, `bytes_changed` |
| 3 | `withWatermark("event_time", "2 minutes")` + `groupBy(window("event_time", "1 minute"), "wiki")` |
| 4 | Parquet sink, `append`, own checkpoint, 30 s trigger |
| 5 | Console sink, `update`, own checkpoint |

Run it (first run downloads the Kafka connector jars, ~20 MB):

```bash
docker compose exec spark /opt/spark/bin/spark-submit /opt/lab/stream_job.py
# reference solution:
docker compose exec spark /opt/spark/bin/spark-submit /opt/lab/solutions/stream_job.py
```

Expected every 30 s:

```
-------------------------------------------
Batch: 0
-------------------------------------------
+-------------------+-------------------+------------+-----+---------+-------------+
|window_start       |window_end         |wiki        |edits|bot_edits|bytes_changed|
+-------------------+-------------------+------------+-----+---------+-------------+
|2026-09-23 16:35:00|2026-09-23 16:36:00|zhwikisource|238  |238      |152557       |
|2026-09-23 16:35:00|2026-09-23 16:36:00|enwiki      |94   |8        |8336         |
|2026-09-23 16:35:00|2026-09-23 16:36:00|wikidatawiki|153  |73       |35692        |
...
```

Open the Spark UI at <http://localhost:4040> -> **Structured Streaming** tab: input rate, process rate, batch duration,
and (per query) the **watermark** and state rows.

## Step 4 - Why are there no Parquet files yet?

```bash
find output -name "*.parquet" | head          # empty for the first ~3 minutes!
```

In **append** mode Spark writes a window **once, when it can no longer change**: when the watermark
(= max event time seen - 2 min) passes the window end. A window 16:35-16:36 is final once an event with
`event_time >= 16:38:00` has been seen, and it's written in the *next* trigger. So the first files appear after ~3-4 minutes:

```
output/wiki_counts/dt=2026-09-23/part-00000-...c000.snappy.parquet
```

The console query uses **update** mode: it prints a window every time its count changes (partial results, not final).

## Step 5 - Look inside the checkpoint

```bash
ls checkpoints/wiki_counts_parquet/                    # commits  metadata  offsets  sources  state
cat checkpoints/wiki_counts_parquet/offsets/$(ls checkpoints/wiki_counts_parquet/offsets | sort -n | tail -1)
```

Expected (last line = Kafka offsets per partition planned for that batch; the header holds the watermark):

```
v1
{"batchWatermarkMs":1790181329000,"batchTimestampMs":1790181480013,"conf":{...}}
{"wikimedia.recentchange":{"0":2992,"1":2054,"2":2074}}
```

`offsets/N` is written **before** batch N runs (write-ahead log), `commits/N` **after** it succeeded. `state/` holds the
partial window counts.

## Step 6 - Kill it and restart (checkpoint recovery)

Keep the producer running. Simulate a crash of the whole Spark "machine":

```bash
docker compose restart spark          # hard kill of the JVM (no graceful shutdown)
docker compose exec spark /opt/spark/bin/spark-submit /opt/lab/solutions/stream_job.py
```

Expected: the first batch printed is **not** `Batch: 0` but the next batch number (e.g. `Batch: 6`), and it contains the
events produced while the job was down - nothing lost, nothing counted twice. After a few minutes, stop the job
(`Ctrl+C` or `docker compose restart spark`) and verify:

```bash
docker compose exec spark /opt/spark/bin/spark-submit /opt/lab/solutions/check_output.py
```

Expected (numbers vary):

```
rows: 83   files: 7
Top 10 wikis by edits:
|wiki        |edits|bot_edits|bot_pct|
|commonswiki |660  |440      |66.7   |
|zhwikisource|419  |419      |100.0  |
|wikidatawiki|288  |146      |50.7   |
|enwiki      |177  |10       |5.6    |
...
duplicate (window_start, wiki) rows: 0   <- must be 0 (exactly-once file sink)
```

The file sink is exactly-once because Spark writes a `_spark_metadata` log next to the Parquet files and readers
ignore files not listed there (e.g. from a batch that crashed half-way).

> `check_output.py` runs a second Spark driver; with only ~4 GB for Docker, stop the streaming job first.

---

## Checkpoint questions

1. What is the difference between the Kafka record `timestamp` and the event's `timestamp` field? Which one did we use and why?
2. With a 2-minute watermark, an edit from 16:31:10 arrives at 16:36:00 while the max event time seen is 16:35:50. Is it counted? What if the watermark were 10 minutes?
3. Why can't a windowed aggregation without a watermark use `append` mode?
4. What would happen if both queries shared one checkpoint directory? If you change the aggregation (e.g. add a column) and restart with the old checkpoint?
5. `startingOffsets=earliest` - does the restarted job re-read the topic from the beginning? Why not?
6. How does Spark make the Parquet sink exactly-once while Kafka only guarantees at-least-once delivery to Spark?
7. What does `maxOffsetsPerTrigger` protect you from after a long outage?

Answers: `solutions/CHECKPOINT_ANSWERS.md`.

## Stretch challenges

1. **Sliding windows**: 5-minute windows sliding every minute (`window("event_time", "5 minutes", "1 minute")`). How many windows does each event belong to? What does that do to state size?
2. **Top-N per window**: in `foreachBatch`, rank wikis by edits per window and write only the top 5 to Parquet.
3. **Bot vs human stream**: route bot edits and human edits to two Kafka topics (Kafka sink needs a `value` column and its own checkpoint).
4. **Late data experiment**: modify `replay_producer.py` to delay 5% of events by 3 minutes. Count how many are dropped (Spark UI -> "Aggregated Number Of Rows Dropped By Watermark").
5. **Real-time mode** (reading exercise): Spark 4.1 added a low-latency *real-time mode* for Structured Streaming - in 4.1 only
   for stateless, single-stage **Scala** queries (e.g. Kafka -> filter -> Kafka), at-least-once. Why can't our windowed
   aggregation use it yet? What latency does a 30 s micro-batch trigger give us?
6. **Flink comparison**: sketch the same job in Flink SQL (`TUMBLE(TABLE ..., DESCRIPTOR(event_time), INTERVAL '1' MINUTE)`).

## Cleanup

```bash
docker compose down -v
rm -rf output checkpoints          # start from scratch next time
```

## Troubleshooting

| Symptom | Fix |
|---|---|
| `403` / `HTTP Error 429` from stream.wikimedia.org | Set `WIKI_CONTACT`; don't open many parallel connections; otherwise use `replay_producer.py`. |
| `requests.exceptions.ConnectionError` | Campus proxy/firewall -> offline fallback (step 2b). |
| `Failed to find data source: kafka` | The `spark.jars.packages` line in `conf/spark-defaults.conf` is missing or the jar download failed (no internet in the container). |
| `UnknownTopicOrPartitionException` | Create the topic (step 1) before starting the job. |
| No Parquet files after 5 min | Producer stopped (watermark doesn't advance without new events!) or you used `update` mode for the file sink. |
| `AnalysisException: Append output mode not supported ... without watermark` | TODO 3 - add `withWatermark` *before* `groupBy`. |
| Changed the query and now it fails on restart with state schema errors | The checkpoint belongs to the old query: `rm -rf checkpoints/<query> output` (or use a new checkpoint path). |
| Container killed (exit 137) | Docker memory too small or two Spark drivers running; stop other labs. |
| Port 4040 busy | Only one Spark app can own 4040 in the container; the second one uses 4041 (not published). |
