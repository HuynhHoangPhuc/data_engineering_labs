# L09 - Checkpoint answers

1. The Kafka record `timestamp` is set by the producer/broker when the record is written (≈ **ingestion/processing time**).
   The event's own `timestamp` field is when the edit happened on the wiki (**event time**). We window on event time so
   results don't change when data arrives late, is replayed, or the job is restarted after an outage (a replay of last
   week's data must land in last week's windows, not "now").

2. Watermark = max event time - 2 min = 16:33:50. The event's window 16:31-16:32 ended before the watermark, so its state
   has already been finalized and dropped -> the late event is **dropped** (not counted). With a 10-minute watermark
   (16:25:50) the window is still open and the event is counted - at the price of keeping 10 minutes of window state
   and emitting append results 10 minutes later.

3. In append mode each output row is emitted once and never changed. Without a watermark Spark can never know that a window
   is complete (a late event could always arrive), so it could never emit anything - Spark rejects the query at analysis time.

4. Sharing a checkpoint: both queries would overwrite each other's offsets/commits/state -> corruption / failure. Each query
   needs its own checkpoint. Changing the aggregation (new grouping key, new aggregate) makes the stored **state schema**
   incompatible; Spark refuses to start (state schema check) - you must start with a new checkpoint (and decide how to
   backfill). Some changes (filters, projections after the aggregation) are allowed.

5. No. `startingOffsets` only applies when a query starts **without** a checkpoint. On restart Spark reads the last
   `offsets/N` and `commits/N` and continues from there (re-running batch N if it had not committed).

6. Replayable source (Kafka offsets are stored in the checkpoint's write-ahead log **before** processing) + deterministic
   batch planning + **idempotent sink**: the file sink records each successful batch in `output/.../_spark_metadata`; files
   of a batch that failed half-way are not listed there and are ignored by readers, and a batch id that was already
   committed is skipped. At-least-once delivery + idempotent commit = exactly-once results.

7. After an outage, millions of records may be waiting. Without a cap, the first micro-batch would try to read them all,
   causing a huge batch (OOM, very long latency). `maxOffsetsPerTrigger` splits the backlog into bounded batches
   (back-pressure) so the job catches up gradually.
