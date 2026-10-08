# L13 - Checkpoint answers

**1. GX 1.x objects.**
*Data Context* (the project: configuration + stores, here the `gx/` folder) -> *Data Source* (`taxi_landing`: how to
reach the data and which engine computes metrics: pandas, Spark, SQL) -> *Data Asset* (`yellow_trips`: a collection of
records: the Parquet files of one feed) -> *Batch Definition* (`monthly`: how the asset is split into batches; one batch
= the file of one month, selected with `batch_parameters`) -> *Expectation Suite* (`yellow_trips_raw`: the rules) ->
*Validation Definition* (`yellow_trips_raw_vd`: pairs a batch definition with a suite) -> *Checkpoint*
(`yellow_trips_raw_cp`: runs one or more validation definitions and then *Actions*, e.g. update Data Docs, notify
Slack) -> *Validation Result* (stored in `gx/uncommitted/validations/`) -> *Data Docs* (static HTML rendered from suites
and results).

**2. `mostly`.** Fares outside 0-1000 already exist in good months (1.26 % in January 2024: refunds/voids recorded as
negative amounts); a strict rule would fail every month and be ignored. `payment_type` has a closed, documented code
list - a single unknown code means the producer changed something, so it must be strict. Choose `mostly` from history:
measure the failure rate over several good months, then set the threshold with a safety margin
(here 1.3 % observed -> allow 2 %), and revisit it when it fires. `mostly` hides a slow drift up to the threshold, so
pair it with monitoring of the observed percentage over time.

**3. 23,842 vs 11,921.** `ExpectCompoundColumnsToBeUnique` marks **every** row whose key is not unique - both the
original and its copy. 11,921 duplicated keys x 2 rows = 23,842 rows.

**4. Dimensions in step 1.5.**
row count -> volume/completeness of the load; column set -> schema conformity (structural validity);
NOT NULL -> completeness; fare range and payment_type set -> validity; compound uniqueness -> uniqueness;
dropoff >= pickup -> consistency (cross-field), here failing *because of* the NULL pickups (a NULL comparison is not
"greater or equal", so those rows count as unexpected).
Not covered by Part 1: **timeliness / freshness** (when the data arrived) - that is Part 2 - and **accuracy** against
the real world (whether a fare is the true fare), which needs a reference source (e.g. reconciliation with payments).

**5. Fresh but incomplete.** Freshness looks at the *newest* record: on Jan 23 the newest trip is from 11:59, only 18 h
before the check, inside the 24 h SLA. But only 29,618 rows arrived instead of ~100,000 -> volume anomaly.
Examples: freshness incident - the nightly export job did not run, or an upstream API credential expired, so no new
data at all; volume incident - an export that times out half way, a filter bug that drops one region, a Kafka consumer
that skips a partition. Data can also be complete but stale (yesterday's file re-delivered).

**6. Excluding anomalies from the baseline.** If the half day (29,618) entered the 7-day window, the mean would drop
and the standard deviation would explode (from ~11 k to ~29 k), so the *next* incidents would have a much smaller
|z| and could go unnoticed for a week - the anomaly "poisons" the baseline. The flip side (seen with `--min-history 3`):
a genuine level change is never learned, so you need a way to accept it (acknowledge, or expire the exclusion).

**7. Seasonality.** Compare against the *same weekday* of previous weeks (or a weekly-seasonal model such as STL /
Prophet), maintain a holiday calendar (expected dips are annotated or use a separate baseline), use a wider threshold
on known special days, alert on *sustained* deviations (2 consecutive days) for warnings but immediately on hard
failures (0 rows, missing partition), and route by severity (warning -> channel, critical -> pager).

**8. OpenLineage Spark listener.** Nothing about your code: it is a `SparkListener` (`spark.extraListeners`) that
receives Spark's job/SQL execution events and inspects the **optimized logical plan** of each action: the relations
read (file scans, JDBC, Hive/Iceberg tables) become input datasets, the write command
(`InsertIntoHadoopFsRelationCommand`, `AppendData`, `CreateTableAsSelect` ...) the output dataset. It needs only the
jar and the transport configuration. A *facet* is an extensible, versioned JSON block attached to a run, job or
dataset: e.g. `schema`, `columnLineage`, `dataSource`, `outputStatistics`, `processing_engine`, `sql`,
`dataQualityAssertions`.

**9. Column-level lineage.** The `columnLineage` facet of `daily_borough_revenue` says `revenue <=
trips_clean.total_amount <= yellow_tripdata.total_amount`. Before the upstream rename you query lineage for
`total_amount` downstream (Marquez `/api/v1/column-lineage?...&withDownstream=true`) and get every job and dataset
column that depends on it - the impact analysis list of owners to notify and code to change - instead of grepping
repositories.

**10. Two gates, atomic publish.** The input gate protects the transform from garbage it was not designed for
(schema drift, partial loads) and quarantines the file before any work is done; the output gate catches bugs in
*our own* transformation (grain change, fan-out joins, wrong filters) that a perfect input cannot prevent. Atomic
rename (`os.replace`, or a table/branch swap in Iceberg/Delta - write-audit-publish) guarantees readers see either the
old version or the complete new one, never a half-written file, and a failed audit leaves `published/` untouched.
