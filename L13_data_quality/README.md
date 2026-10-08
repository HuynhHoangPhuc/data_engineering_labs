# L13 - Data quality, observability & lineage

| | |
|---|---|
| Module | 11 - Data quality, observability & ops |
| Time | 3 - 3.5 hours (Part 1: 60 min, Part 2: 45 min, Part 3: 45 min, Part 4: 30 min) |
| Stack | **Great Expectations (GX Core) 1.24** + DuckDB 1.5 in a Python venv on your laptop; Docker: a tiny webhook receiver (`python:3.12-alpine`), **Marquez 0.50** (OpenLineage backend) + `postgres:18.6`, **Spark 4.1.3** with **openlineage-spark 1.53.0** |
| RAM | Parts 1, 2, 4: ~1.2 GB on the laptop (pandas), < 100 MB in Docker. Part 3: ~1.5 GB in Docker (measured peak; limits add up to 3.1 GB) |
| Prerequisites | L05 (PySpark), L10 (Airflow) recommended; `labs/datasets/data/taxi/yellow_tripdata_2024-01.parquet` + `taxi_zone_lookup.csv` (L00); Python 3.10-3.13 |

## Learning objectives

1. Write **data-quality tests as code** with GX Core 1.x: Data Source -> Data Asset -> Batch Definition, Expectation Suite,
   Validation Definition, Checkpoint, **Data Docs**. Choose thresholds (`mostly`) for real, imperfect data.
2. Watch a suite catch six classic defects (partial load, schema drift, NULLs, invalid values, unknown codes, duplicates).
3. Build a **freshness & volume monitor**: a metrics table, a z-score anomaly rule, a freshness SLA, and **alerts** to a webhook.
4. Capture **lineage** automatically from a Spark job with **OpenLineage** and explore it in **Marquez** (dataset and column level).
5. Put a **quality gate** in a pipeline: bad data is quarantined and **never published** (write-audit-publish).

## The four pillars in this lab

| Question | Tool in this lab | Part |
|---|---|---|
| Is the data **correct**? (schema, nulls, ranges, uniqueness) | Great Expectations checkpoint + Data Docs | 1 |
| Did the data **arrive**, on time and in full? (freshness, volume) | DuckDB metrics table + z-score + SLA + webhook alert | 2 |
| **Where** does the data come from, what breaks if it changes? | OpenLineage (Spark listener) -> Marquez | 3 |
| How do we **stop** bad data reaching consumers? | Gate in the pipeline: checkpoint result decides publish / quarantine | 4 |

## Architecture

```
 laptop (Python venv)                                      Docker (profiles - start one at a time)
 ───────────────────────────────────────────               ──────────────────────────────────────────────
 data/landing/yellow_tripdata_YYYY-MM.parquet               --profile monitor
    │  Part 1: GX checkpoint yellow_trips_raw_cp              alert-receiver :9099  (POST /alert, GET /)
    │          ─> gx/uncommitted/data_docs/ (HTML)                    ▲
    │  Part 4: pipeline.py  gate 1 ─> transform ─> gate 2 ─> publish  │ alerts (JSON)
    │                         └─ quarantine ──────────────────────────┤
 data/daily/yellow_trips/ds=YYYY-MM-DD/                              │
    │  Part 2: monitor.py ─> data/monitor.duckdb (volume_metrics) ────┘
                                                            --profile lineage
                                                              spark (4.1.3 + OpenLineage listener)
                                                                 │ OpenLineage events (HTTP)
                                                                 ▼
                                                              marquez-api :5050 ── marquez-db (postgres 18.6)
                                                              marquez-web :3000  (lineage graph UI)
                                                            --profile spark   (fallback: events -> JSON file)
```

## Files

| Path | Purpose |
|---|---|
| `requirements.txt` | GX Core, DuckDB, pyarrow, requests for the host venv |
| `docker-compose.yml` | profiles `monitor`, `lineage`, `spark` |
| `scripts/land_month.py` | copy a taxi month into `data/landing/` |
| `scripts/make_bad_data.py` | create a **broken** "February" file with 6 injected defects |
| `scripts/land_daily_files.py` | split January into daily partitions, with a half day and a missing day |
| `scripts/webhook_receiver.py` | the alert receiver (stdlib HTTP server) |
| `part1_gx/gx_setup.py` | **starter** (TODO 1-7): GX project, suite, checkpoint |
| `part1_gx/run_checkpoint.py` | runs the checkpoint for a month, prints a summary, exit code 0/1 |
| `part2_monitor/monitor.py` | **starter** (TODO 1-4): freshness & volume monitor |
| `part3_lineage/taxi_lineage_job.py` | **starter** (TODO 1): PySpark raw -> clean -> aggregate |
| `part3_lineage/inspect_events.py` | summarise OpenLineage JSON events (fallback path) |
| `part4_gate/pipeline.py` | **starter** (TODO 1-4): pipeline with two quality gates |
| `marquez/` | Dockerfile + config of the native Marquez API image |
| `solutions/` | complete code for every part + `CHECKPOINT_ANSWERS.md` |

Everything the lab generates goes to `data/`, `gx/` and `output/` (git-ignored).

---

## Step 0 - Python environment and data (10 min)

```bash
cd labs/L13_data_quality
python3 -m venv .venv && source .venv/bin/activate        # Python 3.10-3.13 (tested: 3.12)
pip install -r requirements.txt                            # ~1-2 min, ~250 MB (GX pulls pandas, scipy, altair ...)
export GX_ANALYTICS_ENABLED=false                          # GX sends anonymous usage stats unless disabled
python -c "import great_expectations as gx; print(gx.__version__)"     # 1.24.0
python scripts/land_month.py 2024-01
```

Expected: `[ ok ] landed data/landing/yellow_tripdata_2024-01.parquet (50.0 MB)`.
(If the file is missing: `bash ../datasets/download_taxi.sh 2024-01`.)

> **GX Core 1.x vs 0.x.** GX 1.0 (Aug 2024) replaced the old API completely. Blog posts with `great_expectations init`,
> `get_validator()`, `context.sources`, `add_or_update_expectation_suite(...)`, `SimpleCheckpoint` or YAML checkpoints
> with `action_list` are **0.x** and do not work anymore. In 1.x everything is Python objects:
> `context.data_sources`, `gx.ExpectationSuite`, `gx.expectations.ExpectX(...)` classes, `gx.ValidationDefinition`, `gx.Checkpoint`.

---

## Part 1 - Great Expectations: a test suite for the raw taxi file (60 min)

### 1.1 The GX object model

```
 Data Context (gx/ folder: YAML + JSON, commit it)                         Checkpoint  yellow_trips_raw_cp
   └─ Data Source   taxi_landing   (pandas, files in data/landing/)          ├─ Validation Definition yellow_trips_raw_vd
        └─ Data Asset  yellow_trips  (Parquet files)                          │     ├─ Batch Definition  monthly ─┐
             └─ Batch Definition  monthly  (regex year/month -> 1 file)      │     └─ Expectation Suite yellow_trips_raw
                                                                             └─ Actions: UpdateDataDocsAction (+ Slack, email ... in prod)
  run(batch_parameters={"year": "2024", "month": "01"})  ─>  Validation Result  ─>  Data Docs (HTML)
```

### 1.2 Explore before you write rules

A suite encodes **what you know about the data**. Look first (in `python`):

```python
import duckdb
duckdb.sql("""SELECT count(*) n_rows, count(passenger_count) non_null_pax, min(tpep_pickup_datetime), max(tpep_pickup_datetime),
              sum((fare_amount < 0)::int) neg_fares, list(DISTINCT payment_type ORDER BY payment_type) payment_types
              FROM 'data/landing/yellow_tripdata_2024-01.parquet'""").show()
```

Expected: 2,964,624 rows; 2,824,462 non-null `passenger_count` (~95 %); pickups from **2002**-12-31 to 2024-02-01;
37,448 negative fares; payment types `[0, 1, 2, 3, 4]`. Real data is dirty - our rules must tolerate the known noise
(**`mostly=`**) but catch real breakage.

You can also try one expectation interactively, without saving anything:

```python
import great_expectations as gx
ctx = gx.get_context(mode="ephemeral")
batch = (ctx.data_sources.add_pandas_filesystem("tmp", base_directory="data/landing")
         .add_parquet_asset("trips").add_batch_definition_path("jan", path="yellow_tripdata_2024-01.parquet").get_batch())
batch.validate(gx.expectations.ExpectColumnValuesToBeBetween(column="fare_amount", min_value=0, max_value=1000))
```

-> `"success": false`, `"unexpected_count": 37455` (1.26 %). With `mostly=0.98` it passes.

### 1.3 Build the suite and the checkpoint - TODO 1-7

Open `part1_gx/gx_setup.py`. Four expectations are given; add the rest (each TODO names the class to use):

| # | Dimension | Expectation | Setting |
|---|---|---|---|
| 1 | schema | `ExpectTableColumnsToMatchSet` | the 19 TLC columns, `exact_match=True` (given) |
| 2 | schema | `ExpectColumnValuesToBeInTypeList` | `tpep_pickup_datetime` is datetime64 (given) |
| 3 | schema | `ExpectColumnValuesToBeOfType` | `fare_amount` is `float64` (TODO 1) |
| 4 | volume | `ExpectTableRowCountToBeBetween` | 2,000,000 - 4,500,000 (TODO 2) |
| 5-7 | completeness | `ExpectColumnValuesToNotBeNull` | pickup (given), dropoff, `PULocationID` (TODO 3) |
| 8 | completeness | `ExpectColumnProportionOfNonNullValuesToBeBetween` | `passenger_count` >= 0.90 (TODO 3) |
| 9 | validity | `ExpectColumnValuesToBeBetween` | `fare_amount` 0-1000, `mostly=0.98` (given) |
| 10 | validity | `ExpectColumnValuesToBeBetween` | `trip_distance` 0-100, `mostly=0.99` (TODO 4) |
| 11 | validity | `ExpectColumnValuesToBeInSet` | `payment_type` in 0-6 (TODO 4) |
| 12 | validity | `ExpectColumnValuesToBeBetween` | `PULocationID` 1-265 (TODO 4) |
| 13 | consistency | `ExpectColumnPairValuesAToBeGreaterThanB` | dropoff >= pickup, `mostly=0.999` (TODO 5) |
| 14 | uniqueness | `ExpectCompoundColumnsToBeUnique` | (VendorID, pickup, dropoff, PU, DO, total_amount) (TODO 6) |

TODO 7 creates the checkpoint `yellow_trips_raw_cp` with an `UpdateDataDocsAction`. Then:

```bash
python part1_gx/gx_setup.py
python part1_gx/run_checkpoint.py --month 2024-01
```

Expected (solution):

```
Suite yellow_trips_raw on 2024-01: 14/14 expectations passed
  PASS  expect_column_pair_values_a_to_be_greater_than_b        tpep_dropoff_datetime  56 unexpected (0.002 %)
  PASS  expect_column_proportion_of_non_null_values_to_be_between passenger_count        observed 0.9527
  PASS  expect_column_values_to_be_between                      fare_amount            37,455 unexpected (1.263 %)
  ...
  PASS  expect_compound_columns_to_be_unique                                           0 unexpected (0.000 %)
  PASS  expect_table_columns_to_match_set                                              19 columns
  PASS  expect_table_row_count_to_be_between                                           observed 2,964,624

Checkpoint yellow_trips_raw_cp: SUCCESS
Data Docs : file:///.../labs/L13_data_quality/gx/uncommitted/data_docs/local_site/index.html
```

It takes ~4-5 s and ~1.2 GB of RAM (pandas loads the whole file). `echo $?` prints **0**.
Look at what was saved: `ls gx/ gx/expectations gx/checkpoints` - the suite is a JSON file you can review in a pull request.

### 1.4 Data Docs

```bash
python part1_gx/run_checkpoint.py --month 2024-01 --open      # or open the file:// link above
```

The site lists the suite (*Expectation Suites*) and every run (*Validation Results*, grouped by the run name
`taxi_2024-01`). Click a run: every expectation with its observed value and a sample of unexpected values.
(The pages load Bootstrap/fonts from a CDN; offline they look plain but still work.)

### 1.5 Inject bad data and watch the suite fail

"February just landed - is it good?"

```bash
python scripts/make_bad_data.py
python part1_gx/run_checkpoint.py --month 2024-02 ; echo "exit code: $?"
```

Expected:

```
1. partial load       : 2,964,624 -> 1,192,132 rows (pickups before 2024-02-14)
2. schema drift       : Airport_fee -> airport_fee
3. NULL pickup times  : 23,842 rows
4. negative fares     : 59,606 rows
5. payment_type = 9   : 1,000 rows
6. duplicate rows     : 11,921 rows appended

Suite yellow_trips_raw on 2024-02: 7/14 expectations passed
  FAIL  expect_column_pair_values_a_to_be_greater_than_b        tpep_dropoff_datetime  24,104 unexpected (2.002 %)
  FAIL  expect_column_values_to_be_between                      fare_amount            75,048 unexpected (6.233 %)
  FAIL  expect_column_values_to_be_in_set                       payment_type           1,008 unexpected (0.084 %)
  FAIL  expect_column_values_to_not_be_null                     tpep_pickup_datetime   24,074 unexpected (1.999 %)
  FAIL  expect_compound_columns_to_be_unique                                           23,842 unexpected (1.980 %)
  FAIL  expect_table_columns_to_match_set                                              mismatched {'unexpected': ['airport_fee'], 'missing': ['Airport_fee']}
  FAIL  expect_table_row_count_to_be_between                                           observed 1,204,053
  PASS  ...
Checkpoint yellow_trips_raw_cp: FAILED
exit code: 1
```

Every defect is caught by at least one expectation. Questions to discuss: why is the duplicate count 23,842 when
11,921 rows were duplicated? Why does the dropoff >= pickup rule fail although no dropoff time was changed?
Refresh Data Docs and compare the two runs side by side.

---

## Part 2 - Freshness & volume monitoring with alerts (45 min)

Tests like Part 1 check **content**. Many incidents are about **arrival**: the file is late, missing, or half
of it. That is monitored with **metrics over time**.

### 2.1 Start the alert receiver and simulate a daily feed

```bash
docker compose --profile monitor up -d
curl -s localhost:9099/health                         # ok
python scripts/land_daily_files.py --half-day 2024-01-23 --skip-day 2024-01-25
```

Expected: 30 partitions `data/daily/yellow_trips/ds=2024-01-DD/`; `2024-01-23   29,618 rows  <- HALF DAY (loader died at 12:00)`
and `2024-01-25  SKIPPED (file never arrives)`.

In a second terminal follow the alerts: `docker compose logs -f alert-receiver`.

### 2.2 Complete the monitor - TODO 1-4

`part2_monitor/monitor.py` replays the month **day by day**, as if it ran every morning at 06:00 for the day before:

| Check | Rule (defaults) |
|---|---|
| metrics (TODO 1) | `row_count`, `max(tpep_pickup_datetime)` of the day's partition with DuckDB -> upsert into table `volume_metrics` in `data/monitor.duckdb` |
| volume (TODO 2) | z-score of today's count vs. the last **7 healthy** days: `z = (n - mean) / std`; \|z\| > **3** -> `VOLUME_ANOMALY`; no partition -> `MISSING`; < 7 days of history -> `WARMUP` |
| freshness (TODO 3) | `lag = as_of - newest pickup seen so far`; lag > **24 h** SLA -> `STALE` |
| alert (TODO 4) | POST every failed check as JSON to `http://localhost:9099/alert` |

```bash
python part2_monitor/monitor.py --reset --from 2024-01-01 --to 2024-01-31
```

Expected (solution):

```
2024-01-01  rows= 81,013  z=   n/a  WARMUP          lag=  6.0h FRESH
...
2024-01-15  rows= 77,033  z= -2.21  OK              lag=  6.0h FRESH
...
2024-01-23  rows= 29,618  z= -6.02  VOLUME_ANOMALY  lag= 18.0h FRESH
    -> ALERT warning: volume - row_count 29,618 is -6.0 std from the 7-day mean (threshold +/-3.0)
2024-01-24  rows=105,120  z= +0.59  OK              lag=  6.0h FRESH
2024-01-25  rows=      0  z=   n/a  MISSING         lag= 30.0h STALE
    -> ALERT critical: volume - partition missing (0 rows)
    -> ALERT critical: freshness - newest data is 30.0 h old (SLA 24 h)
...
31 day(s) checked, 3 alert(s)
```

and in the receiver log / at <http://localhost:9099>:

```
ALERT [WARNING] volume yellow_trips 2024-01-23: row_count 29,618 is -6.0 std from the 7-day mean (threshold +/-3.0)
ALERT [CRITICAL] volume yellow_trips 2024-01-25: partition missing (0 rows)
ALERT [CRITICAL] freshness yellow_trips 2024-01-25: newest data is 30.0 h old (SLA 24 h)
```

Note that the **half day passes the freshness check** (its newest trip is from 11:59, only 18 h old) - only the
volume check catches it. And the missing day is caught by both. You need both kinds of monitors.

### 2.3 Explore the metrics store and the thresholds

```bash
python part2_monitor/monitor.py --report | head -15                 # the metrics store (one row per day)
python part2_monitor/monitor.py --reset --from 2024-01-01 --to 2024-01-31 --no-alerts --k 2
python part2_monitor/monitor.py --reset --from 2024-01-01 --to 2024-01-31 --no-alerts --min-history 3
python part2_monitor/monitor.py --reset --from 2024-01-01 --to 2024-01-31          # back to the defaults
```

* **k = 2** -> 9 alerts instead of 3: Martin Luther King Day (Mon Jan 15, z = -2.21), two Sundays/Mondays and two busy
  Wednesdays/Thursdays now fire. Real but *expected* variation = **alert fatigue**; people stop reading alerts.
* **min-history 3** -> 24 alerts. Jan 4 (first normal weekday after New Year, z = +6.37) is flagged; because flagged days
  are excluded from the baseline, the baseline stays stuck on the quiet holiday days and almost every later day "is an
  anomaly". Lessons: a warm-up period matters, and "exclude anomalies from the baseline" needs a way to **accept** a
  genuine level shift (a human acknowledges it, or the exclusion expires).

### 2.4 The late file arrives

```bash
python scripts/land_daily_files.py --only 2024-01-25
python part2_monitor/monitor.py --ds 2024-01-25
```

Expected: `2024-01-25  rows=110,318  z= +0.90  OK  lag=  6.0h FRESH` - the monitor is idempotent (upsert per day), so
re-running a day after a fix simply overwrites its metrics.

---

## Part 3 - Lineage with OpenLineage and Marquez (45 min)

**OpenLineage** is an open standard (LF AI & Data) for lineage events: a **run** of a **job** reads **input datasets**
and writes **output datasets**; *facets* add schemas, column lineage, data-quality metrics, etc. Integrations emit
the events (Spark, Airflow, dbt, Flink ...); a backend such as **Marquez** (the reference implementation),
DataHub, OpenMetadata or a cloud catalog stores them and draws the graph.

```
 spark-submit job.py ──> OpenLineageSparkListener ──(RunEvent JSON: START / RUNNING / COMPLETE)──> transport
                                                                               http  -> Marquez API :5050 -> UI :3000
                                                                               file  -> output/openlineage/events.jsonl
```

### 3.1 Start Marquez + Spark

```bash
docker compose --profile monitor down                          # free memory
docker compose --profile lineage build                         # native Marquez API image (downloads a 41 MB jar, < 1 min)
docker compose --profile lineage up -d --wait                  # ~15 s; first time also pulls marquez-web (~0.4 GB)
docker compose --profile lineage ps                            # marquez-db, marquez-api (healthy), marquez-web, spark
curl -s localhost:5050/api/v1/namespaces | python3 -m json.tool | head
```

Expected: a JSON list containing the namespace `default`. Open the UI at <http://localhost:3000>.

> **Images.** The official `marquezproject/marquez` and `marquez-web` images are published for **amd64 only**
> (latest 0.51.1, March 2025). We build the API ourselves from the release jar on Maven Central (native on Apple Silicon,
> see `marquez/Dockerfile`), and run the small web UI image under emulation (`platform: linux/amd64`, Rosetta/QEMU) -
> slower to start, but it only serves static pages.

### 3.2 Run the job with the OpenLineage listener - TODO 1

Read `conf/spark-defaults.conf`: two lines turn lineage on for **any** Spark job, without code changes:

```properties
spark.jars.packages     io.openlineage:openlineage-spark_2.13:1.53.0
spark.extraListeners    io.openlineage.spark.agent.OpenLineageSparkListener
spark.openlineage.namespace  l13
spark.openlineage.transport.type  http
spark.openlineage.transport.url   http://marquez-api:5000
```

Complete TODO 1 in `part3_lineage/taxi_lineage_job.py` (join + aggregate step), then:

```bash
docker compose exec spark spark-submit /lab/part3_lineage/taxi_lineage_job.py
```

The first run downloads the OpenLineage jar into the `ivy-cache` volume; the job takes ~20-30 s (Spark peaks at ~850 MB).
Expected output ends with:

```
daily_borough_revenue: 234 rows
+-----------+---------+------+----------+-------+
|pickup_date|borough  |trips |revenue   |avg_tip|
|2024-01-25 |Manhattan|97535 |2338580.84|3.16   |
|2024-01-18 |Manhattan|97437 |2279589.98|3.05   |
...
```

(The starter, before TODO 1, stops with `TODO 1 not done yet ...` after writing `trips_clean` - that run is already
visible in Marquez.)

### 3.3 Explore the lineage

* UI <http://localhost:3000> -> namespace selector (top) **l13** -> *Jobs*. The two jobs that matter, named
  `<app name>.<Spark plan node>.<output dataset>`:
  * `taxi_lineage.execute_insert_into_hadoop_fs_relation_command.lineage_trips_clean` (raw -> trips_clean)
  * `taxi_lineage.adaptive_spark_plan.lineage_daily_borough_revenue` (trips_clean + zones -> daily_borough_revenue)

  You also see the parent job `taxi_lineage` (the Spark application) and helper jobs without outputs:
  `...map_partitions_parallel_collection` (Spark reading Parquet footers), `...take_ordered_and_project` (the `.show()`),
  and `taxi_lineage.2` / `.6` stuck in RUNNING (RDD-level Spark jobs without a SQL plan - OpenLineage cannot describe
  them). Every Spark action becomes a job; in production you name your applications well and filter the noise.
* Click the dataset `/lab/output/lineage/daily_borough_revenue` (namespace `file`): the graph shows
  `yellow_tripdata_2024-01.parquet -> [clean job] -> trips_clean -> [aggregate job] <- taxi_zone_lookup.csv -> daily_borough_revenue`.
* Dataset page -> schema, and **column lineage**: `revenue` comes from `trips_clean.total_amount`, which comes from
  the raw `total_amount`.
* The same via the REST API:

```bash
curl -s "localhost:5050/api/v1/namespaces/l13/jobs" | python3 -c "import sys,json;[print(j['name']) for j in json.load(sys.stdin)['jobs']]"
curl -s "localhost:5050/api/v1/column-lineage?nodeId=datasetField:file:/lab/output/lineage/daily_borough_revenue:revenue" \
  | python3 -c "import sys,json;[print(n['id'], '<-', [e['destination'] for e in n['inEdges']]) for n in json.load(sys.stdin)['graph']]"
```

Expected (column-level lineage of `revenue`, 3 hops):

```
datasetField:file:/datasets/data/taxi/yellow_tripdata_2024-01.parquet:total_amount <- []
datasetField:file:/lab/output/lineage/daily_borough_revenue:revenue <- ['datasetField:file:/lab/output/lineage/trips_clean:total_amount']
datasetField:file:/lab/output/lineage/trips_clean:total_amount <- ['datasetField:file:/datasets/data/taxi/yellow_tripdata_2024-01.parquet:total_amount']
```

Run the job a second time: Marquez keeps **run history** (durations, states) and dataset **versions**.

### 3.4 Fallback (or extra): events to a JSON file, no Marquez

If Marquez does not start on your machine (memory, emulation disabled), the lineage events are still the lesson:

```bash
docker compose --profile lineage down
docker compose --profile spark up -d
docker compose exec spark spark-submit \
  --conf spark.openlineage.transport.type=file \
  --conf spark.openlineage.transport.location=/lab/output/openlineage/events.jsonl \
  /lab/part3_lineage/taxi_lineage_job.py
python part3_lineage/inspect_events.py --column revenue
```

Expected: `19 events in .../events.jsonl`, the START/COMPLETE events with `inputs -> outputs` per job
(`--all` adds the RUNNING ones), and

```
Column-level lineage of output column 'revenue':
  /lab/output/lineage/daily_borough_revenue.revenue  <=  file:/lab/output/lineage/trips_clean.total_amount
```

Open `output/openlineage/events.jsonl` and find the `schema`, `columnLineage` and `processing_engine` facets.

---

## Part 4 - A quality gate in a pipeline (30 min)

Checks are only useful if they **stop** something. `part4_gate/pipeline.py` is a small batch pipeline
(in production: an Airflow DAG, see L10) using the **write-audit-publish** pattern:

```
 data/landing/<month>.parquet ─[gate 1: checkpoint yellow_trips_raw_cp]─> transform (DuckDB) ─> data/staging/
        │ FAIL: move to data/quarantine/, alert, exit 1                           │
                                                          [gate 2: suite daily_zone_gate on the output]
                                                                  │ FAIL: keep staging, alert, exit 2
                                                                  ▼
                                                    publish: atomic rename -> data/published/ + _manifest
```

Complete TODO 1-4 (two output expectations - one uses a **suite parameter** `days_in_month` passed at run time -
and the two "block" branches). With the alert receiver running (`docker compose --profile monitor up -d`):

```bash
python part4_gate/pipeline.py --month 2024-01 ; echo "exit $?"
```

Expected: gate 1 passes, `all output expectations passed (6,839 rows)`, `PUBLISHED data/published/daily_zone_2024-01.parquet`, exit 0.

```bash
python scripts/make_bad_data.py            # (re)creates the broken 2024-02 file
python part4_gate/pipeline.py --month 2024-02 ; echo "exit $?"
ls data/quarantine data/published
```

Expected: the 7 failing expectations, `-> ALERT: input_gate - raw file failed yellow_trips_raw_cp -> moved to data/quarantine/...`,
`PIPELINE BLOCKED at gate 1`, exit 1; nothing for February in `data/published/`.

A bug in **our own** code is caught by the output gate:

```bash
python part4_gate/pipeline.py --month 2024-01 --buggy-transform ; echo "exit $?"
```

Expected: `FAIL expect_compound_columns_to_be_unique ['pickup_date', 'zone_id'] {'unexpected_count': 17536}`,
`PIPELINE BLOCKED at gate 2 - data/published/ is unchanged.`, exit 2. The buggy transform grouped by an extra column
(`payment_type`), which silently changed the grain from (day, zone) to (day, zone, payment type): 19,269 rows instead of 6,839.

In Airflow (L10) the same idea is one task per stage: a non-zero exit code / exception in the gate task fails it, and
`publish` is never run (`upstream_failed`).

---

## Checkpoint questions

1. Name the GX 1.x objects between "a Parquet file on disk" and "an HTML report", and say what each one is for.
2. Why does the fare range expectation use `mostly=0.98` while `payment_type` in-set does not? How would you choose `mostly`?
3. The duplicate expectation reports 23,842 unexpected rows for 11,921 injected duplicates. Why?
4. Which data-quality dimension (completeness, validity, consistency, uniqueness, timeliness, volume/accuracy) does each failing expectation in step 1.5 cover? Which dimension is *not* covered by Part 1 at all?
5. On 2024-01-23 the freshness check said FRESH but the volume check fired. Explain, and give a real-world incident of each kind.
6. Why does the monitor exclude anomalous days from the baseline? What goes wrong if it doesn't?
7. MLK day (Jan 15) has z = -2.21. How would you avoid false alarms on holidays and weekends without missing real incidents?
8. What does the OpenLineage Spark listener need to know about your code to produce lineage? What is a *facet*?
9. A column `total_amount` in the raw file is renamed upstream. How does column-level lineage help you find what breaks?
10. In Part 4, why do we validate both the input and the output? Why publish with an atomic rename instead of writing straight to `published/`?

Answers: `solutions/CHECKPOINT_ANSWERS.md`.

## Stretch challenges

1. **Data contract**: turn the suite into a reviewed contract - add `meta` (owner, severity) to each expectation and
   fail the pipeline only on `severity: critical`.
2. **Suite parameters**: make the row-count range of `yellow_trips_raw` a `{"$PARAMETER": ...}` computed from the
   previous three months (Part 2 metrics) instead of a hard-coded 2-4.5 M.
3. **Custom action**: subclass `great_expectations.checkpoint.ValidationAction` to POST a compact JSON summary to the
   webhook receiver, and add it to the checkpoint's `actions`.
4. **Seasonality**: compare each day with the **same weekday** of the previous 4 weeks (download 2023-12 for history).
   Does MLK day still alert? Add a holiday calendar.
5. **GX on Spark**: validate `trips_clean` from Part 3 with `context.data_sources.add_spark(...)` inside the Spark
   container (`pip install great_expectations` there) - same suite, different engine.
6. **Airflow**: replace the hand-written `quality_check` task of L10's `taxi_monthly` DAG with the GX checkpoint and
   make the DAG emit OpenLineage events (`apache-airflow-providers-openlineage`, transport `http://marquez-api:5000`).
7. **dbt**: L12's `dbt build` already gates models with tests. Compare dbt tests with GX expectations - when would you use which?

## Cleanup

```bash
docker compose --profile "*" down -v       # all services of this lab + volumes (Marquez DB, ivy cache)
rm -rf data gx output
deactivate
# optional, frees ~1.9 GB: docker image rm marquezproject/marquez-web:0.50.0 de-labs/marquez-api:0.50.0 eclipse-temurin:17-jre python:3.12-alpine
```

## Troubleshooting

| Symptom | Fix |
|---|---|
| `AttributeError: ... has no attribute 'sources'` / `get_validator` / `add_expectation_suite` | Code from a GX **0.x** tutorial. Use the 1.x API shown in this lab |
| `Checkpoint with name yellow_trips_raw_cp was not found` | Run `part1_gx/gx_setup.py` first (TODO 7 creates the checkpoint) |
| `BatchDefinition ... no batches` / `No data found` for 2024-02 | Run `python scripts/make_bad_data.py` (the file is moved to `data/quarantine/` by Part 4) |
| GX prints `Calculating Metrics` progress bars | Harmless; `gx_setup.py` disables them in `gx/great_expectations.yml` (`progress_bars`) |
| `!! could not deliver alert ... Connection refused` | Start the receiver: `docker compose --profile monitor up -d` (or `python scripts/webhook_receiver.py`) |
| Port 9099 / 3000 / 5050 already in use | Another program uses it; change the left side of `ports:` in `docker-compose.yml` |
| `localhost:5000` shows an AirPlay/ControlCenter error on macOS | That is why the Marquez API is mapped to **5050** |
| `marquez-web` keeps restarting / `exec format error` | amd64 emulation is off: Docker Desktop -> Settings -> General -> "Use Rosetta for x86_64/amd64 emulation". Or use the API + fallback of 3.4 |
| Marquez UI shows no `l13` namespace | Use the namespace dropdown (top); check `docker compose logs spark` / the job output for `OpenLineage` errors; the API must be healthy *before* the job runs |
| `spark-submit` hangs at `:: resolving dependencies ::` | First download of the OpenLineage jar from Maven Central; needs internet (cached afterwards) |
| Exit code 137 | Out of memory: run one profile at a time (`docker compose --profile "*" down`) |
