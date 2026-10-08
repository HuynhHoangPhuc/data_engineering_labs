# L02 — MapReduce with Hadoop Streaming (Python)

| | |
|---|---|
| Module | 1 — Hadoop (HDFS, YARN, MapReduce) |
| Time | 90–120 min |
| Stack | **The L01 cluster** (NameNode, 3 DataNodes, ResourceManager, NodeManager, JobHistory) |
| RAM | ~3 GB while a job runs |
| Prerequisites | L01 (cluster knowledge); taxi file 2024-01 downloaded |

## Learning objectives

1. Write a mapper and a reducer in Python and **test them locally with a Unix pipe**
   (`cat | mapper | sort | reducer`) — the pipe *is* the MapReduce model.
2. Run them on YARN with **Hadoop Streaming** and read the job counters.
3. Implement a real aggregation (trips per pickup zone) including a **map-side join** with the
   distributed cache (`-files`).
4. Measure what a **combiner** does to the shuffle: `Map output materialized bytes` and
   `Reduce shuffle bytes`.
5. Navigate the **YARN ResourceManager UI** (:8088) and the **JobHistory UI** (:19888).

## Architecture

```
 hdfs:///user/root/taxi_csv (6 blocks of 16 MB)
        │ 1 input split per block
        ▼
 ┌─────────┐ ┌─────────┐       ┌────────────┐          ┌──────────┐
 │ map 0   │ │ map 1   │  ...  │ (combiner) │─ shuffle ─► reduce 0 │─► part-00000
 │python3  │ │python3  │       │ sums per   │  (HTTP,  │ reduce 1 │─► part-00001
 │mapper.py│ │mapper.py│       │ map task   │  sorted) └──────────┘
 └─────────┘ └─────────┘       └────────────┘
   YARN containers on the NodeManager (1.5 GB → AM 512 MB + 2 × 384 MB tasks at a time)
   ResourceManager :8088 schedules, JobHistory :19888 keeps finished-job counters & logs
```

Hadoop Streaming runs any executable as mapper/reducer: records go in on **stdin** as text
lines, key/value pairs come out on **stdout** as `key<TAB>value`. Between map and reduce the
framework **sorts by key** — that sort is what your reducer relies on.

## Tasks

### 0. Start (or reuse) the cluster

```bash
cd labs/L01_hdfs
docker compose up -d          # skip if still running from L01
docker compose exec namenode bash
```

Inside the container, this lab folder is mounted at `/lab/L02`:

```bash
root@namenode:/opt/hadoop# ls /lab/L02/src/wordcount /lab/L02/src/zones
root@namenode:/opt/hadoop# hdfs dfsadmin -safemode wait && hdfs dfsadmin -report | grep "Live datanodes"
```

All following commands run **inside the namenode container** (it has `python3`, like every
NodeManager, which is a requirement for Streaming).

### 1. Word count — write and test locally

Get some text (4 public-domain books from Project Gutenberg, ~2.6 MB):

```bash
mkdir -p /lab/L02/work/books && cd /lab/L02/work/books
for id in 1342 11 84 2701; do curl -sfL -o pg$id.txt https://www.gutenberg.org/cache/epub/$id/pg$id.txt; done
ls -lh
# offline? use Hadoop's own text files instead:  cp /opt/hadoop/*.txt /opt/hadoop/LICENSE-binary .
```

Complete **TODO 1–3** in `labs/L02_mapreduce/src/wordcount/mapper.py` and `reducer.py`
(edit them on your laptop with any editor — the folder is shared). Then test with a pipe —
**no Hadoop involved**:

```bash
cd /lab/L02/src/wordcount
echo "Hadoop's HDFS, hadoop!" | python3 mapper.py
# hadoop's	1
# hdfs	1
# hadoop	1
cat /lab/L02/work/books/*.txt | python3 mapper.py | sort | python3 reducer.py | sort -t$'\t' -k2,2nr | head -5
```

Expected top 5: `the 25806`, `and 14340`, `of 14113`, `to 12103`, `a 9045`.

> Question: what happens if you forget `sort` in the pipe? Try it.

### 2. Word count on YARN

```bash
hdfs dfs -mkdir -p /user/root/books
hdfs dfs -put -f /lab/L02/work/books/*.txt /user/root/books/
STREAM=$(ls $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar)

hadoop jar $STREAM \
  -D mapreduce.job.name=wordcount \
  -files mapper.py,reducer.py \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py" \
  -input /user/root/books \
  -output /user/root/wc_out
```

* `-files` ships the scripts to every task's working directory (distributed cache).
* The output directory must **not** exist (`hdfs dfs -rm -r /user/root/wc_out` to re-run).

While it runs, open <http://localhost:8088> → the application → *ApplicationMaster*.
When it finishes, read the counters printed at the end (`Map input records`, `Map output
records`, `Reduce input groups` …) and check the result:

```bash
hdfs dfs -ls /user/root/wc_out
hdfs dfs -cat /user/root/wc_out/part-* | sort -t$'\t' -k2,2nr | head -5
```

Expected: `Launched map tasks=4` (one per file), `Reduce input groups=20967`, same top 5 as locally.

### 3. Prepare taxi data as CSV

MapReduce Streaming reads text, so convert a 1-million-row sample of the Parquet file
(the Hadoop image ships `pyarrow` for this):

```bash
python3 /datasets/parquet_to_csv.py /datasets/data/taxi/yellow_tripdata_2024-01.parquet --show-schema
python3 /datasets/parquet_to_csv.py /datasets/data/taxi/yellow_tripdata_2024-01.parquet \
        /lab/L02/work/yellow_2024-01_1M.csv --limit 1000000
head -2 /lab/L02/work/yellow_2024-01_1M.csv
# 2,2024-01-01 00:57:55,2024-01-01 01:17:43,1,1.72,1,N,186,79,2,17.7,1,0.5,0,0,1,22.7,2.5,0

hdfs dfs -mkdir -p /user/root/taxi_csv
hdfs dfs -D dfs.blocksize=16m -put -f /lab/L02/work/yellow_2024-01_1M.csv /user/root/taxi_csv/
hdfs fsck /user/root/taxi_csv -files -blocks | grep "block(s)"     # 6 blocks -> 6 map tasks
```

### 4. Trips per pickup zone (with a map-side join)

Complete **TODO 4–6** in `src/zones/mapper.py` (the reducer is the same summing logic as word
count and is given). Test locally on 1,000 rows:

```bash
cd /lab/L02/src/zones
cp /datasets/data/taxi/taxi_zone_lookup.csv .      # simulate -files locally
head -1000 /lab/L02/work/yellow_2024-01_1M.csv | python3 mapper.py | sort | python3 reducer.py | sort -t$'\t' -k2,2nr | head -3
rm taxi_zone_lookup.csv
```

Expected: `Manhattan/East Village 59`, `Manhattan/Lincoln Square East 55`, …

### 5. Combiner vs. no combiner

Run the job **twice** — identical except for `-combiner`:

```bash
cd /lab/L02/src/zones
STREAM=$(ls $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar)

# (a) WITHOUT combiner
hadoop jar $STREAM -D mapreduce.job.name=zones_nocombiner -D mapreduce.job.reduces=2 \
  -files mapper.py,reducer.py,/datasets/data/taxi/taxi_zone_lookup.csv \
  -mapper "python3 mapper.py" -reducer "python3 reducer.py" \
  -input /user/root/taxi_csv -output /user/root/zones_out_nocombiner 2>&1 | tee /tmp/nocombiner.log

# (b) WITH combiner (the reducer can be reused: sum is associative and commutative)
hadoop jar $STREAM -D mapreduce.job.name=zones_combiner -D mapreduce.job.reduces=2 \
  -files mapper.py,reducer.py,/datasets/data/taxi/taxi_zone_lookup.csv \
  -mapper "python3 mapper.py" -combiner "python3 reducer.py" -reducer "python3 reducer.py" \
  -input /user/root/taxi_csv -output /user/root/zones_out_combiner 2>&1 | tee /tmp/combiner.log

for f in /tmp/nocombiner.log /tmp/combiner.log; do echo "== $f"; \
  grep -E "Launched map tasks|Map output records|Map output materialized bytes|Combine (input|output) records|Reduce shuffle bytes|Reduce input records" $f; done
bash top_zones.sh /user/root/zones_out_combiner
```

Fill in this table (reference values from a test run in brackets):

| Counter | No combiner | With combiner |
|---|---|---|
| Launched map tasks | (6) | (6) |
| Map output records | (1,000,000) | (1,000,000) |
| Combine output records | (0) | (1,382) |
| Map output materialized bytes | (31,039,071) | (41,169) |
| Reduce shuffle bytes | (31,039,071) | (41,169) |
| Reduce input records | (1,000,000) | (1,382) |
| Wall-clock time | (~28 s) | (~37 s) |

Top zone: `Queens/JFK Airport 59777`, then `Manhattan/Midtown Center 48750`.

### 6. YARN & JobHistory UIs

* <http://localhost:8088> → *Applications* → your 3 jobs → *History* link.
  The link host is `historyserver:19888` — replace it with `localhost:19888`
  (or see Troubleshooting for an `/etc/hosts` line).
* <http://localhost:19888> → job → *Counters* (same numbers as the console), *Map tasks* →
  a task → *logs* (stdout/stderr of your Python script).
* CLI equivalents:

```bash
yarn application -list -appStates FINISHED
mapred job -list all
yarn logs -applicationId <application_id> | less      # aggregated container logs
```

## Checkpoint questions

1. In the local pipe, which command plays the role of the **shuffle & sort**? What breaks without it?
2. Why must the Streaming reducer track `current_key` itself, whereas a Java reducer gets `(key, Iterable<values>)`?
3. How many map tasks ran for the taxi job and why? How many reduce tasks and why?
4. By what factor did the combiner shrink `Reduce shuffle bytes`? Why is the number of combine
   output records (~1,382) close to *number of zones × number of map tasks*?
5. The combiner run was *not faster* here. Why? When would it be much faster?
6. Could you use a combiner to compute the **average** fare per zone by reusing an "average" reducer? What would you do instead?
7. What is the map-side join in this lab, and when would it stop being a good idea?
8. Where do the counters `BAD_RECORDS` (TODO 5) appear, and why is counting bad records better than crashing?

## Stretch challenges

* **Average fare per zone** with a correct combiner: emit `sum,count` pairs and divide only in the reducer.
* **Top-N**: output only the 10 busiest zones using a single reducer (`-D mapreduce.job.reduces=1`)
  that keeps a heap.
* **Partitioner**: with 2 reducers, check which keys landed in `part-00000` vs `part-00001`; explain `HashPartitioner`.
* **Speculative execution / failures**: make the mapper `sys.exit(1)` randomly on 5 % of tasks
  and watch YARN retry attempts (`mapreduce.map.maxattempts`).
* Run the classic Java examples: `hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar wordcount /user/root/books /user/root/wc_java` and compare time with Streaming.

## Cleanup

```bash
exit                                # leave the container
cd labs/L01_hdfs && docker compose down -v
rm -rf labs/L02_mapreduce/work      # local CSV sample and books (git-ignored)
```

## Troubleshooting

| Symptom | Fix |
|---|---|
| `Output directory hdfs://.../wc_out already exists` | `hdfs dfs -rm -r -skipTrash /user/root/wc_out` |
| `PipeMapRed.waitOutputThreads(): subprocess failed with code 1` | Your Python script crashed. Run the local pipe test; read the task's stderr: `yarn logs -applicationId <id> \| grep -A20 Traceback` |
| `code 127` | `python3` not found or wrong `-mapper` string: use `-mapper "python3 mapper.py"` and ship the file with `-files` |
| Job stuck in `ACCEPTED` | No free memory in YARN. Another job running? `yarn application -list`; kill with `yarn application -kill <id>` |
| Containers killed "beyond physical memory limits" | Only if you raised data volume a lot: increase `mapreduce.map.memory.mb` in `L01_hdfs/conf/mapred-site.xml` and restart the cluster |
| History links do not open | Add `127.0.0.1 namenode resourcemanager nodemanager historyserver` to `/etc/hosts` (macOS/Linux: `sudo nano /etc/hosts`) or replace the host with `localhost` |
