# Big Data Engineering — Hands-on Labs

Lab repository for the *Data Engineer Course* by **Huỳnh Hoàng Phúc**.
Every lab is a self-contained Docker Compose stack that runs on a laptop with **8 GB RAM**
(Apple Silicon or x86-64), one stack at a time.

## Prerequisites

* Basic Linux shell, SQL and Python.
* Docker Desktop (macOS/Windows + WSL 2) or Docker Engine + Compose v2 (Linux).
  Docker memory: **4 GB minimum, 6 GB recommended** (Settings → Resources).
* ~20 GB free disk (images + data), internet access for the first run of each lab.
* Start with **[L00_setup](L00_setup/README.md)** — it installs/checks everything and downloads the datasets.

## Get the labs

Clone the repository **into a folder named `labs`** — every lab's instructions assume this
(`cd labs/L01_hdfs`, …):

```bash
git clone https://github.com/HuynhHoangPhuc/data_engineering_labs.git labs
cd labs
bash datasets/download_taxi.sh     # NYC taxi 2024-01 + zone lookup (~48 MB, git-ignored)
```

On Windows, clone inside WSL 2 (e.g. `~/labs`), not under `C:\`. To get updates later:
`cd labs && git pull`.

## Lab index

| ID | Folder | Module | What you do | Main stack |
|---|---|---|---|---|
| L00 | [L00_setup](L00_setup/) | 0 Foundations | Install Docker, get the repo, download datasets, `check_env.sh`; [Foundations exercises](L00_setup/exercises/): Linux CLI, SQL (OLTP vs OLAP on Olist), Python data handling, Docker basics, CSV vs JSON vs Parquet | Docker, Postgres 18, Python (pandas/polars/DuckDB) |
| L01 | [L01_hdfs](L01_hdfs/) | 1 Hadoop | 3-DataNode HDFS, `hdfs dfs`, `fsck -files -blocks -locations`, `setrep`, kill a DataNode → re-replication, safe mode | Hadoop 3.4.3 |
| L02 | [L02_mapreduce](L02_mapreduce/) | 1 Hadoop | Word count with Hadoop Streaming (Python), trips per pickup zone, combiner vs no combiner (shuffle bytes), YARN & JobHistory UIs | Hadoop 3.4.3 (L01 cluster) |
| L03 | [L03_hive](L03_hive/) | 2 Hive | External tables on HDFS, dynamic partitions by month, CSV vs ORC vs Parquet, partition pruning with `EXPLAIN`, bucketing | Hive 4.2.1, Postgres metastore |
| L04 | [L04_ingestion](L04_ingestion/) | 3 Ingestion | Sqoop full / incremental (append, lastmodified) import, saved job, export; the same with Spark JDBC (`partitionColumn`, `numPartitions`, watermark) | Postgres 18 (Olist), Sqoop 1.4.7, Spark 4.1.3 |
| L05 | [L05_pyspark_basics](L05_pyspark_basics/) | 4 Spark I | DataFrame API on taxi data: cleaning, joins, aggregations, windows, Spark SQL, lazy evaluation, `explain()`, Spark UI, partitioned Parquet | Spark 4.1.3 |
| L06 | [L06_spark_tuning](L06_spark_tuning/) | 5 Spark II | Skewed join + salting, broadcast vs sort-merge, AQE on/off, shuffle partitions, repartition vs coalesce, caching — with a results table | Spark 4.1.3 |
| L07 | [L07_kafka](L07_kafka/) | 6 Kafka | 3-broker KRaft cluster, Python producer/consumer, key ordering, consumer-group rebalance, kill a broker | Kafka 4.x |
| L08 | [L08_cdc_debezium](L08_cdc_debezium/) | 3/6 CDC | Debezium Postgres → Kafka change events (insert/update/delete) | Debezium, Kafka, Postgres |
| L09 | [L09_structured_streaming](L09_structured_streaming/) | 7 Streaming | Wikimedia edits → Kafka → windowed counts with watermark → Parquet; checkpoint recovery | Spark Structured Streaming, Kafka |
| L10 | [L10_airflow](L10_airflow/) | 8 Orchestration | Daily DAG download → aggregate (DuckDB) → quality check → publish; backfill, retries, Assets | Airflow 3.x, DuckDB |
| L11 | [L11_lakehouse_iceberg](L11_lakehouse_iceberg/) | 9 Lakehouse | Spark writes Iceberg on object storage, Trino reads; MERGE INTO, time travel, schema evolution | Iceberg, Trino, S3-compatible store |
| L12 | [L12_dbt](L12_dbt/) | 10 Modeling | Star schema from Olist with dbt, tests, docs | dbt-core |
| L13 | [L13_data_quality](L13_data_quality/) | 11 Data quality & ops | Great Expectations (GX Core 1.x) suite + checkpoint + Data Docs on taxi data; freshness/volume monitor with webhook alerts; OpenLineage lineage from a Spark job into Marquez; quality gates that block publishing | GX Core 1.24, DuckDB, Marquez 0.50, Spark 4.1.3 + openlineage-spark 1.53 |
| CAP | [capstone](capstone/) | 12 Capstone | Postgres → Debezium → Kafka → Structured Streaming → Iceberg bronze/silver/gold → dbt → BI, orchestrated by Airflow | everything |

Each lab folder contains `README.md` (objectives, step-by-step tasks with expected output,
checkpoint questions, stretch challenges, cleanup, troubleshooting), the `docker-compose.yml`
and config, starter code with `TODO`s, `solutions/` (complete code + checkpoint answers) and
`INSTRUCTOR_NOTES.md`.

## Repository layout

```
labs/
├── README.md                 ← you are here
├── datasets/                 ← shared by all labs (mounted as /datasets)
│   ├── download_taxi.sh      ← NYC TLC yellow taxi Parquet + zone lookup → datasets/data/taxi/
│   ├── parquet_to_csv.py     ← Parquet → headerless CSV (L02, L03)
│   ├── olist/                ← Olist OLTP schema + synthetic seed for Postgres (L04, L08, L12, CAP)
│   └── data/                 ← downloaded data (git-ignored)
├── images/                   ← locally built images (see below)
│   ├── hadoop/               ← de-labs/hadoop:3.4.3 (multi-arch)
│   └── sqoop/                ← de-labs/sqoop:1.4.7 (FROM de-labs/hadoop)
└── L00_setup/ … L13_data_quality/, capstone/
```

## Running a lab (one stack at a time)

```bash
cd labs/L01_hdfs
docker compose up -d --build     # start (builds local images on first use)
docker compose ps                # status / health
docker compose logs -f <service> # follow a service's log
docker compose exec <service> bash
docker compose down              # stop, KEEP data volumes
docker compose down -v           # stop and DELETE data volumes (clean slate)
```

Before starting a new lab, stop the previous one (`docker ps` should be empty of lab containers);
`bash L00_setup/check_env.sh` warns about leftovers and busy ports.

## Images and versions (verified September 2026)

| Component | Image | Notes |
|---|---|---|
| Hadoop 3.4.3 (HDFS, YARN, MapReduce) | `de-labs/hadoop:3.4.3` built from [`images/hadoop`](images/hadoop/Dockerfile) | Official `apache/hadoop` tags are **amd64-only**; we build from the Apache release tarball (aarch64 native build on ARM) on `eclipse-temurin:11-jdk-noble`, + Python 3 & pyarrow |
| Sqoop 1.4.7 | `de-labs/sqoop:1.4.7` from [`images/sqoop`](images/sqoop/Dockerfile) | Retired to the Apache Attic (2021); runs on Hadoop 3.4 after swapping commons-cli to 1.5.0 and adding commons-lang 2.6 |
| Hive 4.2.1 | `apache/hive:4.2.1` | Official, multi-arch; HiveServer2 + standalone Metastore; Tez local mode |
| Spark 4.1.3 | `spark:4.1.3-python3` | Docker Official Image, multi-arch, Java 17, Python 3.10, Scala 2.13 |
| PostgreSQL 18.6 | `postgres:18.6` | Olist source DB (L04) and Hive metastore DB (L03) |

## Resource tips for 8 GB laptops

* One lab stack at a time; close IDEs/browser tabs you do not need while a stack runs.
* Watch memory with `docker stats`. Exit code **137** = a container was killed for lack of memory.
* Heaps are deliberately small (NameNode 384 MB, DataNodes 192–256 MB, NodeManager offers 1.5 GB
  to YARN, Spark driver 2 GB). Do not raise them unless a README tells you to.
* Disk: Docker Desktop's virtual disk grows but rarely shrinks. Remove images of finished modules
  with `docker image rm <image>`. Avoid `docker system prune -a` if you use Docker for other work.
* Keep only the taxi months you need (`datasets/data/taxi/`); CSV copies made in L02/L03 can be deleted.

## Troubleshooting (all labs)

| Symptom | Likely cause / fix |
|---|---|
| `Cannot connect to the Docker daemon` | Start Docker Desktop (`open -a Docker` on macOS) |
| `Bind for 0.0.0.0:9870 failed: port is already allocated` | Another stack (or program) uses the port: `docker ps`, `docker compose down` in the other lab folder |
| Container exits with **137** | Out of memory: raise Docker memory, stop other stacks |
| `no matching manifest for linux/arm64` | Image has no ARM build; use the course images or `--platform linux/amd64` (emulated) |
| Web UI links point to `http://datanode1:9864`, `historyserver:19888` … | Container hostnames are not resolvable from the laptop — replace with `localhost:<port>` or add them to `/etc/hosts` as `127.0.0.1` |
| Data from a previous run is still there / init scripts did not re-run | Volumes persist: `docker compose down -v` |
| Build fails downloading Apache tarballs | Mirror hiccup: retry, or `--build-arg APACHE_MIRROR=https://archive.apache.org/dist` |
| `bash\r: No such file or directory` (Windows) | CRLF line endings: clone inside WSL with `core.autocrlf=input` |
