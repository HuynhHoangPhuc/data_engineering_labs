# L00 — Setup: Docker, the lab repository and datasets

| | |
|---|---|
| Module | 0 — Foundations |
| Time | 45–60 min (mostly downloads — do it at home before the first lab) |
| You need | A laptop with ≥ 8 GB RAM, ≥ 20 GB free disk, admin rights, internet |

## Learning objectives

1. Install Docker (Desktop on macOS/Windows, Engine on Linux) and give it enough memory.
2. Get the lab repository and understand its layout.
3. Download the shared datasets (NYC taxi, Olist seed is already in the repo).
4. Run your first container and the course sanity check `check_env.sh`.

## How the labs run

```
 your laptop ── docker compose (one lab stack at a time!) ──► containers on a private network
                                                                (Hadoop, Hive, Spark, Kafka, ...)
 labs/datasets/data/  ◄── bind-mounted into the containers as /datasets
```

Every lab folder has a `docker-compose.yml`. **Only run one lab stack at a time** — an 8 GB laptop
cannot hold two. Stop a lab with `docker compose down` (add `-v` to also delete its data volumes).

## Tasks

### 1. Install Docker

| OS | What to install |
|---|---|
| macOS (Apple Silicon or Intel) | [Docker Desktop for Mac](https://docs.docker.com/desktop/setup/install/mac-install/) — pick the Apple-chip build on M1/M2/M3/M4 |
| Windows 10/11 | [Docker Desktop](https://docs.docker.com/desktop/setup/install/windows-install/) with the **WSL 2** backend. Clone the repo **inside WSL** (e.g. `~/labs` in Ubuntu), not under `C:\` — bind mounts are much faster and permissions work |
| Linux | [Docker Engine](https://docs.docker.com/engine/install/) + the Compose plugin (`docker compose version` must work). Add yourself to the `docker` group |

Then give Docker enough resources — **Docker Desktop → Settings → Resources**:

| Setting | Minimum | Recommended (8 GB laptop) |
|---|---|---|
| Memory | 4 GB | **6 GB** |
| CPUs | 4 | 4–6 |
| Swap | 1 GB | 2 GB |
| Virtual disk limit | 40 GB | 64 GB |

Click *Apply & restart*. On Windows/WSL2 the memory is set in `%UserProfile%\.wslconfig`
(`[wsl2]` / `memory=6GB`) followed by `wsl --shutdown`.

> Labs L01–L06 were tested with only **4 GB** for Docker; later labs (Kafka, Airflow, the capstone)
> need 6 GB.

### 2. Hello, Docker

```bash
docker version            # client AND server sections must appear
docker compose version    # v2.x or newer ("docker-compose" v1 is NOT supported)
docker run --rm hello-world
```

Expected: `Hello from Docker!` … If you get `Cannot connect to the Docker daemon`, Docker Desktop
is not running (macOS: `open -a Docker`, wait for the whale icon to stop animating).

A slightly more useful container — a throw-away Linux shell:

```bash
docker run --rm -it ubuntu:24.04 bash -c 'uname -a; cat /etc/os-release | head -2; nproc; free -m'
```

`free -m` shows the memory of the Docker VM (what you set in step 1), not of your laptop.

### 3. Get the lab repository

```bash
# git (recommended) — the folder MUST be named "labs"; all lab instructions use `cd labs/...`
git clone https://github.com/HuynhHoangPhuc/data_engineering_labs.git labs
cd labs
# or: unzip the archive from the course page, rename the folder to labs/ and cd into it
ls
```

```
L00_setup/  L01_hdfs/  L02_mapreduce/  ...  L12_dbt/  L13_data_quality/  capstone/
datasets/   images/    README.md
```

Run every later `cd labs/...` command from the folder that **contains** `labs/`
(e.g. your home directory). Get updates with `git pull` inside `labs/`.

### 4. Download the datasets

```bash
bash datasets/download_taxi.sh              # NYC Yellow Taxi 2024-01 (~48 MB) + zone lookup
# more months for some stretch tasks:
# bash datasets/download_taxi.sh 2024-02 2024-03
ls -lh datasets/data/taxi/
```

Expected:

```
taxi_zone_lookup.csv              12K
yellow_tripdata_2024-01.parquet   48M
```

The Olist e-commerce database seed (`datasets/olist/init/*.sql`) is already in the repo — it is loaded
automatically into Postgres by the labs that need it (L04, L08, L12, capstone). See
[`datasets/olist/README.md`](../datasets/olist/README.md) for loading the full Kaggle data instead.

`datasets/data/` is git-ignored: never commit data files.

### 5. Pre-pull / pre-build the images (optional but saves class time)

```bash
docker pull spark:4.1.3-python3          # L04-L06 (~0.8 GB download)
docker pull apache/hive:4.2.1            # L03   (~1.6 GB download)
docker pull postgres:18.6                # L03, L04
docker compose -f L01_hdfs/docker-compose.yml build    # de-labs/hadoop:3.4.3 (downloads Hadoop, 3-6 min)
docker compose -f L04_ingestion/docker-compose.yml build  # de-labs/sqoop:1.4.7
```

### 6. Run the sanity check

```bash
bash L00_setup/check_env.sh
```

It checks: OS/CPU architecture, host RAM and free disk, git/curl/python3, Docker CLI + daemon +
Compose v2, **memory given to Docker**, whether the lab ports are free, the datasets, and
leftover lab containers. Example (Apple Silicon, 8 GB):

```
== 3. Docker
  [ OK ] docker CLI: Docker version 29.8.0, build 88096ef
  [ OK ] Docker daemon is running (server 29.8.0)
  [ OK ] Docker memory 5920 MB, 6 CPUs
  ...
READY: 1 warning(s), 0 failures.
```

Fix every `[FAIL]`; `[WARN]`s are advice.

## Checkpoint questions

1. What is the difference between an **image** and a **container**?
2. Why does `free -m` inside a container show a different total than your laptop's RAM?
3. What does `docker compose down -v` delete that `docker compose down` does not?
4. Why do all labs mount `labs/datasets` into the containers instead of copying data into the images?
5. Your laptop is Apple Silicon (arm64). What happens if an image only exists for `linux/amd64`?

## Stretch

* `docker stats` while a lab runs — which container uses the most memory?
* `docker system df` — how much disk do images, containers, volumes and build cache use?
  (Only clean up things you created: `docker image rm <image>`; avoid blanket `prune` commands on a
  machine that you also use for other projects.)
* Read `labs/images/hadoop/Dockerfile`: why do we build our own Hadoop image?

## Foundations exercises

Warm-ups for Module 0 in [`exercises/`](exercises/README.md) (3-4 h, each part stands alone): (a) Linux CLI on a taxi CSV
sample, (b) SQL on the Olist Postgres incl. an OLTP vs OLAP query comparison, (c) Python data handling (pandas vs polars
vs DuckDB, generators), (d) Docker basics, (e) file formats - CSV vs JSON vs Parquet size and query speed.

## Cleanup

Nothing to clean up — `hello-world`/`ubuntu` containers were started with `--rm`.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `permission denied while trying to connect to the Docker daemon socket` (Linux) | `sudo usermod -aG docker $USER`, log out and in |
| `docker: 'compose' is not a docker command` | Install the Compose v2 plugin (`docker-compose-plugin` package) or update Docker Desktop |
| `download_taxi.sh` fails with `403`/`404` | Check the month exists on the [TLC page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page); behind a proxy set `https_proxy` |
| `no matching manifest for linux/arm64` | That image has no ARM build. Our labs avoid this; for other images use `--platform linux/amd64` (emulated, slower) |
| Windows: `bash\r: No such file or directory` | The repo was cloned with CRLF line endings. In WSL: `git config --global core.autocrlf input` and clone again |
| Low disk space | Remove images of labs you finished: `docker image ls`, `docker image rm <name>` |
