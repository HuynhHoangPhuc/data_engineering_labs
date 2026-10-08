# L00 exercises - Foundations warm-ups

| | |
|---|---|
| Module | 0 - Foundations: Linux, SQL, Python, Docker, OLTP vs OLAP, file formats |
| Time | 3-4 hours in total; each part (a)-(e) is 30-50 min and can be done on its own |
| Stack | Your shell, a Python 3.10+ venv (pandas, polars, DuckDB, pyarrow), `postgres:18.6` (Olist), `python:3.12-alpine` |
| RAM | < 1.5 GB on the laptop; Docker: ~60 MB (Postgres), ~40 MB (part d) |
| Prerequisites | [L00 setup](../README.md) done: Docker works, `labs/datasets/data/taxi/yellow_tripdata_2024-01.parquet` + `taxi_zone_lookup.csv` downloaded |

## Learning objectives

1. **(a) Linux CLI**: inspect and aggregate a CSV with pipes (`head`, `cut`, `sort`, `uniq`, `grep`, `awk`), use
   redirection, file permissions and environment variables.
2. **(b) SQL**: joins, `GROUP BY`/`HAVING`, CTEs and window functions on the Olist e-commerce database; compare an
   **OLTP** query (one row by key) with an **OLAP** query (scan + aggregate) using `EXPLAIN ANALYZE`.
3. **(c) Python data handling**: CSV vs Parquet with pandas; the same aggregation in pandas, polars and DuckDB;
   generator-based streaming for files larger than memory.
4. **(d) Docker basics**: images vs containers, `run`, ports, bind mounts vs named volumes, `logs`/`exec`,
   `docker compose up/down`, `docker stats`.
5. **(e) File formats**: write the same data as CSV, JSON, Parquet (snappy/zstd) and measure size and query time.

## Files

| Path | What |
|---|---|
| `requirements.txt` | pandas, pyarrow, polars, duckdb |
| `docker-compose.yml` | Postgres 18.6 seeded with the Olist schema + synthetic data (`labs/datasets/olist/init`) |
| `a_linux/my_answers.sh` | **starter**: one TODO per CLI task |
| `b_sql/queries.sql` | **starter**: B1-B2 done, B3-B7 TODO |
| `b_sql/oltp_vs_olap.sql` | complete script: OLTP vs OLAP plans |
| `c_python/data_handling.py` | **starter** (TODO 1-5) |
| `d_docker/` | `www/index.html` + a 2-service `docker-compose.yml` |
| `e_formats/formats.py` | **starter** (TODO 1-3) |
| `solutions/` | `a_linux.sh`, `b_sql.sql`, `c_python/`, `e_formats/`, `ANSWERS.md` (expected output + checkpoint answers) |

`data/` (generated samples) is git-ignored.

---

## Step 0 - Python venv and a CSV sample (10 min)

```bash
cd labs/L00_setup/exercises
python3 --version                         # 3.10 or newer
python3 -m venv .venv
source .venv/bin/activate                 # Windows (WSL): the same; PowerShell: .venv\Scripts\Activate.ps1
pip install -r requirements.txt
python ../../datasets/parquet_to_csv.py ../../datasets/data/taxi/yellow_tripdata_2024-01.parquet \
       data/taxi_sample.csv --limit 100000 --header
```

Expected: `wrote 100,000 rows -> data/taxi_sample.csv (9.3 MB)`. The sample is the **first** 100,000 rows of the
file (pickups on 1-2 January, plus a few stray dates - see A8).

---

## (a) Linux CLI (40 min)

Columns of the CSV (1-based, as `cut`/`awk` count them): 2 `tpep_pickup_datetime`, 5 `trip_distance`,
7 `store_and_fwd_flag`, 8 `PULocationID`, 10 `payment_type`, 11 `fare_amount`, 17 `total_amount`.

### a.1 Pipes and text tools

Write one pipeline per task into `a_linux/my_answers.sh`, then `bash a_linux/my_answers.sh`.

| Task | Hint | Expected |
|---|---|---|
| A1 number of data rows | `tail -n +2`, `wc -l` | 100000 |
| A2 header, one column per line, numbered | `head -1`, `tr ',' '\n'`, `nl` | 19 lines, `1 VendorID` ... `19 Airport_fee` |
| A3 trips per `payment_type` | `cut -d, -f10 \| sort \| uniq -c \| sort -rn` | `75748 1`, `21277 2`, `2129 4`, `846 3` |
| A4 top 5 pickup zone ids | same idea, column 8 | `8035 132`, `4302 186`, `4073 161`, `3879 138`, `3360 237` |
| A5 average fare, total revenue | `awk -F, 'NR>1 {s+=$11} END {print s/(NR-1)}'` | `avg_fare=21.27 revenue=3004380.42` |
| A6 trips longer than 20 miles | `awk -F, 'NR>1 && $5>20'` | 1794 |
| A7 `store_and_fwd_flag = Y` | `grep -c ',Y,'` | 312 |
| A8 top 3 pickup hours | `cut -d, -f2 \| cut -c12-13` | `7708 12`, `7006 11`, `6461 00` |
| A9 top 5 pickup **zone names** | `awk` with two files (`NR==FNR` = first file) | JFK Airport 8035, Penn Station/Madison Sq West 4302, Midtown Center 4073, LaGuardia Airport 3879, Upper East Side South 3360 |
| A10 rows with `fare_amount <= 0` to `data/bad_fares.csv` with header | `{ head -1 f; awk ...; } > out` | 1612 rows |

Also try: `cut -d, -f2 data/taxi_sample.csv | cut -c1-10 | sort | uniq -c` - why are there trips from 2023-12-31
in a January file? (Answer in L05/L13: data quality.)

### a.2 Redirection, files, processes

```bash
ls -lh data/ ; du -sh data/ ; df -h .                       # sizes of files, folders, disks
wc -l data/taxi_sample.csv > data/count.txt                 # > overwrite, >> append
ls not-there 2> data/errors.txt ; cat data/errors.txt       # 2> redirects stderr
ls data/*.csv | xargs -n1 wc -l                             # xargs: lines -> arguments
find .. -name "*.sh" -size -2k                              # find files by name/size
ps aux | grep -i docker | head -3                           # running processes
```

### a.3 Permissions

```bash
mkdir -p data/perm && cd data/perm
printf '#!/usr/bin/env bash\necho "Hello from $0, TAXI_FILE=${TAXI_FILE:-<unset>}"\n' > hello.sh
ls -l hello.sh            # -rw-r--r--   owner can read/write, group/others read
./hello.sh                # permission denied (exit code 126)
chmod u+x hello.sh        # or chmod 744
ls -l hello.sh            # -rwxr--r--
./hello.sh                # Hello from ./hello.sh, TAXI_FILE=<unset>
echo "DB_PASSWORD=olist" > secrets.env && chmod 600 secrets.env && ls -l secrets.env    # -rw-------
umask                     # 022 -> new files get 644, new folders 755
```

### a.4 Environment variables

```bash
TAXI_FILE=a.csv; ./hello.sh          # <unset>: a shell variable is NOT passed to child processes
export TAXI_FILE; ./hello.sh         # a.csv:  exported = environment variable, inherited by children
TAXI_FILE=b.csv ./hello.sh           # b.csv:  set for one command only
printf 'DB_USER=olist\nDB_PASSWORD=olist\n' > .env
set -a; source .env; set +a; env | grep ^DB_     # load a .env file (what docker compose does with .env)
echo $PATH | tr ':' '\n' | head -3 ; which python3   # how the shell finds programs
cd ../..
```

Solution script: `bash solutions/a_linux.sh`.

---

## (b) SQL on the Olist OLTP database (50 min)

```bash
docker compose up -d                           # postgres:18.6, seeded on first start (~10 s)
docker compose ps                              # l00-postgres ... (healthy)
docker compose exec postgres psql -U olist -c "\dt"
```

Expected: 6 tables - `customers`, `sellers`, `products`, `orders`, `order_items`, `order_payments` (the Olist
schema; see `labs/datasets/olist/init/01_schema.sql` for the columns and keys). Draw the relationships:
`customers 1-n orders 1-n order_items n-1 products / sellers`, `orders 1-n order_payments`.

### b.1 Queries - complete `b_sql/queries.sql`

```bash
docker compose exec postgres psql -U olist -f /sql/queries.sql      # b_sql/ is mounted at /sql
```

| # | Concept | Task | Expected (solution) |
|---|---|---|---|
| B1 | `UNION ALL` | rows per table (given) | customers 5000, sellers 312, products 1500, orders 5000, order_items 5852, order_payments 5472 |
| B2 | `JOIN` + `GROUP BY` | delivered orders per customer state, top 5 (given) | SP 2013, RJ 608, MG 567, RS 253, PR 239 |
| B3 | `HAVING` | categories with > 300 items sold, by revenue | 6 rows: `beleza_saude` 504 items, avg 117.11, revenue 59,023.03 first; `utilidades_domesticas` 27,081.90 last |
| B4 | anti-join (`LEFT JOIN ... IS NULL`, `NOT EXISTS`) | orders without items per status; products never sold | `unavailable` 37 orders without items; 217 products never sold (with `NOT EXISTS`) |
| B5 | CTEs + `LAG` | 2018 monthly revenue + month-over-month growth | 8 months (2018-01 .. 2018-08): Jan 292 orders / 39,016.29, Feb -11.2 %, ..., Aug 380 / 55,855.11 / +20.4 % |
| B6 | `RANK() OVER (PARTITION BY ...)` | top 2 sellers by revenue per state, 3 biggest states | SP: `02defec5` 82,512.47, `629356d1` 40,937.11; PR: `afecf159` 16,928.81, ...; MG: `81a647ce` 13,400.44, ... |
| B7 | running total, moving average | cumulative revenue per day, 1-7 March 2018 | 2018-03-01 1,851.54 ... 2018-03-07 773.85, running total 9,531.62 |

Solution: `docker compose exec -T postgres psql -U olist < solutions/b_sql.sql`.

### b.2 OLTP vs OLAP - run `b_sql/oltp_vs_olap.sql`

```bash
docker compose exec -T postgres psql -U olist < b_sql/oltp_vs_olap.sql
```

The script makes a 1.17 M-row copy of `order_items`, then runs (with `EXPLAIN (ANALYZE, BUFFERS)`):

| | OLTP: one order line by key | OLAP: revenue per seller state |
|---|---|---|
| Plan | `Index Scan using order_items_big_pkey` (B-tree) | `Parallel Seq Scan` (2 workers) -> `Hash Join` sellers -> `Partial HashAggregate` -> `Gather Merge` -> `Finalize GroupAggregate` |
| Rows read | 1 | 1,170,400 |
| Buffers (8 kB pages) | 10 (`hit=4 read=6`) | ~25,000 (`hit=2895 read=22091`) = the whole 195 MB table |
| Time | ~0.2 ms (update in a transaction: ~0.1 ms) | ~160 ms |

Discuss: an OLTP database (row store, B-tree indexes, many tiny transactions) is built for the left column; an
analytics engine (column store: Parquet, DuckDB, warehouses) for the right one - it would read only the `seller_id`
and `price` columns instead of every 8 kB page of every row. That is why we copy data out of OLTP systems into
data lakes/warehouses (Modules 3, 9, 10). Part (e) shows the column-store side.

```bash
docker compose down -v                         # when you are done with (b)
```

---

## (c) Python data handling (40 min)

Complete TODO 1-5 in `c_python/data_handling.py`, run `python c_python/data_handling.py`.
Expected (solution, Apple M-series laptop; your times will differ):

```
1. pandas: CSV vs Parquet
  pd.read_csv(sample, 100k rows)                   0.176 s
  dtypes inferred from text, e.g. passenger_count -> int64, store_and_fwd_flag -> str
  memory: 15.3 MB for 100,000 rows
  pd.read_parquet(full month, 2 columns)           0.106 s
  2,964,624 rows; the types come from the Parquet schema (no guessing)

2. avg fare and trip count per payment_type on the full month (3 M rows)
  pandas  read_parquet + groupby                   0.120 s
  polars  scan_parquet (lazy) + group_by           0.116 s
  DuckDB  SQL directly on the Parquet file         0.038 s
 payment_type   trips  avg_fare
            0  140162     20.02
            1 2319046     18.56
            2  439191     17.87
            3   19597      6.75
            4   46628      1.33
  -> all three engines return the same numbers

3. generators: process the CSV one row at a time
  10,464 trips with fare > 50, revenue 982,596.86 - 1.26 s, peak Python memory 0.04 MB
  the same file loaded at once with pandas: peak 27.2 MB
  batches instead of rows: pyarrow iter_batches over the 3 M-row Parquet file
    batch 1: 500,000 rows   ... batch 6: 464,624 rows
```

Take-aways: reading 2 of 19 columns of 3 M Parquet rows is faster than parsing 100 k CSV rows; pandas, polars
(lazy, multi-threaded) and DuckDB (SQL, multi-threaded) agree on the numbers, the API differs. A generator keeps
memory flat whatever the file size - the idea behind streaming and chunked processing (`chunksize=`, `iter_batches`,
Spark partitions). (`tracemalloc` makes the generator loop slower; without it ~0.2 s.)

---

## (d) Docker basics (40 min)

### d.1 Images, containers, architecture

```bash
docker pull python:3.12-alpine                        # ~20 MB download (also used by L13)
docker image ls python
docker run --rm python:3.12-alpine python -c "import platform; print(platform.machine(), platform.python_version())"
docker ps -a | head -3                                # --rm: the container is gone after it exits
```

Expected: `aarch64 3.12.x` on Apple Silicon, `x86_64 3.12.x` on Intel/AMD - the same image tag is a multi-arch
manifest list.

### d.2 Ports, bind mounts, logs, exec

```bash
docker run -d --name l00-web -p 8000:8000 -v "$PWD/d_docker/www:/www:ro" \
  python:3.12-alpine python -m http.server 8000 --directory /www
curl -s localhost:8000 | grep h1                      # <body><h1>Hello from a bind mount!</h1>
# edit d_docker/www/index.html on your laptop, then curl again -> the change is visible immediately
docker logs l00-web | tail -2                         # the HTTP access log
docker exec -it l00-web sh -c 'ls -l /www; touch /www/x'   # touch: Read-only file system (we mounted :ro)
docker run -d --name l00-web2 -p 8001:8000 python:3.12-alpine python -m http.server 8000
curl -s localhost:8001 | head -3                      # a 2nd container: same container port, different HOST port
docker run -d --name l00-web3 -p 8000:8000 python:3.12-alpine sleep 60   # error: port is already allocated
docker rm -f l00-web l00-web2 l00-web3
```

### d.3 Named volumes

```bash
docker volume create l00-vol
docker run --rm -v l00-vol:/data python:3.12-alpine sh -c 'echo "written at $(date -u)" > /data/note.txt'
docker run --rm -v l00-vol:/data python:3.12-alpine cat /data/note.txt     # data outlived the 1st container
docker volume ls | grep l00 ; docker volume rm l00-vol
```

### d.4 Compose: up, ps, logs, exec, stats, down

```bash
cd d_docker
docker compose up -d                       # network + volume + 2 containers
docker compose ps
sleep 2; curl -s localhost:8000 | grep h1  # give the web server a second to start
sleep 6; docker compose exec counter tail -3 /data/ticks.txt
docker stats --no-stream                   # CPU / memory per container (MEM LIMIT = mem_limit in the YAML)
docker compose down                        # containers + network removed, volume KEPT
docker compose up -d && docker compose exec counter wc -l /data/ticks.txt   # the old ticks are still there
docker compose down -v                     # now the volume is deleted too
docker system df                           # disk used by images, containers, volumes, build cache
cd ..
```

Expected `docker stats` (values vary): `l00_docker-web-1  0.05%  13.2MiB / 64MiB`, `l00_docker-counter-1  0.00%  1.6MiB / 32MiB`;
after `down` + `up` the tick file still has the old lines (`7 /data/ticks.txt` in our run); after `down -v` the volume is gone.

---

## (e) File formats: CSV vs JSON vs Parquet (30 min)

Complete TODO 1-3 in `e_formats/formats.py` and run `python e_formats/formats.py` (1,000,000 taxi rows; ~30 s,
~600 MB of files in `data/formats/`).

Reference results (solution; Apple Silicon M-series, DuckDB 1.5.6, pandas 3.0.6 - times vary, sizes do not):

| Format | Size (MB) | vs CSV | Write (s) | DuckDB query (s) | pandas, 2 columns (s) |
|---|---|---|---|---|---|
| CSV | 101.4 | 100 % | 0.55 | 0.268 | 0.591 |
| CSV + gzip | 18.9 | 19 % | 6.02 | 0.519 | 0.642 |
| JSON lines | 416.4 | 411 % | 1.15 | 0.206 | n/a |
| Parquet, uncompressed | 29.1 | 29 % | 0.30 | 0.015 | 0.178 |
| Parquet, snappy | 20.5 | 20 % | 0.17 | 0.010 | 0.027 |
| Parquet, zstd | 15.3 | 15 % | 0.23 | 0.015 | 0.029 |

Query: `SELECT payment_type, count(*), avg(fare_amount) ... GROUP BY payment_type` (same result for every file).

Observations:

* **Columnar + typed + compressed**: Parquet is 3.5x smaller than CSV even *without* compression (binary types,
  dictionary/run-length encoding), and the query reads only 2 of 19 column chunks: ~20x faster than CSV.
* gzip makes CSV small but slow to write and read, and a `.csv.gz` file **cannot be split** between parallel readers.
* JSON repeats every key on every line: 4x larger than CSV - fine for APIs and events, poor for analytics.
* snappy vs zstd: zstd is ~25 % smaller, decompression a bit slower - the usual lake default today is zstd or snappy.
* The Parquet footer (TODO 3) stores **min/max statistics** per row group (here 9 row groups): engines skip row
  groups whose range cannot match a filter (predicate pushdown). The minimum pickup time is **2002-12-31** - a data
  quality issue visible from metadata alone.

```bash
rm -rf data/formats            # ~600 MB
```

---

## Checkpoint questions

1. In A3, why must `sort` come before `uniq -c`? What would `uniq -c` alone print?
2. A9 uses `awk` with two files. What do `NR` and `FNR` mean, and why is `NR == FNR` true only for the first file?
3. `chmod 600 secrets.env` vs `chmod 644`: who can read the file in each case? Why does a script need `x`?
4. What is the difference between `TAXI_FILE=a.csv` and `export TAXI_FILE=a.csv`? How does `docker compose` use a `.env` file?
5. In B5, why do we sum payments per order in a CTE *before* joining to orders? What happens to revenue otherwise?
6. Explain the difference between `RANK()`, `DENSE_RANK()` and `ROW_NUMBER()` with an example of ties.
7. From the OLTP vs OLAP plans: which access method does each query use, and why is the OLTP query fast regardless of table size?
8. Why can DuckDB answer the aggregation on a Parquet file faster than on the CSV file with the same rows?
9. What is the difference between a **bind mount** and a **named volume**? Which one survives `docker compose down`? `down -v`?
10. Which format would you choose for: (a) a nightly export to a partner, (b) a data lake table, (c) a Kafka event? Why?

Answers: [`solutions/ANSWERS.md`](solutions/ANSWERS.md).

## Cleanup

```bash
docker compose down -v                     # (b) Postgres
(cd d_docker && docker compose down -v)    # (d), if still running
docker rm -f l00-web l00-web2 l00-web3 2>/dev/null; docker volume rm l00-vol 2>/dev/null
rm -rf data
deactivate
```

## Troubleshooting

| Symptom | Fix |
|---|---|
| `awk`/`sort` results differ slightly on macOS vs Linux | BSD vs GNU tools; the commands here work on both. For locale-dependent sorting use `LC_ALL=C sort` |
| `uniq -c` shows the same value several times | Input was not sorted - `sort` first |
| `psql: error: connection ... failed` | Container not healthy yet (`docker compose ps`), or another lab's Postgres owns port 5432 (`docker ps`) |
| `relation "orders" does not exist` | The volume was created before the init scripts were mounted: `docker compose down -v && docker compose up -d` |
| `pip install` fails building pandas/polars | Python too old/new for the wheels: use Python 3.10-3.13 |
| `Bind for 0.0.0.0:8000 failed: port is already allocated` | That is exercise d.2 on purpose - or another program uses 8000 |
| `ModuleNotFoundError: pyarrow` when making the sample | Activate the venv first (`source .venv/bin/activate`) |
