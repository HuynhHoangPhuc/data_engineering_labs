#!/usr/bin/env bash
# L03 reference run, from a fresh stack:
#   cd labs/L03_hive && docker compose up -d --build && bash solutions/l03_run_all.sh
# Uses CSV_LIMIT rows of 2024-01 (default: all ~2.96 M rows; set CSV_LIMIT=1000000 for a quick run).
set -eu   # no pipefail: "cmd | grep -q" would fail on SIGPIPE
cd "$(dirname "$0")/.."
CSV_LIMIT=${CSV_LIMIT:-0}
MONTHS=${MONTHS:-2024-01}

echo "### waiting for HDFS and HiveServer2"
until docker compose exec -T namenode hdfs dfsadmin -safemode get 2>/dev/null | grep -q OFF; do sleep 3; done
until docker compose exec -T hiveserver2 bash -c 'echo > /dev/tcp/localhost/10000' 2>/dev/null; do sleep 3; done

echo "### 1. CSV files into HDFS"
for m in ${MONTHS}; do
  docker compose exec -T namenode bash -c "
    f=/datasets/data/taxi/csv/yellow_${m}.csv
    [ -s \$f ] || python3 /datasets/parquet_to_csv.py /datasets/data/taxi/yellow_tripdata_${m}.parquet \$f --limit ${CSV_LIMIT}
    hdfs dfs -mkdir -p /data/taxi_csv /data/zones
    hdfs dfs -put -f \$f /data/taxi_csv/"
done
docker compose exec -T namenode bash -c 'hdfs dfs -put -f /datasets/data/taxi/taxi_zone_lookup.csv /data/zones/ && hdfs dfs -ls /data/taxi_csv /data/zones'

for f in 01_external_tables 02_partitioned_tables 03_compare_formats 04_partition_pruning 05_bucketing; do
  echo "### ${f}"
  ./run_sql.sh solutions/${f}.sql | grep -v -E "^INFO|^WARN|^\|\s+[a-z_]+\s+[A-Za-z]" | grep -E "^\||selected|affected|Error" || true
done

echo "### storage per format"
docker compose exec -T namenode hdfs dfs -du -h /data/taxi_csv /user/hive/warehouse/taxi.db
echo "### metastore tables (Postgres)"
docker compose exec -T postgres psql -U hive -d metastore -c \
  'SELECT t."TBL_NAME", t."TBL_TYPE", s."LOCATION" FROM "TBLS" t JOIN "SDS" s ON t."SD_ID" = s."SD_ID" ORDER BY 1;'
