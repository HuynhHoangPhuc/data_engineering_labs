#!/usr/bin/env bash
# L01 reference walkthrough -- runs every task non-interactively from the laptop.
#   cd labs/L01_hdfs && docker compose up -d --build && bash solutions/l01_run_all.sh
# Takes ~3 min (most of it waiting for the NameNode to declare datanode3 dead).
set -eu   # no pipefail: "cmd | grep -q" would fail on SIGPIPE
cd "$(dirname "$0")/.."
nn() { docker compose exec -T namenode bash -c "$*" 2>&1 | grep -v -E "^WARNING: |NativeCodeLoader"; }

echo "### 1. wait for 3 live DataNodes"
until docker compose exec -T namenode hdfs dfsadmin -report 2>/dev/null | grep -q "Live datanodes (3)"; do sleep 3; done
nn 'hdfs dfsadmin -safemode wait; hdfs dfsadmin -report | grep -E "Live datanodes|^Name"'

echo "### 2. dfs shell basics"
nn 'hdfs dfs -mkdir -p /user/root /data/taxi/default_blocks /data/taxi/small_blocks
hdfs dfs -put -f /datasets/data/taxi/taxi_zone_lookup.csv /data/taxi/
hdfs dfs -cat /data/taxi/taxi_zone_lookup.csv | grep -c Manhattan
hdfs dfs -stat "%n blocksize=%o repl=%r size=%b" /data/taxi/taxi_zone_lookup.csv      # TODO 2.1'

echo "### 3. default vs 16 MB blocks"
nn 'hdfs dfs -put -f /datasets/data/taxi/yellow_tripdata_2024-01.parquet /data/taxi/default_blocks/
hdfs dfs -D dfs.blocksize=16m -put -f /datasets/data/taxi/yellow_tripdata_2024-01.parquet /data/taxi/small_blocks/
hdfs dfs -stat "%n blocksize=%o repl=%r size=%b" /data/taxi/*/yellow_tripdata_2024-01.parquet
hdfs fsck /data/taxi/small_blocks -files -blocks -locations | grep -E "bytes|Live_repl"
hdfs fsck /data/taxi/default_blocks -files -blocks | grep -E "bytes|len="'

echo "### 4. setrep 2 + TODO 4.1 (replication 1 at write time)"
nn 'hdfs dfs -setrep -w 2 /data/taxi/small_blocks/yellow_tripdata_2024-01.parquet
hdfs dfs -D dfs.replication=1 -put -f /datasets/data/taxi/taxi_zone_lookup.csv /tmp/one_replica.csv
hdfs dfs -ls /tmp/one_replica.csv /data/taxi/small_blocks
hdfs dfs -du -h /data/taxi'
nn 'hdfs dfs -rm -skipTrash /tmp/one_replica.csv'   # a replication-1 file would go MISSING in step 5

echo "### 5. kill datanode3"
nn 'hdfs fsck /data/taxi/small_blocks -files -blocks -locations | grep Live_repl'
docker compose stop datanode3
start=$(date +%s)
until docker compose exec -T namenode hdfs dfsadmin -report 2>/dev/null | grep -q "Live datanodes (2)"; do sleep 5; done
echo "datanode3 reported dead after $(( $(date +%s) - start )) s"
# The heartbeat monitor runs every recheck-interval (30 s): only then are the dead node's replicas
# removed from the block map -> wait until the replication-3 file shows up as under-replicated.
until docker compose exec -T namenode hdfs fsck /data/taxi/default_blocks 2>/dev/null | grep -q "Under replicated"; do sleep 3; done
echo "replicas of datanode3 removed from the block map after $(( $(date +%s) - start )) s"
# re-replication of the replication-2 file: poll until its blocks are fully replicated again
t1=$(date +%s)
until ! docker compose exec -T namenode hdfs fsck /data/taxi/small_blocks 2>/dev/null | grep -q "Under replicated"; do
  sleep 3; [ $(( $(date +%s) - t1 )) -gt 180 ] && { echo "re-replication not finished after 180 s"; break; }
done
echo "replication-2 file healed $(( $(date +%s) - t1 )) s after the replicas were removed"
nn 'hdfs fsck /data -files -blocks -locations | grep -E "^/data/taxi/.*parquet|Live_repl|Under-replicated blocks|Status"
hdfs dfs -get -f /data/taxi/default_blocks/yellow_tripdata_2024-01.parquet /tmp/check.parquet
md5sum /tmp/check.parquet /datasets/data/taxi/yellow_tripdata_2024-01.parquet'
docker compose start datanode3
until docker compose exec -T namenode hdfs dfsadmin -report 2>/dev/null | grep -q "Live datanodes (3)"; do sleep 3; done
sleep 10
nn 'hdfs fsck / | grep -E "Under-replicated|Over-replicated|Status"'

echo "### 6. safe mode"
nn 'hdfs dfsadmin -safemode enter
hdfs dfs -put /datasets/data/taxi/taxi_zone_lookup.csv /tmp/x.csv || echo "(expected failure: safe mode)"
hdfs dfsadmin -safemode leave'
echo "### done"
