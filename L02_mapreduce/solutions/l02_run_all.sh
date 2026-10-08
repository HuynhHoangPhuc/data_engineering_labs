#!/usr/bin/env bash
# L02 reference run (uses the SOLUTION mapper/reducer). From the laptop:
#   cd labs/L01_hdfs && docker compose up -d && bash ../L02_mapreduce/solutions/l02_run_all.sh
# ~3 minutes. Prints the counters needed for the combiner table.
set -eu   # no pipefail: "cmd | grep -q" would fail on SIGPIPE
cd "$(dirname "$0")/../../L01_hdfs"
until docker compose exec -T namenode hdfs dfsadmin -report 2>/dev/null | grep -q "Live datanodes (3)"; do sleep 3; done

docker compose exec -T namenode bash -s <<'IN_CONTAINER'
set -eu   # no pipefail: "cmd | grep -q" would fail on SIGPIPE
hdfs dfsadmin -safemode wait
STREAM=$(ls $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar)
SOL=/lab/L02/solutions

echo "### 1. books"
mkdir -p /lab/L02/work/books && cd /lab/L02/work/books
for id in 1342 11 84 2701; do
  [ -s pg$id.txt ] || curl -sfL -o pg$id.txt https://www.gutenberg.org/cache/epub/$id/pg$id.txt || true
done
ls *.txt >/dev/null 2>&1 || cp /opt/hadoop/*.txt /opt/hadoop/LICENSE-binary .   # offline fallback
echo "local pipe:"; cat *.txt | python3 $SOL/wordcount/mapper.py | sort | python3 $SOL/wordcount/reducer.py | sort -t$'\t' -k2,2nr | head -5

echo "### 2. word count on YARN"
hdfs dfs -mkdir -p /user/root/books && hdfs dfs -put -f /lab/L02/work/books/* /user/root/books/
hdfs dfs -rm -r -f -skipTrash /user/root/wc_out >/dev/null
cd $SOL/wordcount
hadoop jar $STREAM -D mapreduce.job.name=wordcount -files mapper.py,reducer.py \
  -mapper "python3 mapper.py" -reducer "python3 reducer.py" \
  -input /user/root/books -output /user/root/wc_out 2>&1 | grep -E "Launched map|Reduce input groups|completed successfully"
hdfs dfs -cat /user/root/wc_out/part-* | sort -t$'\t' -k2,2nr | head -5

echo "### 3. taxi CSV sample"
[ -s /lab/L02/work/yellow_2024-01_1M.csv ] || python3 /datasets/parquet_to_csv.py \
   /datasets/data/taxi/yellow_tripdata_2024-01.parquet /lab/L02/work/yellow_2024-01_1M.csv --limit 1000000
hdfs dfs -mkdir -p /user/root/taxi_csv
hdfs dfs -D dfs.blocksize=16m -put -f /lab/L02/work/yellow_2024-01_1M.csv /user/root/taxi_csv/

echo "### 4-5. zones with / without combiner"
cd $SOL/zones
for mode in nocombiner combiner; do
  hdfs dfs -rm -r -f -skipTrash /user/root/zones_out_$mode >/dev/null
  COMB=(); [ $mode = combiner ] && COMB=(-combiner "python3 reducer.py")
  start=$(date +%s)
  hadoop jar $STREAM -D mapreduce.job.name=zones_$mode -D mapreduce.job.reduces=2 \
    -files mapper.py,reducer.py,/datasets/data/taxi/taxi_zone_lookup.csv \
    -mapper "python3 mapper.py" "${COMB[@]}" -reducer "python3 reducer.py" \
    -input /user/root/taxi_csv -output /user/root/zones_out_$mode > /tmp/$mode.log 2>&1
  echo "== $mode ($(( $(date +%s) - start )) s)"
  grep -E "Launched map tasks|Map output records|Map output materialized bytes|Combine (input|output) records|Reduce shuffle bytes|Reduce input records" /tmp/$mode.log
done
bash top_zones.sh /user/root/zones_out_combiner
echo "total trips: $(hdfs dfs -cat /user/root/zones_out_combiner/part-* | awk -F'\t' '{s+=$2} END {print s}')"
IN_CONTAINER
