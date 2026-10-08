#!/usr/bin/env bash
# L04 Part 1 -- all Sqoop commands (reference). Run INSIDE the sqoop container:
#   docker compose exec sqoop bash /lab/solutions/sqoop_all.sh
# Each step prints a header; ~4-6 minutes in total (every Sqoop job = JVM start + codegen + javac).
set -eu   # (no pipefail: "hdfs dfs -cat | head" closes the pipe early on purpose)
JDBC=jdbc:postgresql://postgres:5432/olist
mkdir -p /lab/work
echo -n olist > /lab/work/.pg_password           # password file (no trailing newline!)
chmod 400 /lab/work/.pg_password
CONN=(--connect "$JDBC" --username olist --password-file file:///lab/work/.pg_password)
NULLS=(--null-string '\\N' --null-non-string '\\N' --input-null-string '\\N' --input-null-non-string '\\N')   # write NULL as \N (Hive/Spark convention)
hdfs dfs -mkdir -p /data/olist/sqoop

echo "### 1. explore the source"
sqoop list-tables "${CONN[@]}" 2>/dev/null
sqoop eval "${CONN[@]}" --query "SELECT min(order_seq), max(order_seq), count(*) FROM orders" 2>/dev/null

echo "### 2. full import, 4 mappers split on order_seq"
sqoop import "${CONN[@]}" --table orders --split-by order_seq -m 4 "${NULLS[@]}" \
  --target-dir /data/olist/sqoop/orders_full --delete-target-dir 2>&1 \
  | grep -E "BoundingValsQuery|Retrieved|Transferred|number of splits" || true
hdfs dfs -ls /data/olist/sqoop/orders_full
hdfs dfs -cat /data/olist/sqoop/orders_full/part-m-00000 | head -2
for f in $(hdfs dfs -ls -C /data/olist/sqoop/orders_full/part-m-*); do echo "$f $(hdfs dfs -cat $f | wc -l)"; done

echo "### 2b. skewed split column: customers split on customer_zip_code_prefix"
sqoop import "${CONN[@]}" --table customers --split-by customer_zip_code_prefix -m 4 "${NULLS[@]}" \
  --target-dir /data/olist/sqoop/customers_full --delete-target-dir 2>&1 | grep -E "BoundingValsQuery|Retrieved" || true
for f in $(hdfs dfs -ls -C /data/olist/sqoop/customers_full/part-m-*); do echo "$f $(hdfs dfs -cat $f | wc -l)"; done

echo "### 3. incremental APPEND on order_seq (initial load = everything > 0)"
hdfs dfs -rm -r -f -skipTrash /data/olist/sqoop/orders_append >/dev/null
sqoop import "${CONN[@]}" --table orders --split-by order_seq -m 2 "${NULLS[@]}" \
  --target-dir /data/olist/sqoop/orders_append \
  --incremental append --check-column order_seq --last-value 0 2>&1 \
  | grep -E "Incremental import|--last-value|Retrieved|Lower bound|Upper bound" || true

echo "### 3b. incremental LASTMODIFIED, initial load (everything changed after 1970)"
hdfs dfs -rm -r -f -skipTrash /data/olist/sqoop/orders_lastmod >/dev/null
sqoop import "${CONN[@]}" --table orders -m 1 "${NULLS[@]}" \
  --target-dir /data/olist/sqoop/orders_lastmod \
  --incremental lastmodified --check-column updated_at --last-value '1970-01-01 00:00:00' 2>&1 \
  | tee /tmp/lastmod.log | grep -E "Lower bound|Upper bound|Retrieved|--last-value" || true
# remember the value Sqoop tells us to use next time
sed -n 's/.*--last-value \(.*\)$/\1/p' /tmp/lastmod.log | tail -1 > /lab/work/lastmod_value.txt
echo "next --last-value: $(cat /lab/work/lastmod_value.txt)"

echo "### 4. saved job (keeps --last-value in the Sqoop metastore)"
sqoop job --delete orders_incr >/dev/null 2>&1 || true
sqoop job --create orders_incr -- import "${CONN[@]}" --table orders --split-by order_seq -m 1 "${NULLS[@]}" \
  --target-dir /data/olist/sqoop/orders_append \
  --incremental append --check-column order_seq --last-value 5000 2>/dev/null
sqoop job --list 2>/dev/null
sqoop job --show orders_incr 2>/dev/null | grep -E "incremental.last.value|incremental.col|incremental.mode"
echo "(now run sql/simulate_changes.sql on postgres, then: sqoop job --exec orders_incr)"
