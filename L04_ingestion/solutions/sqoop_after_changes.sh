#!/usr/bin/env bash
# L04 Part 1 (continued) -- run AFTER sql/simulate_changes.sql. Inside the sqoop container:
#   docker compose exec sqoop bash /lab/solutions/sqoop_after_changes.sh
set -eu   # (no pipefail: "hdfs dfs -cat | head" closes the pipe early on purpose)
JDBC=jdbc:postgresql://postgres:5432/olist
CONN=(--connect "$JDBC" --username olist --password-file file:///lab/work/.pg_password)
NULLS=(--null-string '\\N' --null-non-string '\\N' --input-null-string '\\N' --input-null-non-string '\\N')

echo "### 4b. exec the saved job -> only the NEW orders are appended"
sqoop job --exec orders_incr 2>&1 | grep -E "Lower bound|Upper bound|Retrieved|--last-value" || true
sqoop job --show orders_incr 2>/dev/null | grep "incremental.last.value"
hdfs dfs -ls /data/olist/sqoop/orders_append
echo "rows in append dir: $(hdfs dfs -cat '/data/olist/sqoop/orders_append/part-m-*' | wc -l)"

echo "### 5. incremental LASTMODIFIED + --merge-key: new AND updated rows, merged into the snapshot"
LAST=$(cat /lab/work/lastmod_value.txt)
echo "using --last-value '$LAST'"
sqoop import "${CONN[@]}" --table orders -m 1 "${NULLS[@]}" \
  --target-dir /data/olist/sqoop/orders_lastmod \
  --incremental lastmodified --check-column updated_at --last-value "$LAST" --merge-key order_id 2>&1 \
  | grep -E "Lower bound|Upper bound|Retrieved|--last-value|merge|Merge" || true
hdfs dfs -ls /data/olist/sqoop/orders_lastmod
echo "rows in lastmod dir: $(hdfs dfs -cat '/data/olist/sqoop/orders_lastmod/part-*' | wc -l)   (= baseline + the new orders; updated orders were replaced in place, not duplicated)"
hdfs dfs -cat '/data/olist/sqoop/orders_lastmod/part-*' | grep -E ',(created|delivered),' | awk -F, -v d="$(date -u +%Y-%m-%d)" 'index($0, d)' | head -5

echo "### 6. export: HDFS -> new Postgres table orders_copy"
sqoop eval "${CONN[@]}" --query "DROP TABLE IF EXISTS orders_copy" >/dev/null 2>&1
sqoop eval "${CONN[@]}" --query "CREATE TABLE orders_copy (LIKE orders INCLUDING DEFAULTS)" >/dev/null 2>&1
sqoop export "${CONN[@]}" --table orders_copy --export-dir /data/olist/sqoop/orders_full -m 2 \
  --input-null-string '\\N' --input-null-non-string '\\N' 2>&1 | grep -E "Exported|Transferred" || true
sqoop eval "${CONN[@]}" --query "SELECT count(*) AS rows_exported FROM orders_copy" 2>/dev/null
