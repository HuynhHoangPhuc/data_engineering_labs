-- L11 step 3 (Trino): the SAME table, read by another engine through the same REST catalog
--   docker compose exec trino trino --catalog lake --schema nyc -f /dev/stdin < solutions/02_trino_queries.sql
SHOW TABLES;
SELECT count(*) AS trips FROM trips;

-- busiest days
SELECT CAST(pickup_ts AS date) AS day, count(*) AS trips, round(sum(total_amount)) AS revenue
FROM trips
WHERE pickup_ts >= TIMESTAMP '2024-01-01' AND pickup_ts < TIMESTAMP '2024-02-01'
GROUP BY 1 ORDER BY trips DESC LIMIT 5;

-- partition pruning: only one day's files are read
EXPLAIN ANALYZE
SELECT count(*) FROM trips
WHERE pickup_ts >= TIMESTAMP '2024-01-15' AND pickup_ts < TIMESTAMP '2024-01-16';

-- metadata tables in Trino use the "$" suffix
SELECT snapshot_id, operation, committed_at FROM "trips$snapshots" ORDER BY committed_at;
