-- L03 step 4: same query, three storage formats. Beeline prints the elapsed time
-- after every statement ("... rows selected (12.3 seconds)").
USE taxi;
SET hive.compute.query.using.stats = false;      -- force a real scan (no answers from metastore stats)
SET hive.query.results.cache.enabled = false;    -- no cached results between runs
SET hive.fetch.task.conversion = none;

-- Q1: busiest pickup zones in January 2024 (reads 2 of 19 columns)
SELECT pulocationid, count(*) AS trips, round(avg(fare_amount), 2) AS avg_fare
FROM trips_csv
WHERE tpep_pickup_datetime >= '2024-01-01' AND tpep_pickup_datetime < '2024-02-01'
GROUP BY pulocationid ORDER BY trips DESC LIMIT 5;

SELECT pulocationid, count(*) AS trips, round(avg(fare_amount), 2) AS avg_fare
FROM trips_orc
WHERE pickup_month = '2024-01'
GROUP BY pulocationid ORDER BY trips DESC LIMIT 5;

SELECT pulocationid, count(*) AS trips, round(avg(fare_amount), 2) AS avg_fare
FROM trips_parquet
WHERE pickup_month = '2024-01'
GROUP BY pulocationid ORDER BY trips DESC LIMIT 5;

-- Q2: join with the zone names (small table -> map join)
SELECT z.borough, count(*) AS trips, CAST(round(sum(t.total_amount)) AS BIGINT) AS revenue
FROM trips_orc t JOIN zones z ON t.pulocationid = CAST(z.locationid AS INT)
WHERE t.pickup_month = '2024-01'
GROUP BY z.borough ORDER BY trips DESC;
