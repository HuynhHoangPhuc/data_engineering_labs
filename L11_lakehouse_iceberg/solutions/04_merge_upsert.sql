-- L11 step 6 (Spark SQL): MERGE INTO = idempotent upserts (and row-level DELETE)

-- A small aggregate table keyed by (trip_date, pu_location_id), first loaded with Jan 1-15
CREATE TABLE IF NOT EXISTS lake.nyc.zone_daily (
    trip_date       DATE,
    pu_location_id  INT,
    trips           BIGINT,
    revenue         DOUBLE,
    updated_at      TIMESTAMP
) USING iceberg
PARTITIONED BY (months(trip_date));

INSERT INTO lake.nyc.zone_daily
SELECT CAST(pickup_ts AS DATE), pu_location_id, count(*), round(sum(total_amount), 2), current_timestamp()
FROM lake.nyc.trips
WHERE pickup_ts >= '2024-01-01' AND pickup_ts < '2024-01-16'
GROUP BY 1, 2;

SELECT count(*) AS rows_before_merge, max(trip_date) AS max_day FROM lake.nyc.zone_daily;

-- Re-compute Jan 10-20 (overlaps 10-15 -> UPDATE, 16-20 -> INSERT) and upsert it
MERGE INTO lake.nyc.zone_daily t
USING (
    SELECT CAST(pickup_ts AS DATE) AS trip_date, pu_location_id,
           count(*) AS trips, round(sum(total_amount), 2) AS revenue
    FROM lake.nyc.trips
    WHERE pickup_ts >= '2024-01-10' AND pickup_ts < '2024-01-21'
    GROUP BY 1, 2
) s
ON t.trip_date = s.trip_date AND t.pu_location_id = s.pu_location_id
WHEN MATCHED AND (t.trips <> s.trips OR t.revenue <> s.revenue) THEN
    UPDATE SET trips = s.trips, revenue = s.revenue, updated_at = current_timestamp()
WHEN NOT MATCHED THEN
    INSERT (trip_date, pu_location_id, trips, revenue, updated_at)
    VALUES (s.trip_date, s.pu_location_id, s.trips, s.revenue, current_timestamp());

SELECT count(*) AS rows_after_merge, max(trip_date) AS max_day FROM lake.nyc.zone_daily;
-- run the MERGE a second time: nothing changes (idempotent) -> check the snapshot summary
SELECT snapshot_id, operation, summary['added-records'] AS added, summary['deleted-records'] AS deleted
FROM lake.nyc.zone_daily.snapshots ORDER BY committed_at;

-- Row-level delete on the big table: drop trips with bogus pickup dates (outside Jan 2024)
SELECT count(*) AS bogus FROM lake.nyc.trips WHERE pickup_ts < '2024-01-01' OR pickup_ts >= '2024-02-01';
DELETE FROM lake.nyc.trips WHERE pickup_ts < '2024-01-01' OR pickup_ts >= '2024-02-01';
