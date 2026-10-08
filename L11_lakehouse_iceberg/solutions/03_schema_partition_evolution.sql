-- L11 steps 4-5 (Spark SQL): schema evolution and partition evolution (metadata-only operations)

-- Schema evolution: add, rename, widen - no data files are rewritten
ALTER TABLE lake.nyc.trips ADD COLUMN congestion_surcharge DOUBLE COMMENT 'added in step 4';
ALTER TABLE lake.nyc.trips RENAME COLUMN trip_distance TO trip_distance_mi;
ALTER TABLE lake.nyc.trips ALTER COLUMN vendor_id TYPE BIGINT;          -- int -> bigint is a safe widening
DESCRIBE TABLE lake.nyc.trips;
-- Old rows return NULL for the new column; new writes can fill it
SELECT count(*) AS rows_with_surcharge FROM lake.nyc.trips WHERE congestion_surcharge IS NOT NULL;

-- Partition evolution: from now on, also partition by vendor (old files keep the old spec!)
ALTER TABLE lake.nyc.trips ADD PARTITION FIELD vendor_id;

-- write some new data under the new spec (late-arriving trips of Jan 31, re-inserted as a demo)
INSERT INTO lake.nyc.trips
SELECT VendorID, CAST(tpep_pickup_datetime AS TIMESTAMP_NTZ), CAST(tpep_dropoff_datetime AS TIMESTAMP_NTZ),
       passenger_count, trip_distance, PULocationID, DOLocationID, payment_type,
       fare_amount, tip_amount, total_amount, congestion_surcharge            -- column order = table order
FROM parquet.`/data/taxi/yellow_tripdata_2024-01.parquet`
WHERE tpep_pickup_datetime >= '2024-01-31 23:00:00' AND tpep_pickup_datetime < '2024-02-01';

-- Each data file remembers the partition spec it was written with
SELECT spec_id, count(*) AS files, sum(record_count) AS rows FROM lake.nyc.trips.files GROUP BY spec_id;
