-- L11 step 2 (Spark SQL): create a partitioned Iceberg table and load one month of taxi trips
--   docker compose exec spark /opt/spark/bin/spark-sql -f /opt/lab/solutions/01_create_and_load.sql
CREATE NAMESPACE IF NOT EXISTS lake.nyc;

CREATE TEMPORARY VIEW raw_trips
USING parquet OPTIONS (path '/data/taxi/yellow_tripdata_2024-01.parquet');

CREATE TABLE IF NOT EXISTS lake.nyc.trips (
    vendor_id        INT,
    pickup_ts        TIMESTAMP_NTZ,      -- NYC local time, no zone -> Iceberg "timestamp"
    dropoff_ts       TIMESTAMP_NTZ,
    passenger_count  BIGINT,
    trip_distance    DOUBLE,
    pu_location_id   INT,
    do_location_id   INT,
    payment_type     BIGINT,
    fare_amount      DOUBLE,
    tip_amount       DOUBLE,
    total_amount     DOUBLE
)
USING iceberg
PARTITIONED BY (days(pickup_ts))                 -- hidden partitioning: no extra "date" column needed
TBLPROPERTIES ('format-version' = '2');

INSERT INTO lake.nyc.trips
SELECT VendorID, CAST(tpep_pickup_datetime AS TIMESTAMP_NTZ), CAST(tpep_dropoff_datetime AS TIMESTAMP_NTZ),
       passenger_count, trip_distance, PULocationID, DOLocationID, payment_type,
       fare_amount, tip_amount, total_amount
FROM raw_trips;

SELECT count(*) AS trips FROM lake.nyc.trips;
SELECT partition, record_count, file_count FROM lake.nyc.trips.partitions ORDER BY partition LIMIT 5;
