-- L03 step 3: managed, columnar, partitioned copies of the CSV data.
USE taxi;
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;   -- allow ALL partition columns to be dynamic

DROP TABLE IF EXISTS trips_orc;
CREATE TABLE trips_orc (
  vendorid INT, tpep_pickup_datetime TIMESTAMP, tpep_dropoff_datetime TIMESTAMP,
  passenger_count BIGINT, trip_distance DOUBLE, ratecodeid BIGINT, store_and_fwd_flag STRING,
  pulocationid INT, dolocationid INT, payment_type BIGINT, fare_amount DOUBLE, extra DOUBLE,
  mta_tax DOUBLE, tip_amount DOUBLE, tolls_amount DOUBLE, improvement_surcharge DOUBLE,
  total_amount DOUBLE, congestion_surcharge DOUBLE, airport_fee DOUBLE
)
PARTITIONED BY (pickup_month STRING)
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'ZLIB');

-- Dynamic partitioning: the LAST column of the SELECT feeds the partition column.
INSERT OVERWRITE TABLE trips_orc PARTITION (pickup_month)
SELECT t.*, date_format(tpep_pickup_datetime, 'yyyy-MM') AS pickup_month
FROM trips_csv t;

DROP TABLE IF EXISTS trips_parquet;
CREATE TABLE trips_parquet (
  vendorid INT, tpep_pickup_datetime TIMESTAMP, tpep_dropoff_datetime TIMESTAMP,
  passenger_count BIGINT, trip_distance DOUBLE, ratecodeid BIGINT, store_and_fwd_flag STRING,
  pulocationid INT, dolocationid INT, payment_type BIGINT, fare_amount DOUBLE, extra DOUBLE,
  mta_tax DOUBLE, tip_amount DOUBLE, tolls_amount DOUBLE, improvement_surcharge DOUBLE,
  total_amount DOUBLE, congestion_surcharge DOUBLE, airport_fee DOUBLE
)
PARTITIONED BY (pickup_month STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

INSERT OVERWRITE TABLE trips_parquet PARTITION (pickup_month)
SELECT t.*, date_format(tpep_pickup_datetime, 'yyyy-MM') AS pickup_month
FROM trips_csv t;

SHOW PARTITIONS trips_orc;
SELECT pickup_month, count(*) AS trips FROM trips_orc GROUP BY pickup_month ORDER BY pickup_month;
