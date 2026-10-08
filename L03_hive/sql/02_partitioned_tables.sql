-- L03 step 3: managed, columnar, partitioned copies of the CSV data.
USE taxi;
SET hive.exec.dynamic.partition = true;
-- TODO 2: dynamic partitioning is "strict" by default (at least one static partition value
--         required). Set hive.exec.dynamic.partition.mode so ALL partition values can be dynamic.


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
SELECT t.*,
       NULL /* TODO 3: replace NULL by an expression that turns tpep_pickup_datetime into 'yyyy-MM' */ AS pickup_month
FROM trips_csv t;

-- TODO 4: create trips_parquet: same columns and partitioning as trips_orc, STORED AS PARQUET
--         (TBLPROPERTIES ('parquet.compression' = 'SNAPPY')), and fill it the same way.


SHOW PARTITIONS trips_orc;
SELECT pickup_month, count(*) AS trips FROM trips_orc GROUP BY pickup_month ORDER BY pickup_month;
