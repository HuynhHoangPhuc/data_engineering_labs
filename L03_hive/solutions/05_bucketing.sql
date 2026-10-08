-- L03 step 6: bucketing = hash(column) mod N -> a fixed number of files per partition/table.
USE taxi;

DROP TABLE IF EXISTS trips_bucketed;
CREATE TABLE trips_bucketed (
  tpep_pickup_datetime TIMESTAMP,
  pulocationid INT,
  dolocationid INT,
  fare_amount DOUBLE,
  total_amount DOUBLE
)
CLUSTERED BY (pulocationid) SORTED BY (pulocationid) INTO 4 BUCKETS
STORED AS ORC;

INSERT OVERWRITE TABLE trips_bucketed
SELECT tpep_pickup_datetime, pulocationid, dolocationid, fare_amount, total_amount
FROM trips_orc WHERE pickup_month = '2024-01';

DESCRIBE FORMATTED trips_bucketed;

-- Sampling reads just one bucket (~1/4 of the data):
SELECT count(*) FROM trips_bucketed TABLESAMPLE (BUCKET 1 OUT OF 4 ON pulocationid);
SELECT count(*) FROM trips_bucketed;

-- Every row of zone 132 (JFK) is in exactly one bucket file:
SELECT INPUT__FILE__NAME AS file, count(*) AS rows_for_jfk
FROM trips_bucketed WHERE pulocationid = 132 GROUP BY INPUT__FILE__NAME;
