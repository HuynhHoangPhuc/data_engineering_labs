-- L03 step 2: external tables over files that already sit in HDFS.
-- Hive only stores METADATA (schema + location) in the metastore; DROP TABLE keeps the files.
CREATE DATABASE IF NOT EXISTS taxi;
USE taxi;

DROP TABLE IF EXISTS trips_csv;
CREATE EXTERNAL TABLE trips_csv (
  vendorid              INT,
  tpep_pickup_datetime  TIMESTAMP,
  tpep_dropoff_datetime TIMESTAMP,
  passenger_count       BIGINT,
  trip_distance         DOUBLE,
  ratecodeid            BIGINT,
  store_and_fwd_flag    STRING,
  pulocationid          INT,
  dolocationid          INT,
  payment_type          BIGINT,
  fare_amount           DOUBLE,
  extra                 DOUBLE,
  mta_tax               DOUBLE,
  tip_amount            DOUBLE,
  tolls_amount          DOUBLE,
  improvement_surcharge DOUBLE,
  total_amount          DOUBLE,
  congestion_surcharge  DOUBLE,
  airport_fee           DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/taxi_csv';

-- The zone lookup has a header line and "quoted","values" -> OpenCSVSerde
-- (OpenCSVSerde reads every column as STRING; we cast when querying).
DROP TABLE IF EXISTS zones;
CREATE EXTERNAL TABLE zones (
  locationid   STRING,
  borough      STRING,
  zone         STRING,
  service_zone STRING
)
-- TODO 1: complete the table definition:
--   * use the SerDe 'org.apache.hadoop.hive.serde2.OpenCSVSerde'  (ROW FORMAT SERDE '...')
--   * text file stored in HDFS directory /data/zones
--   * skip the header line  (TBLPROPERTIES ('skip.header.line.count' = '1'))
;

SHOW TABLES;
SELECT * FROM trips_csv LIMIT 3;
SELECT * FROM zones LIMIT 3;
SELECT count(*) AS csv_rows FROM trips_csv;
