-- =====================================================================================
-- L11 worksheet (STARTER). Work through it in the interactive Spark SQL shell:
--     docker compose exec spark /opt/spark/bin/spark-sql
-- and in the Trino CLI for the Trino parts:
--     docker compose exec trino trino --catalog lake --schema nyc
-- Solutions: solutions/01_*.sql ... 06_*.sql
-- =====================================================================================

-- ---------- Step 2: create + load (Spark) ----------
CREATE NAMESPACE IF NOT EXISTS lake.nyc;

CREATE TEMPORARY VIEW raw_trips
USING parquet OPTIONS (path '/data/taxi/yellow_tripdata_2024-01.parquet');

-- TODO 2.1: CREATE TABLE lake.nyc.trips (...) USING iceberg with columns
--   vendor_id INT, pickup_ts TIMESTAMP_NTZ, dropoff_ts TIMESTAMP_NTZ, passenger_count BIGINT,
--   trip_distance DOUBLE, pu_location_id INT, do_location_id INT, payment_type BIGINT,
--   fare_amount DOUBLE, tip_amount DOUBLE, total_amount DOUBLE
--   partitioned by DAY of pickup_ts using a *partition transform* (hidden partitioning),
--   TBLPROPERTIES ('format-version' = '2')

-- TODO 2.2: INSERT INTO lake.nyc.trips SELECT ... FROM raw_trips
--   (source columns: VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance,
--    PULocationID, DOLocationID, payment_type, fare_amount, tip_amount, total_amount)

-- TODO 2.3: how many rows? how many partitions? (hint: the metadata table lake.nyc.trips.partitions)


-- ---------- Step 3: query from Trino ----------
-- TODO 3.1 (Trino): count rows; top 5 days by number of trips in January 2024
-- TODO 3.2 (Trino): EXPLAIN ANALYZE a query for ONE day. How many files/rows were read? (partition pruning)
-- TODO 3.3 (Trino): list the snapshots:  SELECT * FROM "trips$snapshots";


-- ---------- Step 4: schema evolution (Spark) ----------
-- TODO 4.1: add a column congestion_surcharge DOUBLE
-- TODO 4.2: rename trip_distance -> trip_distance_mi
-- TODO 4.3: widen vendor_id INT -> BIGINT
-- TODO 4.4 (Trino): DESCRIBE trips - does Trino see the changes without any refresh? Were data files rewritten?


-- ---------- Step 5: partition evolution (Spark) ----------
-- TODO 5.1: ALTER TABLE ... ADD PARTITION FIELD vendor_id
-- TODO 5.2: insert the trips of 2024-01-31 23:00-24:00 again (from raw_trips) - careful with the column ORDER
-- TODO 5.3: SELECT spec_id, count(*) FROM lake.nyc.trips.files GROUP BY spec_id   -- what do you see?


-- ---------- Step 6: MERGE INTO (Spark) ----------
-- TODO 6.1: create lake.nyc.zone_daily (trip_date DATE, pu_location_id INT, trips BIGINT, revenue DOUBLE,
--           updated_at TIMESTAMP) partitioned by months(trip_date); fill it with Jan 1-15 aggregates
-- TODO 6.2: MERGE the re-computed aggregates of Jan 10-20: update changed rows, insert new ones
-- TODO 6.3: run the MERGE again. What changed? (look at lake.nyc.zone_daily.snapshots summary)
-- TODO 6.4: DELETE the trips whose pickup_ts is outside January 2024


-- ---------- Step 7-8: metadata tables + time travel (Spark and Trino) ----------
-- TODO 7.1: list snapshots, history, files of lake.nyc.trips
-- TODO 7.2: count rows AS OF the first snapshot (VERSION AS OF <id>) and now
-- TODO 7.3 (Trino): same with  SELECT count(*) FROM trips FOR VERSION AS OF <id>;


-- ---------- Step 9: maintenance (Spark) ----------
-- TODO 9.1: 8 tiny INSERTs into zone_daily for 2024-02-01 -> check file_count in .partitions
-- TODO 9.2: CALL lake.system.rewrite_data_files(...)       -> file_count again
-- TODO 9.3: CALL lake.system.expire_snapshots(...) keeping only the last snapshot
-- TODO 9.4: time travel to an expired snapshot - what happens?
