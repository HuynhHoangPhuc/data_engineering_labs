-- L11 step 9 (Spark SQL): table maintenance with stored procedures

-- Create the "small files problem": 10 tiny appends into the same partition
INSERT INTO lake.nyc.zone_daily VALUES (DATE '2024-02-01', 1, 1, 1.0, current_timestamp());
INSERT INTO lake.nyc.zone_daily VALUES (DATE '2024-02-01', 2, 1, 1.0, current_timestamp());
INSERT INTO lake.nyc.zone_daily VALUES (DATE '2024-02-01', 3, 1, 1.0, current_timestamp());
INSERT INTO lake.nyc.zone_daily VALUES (DATE '2024-02-01', 4, 1, 1.0, current_timestamp());
INSERT INTO lake.nyc.zone_daily VALUES (DATE '2024-02-01', 5, 1, 1.0, current_timestamp());
INSERT INTO lake.nyc.zone_daily VALUES (DATE '2024-02-01', 6, 1, 1.0, current_timestamp());
INSERT INTO lake.nyc.zone_daily VALUES (DATE '2024-02-01', 7, 1, 1.0, current_timestamp());
INSERT INTO lake.nyc.zone_daily VALUES (DATE '2024-02-01', 8, 1, 1.0, current_timestamp());
SELECT partition, file_count, record_count FROM lake.nyc.zone_daily.partitions;

-- 1) compaction: rewrite small files into bigger ones
CALL lake.system.rewrite_data_files(table => 'lake.nyc.zone_daily', options => map('min-input-files', '2'));
SELECT partition, file_count, record_count FROM lake.nyc.zone_daily.partitions;

-- 2) expire old snapshots (their unreferenced files are physically deleted); keep only the latest 1
SELECT count(*) AS snapshots_before FROM lake.nyc.zone_daily.snapshots;
CALL lake.system.expire_snapshots(table => 'lake.nyc.zone_daily', older_than => current_timestamp(), retain_last => 1);
SELECT count(*) AS snapshots_after FROM lake.nyc.zone_daily.snapshots;

-- 3) rewrite manifests (metadata compaction) on the big table
CALL lake.system.rewrite_manifests('lake.nyc.trips');
