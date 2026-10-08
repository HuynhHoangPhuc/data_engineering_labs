-- L11 steps 7-8 (Spark SQL): metadata tables and time travel

SELECT committed_at, snapshot_id, parent_id, operation,
       summary['added-data-files'] AS added_files, summary['total-records'] AS total_records
FROM lake.nyc.trips.snapshots ORDER BY committed_at;

SELECT made_current_at, snapshot_id, is_current_ancestor FROM lake.nyc.trips.history;

SELECT file_path, partition, record_count, file_size_in_bytes
FROM lake.nyc.trips.files ORDER BY file_size_in_bytes LIMIT 5;

-- Time travel. VERSION AS OF needs a LITERAL snapshot id (no subquery): copy the FIRST snapshot_id
-- printed by the first query above and paste it instead of <FIRST_SNAPSHOT_ID>.
SELECT count(*) AS rows_now FROM lake.nyc.trips;
-- SELECT count(*) AS rows_first_snapshot FROM lake.nyc.trips VERSION AS OF <FIRST_SNAPSHOT_ID>;
-- Equivalent: the table name suffix form           FROM lake.nyc.trips.snapshot_id_<FIRST_SNAPSHOT_ID>
-- Or by time (any timestamp after the first commit): FROM lake.nyc.trips TIMESTAMP AS OF '2026-09-23 17:31:40'

-- Undo the DELETE and the late re-insert by moving the table's current pointer back (metadata-only):
-- CALL lake.system.rollback_to_snapshot('lake.nyc.trips', <FIRST_SNAPSHOT_ID>);
