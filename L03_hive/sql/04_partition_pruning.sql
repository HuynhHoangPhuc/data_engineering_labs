-- L03 step 5: does Hive read all partitions or only the ones we need?
USE taxi;

-- (a) filter on the PARTITION column -> only 1 partition is an input
EXPLAIN DEPENDENCY
SELECT count(*) FROM trips_orc WHERE pickup_month = '2024-01';

-- (b) filter on a normal column -> every partition is an input
-- TODO 5: write the same count for January 2024, but filter on tpep_pickup_datetime
--         instead of pickup_month, prefixed with EXPLAIN DEPENDENCY. How many partitions are read?


-- (c) the full plan: look for "filterExpr", "Statistics: Num rows" and the partition list
EXPLAIN EXTENDED
SELECT count(*) FROM trips_orc WHERE pickup_month = '2024-01';

-- (d) where does a partition live?
DESCRIBE FORMATTED trips_orc PARTITION (pickup_month = '2024-01');
