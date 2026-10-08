-- L00 exercise (b2) - OLTP-style vs OLAP-style queries on the same database. Complete - run it and read the plans:
--   docker compose exec -T postgres psql -U olist < b_sql/oltp_vs_olap.sql
\pset footer off

\echo '== setup: a bigger copy of order_items (200 x 5,852 = 1,170,400 rows) so the difference is visible'
DROP TABLE IF EXISTS order_items_big;
CREATE TABLE order_items_big AS
SELECT g AS copy_no, oi.*
FROM order_items oi CROSS JOIN generate_series(1, 200) AS g;
ALTER TABLE order_items_big ADD PRIMARY KEY (copy_no, order_id, order_item_id);   -- builds a B-tree index
ANALYZE order_items_big;
SELECT pg_size_pretty(pg_total_relation_size('order_items_big')) AS table_plus_index_size,
       (SELECT count(*) FROM order_items_big) AS rows;

\echo '== OLTP: fetch ONE order line by its key (what an online shop does thousands of times per second)'
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
SELECT * FROM order_items_big
WHERE copy_no = 42 AND order_id = (SELECT order_id FROM orders WHERE order_seq = 1000) AND order_item_id = 1;

\echo '== OLTP: a short write transaction (touches a single row, protected by row locks + WAL)'
BEGIN;
EXPLAIN (ANALYZE, COSTS OFF)
UPDATE order_items_big SET freight_value = freight_value + 1
WHERE copy_no = 42 AND order_id = (SELECT order_id FROM orders WHERE order_seq = 1000) AND order_item_id = 1;
ROLLBACK;

\echo '== OLAP: revenue per seller state over ALL rows (what an analyst / dashboard does)'
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
SELECT s.seller_state, count(*) AS items, sum(b.price) AS revenue
FROM order_items_big b
JOIN sellers s ON s.seller_id = b.seller_id
GROUP BY s.seller_state
ORDER BY revenue DESC;

\echo '== OLAP query result (top 5)'
SELECT s.seller_state, count(*) AS items, sum(b.price) AS revenue
FROM order_items_big b
JOIN sellers s ON s.seller_id = b.seller_id
GROUP BY s.seller_state
ORDER BY revenue DESC
LIMIT 5;

\echo '== The OLAP query only needs 2 of the 9 columns, but a row store reads whole rows (all 8 kB pages):'
SELECT relpages AS pages_8kB, pg_size_pretty(relpages::bigint * 8192) AS heap_size
FROM pg_class WHERE relname = 'order_items_big';

DROP TABLE order_items_big;
