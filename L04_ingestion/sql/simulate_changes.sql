-- Simulate one "business day" on the OLTP side:
--   * 3 NEW orders (new customers)            -> picked up by incremental APPEND (order_seq > last)
--   * 2 EXISTING orders change status         -> only picked up by LASTMODIFIED / watermark on updated_at
-- Run:  docker compose exec postgres psql -U olist -d olist -f /lab/sql/simulate_changes.sql
WITH c AS (
  INSERT INTO customers (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state)
  SELECT md5('lab-c-' || clock_timestamp()::text || g), md5('lab-u-' || clock_timestamp()::text || g),
         1310, 'sao paulo', 'SP'
  FROM generate_series(1, 3) AS g
  RETURNING customer_id
)
INSERT INTO orders (order_id, customer_id, order_status, order_purchase_timestamp)
SELECT md5('lab-o-' || customer_id), customer_id, 'created', now()::timestamp(0) FROM c;

UPDATE orders
SET order_status = 'delivered', order_delivered_customer_date = now()::timestamp(0)
WHERE order_id IN (SELECT order_id FROM orders WHERE order_status = 'shipped' ORDER BY order_seq LIMIT 2);

SELECT order_seq, order_id, order_status, updated_at
FROM orders
WHERE updated_at > now() - interval '2 minutes'
ORDER BY updated_at, order_seq;
