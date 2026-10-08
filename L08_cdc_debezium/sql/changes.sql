-- L08 step 4: run these ONE BY ONE in psql and watch cdc_consumer.py after each.
--   docker compose exec postgres psql -U olist

-- 1) INSERT: a new customer places an order                       -> op "c" in olist.public.customers and .orders
INSERT INTO customers (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state)
VALUES (md5('lab-cust-1'), md5('lab-uniq-1'), 1310, 'sao paulo', 'SP');

INSERT INTO orders (order_id, customer_id, order_status, order_purchase_timestamp)
VALUES (md5('lab-order-1'), md5('lab-cust-1'), 'created', now());

-- 2) UPDATE with the default REPLICA IDENTITY (primary key only)   -> op "u", before = null
UPDATE orders SET order_status = 'approved', order_approved_at = now()
WHERE order_id = md5('lab-order-1');

-- 3) Log the full old row on every change
ALTER TABLE orders REPLICA IDENTITY FULL;

-- 4) UPDATE again                                                   -> op "u", before = full old row
UPDATE orders SET order_status = 'invoiced'
WHERE order_id = md5('lab-order-1');

-- 5) Multi-row UPDATE in ONE transaction                            -> 3 events with the same txId
BEGIN;
UPDATE orders SET order_status = 'canceled'
WHERE order_id IN (SELECT order_id FROM orders WHERE order_status = 'processing' LIMIT 3);
COMMIT;

-- 6) DELETE                                                         -> op "d" (before = old row) + tombstone
DELETE FROM orders WHERE order_id = md5('lab-order-1');

-- 7) Check the replication slot Debezium uses
SELECT slot_name, plugin, active, confirmed_flush_lsn FROM pg_replication_slots;
