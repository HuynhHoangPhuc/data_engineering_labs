-- TODO (step 4): staging model for order_items
--   * build a single-column key:   order_id || '-' || order_item_id   as order_item_key
--   * rename shipping_limit_date -> shipping_limit_at
--   * cast price and freight_value to numeric(12,2)
--   * keep order_id, order_item_id, product_id, seller_id, updated_at
select 1 as todo
