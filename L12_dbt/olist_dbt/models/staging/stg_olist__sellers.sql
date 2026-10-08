-- TODO (step 4): staging model for sellers, same pattern as stg_olist__customers.sql
--   * select from {{ source('olist', 'sellers') }}
--   * zip code prefix -> 5-char text with leading zeros (lpad)
--   * city -> initcap, state -> upper(trim(...))
--   * keep seller_id and updated_at
select 1 as todo
