# L12 - Dimensional modeling with dbt: an Olist star schema

| | |
|---|---|
| Module | 10 - Modeling & transformation |
| Time | 3 hours |
| Stack | `postgres:18.6` (Olist, from `labs/datasets/olist`), **dbt-core 1.12 + dbt-postgres 1.11** in a Python venv on your laptop |
| RAM | ~150 MB for Docker |

## Learning objectives

1. Organize transformations in layers: **sources -> staging -> intermediate -> marts** (the dbt version of bronze/silver/gold).
2. Build a **star schema**: `fact_orders` (grain: order), `fact_order_items` (grain: order line), `dim_customers`, `dim_products`, `dim_sellers`, `dim_date`.
3. Use `ref()` / `source()` to get a dependency graph (DAG) instead of hard-coded table names.
4. Test data with generic tests (`unique`, `not_null`, `relationships`, `accepted_values`), a **custom generic test** and a **singular test**.
5. Load reference data with **seeds**, check **source freshness**, and publish **documentation + lineage** (`dbt docs`).

## Why dbt-postgres (and not Spark/DuckDB)?

- The Olist data already lives in Postgres (L04, L08 use the same database) - no extra copy step, ~150 MB of RAM.
- dbt pushes SQL down to the database; the modeling concepts are identical on Snowflake/BigQuery/Trino/Spark - only the adapter changes.
- `dbt-duckdb` would also be light, but it would need an extra export of the OLTP tables to files first; `dbt-trino` / `dbt-spark` are covered by the capstone bonus.
- Caveat: in real life you would not run analytics in the OLTP database - you would point dbt at a warehouse/replica. Here we
  keep models in separate schemas (`staging`, `marts`, ...) of the same database for simplicity.

## Architecture / lineage

```
 sources (public.*)            staging (views)                 intermediate (ephemeral)        marts (tables)
 customers ------------------> stg_olist__customers ---------------------------------------->  dim_customers <-- seed brazil_states
 sellers   ------------------> stg_olist__sellers   ---------------------------------------->  dim_sellers   <-- seed brazil_states
 products  ------------------> stg_olist__products  ---------------------------------------->  dim_products  <-- seed product_category_name_translation
 orders    ------------------> stg_olist__orders    ------------------------------+---------->  fact_orders   (1 row / order)
 order_items ----------------> stg_olist__order_items -> int_order_items_summary -+           \  fact_order_items (1 row / line)
 order_payments -------------> stg_olist__order_payments -> int_order_payments_summary -+      dim_date (generated)
```

## Files

| Path | Purpose |
|---|---|
| `docker-compose.yml` | Olist Postgres on `localhost:5432` |
| `requirements.txt` | dbt-core + dbt-postgres |
| `olist_dbt/` | **Starter** project - models with `select 1 as todo` stubs, see TODO comments |
| `solutions/olist_dbt/` | Complete project (all 76 nodes pass `dbt build`) |
| `solutions/CHECKPOINT_ANSWERS.md` | Answers |

---

## Step 1 - Start Postgres and install dbt

```bash
cd labs/L12_dbt
docker compose up -d
docker compose exec postgres psql -U olist -c "\dt"          # 6 tables: customers ... order_payments

python3 -m venv .venv && source .venv/bin/activate          # Python 3.10-3.13
pip install -r requirements.txt
dbt --version                                               # Core 1.12.x, postgres 1.11.x
```

## Step 2 - Explore the project

```bash
cd olist_dbt
dbt debug          # reads ./profiles.yml -> "All checks passed!"
```

Read `dbt_project.yml` (layer -> schema + materialization), `profiles.yml` (connection, env-var overridable),
`macros/generate_schema_name.sql` (why the schemas are `staging`/`marts`, not `analytics_staging` - and why the
`ci` target gets `ci_`-prefixed schemas, see Step 9), and `models/staging/_sources.yml`.

## Step 3 - Seeds and source freshness

```bash
dbt seed
dbt source freshness
```

Expected: 2 seeds loaded into schema `seeds`; freshness **ERROR STALE** for every source - the synthetic data has
`updated_at` values from 2016-2018 (older than the 7-day `error_after`). Now simulate the business:

```bash
docker compose exec postgres psql -U olist -c \
  "UPDATE orders SET order_status='shipped' WHERE order_id=(SELECT order_id FROM orders WHERE order_status='processing' LIMIT 1)"
dbt source freshness
```

Expected: `PASS freshness of olist.orders` (the trigger bumped `updated_at` to now), the others still stale.
In production freshness runs before `dbt build` and stops the pipeline when the loader is broken.

## Step 4 - Staging models

Two are done for you: `stg_olist__customers.sql`, `stg_olist__orders.sql`. Build them:

```bash
dbt build --select stg_olist__customers stg_olist__orders
```

Complete the TODOs in `stg_olist__sellers.sql`, `stg_olist__order_items.sql`, `stg_olist__order_payments.sql`, then

```bash
dbt build --select staging
```

Staging rules: one model per source table, rename/cast/clean only, **no joins, no aggregations**.
Look at the SQL dbt actually ran: `target/compiled/olist/models/staging/...` and `target/run/...`.

## Step 5 - Intermediate models

`int_order_items_summary.sql` is done; complete `int_order_payments_summary.sql` (one row per order).
They are **ephemeral**: never created in the database, inlined as CTEs where they are `ref`-ed (check `target/compiled`).

## Step 6 - Marts: the star schema

`dim_date`, `dim_customers`, `dim_products`, `fact_order_items` are provided. Complete `dim_sellers.sql` and
`fact_orders.sql` (read the comments: column list, `is_late`, `delivery_days`, `var('late_threshold_days')`).

```bash
dbt build --select +fact_orders      # fact_orders and everything upstream
dbt build                             # everything: seeds, models, snapshot, tests
```

Expected (solution):

```
Finished running 2 seeds, 1 snapshot, 6 table models, 61 data tests, 6 view models in 0 hours 0 minutes and 1.08 seconds
Done. PASS=76 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=76
```

Query your star:

```bash
docker compose exec postgres psql -U olist -c "
SELECT d.year_month, c.customer_region, count(*) AS orders, round(sum(f.payment_value)) AS revenue,
       round(100.0 * avg(f.is_late::int), 1) AS late_pct
FROM marts.fact_orders f
JOIN marts.dim_customers c ON c.customer_key = f.customer_key
JOIN marts.dim_date d      ON d.date_key = f.order_date_key
WHERE f.order_status = 'delivered' AND d.year = 2018
GROUP BY 1, 2 ORDER BY 1, 2 LIMIT 10"
```

## Step 7 - Tests: generic, custom generic, singular

Tests are declared in the `_*.yml` files (`data_tests:`). Complete:

1. `tests/generic/positive_value.sql` - a custom **generic** test (see the TODO block; it's used by several models).
2. A **singular** test `tests/assert_payments_match_order_value.sql` (see `tests/README_TODO.md`): payments must equal
   items + freight for orders with items; configured with `warn_if='>0'`, `error_if='>50'`.

Break the data on purpose and watch the tests catch it - and the downstream models get **skipped**:

```bash
docker compose exec postgres psql -U olist -c \
  "UPDATE order_items SET price = -price WHERE ctid IN (SELECT ctid FROM order_items LIMIT 2)"
dbt build --select stg_olist__order_items+
```

Expected:

```
FAIL 2 positive_value_stg_olist__order_items_price ....... [FAIL 2]
SKIP relation marts.fact_order_items ..................... [SKIP]
SKIP relation marts.fact_orders due to ephemeral model status 'skipped'
```

`dbt build` runs tests right after each model, so bad data never reaches the marts. Inspect the failing rows:
`dbt test --select stg_olist__order_items --store-failures` then look at the tables in schema `dbt_test__audit`.
Fix the data afterwards:
`docker compose exec postgres psql -U olist -c "UPDATE order_items SET price = abs(price) WHERE price < 0"`.

## Step 8 - Documentation and lineage

```bash
dbt docs generate
dbt docs serve --port 8081          # http://localhost:8081  (Ctrl+C to stop)
```

Browse a model, its columns and tests, then click the **lineage graph** (bottom right) of `fact_orders`.

## Step 9 - CI target: build without touching production

A CI run (e.g. on every pull request) must build and test your changes **without overwriting** the tables people use.
`profiles.yml` has a second target, `ci` (schema `ci`). On its own that is not enough: our
`macros/generate_schema_name.sql` returns the custom schema unchanged (`marts`, not `analytics_marts`), so a naive macro would
make `--target ci` rebuild the real `marts` tables. The macro therefore has a third branch:

```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- if custom_schema_name is none -%} {{ target.schema }}
  {%- elif target.name == 'ci' -%} {{ target.schema }}_{{ custom_schema_name | trim }}
  {%- else -%} {{ custom_schema_name | trim }}
  {%- endif -%}
{%- endmacro %}
```

```bash
dbt build --target ci          # -> ci_staging, ci_marts, ci_seeds, ci_snapshots (marts/staging untouched)
```

Expected: the same `PASS=76`, and `\dn` in psql now also lists the `ci_*` schemas. Snapshots use `+schema` (not the
legacy `+target_schema`) in `dbt_project.yml`, so they follow the same rule. "Slim CI" variant: build only what changed and
read unchanged parents from production artifacts:

```bash
mkdir -p prod-artifacts && cp target/manifest.json prod-artifacts/     # manifest of a dev/prod run
dbt build --select state:modified+ --defer --state ./prod-artifacts --target ci
```

Clean up with `DROP SCHEMA IF EXISTS ci, ci_staging, ci_marts, ci_seeds, ci_snapshots CASCADE;` (or `docker compose down -v`).

---

## Checkpoint questions

1. What is the grain of `fact_orders` and of `fact_order_items`? Why do we need both?
2. Why must items and payments be aggregated to the order level *before* joining them to orders? What happens to revenue if you join `orders -> order_items -> order_payments` directly?
3. Why are staging models views, intermediate ephemeral and marts tables? When would you make a mart `incremental`?
4. What does `ref('stg_olist__orders')` give you that `staging.stg_olist__orders` doesn't?
5. `dim_customers` has one row per `customer_id`, but Olist creates a new `customer_id` for every order of the same person. What key would you use to count *unique shoppers*? How would you build a customer dimension at the person level?
6. Source freshness says ERROR for `customers` but PASS for `orders`. Is the pipeline broken? What would you configure differently for slowly changing reference tables?
7. What is the difference between a generic and a singular test? Which one did you write for "payments = items + freight", and why?
8. The snapshot `customers_snapshot` (in `snapshots/`) was built by `dbt build`. What problem does it solve, and what columns did dbt add?

Answers: `solutions/CHECKPOINT_ANSWERS.md`.

## Stretch challenges

1. **SCD2 in action**: `UPDATE customers SET customer_city='campinas' WHERE customer_id = '<some id>'`, run `dbt snapshot`, and query `snapshots.customers_snapshot` for that id (two versions, `dbt_valid_to` set on the old one). Join facts to the version valid at order time.
2. **Incremental model**: make `fact_order_items` `materialized='incremental'` with `unique_key='order_item_key'` and `is_incremental()` filtering on `updated_at`.
3. **Packages**: add `dbt_utils` (`packages.yml`, `dbt deps`) and replace `dim_date` with `dbt_utils.date_spine`; use `dbt_utils.generate_surrogate_key` for surrogate keys.
4. **Unit tests** (dbt >= 1.8): write a `unit_tests:` block for `fact_orders.is_late` with 3 hand-made input rows.
5. **Exposures & contracts**: declare the dashboard that uses `fact_orders` as an `exposure`; enforce a `contract` on `fact_orders`.
6. **Real data**: load the Kaggle dataset (`labs/datasets/olist/README.md`) and rerun - which tests start to fail or warn, and why?

## Cleanup

```bash
docker compose down -v
deactivate
```

## Troubleshooting

| Symptom | Fix |
|---|---|
| `dbt debug`: connection refused | Postgres not running or another lab owns 5432 (`docker ps`). Override the port: `DBT_PORT=5433 dbt debug` if you remap it. |
| `Could not find profile named 'olist'` | Run dbt **inside** `olist_dbt/` (profiles.yml is there) or `export DBT_PROFILES_DIR=$(pwd)`. |
| `relation "public.customers" does not exist` | The volume was created before the init scripts were mounted: `docker compose down -v && docker compose up -d`. |
| Starter `dbt build` shows many ERRORs | Expected until the TODO models are written: their tests reference columns that don't exist yet. Use `--select` on finished parts. |
| Schemas named `analytics_staging` | You removed/renamed `macros/generate_schema_name.sql`. |
| `dbt docs serve` port in use | `--port 8082`. |
| Deprecation warnings about test arguments | This project uses the dbt >= 1.10 syntax `arguments:` under generic tests; older dbt versions need the arguments at the top level. |
