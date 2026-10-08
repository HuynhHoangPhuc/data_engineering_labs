# L12 - Instructor notes

## Timing (≈ 3 h)

| Block | Minutes |
|---|---|
| Kimball in 20 minutes: facts, dimensions, grain, star vs snowflake, SCD types | 20 |
| Steps 1-3: setup, project tour, seeds, freshness | 25 |
| Step 4-5: staging + intermediate TODOs | 35 |
| Step 6: marts (fan-out discussion on the whiteboard!) | 40 |
| Step 7: tests incl. custom test, break-the-data demo | 30 |
| Step 8 + checkpoint discussion | 30 |

## Tested (Sep 2026)

dbt-core 1.12.5 + dbt-postgres 1.11.0 (Python 3.12 venv) against `postgres:18.6` seeded with the synthetic Olist data:

- `dbt debug`, `dbt seed` (2), `dbt source freshness` (5 x ERROR STALE, then orders PASS after an UPDATE)
- solution `dbt build`: **PASS=76** (2 seeds, 1 snapshot, 6 tables, 6 views, 61 tests) in ~1 s; no deprecation warnings
- negative prices -> `positive_value` FAIL 2, downstream facts SKIPPED
- `dbt docs generate` + `dbt docs serve` (HTTP 200, catalog.json served)
- starter project parses; `dbt build` fails only on the TODO stubs (expected)

## Common student errors

- Writing joins in staging models. Keep staging 1:1 with sources.
- Joining items and payments together -> inflated revenue. Let them discover it: compare `sum(payment_value)` in their
  fact with `SELECT sum(payment_value) FROM order_payments` (675,734.67 on the synthetic seed).
- Hard-coding `staging.stg_...` instead of `ref()`.
- Old YAML syntax from blog posts (`tests:` + arguments at top level) - works with warnings in dbt 1.10+; we teach `data_tests:` + `arguments:`.
- Running dbt from `labs/L12_dbt` instead of `olist_dbt/` -> profile not found.
- Port 5432 already used by L08's Postgres.

## Real Kaggle data notes

With the real dataset: some product categories have no translation (`pc_gamer`, `portateis_cozinha...`) -> `category_name`
falls back to Portuguese; a few orders have payments != items + freight (the singular test WARNs, by design); there are
~600 delivered orders without `order_delivered_customer_date`. Good discussion material.

## Grading hints

| Item | Points |
|---|---|
| Staging TODOs (3 models) | 15 |
| `int_order_payments_summary` | 10 |
| `dim_sellers` + `fact_orders` (correct grain, no fan-out, `is_late`) | 25 |
| Custom generic test + singular test | 20 |
| `dbt build` all green (screenshot) + docs lineage screenshot | 15 |
| Checkpoint answers | 15 |
