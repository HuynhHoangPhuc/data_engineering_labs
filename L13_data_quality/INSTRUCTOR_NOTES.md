# L13 - Instructor notes

## Timing (≈ 3-3.5 h)

| Block | Minutes |
|---|---|
| Why data quality: dimensions, cost of bad data, "tests vs monitors vs lineage" (slides, Module 11) | 15 |
| Step 0 + Part 1: GX object model, explore, TODO 1-7, Data Docs, inject bad data | 60 |
| Part 2: daily feed simulation, monitor TODOs, threshold experiments (k, min-history) | 45 |
| Part 3: OpenLineage concepts, Marquez UI tour, column lineage (or the file fallback) | 45 |
| Part 4: write-audit-publish, gates, exit codes; link to L10 Airflow | 25 |
| Checkpoint discussion | 15 |

Have students do **Step 0 at home** (pip install of GX ~250 MB) and pre-pull/build the Part 3 images:
`docker compose --profile lineage pull --ignore-buildable && docker compose --profile lineage build` (~1.5 GB download, of which
`marquez-web` is ~0.4 GB compressed / 1.34 GB on disk). `spark:4.1.3-python3` and `postgres:18.6` are shared with earlier labs.

## Version decisions (verified Oct 2026)

| Component | Version | Notes |
|---|---|---|
| Great Expectations | **GX Core 1.24.0** (PyPI `great_expectations`) | 1.x API only (Data Sources / Batch Definitions / `gx.expectations.*` classes / ValidationDefinition / Checkpoint). Supports Python 3.10-3.14; works with pandas 3.0.6. Set `GX_ANALYTICS_ENABLED=false` |
| DuckDB | 1.5.6 | metrics store + transform |
| OpenLineage Spark | `io.openlineage:openlineage-spark_2.13:1.53.0` | latest on Maven Central; changelog lists Spark 4.0/4.1/4.2 support. Downloaded at first `spark-submit` via `spark.jars.packages` into the `ivy-cache` volume |
| Marquez API | **0.50.0**, built locally as `de-labs/marquez-api:0.50.0` (`marquez/Dockerfile`) | `marquezproject/marquez` images are **amd64-only** (latest 0.51.1, Mar 2025; no release since). The API jar on Maven Central (0.50.0 is the newest published there) runs natively on `eclipse-temurin:17-jre` (arm64). Checksum pinned in the Dockerfile |
| Marquez web | `marquezproject/marquez-web:0.50.0`, `platform: linux/amd64` | Node 18 app, amd64-only -> emulated on Apple Silicon. Same minor as the API |
| Marquez DB | `postgres:18.6` | shared course image instead of the Postgres 14 the Marquez compose uses; Marquez's bundled Flyway (8.5) prints a "PostgreSQL 18 newer than tested" warning but migrations succeed |
| Alert receiver | `python:3.12-alpine` + `scripts/webhook_receiver.py` | stdlib only, ~20 MB RSS; also reused by L00 (d) |

Marquez is still the reference OpenLineage backend but its release cadence has slowed (last release March 2025,
commits until April 2026). Mention DataHub, OpenMetadata, Atlan/Collibra and cloud catalogs (Unity Catalog, Dataplex,
Purview) as consumers of the same OpenLineage events.

## Common student issues

- **0.x tutorials**: the most frequent error. Anything with `context.sources`, `get_validator`, `great_expectations init`,
  `checkpoint.yml` or `ExpectationConfiguration(expectation_type=...)` is 0.x.
- Running scripts from the wrong folder: all scripts locate the lab folder themselves, but the venv must be active.
- `run_checkpoint.py --month 2024-02` after Part 4: the broken file was moved to `data/quarantine/` -> re-run
  `scripts/make_bad_data.py`.
- Type expectations: pandas 2 reads Parquet timestamps as `datetime64[ns]`, pandas 3 as `datetime64[us]` - that is why
  the suite uses `ExpectColumnValuesToBeInTypeList`. Good discussion of "brittle" checks.
- Students set `mostly` on everything to make the suite green. Ask them to justify each threshold with the observed rate.
- Marquez UI: the namespace dropdown defaults to `default`; students think nothing arrived. Select `l13`.
- If emulation is disabled in Docker Desktop, `marquez-web` fails; the API + `curl` and the file fallback still teach everything.

## Discussion prompts

- Where should checks run: producer side (contracts), on landing (Part 1), after transforms (Part 4 gate 2), in the
  warehouse (dbt tests, L12)? Cost of each.
- Alerting hygiene: severity, routing, ownership, runbooks, de-duplication; "an alert nobody acts on should be deleted".
- Lineage use-cases: impact analysis before a schema change, root-cause analysis of a bad dashboard, compliance
  (where does PII flow?), cost attribution.

## Grading hints

| Item | Points |
|---|---|
| Part 1: complete suite (14 expectations), checkpoint, Data Docs screenshot of the failing 2024-02 run | 30 |
| Part 2: monitor TODOs; output shows the 3 alerts; one paragraph on threshold choice | 25 |
| Part 3: Marquez lineage graph screenshot (or `inspect_events.py` output) incl. column lineage of `revenue` | 15 |
| Part 4: gate TODOs; the three runs with exit codes 0 / 1 / 2 | 15 |
| Checkpoint answers | 15 |

## Tested (Oct 2026, Apple Silicon 8 GB, Docker Desktop VM ~4 GB)

`bash solutions/run_all.sh`, `--lineage` and `--file` pass from a clean state (~30 s for Parts 1/2/4 + ~45 s Part 3):
checkpoint SUCCESS on 2024-01 (14/14) and FAILED on the injected 2024-02 (7/14, exit 1); monitor replay -> exactly 3 alerts
received by the `alert-receiver` container; pipeline exit codes 0 / 2 (buggy transform) / 1 (bad input, quarantined);
Marquez shows both write jobs, 4 datasets, 3-hop column lineage for `revenue`; file transport -> 19 events.
Starters run without crashing (the Part 3 starter stops with the TODO message after writing `trips_clean`).
Measured memory: Spark job peak ~850 MB, marquez-api ~290-350 MB, marquez-web (emulated) ~115-200 MB, marquez-db ~80 MB.
