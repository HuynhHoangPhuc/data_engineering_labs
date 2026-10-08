#!/usr/bin/env bash
# L13 - run all SOLUTIONS end to end (instructor smoke test). From labs/L13_data_quality with the venv active:
#   bash solutions/run_all.sh            # Parts 1, 2, 4 (host) + alert receiver
#   bash solutions/run_all.sh --lineage  # + Part 3 with Marquez (profile lineage)
#   bash solutions/run_all.sh --file     # + Part 3 fallback (profile spark, OpenLineage file transport)
set -uo pipefail
cd "$(dirname "$0")/.."
export GX_ANALYTICS_ENABLED=false
step() { printf '\n\033[1;33m### %s\033[0m\n' "$*"; }
expect_exit() { local want=$1; shift; "$@"; local got=$?; if [ "$got" != "$want" ]; then echo "!! expected exit $want, got $got: $*"; FAILED=1; fi; }
FAILED=0

step "reset generated state"
rm -rf data gx output
docker compose --profile monitor up -d
for _ in $(seq 1 20); do curl -fs localhost:9099/health >/dev/null && break; sleep 1; done

step "Part 1 - GX suite + checkpoint"
python scripts/land_month.py 2024-01
python solutions/part1_gx/gx_setup.py
expect_exit 0 python part1_gx/run_checkpoint.py --month 2024-01
python scripts/make_bad_data.py
expect_exit 1 python part1_gx/run_checkpoint.py --month 2024-02

step "Part 2 - freshness & volume monitor"
python scripts/land_daily_files.py --half-day 2024-01-23 --skip-day 2024-01-25 | tail -3
expect_exit 1 python solutions/part2_monitor/monitor.py --reset --from 2024-01-01 --to 2024-01-31
python scripts/land_daily_files.py --only 2024-01-25
expect_exit 0 python solutions/part2_monitor/monitor.py --ds 2024-01-25

step "Part 4 - quality gates"
expect_exit 0 python solutions/part4_gate/pipeline.py --month 2024-01
expect_exit 2 python solutions/part4_gate/pipeline.py --month 2024-01 --buggy-transform
expect_exit 1 python solutions/part4_gate/pipeline.py --month 2024-02
ls data/published data/quarantine
step "alerts received"
curl -s localhost:9099/
docker compose --profile monitor down

if [ "${1:-}" = "--lineage" ]; then
  step "Part 3 - OpenLineage -> Marquez"
  docker compose --profile lineage up -d --build --wait
  docker compose exec spark spark-submit /lab/solutions/part3_lineage/taxi_lineage_job.py
  sleep 3
  curl -s localhost:5050/api/v1/namespaces/l13/jobs | python3 -c "import sys,json;[print(' job:', j['name']) for j in json.load(sys.stdin)['jobs']]"
  curl -s "localhost:5050/api/v1/column-lineage?nodeId=datasetField:file:/lab/output/lineage/daily_borough_revenue:revenue" \
    | python3 -c "import sys,json;[print(' node:', n['id']) for n in json.load(sys.stdin)['graph']]"
  docker compose --profile lineage down
elif [ "${1:-}" = "--file" ]; then
  step "Part 3 fallback - OpenLineage -> JSON file"
  docker compose --profile spark up -d
  docker compose exec spark spark-submit \
    --conf spark.openlineage.transport.type=file \
    --conf spark.openlineage.transport.location=/lab/output/openlineage/events.jsonl \
    /lab/solutions/part3_lineage/taxi_lineage_job.py
  python part3_lineage/inspect_events.py --column revenue | tail -4
  docker compose --profile spark down
fi

step "done"
[ "$FAILED" = 0 ] && echo "ALL CHECKS AS EXPECTED" || { echo "SOME CHECKS FAILED"; exit 1; }
