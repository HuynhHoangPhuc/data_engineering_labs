#!/usr/bin/env bash
# Register (or update) the capstone Debezium connector.
set -euo pipefail
cd "$(dirname "$0")/.."
until curl -fs localhost:8083/ >/dev/null; do echo "waiting for Kafka Connect..."; sleep 3; done
if curl -fs localhost:8083/connectors/olist-cdc >/dev/null; then
  python3 -c "import json;print(json.dumps(json.load(open('connectors/olist-cdc.json'))['config']))" \
    | curl -s -X PUT -H "Content-Type: application/json" --data @- localhost:8083/connectors/olist-cdc/config >/dev/null
  echo "updated"
else
  curl -s -X POST -H "Content-Type: application/json" --data @connectors/olist-cdc.json localhost:8083/connectors >/dev/null
  echo "created"
fi
sleep 5
curl -s localhost:8083/connectors/olist-cdc/status; echo
