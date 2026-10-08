#!/usr/bin/env bash
# Load the real Kaggle Olist CSVs into a running course Postgres container.
#
# Usage: ./load_kaggle.sh <postgres-container-name> <dir-with-olist-csvs>
#   e.g. ./load_kaggle.sh l04_ingestion-postgres-1 ~/Downloads/brazilian-ecommerce
# The container must already have the schema (01_schema.sql ran at first start).
set -euo pipefail
if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <postgres-container> <kaggle-csv-dir>" >&2; exit 1
fi
CONTAINER=$1; SRC=$2
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
for f in customers sellers products orders order_items order_payments; do
  [ -f "${SRC}/olist_${f}_dataset.csv" ] || { echo "missing ${SRC}/olist_${f}_dataset.csv" >&2; exit 1; }
done
docker exec "${CONTAINER}" mkdir -p /tmp/kaggle
for f in customers sellers products orders order_items order_payments; do
  docker cp "${SRC}/olist_${f}_dataset.csv" "${CONTAINER}:/tmp/kaggle/"
done
docker cp "${HERE}/load_kaggle_csv.sql" "${CONTAINER}:/tmp/load_kaggle_csv.sql"
docker exec "${CONTAINER}" sh -c 'psql -U "$POSTGRES_USER" -d "${POSTGRES_DB:-$POSTGRES_USER}" -f /tmp/load_kaggle_csv.sql'
