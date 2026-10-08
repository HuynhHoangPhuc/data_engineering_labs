#!/usr/bin/env bash
# Download NYC TLC Yellow Taxi trip records (Parquet) + the taxi zone lookup CSV.
#
# Usage:
#   ./download_taxi.sh                 # default: 2024-01
#   ./download_taxi.sh 2024-01 2024-02 # several months
#
# Output (shared by ALL labs, git-ignored):
#   labs/datasets/data/taxi/yellow_tripdata_YYYY-MM.parquet   (~50 MB / month, ~3 M rows)
#   labs/datasets/data/taxi/taxi_zone_lookup.csv
#
# Source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
# Files are served from the TLC CloudFront distribution (verified Sep 2026):
#   https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet
#   https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
set -euo pipefail

BASE_URL="${TLC_BASE_URL:-https://d37ci6vzurychx.cloudfront.net}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_DIR="${SCRIPT_DIR}/data/taxi"
mkdir -p "${OUT_DIR}"

if [ "$#" -eq 0 ]; then
  set -- 2024-01
fi

fetch() {  # url dest
  local url=$1 dest=$2
  if [ -s "${dest}" ]; then
    echo "[skip] $(basename "${dest}") already exists ($(du -h "${dest}" | cut -f1))"
    return 0
  fi
  echo "[get ] ${url}"
  # download to a temp file first so an interrupted download never looks complete
  if curl -fL --retry 3 --retry-delay 2 --progress-bar -o "${dest}.part" "${url}"; then
    mv "${dest}.part" "${dest}"
    echo "[ ok ] ${dest} ($(du -h "${dest}" | cut -f1))"
  else
    rm -f "${dest}.part"
    echo "[FAIL] could not download ${url}" >&2
    return 1
  fi
}

rc=0
for month in "$@"; do
  if ! [[ "${month}" =~ ^[0-9]{4}-(0[1-9]|1[0-2])$ ]]; then
    echo "Invalid month '${month}' (expected YYYY-MM)" >&2
    rc=1; continue
  fi
  fetch "${BASE_URL}/trip-data/yellow_tripdata_${month}.parquet" \
        "${OUT_DIR}/yellow_tripdata_${month}.parquet" || rc=1
done

fetch "${BASE_URL}/misc/taxi_zone_lookup.csv" "${OUT_DIR}/taxi_zone_lookup.csv" || rc=1

echo
echo "Contents of ${OUT_DIR}:"
ls -lh "${OUT_DIR}"
exit ${rc}
