#!/usr/bin/env bash
# Run a .sql file through Beeline (non-interactive) and show per-statement timings.
#   ./run_sql.sh sql/01_external_tables.sql
#   ./run_sql.sh solutions/03_compare_formats.sql
# (Hive 4.2's beeline -f needs a real terminal; passing the file content with -e is
#  more robust in scripts, so this wrapper does that.)
set -euo pipefail
cd "$(dirname "$0")"
FILE=${1:?usage: ./run_sql.sh <file.sql>}
docker compose exec -T -e JAVA_TOOL_OPTIONS="-Dorg.jline.terminal.provider=dumb" hiveserver2 \
  beeline -u "jdbc:hive2://localhost:10000/" -n hive --showElapsedTime=true --hiveconf hive.server2.in.place.progress=false -e "$(cat "$FILE")" 2>&1 \
  | grep -v -E "^Picked up JAVA_TOOL_OPTIONS|^SLF4J|NativeCodeLoader|^\s*$"
