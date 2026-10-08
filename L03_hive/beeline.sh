#!/usr/bin/env bash
# Open Beeline (Hive's JDBC CLI) inside the hiveserver2 container.
#   ./beeline.sh                          # interactive SQL prompt
#   ./beeline.sh -f /sql/01_external_tables.sql   # run a script (./sql is mounted at /sql)
#   ./beeline.sh -e "SHOW DATABASES;"
# Without a terminal (scripts/CI) jline needs the "dumb" terminal provider.
cd "$(dirname "$0")"
URL="jdbc:hive2://localhost:10000/"
if [ -t 0 ] && [ -t 1 ]; then
  exec docker compose exec hiveserver2 beeline -u "$URL" -n hive "$@"
else
  exec docker compose exec -T -e JAVA_TOOL_OPTIONS="-Dorg.jline.terminal.provider=dumb" \
       hiveserver2 beeline -u "$URL" -n hive "$@"
fi
