#!/usr/bin/env bash
# Entrypoint for the de-labs/hadoop image.
#   namenode        -> formats the NameNode on first start, then runs it
#   datanode        -> waits for the NameNode RPC port, then runs a DataNode
#   resourcemanager / nodemanager / historyserver -> YARN / MapReduce daemons
#   anything else   -> executed as-is (e.g. bash, sqoop, hdfs dfs ...)
set -euo pipefail

wait_for() {  # host port
  local host=$1 port=$2
  for _ in $(seq 1 60); do
    nc -z "$host" "$port" 2>/dev/null && return 0
    sleep 2
  done
  echo "WARN: $host:$port not reachable after 120s, starting anyway" >&2
}

case "${1:-bash}" in
  namenode)
    if [ ! -d /hadoop/dfs/name/current ]; then
      echo ">>> Formatting NameNode (first start)"
      hdfs namenode -format -nonInteractive -force -clusterId "${CLUSTER_ID:-de-labs-cluster}"
    fi
    exec hdfs namenode
    ;;
  datanode)
    wait_for namenode 8020
    exec hdfs datanode
    ;;
  resourcemanager)
    wait_for namenode 8020
    exec yarn resourcemanager
    ;;
  nodemanager)
    wait_for resourcemanager 8031
    exec yarn nodemanager
    ;;
  historyserver)
    wait_for namenode 8020
    # the history server needs its staging/done dirs on HDFS
    hdfs dfs -mkdir -p /tmp/hadoop-yarn/staging /mr-history /user/root || true
    hdfs dfs -chmod -R 1777 /tmp /mr-history || true
    exec mapred historyserver
    ;;
  *)
    exec "$@"
    ;;
esac
