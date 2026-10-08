#!/usr/bin/env bash
# Print the 10 busiest pickup zones from a finished job's output directory.
# Usage: bash top_zones.sh /user/root/zones_out_combiner
hdfs dfs -cat "${1:-/user/root/zones_out_combiner}/part-*" | sort -t$'\t' -k2,2nr | head -10
