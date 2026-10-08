#!/usr/bin/env python3
"""Trips per pickup zone -- MAPPER (Hadoop Streaming).

Input : headerless taxi CSV produced by labs/datasets/parquet_to_csv.py
        col 0 VendorID, 1 pickup ts, 2 dropoff ts, 3 passenger_count,
        4 trip_distance, 5 RatecodeID, 6 store_and_fwd_flag,
        7 PULocationID, 8 DOLocationID, 9 payment_type, 10 fare_amount, ...
Output: "<Borough>/<Zone><TAB>1"

Map-side join: if taxi_zone_lookup.csv is in the working directory
(shipped with  -files .../taxi_zone_lookup.csv), LocationIDs are replaced by
"Borough/Zone" names; otherwise the numeric id is emitted.
"""
import csv
import os
import sys

PU_COL = 7

zones = {}
if os.path.exists("taxi_zone_lookup.csv"):
    with open("taxi_zone_lookup.csv", newline="") as f:
        for row in csv.DictReader(f):  # LocationID,Borough,Zone,service_zone
            zones[row["LocationID"]] = f"{row['Borough']}/{row['Zone']}"

for line in sys.stdin:
    fields = line.rstrip("\n").split(",")
    if len(fields) <= PU_COL or not fields[PU_COL].isdigit():
        # Hadoop counters are updated through stderr in Streaming:
        sys.stderr.write("reporter:counter:TaxiLab,BAD_RECORDS,1\n")
        continue
    loc = fields[PU_COL]
    print(f"{zones.get(loc, loc)}\t1")
