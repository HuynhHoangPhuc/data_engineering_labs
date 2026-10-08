#!/usr/bin/env python3
"""Trips per pickup zone -- MAPPER (Hadoop Streaming).

Input lines are headerless taxi CSV rows (labs/datasets/parquet_to_csv.py):
  0 VendorID, 1 tpep_pickup_datetime, 2 tpep_dropoff_datetime, 3 passenger_count,
  4 trip_distance, 5 RatecodeID, 6 store_and_fwd_flag, 7 PULocationID,
  8 DOLocationID, 9 payment_type, 10 fare_amount, ... 16 total_amount, ...

Goal: emit "<zone>\t1" per trip.  If the file taxi_zone_lookup.csv is present in
the task's working directory (ship it with  -files .../taxi_zone_lookup.csv),
translate the numeric PULocationID into "Borough/Zone"  -> a MAP-SIDE JOIN.
"""
import csv
import os
import sys

PU_COL = 7

zones = {}
if os.path.exists("taxi_zone_lookup.csv"):
    with open("taxi_zone_lookup.csv", newline="") as f:
        for row in csv.DictReader(f):  # header: LocationID,Borough,Zone,service_zone
            # TODO 4: fill the dict:  "132" -> "Queens/JFK Airport"
            pass

for line in sys.stdin:
    fields = line.rstrip("\n").split(",")
    # TODO 5: skip malformed lines (too few fields or non-numeric PULocationID) and
    #         count them with a Hadoop counter by writing to STDERR:
    #             sys.stderr.write("reporter:counter:TaxiLab,BAD_RECORDS,1\n")
    # TODO 6: print "<zone name or id>\t1"
    pass
