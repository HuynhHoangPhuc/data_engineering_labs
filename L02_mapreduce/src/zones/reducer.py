#!/usr/bin/env python3
"""Trips per zone -- REDUCER (also usable as COMBINER).

Identical logic to the word-count reducer: sum the counts per key.
Input arrives SORTED by key, so all counts of one zone are adjacent:
    Manhattan/Midtown Center\t1
    Manhattan/Midtown Center\t1
    Manhattan/Midtown East\t1
We keep a running total and emit it whenever the key changes.
"""
import sys

current_key, current_count = None, 0

for line in sys.stdin:
    key, _, count = line.rstrip("\n").partition("\t")
    try:
        count = int(count)
    except ValueError:
        continue  # skip malformed lines instead of failing the whole task
    if key == current_key:
        current_count += count
    else:
        if current_key is not None:
            print(f"{current_key}\t{current_count}")
        current_key, current_count = key, count

if current_key is not None:  # do not forget the last key!
    print(f"{current_key}\t{current_count}")
