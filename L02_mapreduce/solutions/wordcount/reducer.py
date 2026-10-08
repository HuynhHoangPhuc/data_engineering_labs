#!/usr/bin/env python3
"""Word count -- REDUCER (also usable as COMBINER).

Input arrives SORTED by key, so all counts of one word are adjacent:
    apache\t1
    apache\t1
    hadoop\t1
We keep a running total and emit it whenever the key changes.
"""
import sys

current_word, current_count = None, 0

for line in sys.stdin:
    word, _, count = line.rstrip("\n").partition("\t")
    try:
        count = int(count)
    except ValueError:
        continue  # skip malformed lines instead of failing the whole task
    if word == current_word:
        current_count += count
    else:
        if current_word is not None:
            print(f"{current_word}\t{current_count}")
        current_word, current_count = word, count

if current_word is not None:  # do not forget the last key!
    print(f"{current_word}\t{current_count}")
