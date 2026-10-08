#!/usr/bin/env python3
"""Word count -- REDUCER (Hadoop Streaming).

Hadoop SORTS the map output by key before calling the reducer, so all lines
with the same word arrive one after another:
    apache\t1
    apache\t1
    hadoop\t1
Unlike the Java API you do NOT get (key, [values]) -- you must detect the
key change yourself.

Goal: print "<word>\t<total>" once per word.
"""
import sys

current_word, current_count = None, 0

for line in sys.stdin:
    word, _, count = line.rstrip("\n").partition("\t")
    count = int(count)
    # TODO 2: if word == current_word, add to the running total;
    #         otherwise print the previous word's total (if any) and start a new one.
    pass

# TODO 3: do not forget to print the LAST word's total here.
