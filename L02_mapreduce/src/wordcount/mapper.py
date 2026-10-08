#!/usr/bin/env python3
"""Word count -- MAPPER (Hadoop Streaming).

Hadoop feeds the input split to this script line by line on STDIN.
Whatever you print to STDOUT is the map output: one  key<TAB>value  per line.

Goal: for every word in the line, emit  "<word>\t1".
Normalise words to lower case and keep only letters and apostrophes,
e.g. "Hadoop's HDFS, hadoop!" -> hadoop's 1 / hdfs 1 / hadoop 1
"""
import re
import sys

WORD = re.compile(r"[a-z][a-z']*")

for line in sys.stdin:
    # TODO 1: lower-case the line, find all words with WORD.findall(...)
    #         and print "<word>\t1" for each of them.
    pass
