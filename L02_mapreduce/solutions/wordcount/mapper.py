#!/usr/bin/env python3
"""Word count -- MAPPER (Hadoop Streaming).

Reads raw text lines on stdin, writes one "word<TAB>1" line per word.
Hadoop sorts mapper output by key before the reducer sees it.
"""
import re
import sys

WORD = re.compile(r"[a-z][a-z']*")

for line in sys.stdin:
    for word in WORD.findall(line.lower()):
        print(f"{word}\t1")
