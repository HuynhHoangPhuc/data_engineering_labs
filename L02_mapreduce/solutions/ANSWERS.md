# L02 — Solutions

* Complete code: [`wordcount/mapper.py`](wordcount/mapper.py), [`wordcount/reducer.py`](wordcount/reducer.py),
  [`zones/mapper.py`](zones/mapper.py), [`zones/reducer.py`](zones/reducer.py)
* End-to-end reference run: [`l02_run_all.sh`](l02_run_all.sh)

## TODO solutions (short)

```python
# TODO 1 (wordcount/mapper.py)
for word in WORD.findall(line.lower()):
    print(f"{word}\t1")

# TODO 2/3 (wordcount/reducer.py)
    if word == current_word:
        current_count += count
    else:
        if current_word is not None:
            print(f"{current_word}\t{current_count}")
        current_word, current_count = word, count
if current_word is not None:
    print(f"{current_word}\t{current_count}")

# TODO 4 (zones/mapper.py)
zones[row["LocationID"]] = f"{row['Borough']}/{row['Zone']}"
# TODO 5/6
if len(fields) <= PU_COL or not fields[PU_COL].isdigit():
    sys.stderr.write("reporter:counter:TaxiLab,BAD_RECORDS,1\n"); continue
print(f"{zones.get(fields[PU_COL], fields[PU_COL])}\t1")
```

## Reference results (Apple M-series, Docker 4 GB, 2024-01, 1 M rows)

| Counter | No combiner | With combiner |
|---|---|---|
| Launched map tasks | 6 | 6 |
| Map output records | 1,000,000 | 1,000,000 |
| Map output bytes | 29,038,999 | 29,038,999 |
| Combine input / output records | 0 / 0 | 1,000,000 / 1,382 |
| Map output materialized bytes | 31,039,071 | 41,169 |
| Reduce shuffle bytes | 31,039,071 | 41,169 |
| Reduce input records | 1,000,000 | 1,382 |
| Wall clock | ~28 s | ~37 s |

Word count: 4 map tasks, `Reduce input groups=20967`, top words the 25806 / and 14340 / of 14113.
Zones top 3: Queens/JFK Airport 59777, Manhattan/Midtown Center 48750, Manhattan/Upper East Side South 47260.
The sum of all zone counts is exactly 1,000,000 (no bad records in this sample).

## Checkpoint answers

1. `sort`. It groups equal keys together, which is what the shuffle guarantees across the
   cluster (partition by key → merge-sort on each reducer). Without `sort` the reducer sees the
   same word in several non-adjacent runs and prints it many times with partial counts.
2. Streaming only gives the reducer a sorted *stream of lines*; the "group" abstraction lives in
   the Java framework. The Python reducer must detect key boundaries itself and flush the last key.
3. **6 map tasks** = 6 input splits = 6 HDFS blocks of 16 MB for the 92.8 MB CSV (one split per
   block by default). Word count had 4 maps because each book (< 1 block) is its own split.
   **2 reduce tasks** because we set `-D mapreduce.job.reduces=2` (default is 1) — the reducer
   count is a user choice, not derived from data.
4. ~754× (31,039,071 → 41,169 bytes). Each map task outputs one record per *distinct zone it saw*:
   ~230–260 zones × 6 maps ≈ 1,382. Only those partial sums cross the network.
5. On one laptop the "network" shuffle is a local disk/loopback copy — nearly free — while the
   combiner adds a Python process that must re-read, parse and sort 1 M records per map. On a real
   cluster where the shuffle crosses a 10 GbE network and reducers must merge-sort GBs from
   hundreds of mappers, cutting shuffle volume by 99.9 % dominates and the combiner wins.
6. No — an average of averages is wrong (partitions have different counts). Make the combiner
   emit partial `(sum, count)` pairs and let the reducer compute `sum/count`. A combiner must be
   associative & commutative, and its output format must equal its input format; Hadoop may run
   it 0, 1 or many times.
7. Each mapper loads the small `taxi_zone_lookup.csv` (265 rows, shipped via the distributed cache
   `-files`) into a dict and replaces IDs with names — no shuffle needed for the join. It stops
   being a good idea when the "small" side no longer fits comfortably in each task's memory; then
   you need a reduce-side join (both datasets shuffled by key). Spark calls the same idea a
   **broadcast join** (L06).
8. In the job counters (console at the end of the job, and JobHistory UI → Counters → group
   `TaxiLab`). A few malformed lines should not kill a job that processes billions of rows; counting
   them makes data-quality problems visible and lets you set a threshold.
