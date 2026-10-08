# L06 — My results

Machine: ______ (CPU / RAM / Docker memory)   Spark: 4.1.3 local[4]   Date: ______

Paste the table printed at the end of `spark-submit /lab/src/l06_experiments.py` (also in
`results/results.csv`). Run the script **twice** and use the second run.

| experiment | variant | seconds | total tasks | task median ms | task max ms | plan (final) | note |
|---|---|---|---|---|---|---|---|
| | | | | | | | |

## Observations (2–4 sentences each)

**E1 shuffle partitions / AQE coalescing**

**E2 broadcast vs sort-merge**

**E3 skew: baseline vs salting vs AQE**

**E4 repartition vs coalesce**

**E5 cache**

## Recommendations for a production job on a 20-node cluster
