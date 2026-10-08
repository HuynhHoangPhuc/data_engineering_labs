# L06 — Instructor notes

## Timing (≈ 2 h)

| Block | Min |
|---|---|
| Recap: shuffle, partitions, join strategies, skew, AQE | 20 |
| E1 shuffle partitions + AQE coalescing | 15 |
| E2 broadcast vs sort-merge | 15 |
| E3 skew: baseline, salting, AQE skew join (UI: task timeline, SQL tab) | 35 |
| E4 repartition vs coalesce | 10 |
| E5 cache | 10 |
| Results table discussion | 15 |

## Setup facts

* Same image/config style as L05 (`local[4]`, 2 GB driver). All experiments run on 1 taxi month +
  a synthetic 10 M-row skewed fact table generated with `spark.range` (no extra download).
  Override with `L06_FACT_ROWS=20000000` on machines with more memory/disk.
* Timings come from `time.time()` around a **noop** write (full execution, no output), the plan and task
  statistics from the Spark REST API (`/api/v1/applications/.../sql`, `/stages/.../taskSummary`) — the
  same data the UI shows. The script appends to `results/results.csv` (students) or
  `results/results_solution.csv` (reference).
* On a single laptop, differences are smaller than on a cluster (no network); emphasise the **task
  max vs median** columns for skew rather than absolute seconds. Numbers vary ±20 % run to run; ask
  students to run twice.

## Common student errors

| Error | Fix |
|---|---|
| Skew "disappears" | the dimension got broadcast — threshold must be `-1` for the baseline |
| AQE skew join does not trigger | thresholds too high for lab-sized data; keep the lowered values (16 MB / factor 3) and disable coalescing |
| Salting gives different totals | dimension not replicated for every salt value, or salt range mismatch |
| E4 `coalesce(1)` "fast" | small data; discuss what happens with 100 GB (one task does all the work, also the upstream stage) |
| Forgetting `unpersist()` | Storage tab keeps the cache; memory pressure in later experiments |

## Grading hints

Collect `results/results.csv` + the filled `results/RESULTS.md` (observations). Full marks require
interpreting the skew stats (max ≫ median task time), naming the AQE plan nodes seen
(`AQEShuffleRead`, `BroadcastHashJoin` after demotion) and a justified recommendation per experiment.
