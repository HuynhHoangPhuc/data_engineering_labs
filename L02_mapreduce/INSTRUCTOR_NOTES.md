# L02 — Instructor notes

## Timing (≈ 110 min)

| Block | Min |
|---|---|
| Recap of the MapReduce model with the Unix pipe analogy | 10 |
| 1. Word count mapper/reducer, local pipe test | 25 |
| 2. First YARN job + YARN UI tour | 15 |
| 3. CSV sample preparation | 5 |
| 4. Zones mapper (map-side join) | 20 |
| 5. Combiner experiment, fill in the table | 20 |
| 6. JobHistory UI, logs; checkpoint discussion | 15 |

## Setup notes

* Uses the **L01 cluster** (`labs/L01_hdfs/docker-compose.yml`); `labs/L02_mapreduce` is mounted
  at `/lab/L02` inside every Hadoop container, so students edit on the laptop and run in the container.
* Gutenberg downloads need internet; offline fallback is documented (Hadoop's license texts).
* The NodeManager offers 1.5 GB to YARN (AM 512 MB + 2 concurrent 384 MB tasks) so the whole
  stack stays under ~3.5 GB. With 6 map tasks you will see two "waves" of maps in the UI —
  a nice illustration of slots/containers.

## Common student errors

| Error | Fix |
|---|---|
| Forgetting to flush the last key in the reducer | Last word missing — compare `wc -l` of local vs expected |
| `int(count)` crash on a header or empty line | Guard with try/except (solution does) |
| Using `print(word, 1)` (space instead of tab) | Streaming splits key/value at the first TAB; with a space the whole line becomes the key |
| Re-running into an existing output dir | `hdfs dfs -rm -r -skipTrash <dir>` |
| Windows line endings in .py files (students on Windows) | `/usr/bin/env: 'python3\r'` — we call `python3 mapper.py` explicitly to avoid the shebang; convert with `dos2unix` if needed |
| Expecting the combiner run to be faster | Great discussion point — see ANSWERS Q5 |

## Grading hints

Collect: the two Python files, the filled combiner table, answers to Q3–Q6. Full marks for Q6
require the (sum, count) idea. Bonus: a working average-fare stretch challenge.

## Talking points

* MapReduce is legacy for new pipelines (Spark replaced it, Hive moved to Tez), but the
  **map → shuffle/sort → reduce** model and the combiner idea are exactly what Spark does in
  `groupBy().agg()` with partial aggregation (L05/L06: look for `HashAggregate (partial)` in `explain()`).
