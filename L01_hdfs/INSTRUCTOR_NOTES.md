# L01 — Instructor notes

## Timing (≈ 110 min)

| Block | Min |
|---|---|
| Image build / cluster start (do the build **before** class: `docker compose build`) | 5–10 |
| 1. Cluster tour + NameNode UI | 10 |
| 2. `hdfs dfs` shell | 15 |
| 3. Blocks + fsck + physical block files | 20 |
| 4. setrep | 10 |
| 5. Kill a DataNode (incl. ~90 s wait — use it to discuss heartbeats, stale vs dead) | 25 |
| 6. Safe mode | 10 |
| Checkpoint discussion | 15 |

## Before class

* Students must run **L00** at home: Docker memory ≥ 4 GB (6 GB recommended), taxi file downloaded.
* Pre-build the image: `cd labs/L01_hdfs && docker compose build` (downloads ~500 MB from
  dlcdn.apache.org). On a slow classroom network, build once and distribute with
  `docker save de-labs/hadoop:3.4.3 | gzip > hadoop.tgz` / `docker load < hadoop.tgz`.
* Image is ~2.2 GB on disk (JDK 11 + Hadoop 3.4.3 + Python/pyarrow for L02/L03).

## Common student errors

| Error | Cause / fix |
|---|---|
| Running `hdfs ...` on the laptop | They must be inside `docker compose exec namenode bash` |
| `put: File exists` | Re-running a step. Use `-put -f` or `-rm` first |
| `No such file or directory: /datasets/data/taxi/...` | Taxi file not downloaded (`bash labs/datasets/download_taxi.sh`) |
| Kill a DataNode that holds *no* replica of the rep-2 file | Nothing gets re-replicated — ask them to check `fsck` locations first (all 3 blocks usually span all 3 nodes, but not always) |
| Impatience in step 5 | Death takes ~90 s by design, plus up to 30 s until the heartbeat monitor removes the replicas; with defaults it would be 10.5 min (good discussion) |
| Everything gone after `docker compose down -v` | `-v` deletes volumes; `down` without `-v` keeps HDFS |
| `Incompatible clusterIDs` | Mixed volumes from an older run → `down -v` |

## Grading hints (if collected)

Ask for a short report with: (a) `fsck -files -blocks -locations` output for the 16 MB-block file
before and after killing a DataNode, (b) time until the node was declared dead, (c) answers to
checkpoint questions 1, 4, 6, 7. Full marks require explaining *why* the rep-3 files stayed
under-replicated (Q4) and why block locations are not persisted (Q3/Q7).

## Talking points

* HDFS write pipeline: client → DN1 → DN2 → DN3, acks flow back; the NameNode never sees data.
* Replica placement (rack-aware): 1st on writer's node (if it is a DataNode), 2nd on another rack,
  3rd on same rack as 2nd. Here we have 1 rack, so placement is random.
* Stale (30 s) vs dead (10.5 min default) — why two thresholds.
* Modern context: HDFS is still used on-prem, but most new platforms use object stores
  (S3/GCS/ADLS) — no NameNode, no data locality, different consistency/performance model.
  Erasure coding (stretch) is how Hadoop 3 cut storage overhead from 200 % to ~50 %.
