# L01 — Solutions

Full scripted walkthrough: [`l01_run_all.sh`](l01_run_all.sh) (run from `labs/L01_hdfs`
after `docker compose up -d --build`; ~3 min).

## TODOs

**TODO 2.1**
```bash
hdfs dfs -stat "%n blocksize=%o repl=%r size=%b" /data/taxi/taxi_zone_lookup.csv
# taxi_zone_lookup.csv blocksize=134217728 repl=3 size=12331
```

**TODO 4.1**
```bash
hdfs dfs -D dfs.replication=1 -put /datasets/data/taxi/taxi_zone_lookup.csv /tmp/one_replica.csv
hdfs dfs -ls /tmp/one_replica.csv     # 2nd column = 1
```
`dfs.replication` and `dfs.blocksize` are *client-side* settings: the writer decides them per
file; `hdfs-site.xml` only supplies the default.

## Observed results (reference run, Apple M-series, Docker 4 GB)

| Step | Result |
|---|---|
| default block size upload | 1 block, `len=49961641`, `Live_repl=3` |
| 16 MB block size upload | 3 blocks: 16777216 + 16777216 + 16407209 bytes |
| `du -h /data/taxi` | `47.6 M  142.9 M` per copy (logical vs. raw ×3); `95.3 M` raw after `setrep 2` |
| `docker compose stop datanode3` | reported dead after **~90 s**; its replicas removed from the block map at **~100 s** (next heartbeat-monitor run, ≤ 30 s later); replication-2 blocks re-replicated 1–2 s after that |
| fsck while dead | `Under-replicated blocks: 2 (40.0 %)` — the 2 replication-3 files; `Status: HEALTHY` |
| after restart | `Under-replicated 0`, `Over-replicated 0` (excess replicas deleted) |
| safe mode | `put: Cannot create file/tmp/x.csv._COPYING_. Name node is in safe mode.` |

## Checkpoint answers

1. **300 MB, 128 MB blocks, replication 3** → ⌈300/128⌉ = **3 blocks** (128 + 128 + 44 MB),
   **9 block replicas**, ~**900 MB** raw disk (plus tiny `.meta` checksum files). The NameNode keeps
   3 block objects in RAM (≈150 bytes each), regardless of replication.
2. The file simply ends there: 49,961,641 − 2 × 16,777,216 = 16,407,209 bytes. A block is a
   *maximum* size; the last block file on the DataNode is exactly as large as its data, so no
   disk is wasted (unlike file-system blocks of e.g. 4 KB that are padded).
3. Block → DataNode locations are held **only in NameNode memory**. They are *not* in the fsimage
   or edit log. After a restart the NameNode rebuilds them from DataNode **block reports** — that
   is why it sits in safe mode until enough blocks are reported.
4. Re-replication needs a DataNode that does not already hold a replica. With 2 live nodes a
   replication-2 block can be copied to the node that lacks it, but a replication-3 block
   already has a copy on both remaining nodes — there is nowhere to put the third one. HDFS never
   puts two replicas of a block on the same DataNode.
5. `Status: HEALTHY` with `Under-replicated blocks: 2`. HEALTHY means every block has at least
   `dfs.namenode.replication.min` (1) live replica, so all data is readable. Under-replication is a
   *risk* (less redundancy), not data loss. MISSING/CORRUPT would be an error.
6. With the default (`recheck-interval` 5 min) a node is declared dead after
   2 × 5 min + 10 × 3 s = **10.5 min**. A long timeout avoids a *replication storm*: a node that
   reboots or has a short network glitch would otherwise trigger copying terabytes of blocks that
   become redundant again a minute later. Clients still avoid *stale* nodes (no heartbeat for 30 s)
   for reads/writes in the meantime.
7. At start the NameNode knows the namespace (from fsimage + edits) but not where blocks are. Until
   ≥ 99.9 % of blocks have been reported it cannot tell "missing" from "not reported yet" — if it
   allowed writes/deletes or started re-replicating it would make wrong decisions (e.g. schedule
   massive re-replication of blocks that simply have not been reported yet). Safe mode = read-only
   until the block map is trustworthy.
8. Every file, directory and block is an object in NameNode **heap** (~150 B each). 1 M tiny files
   = ≥ 2 M objects ≈ 300 MB heap for only 1 GB of data, plus 1 M RPCs and 1 M map tasks later
   (one split per file). HDFS is designed for fewer, larger files — the "small files problem".
   Remedies: merge files, use container formats (Parquet/ORC, SequenceFile, HAR).
