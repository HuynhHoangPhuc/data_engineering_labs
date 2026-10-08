# L11 - Checkpoint answers

1. For each table the catalog stores essentially **one pointer**: the location of the current `metadata.json`
   (plus namespace/table names and properties). A commit writes new data files, new manifests, a new manifest list and a new
   `metadata.json`, then asks the catalog to swap the pointer **atomically, only if it still points to the version the
   writer started from** (optimistic concurrency / compare-and-swap). The loser of a race gets a `CommitFailedException`,
   re-reads the new metadata, re-validates (e.g. no conflicting deletes) and retries.

2. Iceberg stores the partition **transform** (`days(pickup_ts)`) in the table metadata and the partition value of every file
   in the manifests. When a query filters on `pickup_ts`, the planner applies the same transform to the predicate and prunes
   files by the manifests' partition values and column statistics. With Hive-style partitioning the partition column is a
   separate physical column: if the user filters on `pickup_ts` instead of `pickup_date`, **no pruning happens** (full scan),
   and writers must fill `pickup_date` consistently themselves.

3. Iceberg identifies columns by a permanent **field id** stored in the Parquet files, so a rename only changes the name in the
   metadata; old files are still read correctly. Plain Parquet + Hive resolves columns by **name** (or position): after a rename
   old files have no column with the new name -> NULLs, or worse, positional mapping silently reads the wrong column.

4. Planning happens per partition spec: for files written with spec 0 (day) Iceberg can only prune by day; for files with spec
   1 (day + vendor) it prunes by day and vendor. Both sets are combined in one scan; column min/max statistics may prune further.

5. The default for Spark writes is **copy-on-write**: any data file that contains at least one matched row is rewritten in full
   (here the one file of the January partition, 3,351 rows) and replaced by a new file with the merged content (4,484 rows).
   The snapshot summary therefore counts all rows of the rewritten file. With **merge-on-read**, the MERGE writes only the new rows
   plus small **delete files** (position deletes / deletion vectors in format v3) - faster writes, slower reads until compaction.

6. `expire_snapshots` removes old snapshots from the metadata and physically deletes data/manifest files **no longer referenced by
   any remaining snapshot** (time travel to them is no longer possible). `remove_orphan_files` deletes files in the table location
   that are **not referenced by any metadata at all** (e.g. leftovers of failed/aborted writes) - it needs a safety margin
   (`older_than`) so it does not delete files of in-flight commits.

7. Readers only see what the catalog's current `metadata.json` references. Spark's new files become visible only when the atomic
   pointer swap of the commit succeeds; until then Trino keeps reading the previous snapshot (snapshot isolation). Half-written
   files are never referenced, so they're invisible (and later cleaned up by `remove_orphan_files`).
