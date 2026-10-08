# L01 — HDFS: blocks, replicas and a DataNode failure

| | |
|---|---|
| Module | 1 — Hadoop (HDFS, YARN, MapReduce) |
| Time | 90–120 min |
| Stack | Hadoop 3.4.3: 1 NameNode, 3 DataNodes, 1 ResourceManager, 1 NodeManager, 1 JobHistory server (the same cluster is reused in **L02**) |
| RAM | ~1.5 GB idle, ~3 GB while a MapReduce job runs |
| Prerequisites | [L00](../L00_setup/README.md) done; `labs/datasets/data/taxi/yellow_tripdata_2024-01.parquet` downloaded |

## Learning objectives

By the end of this lab you can:

1. Start a multi-node HDFS cluster with Docker Compose and read its health from the CLI and the NameNode web UI.
2. Use the `hdfs dfs` file-system shell (mkdir, put, ls, du, cat, get, mv, rm …).
3. Explain how a file becomes **blocks** and **replicas**, and prove it with `hdfs fsck -files -blocks -locations`.
4. Change the replication factor of a file with `setrep` and the block size at write time with `-D dfs.blocksize`.
5. Kill a DataNode, watch the NameNode detect it, report **under-replicated blocks**, and **re-replicate** them automatically.
6. Explain and use **safe mode**.

## Architecture

```
                   your laptop (host)
   browser ──► :9870 NameNode UI   :8088 YARN UI   :19888 JobHistory UI
 ────────────────────────────────────────────────────────────────────────
  docker network l01_hdfs_default
  ┌──────────────┐  metadata only   ┌────────────┐ ┌────────────┐ ┌────────────┐
  │  namenode    │◄──heartbeats─────│ datanode1  │ │ datanode2  │ │ datanode3  │
  │  (fsimage,   │  every 3 s +     │ blocks on  │ │ blocks on  │ │ blocks on  │
  │  edit log)   │  block reports   │ volume     │ │ volume     │ │ volume     │
  └──────▲───────┘                  └─────▲──────┘ └─────▲──────┘ └─────▲──────┘
         │ 1. "where do I write?"         │ 2. data is streamed in a pipeline
  hdfs dfs client ────────────────────────┴──► dn ──► dn ──► dn (replication 3)
  (we run it inside the namenode container = our "edge node")

  resourcemanager + nodemanager + historyserver  → used in L02 (MapReduce on YARN)
```

Configuration lives in [`conf/`](conf/) and is mounted into every container. Two
lab-only tweaks in [`conf/hdfs-site.xml`](conf/hdfs-site.xml) are worth reading now:

* `dfs.namenode.heartbeat.recheck-interval = 30000` → a silent DataNode is declared
  **dead after ~90 s** (2 × 30 s + 10 × 3 s heartbeat) instead of the default **10.5 min**.
* `dfs.permissions.enabled = false` → no permission errors while learning (never in production).

> **Why a custom image?** The official `apache/hadoop` images are published for
> `linux/amd64` only. `labs/images/hadoop/Dockerfile` builds the same Hadoop 3.4.3
> release natively for arm64 (Apple Silicon) and amd64. The first `up --build`
> downloads ~500 MB and takes 3–6 minutes; later starts take seconds.

## Tasks

All commands are run from `labs/L01_hdfs/` on your laptop unless the prompt shows
`root@namenode:/opt/hadoop#` (inside the container).

### 1. Start the cluster

```bash
cd labs/L01_hdfs
docker compose up -d --build
docker compose ps
```

Expected: 7 services `Up`, `namenode` becomes `(healthy)` after ~20 s.

Open a shell on the "edge node" (we use the NameNode container as the client):

```bash
docker compose exec namenode bash
```

Wait until all 3 DataNodes have registered:

```bash
root@namenode:/opt/hadoop# hdfs dfsadmin -report | grep -E "Live datanodes|^Name|^Hostname"
Live datanodes (3):
Name: 172.20.0.2:9866 (l01_hdfs-datanode3-1.l01_hdfs_default)
Hostname: datanode3
...
```

**Write down the IP → hostname mapping** — `fsck` prints IPs later. (Your IPs will differ.)

Open <http://localhost:9870> → *Overview* (capacity, live nodes, safe mode status) and
*Datanodes* tab.

### 2. The `hdfs dfs` shell

```bash
hdfs dfs -mkdir -p /user/root /data/taxi
hdfs dfs -ls /
hdfs dfs -put /datasets/data/taxi/taxi_zone_lookup.csv /data/taxi/
hdfs dfs -ls /data/taxi
hdfs dfs -head /data/taxi/taxi_zone_lookup.csv
hdfs dfs -cat /data/taxi/taxi_zone_lookup.csv | grep -c Manhattan      # 70
hdfs dfs -cp /data/taxi/taxi_zone_lookup.csv /user/root/zones.csv
hdfs dfs -mv /user/root/zones.csv /user/root/zones_copy.csv
hdfs dfs -get /user/root/zones_copy.csv /tmp/ && ls -l /tmp/zones_copy.csv
hdfs dfs -rm /user/root/zones_copy.csv
hdfs dfs -du -h /data/taxi
hdfs dfs -count -q -h /data
```

`/datasets` inside the container is the `labs/datasets` folder of your laptop (bind mount).

In `hdfs dfs -ls` output the **2nd column is the replication factor** (`3`), and
`du -h` prints two sizes: logical size and raw space consumed by all replicas.

> **TODO 2.1** — Using `hdfs dfs -stat`, print *name, block size, replication and size* of
> `taxi_zone_lookup.csv` (hint: `hdfs dfs -stat` accepts a format string with `%n %o %r %b`).

### 3. Blocks: default vs. small block size

The January 2024 file is ~48 MB (49,961,641 bytes). Upload it twice — once with
the default 128 MB block size, once with **16 MB blocks**:

```bash
hdfs dfs -mkdir -p /data/taxi/default_blocks /data/taxi/small_blocks
hdfs dfs -put /datasets/data/taxi/yellow_tripdata_2024-01.parquet /data/taxi/default_blocks/
hdfs dfs -D dfs.blocksize=16m -put /datasets/data/taxi/yellow_tripdata_2024-01.parquet /data/taxi/small_blocks/
hdfs dfs -stat "%n blocksize=%o repl=%r size=%b" /data/taxi/*/yellow_tripdata_2024-01.parquet
```

Now ask the NameNode where the blocks are:

```bash
hdfs fsck /data/taxi/small_blocks -files -blocks -locations
```

Expected (IDs and IPs differ):

```
/data/taxi/small_blocks/yellow_tripdata_2024-01.parquet 49961641 bytes, replicated: replication=3, 3 block(s):  OK
0. BP-...:blk_1073741826_1002 len=16777216 Live_repl=3  [DatanodeInfoWithStorage[172.20.0.7:9866,...], [172.20.0.6:9866,...], [172.20.0.2:9866,...]]
1. BP-...:blk_1073741827_1003 len=16777216 Live_repl=3  [...]
2. BP-...:blk_1073741828_1004 len=16407209 Live_repl=3  [...]
Status: HEALTHY
```

Run the same `fsck` on `/data/taxi/default_blocks` and compare (1 block).

Look at the **physical block files** on a DataNode (from your laptop, in a 2nd terminal):

```bash
docker compose exec datanode1 bash -c 'find /hadoop/dfs/data -name "blk_*" -exec ls -l {} \;'
```

Each block is an ordinary Linux file `blk_<id>` plus a `blk_<id>_<genstamp>.meta` checksum file.
Notice the last block is *smaller* than 16 MB: HDFS does not pad blocks.

### 4. Replication factor

```bash
hdfs dfs -setrep -w 2 /data/taxi/small_blocks/yellow_tripdata_2024-01.parquet
hdfs dfs -ls /data/taxi/small_blocks
hdfs fsck /data/taxi/small_blocks -files -blocks -locations | grep Live_repl
hdfs dfs -du -h /data/taxi
```

Expected: `Live_repl=2` for all 3 blocks; `du` raw size of `small_blocks` drops from 142.9 M to 95.3 M.

> **TODO 4.1** — Upload `taxi_zone_lookup.csv` to `/tmp/one_replica.csv` **with replication 1**
> in a single command (no `setrep`). Hint: `-D` works for any client-side property.

### 5. Kill a DataNode → under-replication → re-replication

The file in `small_blocks` now has replication 2 and there are 3 DataNodes, so if one
dies HDFS can re-create the lost replicas on the third node. Files with replication 3
cannot be healed (only 2 nodes left) and will stay **under-replicated**.

1. Choose a victim: a DataNode that holds replicas of the `small_blocks` file
   (map the IPs from `fsck` to hostnames with `hdfs dfsadmin -report`). Below we use `datanode3`.
2. In a **second terminal** on your laptop:

   ```bash
   docker compose stop datanode3
   ```

3. Back in the namenode shell, watch the NameNode's view (Ctrl-C to stop):

   ```bash
   watch -n 5 'hdfs dfsadmin -report | grep -E "Live datanodes|Dead datanodes|Under replicated"'
   # no 'watch' installed?  while true; do hdfs dfsadmin -report | grep -E "Live|Dead|Under"; sleep 5; done
   ```

   * after ~15 s the node is **stale** (UI *Datanodes* tab: "Last contact" keeps growing)
   * after **~90 s**: `Live datanodes (2)`, `Dead datanodes (1)`
   * the NameNode's heartbeat monitor runs every `recheck-interval` (30 s): **up to 30 s later** it
     removes the dead node's replicas from the block map (NameNode log:
     `BLOCK* removeDeadDatanode: lost heartbeat from ...`) — only now does `Under replicated blocks`
     jump up and re-replication start. Until then `fsck` still lists the dead node as a location.

4. Inspect:

   ```bash
   hdfs fsck /data -files -blocks -locations | grep -E "^/data|Live_repl|Under replicated|Status"
   ```

   Expected:

   ```
   /data/taxi/default_blocks/yellow_tripdata_2024-01.parquet ... Under replicated BP-...:blk_... Target Replicas is 3 but found 2 live replica(s) ...
   /data/taxi/small_blocks/yellow_tripdata_2024-01.parquet ... replication=2, 3 block(s):  OK
   0. ... Live_repl=2 [ two IPs that are NOT datanode3 ]
   ...
   Status: HEALTHY
    Under-replicated blocks:	2 (40.0 %)
   ```

   If you kept `/tmp/one_replica.csv` from TODO 4.1 and its only replica lived on `datanode3`,
   `fsck` now reports it as **MISSING / CORRUPT** — replication 1 means no redundancy at all.

   The replication-2 file is **re-replicated within seconds** of the replicas being removed —
   compare its block locations with the ones you noted before. The replication-3 files remain
   under-replicated but readable: `Status: HEALTHY` means *no data is missing*.

5. Prove the data is still readable:

   ```bash
   hdfs dfs -get /data/taxi/default_blocks/yellow_tripdata_2024-01.parquet /tmp/check.parquet
   md5sum /tmp/check.parquet /datasets/data/taxi/yellow_tripdata_2024-01.parquet
   ```

6. Bring the node back and watch HDFS delete the now **excess** replicas:

   ```bash
   docker compose start datanode3      # laptop terminal
   hdfs fsck / | grep -E "Under-replicated|Over-replicated|Status"   # namenode shell, after ~10 s
   ```

   Expected: `Under-replicated blocks: 0`, `Over-replicated blocks: 0`, `HEALTHY`.

### 6. Safe mode

```bash
hdfs dfsadmin -safemode get
hdfs dfsadmin -safemode enter
hdfs dfs -put /datasets/data/taxi/taxi_zone_lookup.csv /tmp/x.csv
# put: Cannot create file/tmp/x.csv._COPYING_. Name node is in safe mode.
hdfs dfs -ls /data/taxi          # reads still work
hdfs dfsadmin -safemode leave
```

Now watch the *automatic* safe mode at startup. In the laptop terminal:

```bash
docker compose restart namenode
docker compose exec namenode hdfs dfsadmin -safemode get     # "Safe mode is ON" for a few seconds
docker compose exec namenode hdfs dfsadmin -safemode wait    # blocks until OFF
```

The NameNode stays read-only until DataNodes have reported ≥ 99.9 %
(`dfs.namenode.safemode.threshold-pct`) of the blocks, plus a 5 s extension (lab setting).

### 7. (Optional) Read the NameNode's metadata

```bash
hdfs dfsadmin -safemode enter && hdfs dfsadmin -saveNamespace && hdfs dfsadmin -safemode leave
ls -l /hadoop/dfs/name/current/ | tail
hdfs oiv -p XML -i $(ls /hadoop/dfs/name/current/fsimage_* | grep -v md5 | tail -1) -o /tmp/fsimage.xml
grep -o "<name>[^<]*</name>" /tmp/fsimage.xml | head -20
```

The fsimage contains the namespace (names, permissions, block IDs) — **but no block
locations**: those are rebuilt from DataNode block reports at every start (→ safe mode).

## Checkpoint questions

1. A 300 MB file is written with the default block size and replication. How many blocks,
   how many block replicas, and how much raw disk space are used?
2. Why is the last block of the taxi file smaller than 16 MB, and does it waste 16 MB of disk?
3. Where is the list of *block locations* stored? What happens to it when the NameNode restarts?
4. After `datanode3` died, why was the replication-2 file healed but the replication-3 files were not?
5. What did the `fsck` status say while blocks were under-replicated, and why is that not an error?
6. What would happen with the default `dfs.namenode.heartbeat.recheck-interval` (5 min)?
   Why is a long timeout a *good* default in production?
7. Why does the NameNode start in safe mode, and why must writes be blocked in it?
8. `hdfs dfs -put` of a 1 KB file vs. 1 KB × 1 million files: why is the second one a problem for HDFS?

## Stretch challenges

* **Erasure coding**: `hdfs ec -enablePolicy -policy XOR-2-1-1024k`, create `/data/ec`,
  `hdfs ec -setPolicy -path /data/ec -policy XOR-2-1-1024k`, upload the taxi file and compare
  `hdfs dfs -du -h` with the replicated copy (≈ 1.5× instead of 3× raw size). The warning
  about racks is expected on a 1-rack cluster.
* **Decommission** a DataNode gracefully instead of killing it (add an `dfs.hosts.exclude` file,
  `hdfs dfsadmin -refreshNodes`) and compare with the crash scenario.
* **Balancer**: add a 4th DataNode to `docker-compose.yml`, start it, run `hdfs balancer -threshold 5`.
* **WebHDFS**: `curl -s "http://localhost:9870/webhdfs/v1/data/taxi?op=LISTSTATUS" | python3 -m json.tool`.
* Corrupt a block on purpose (`docker compose exec datanode1 bash`, overwrite a `blk_` file),
  then read the file and run `fsck` — what does HDFS do?

## Cleanup

```bash
docker compose down -v      # stops containers AND deletes HDFS volumes
```

Skip `-v` if you want to keep the data for **L02** (you can also just leave the cluster running
and go straight to L02).

## Troubleshooting

| Symptom | Fix |
|---|---|
| `Live datanodes (0)` right after start | DataNodes wait for the NameNode RPC port; give it 20–30 s. `docker compose logs datanode1` |
| `Name node is in safe mode` on write | Normal for a few seconds after start: `hdfs dfsadmin -safemode wait` |
| `Incompatible clusterIDs` in a DataNode log | Old DataNode volume + newly formatted NameNode. `docker compose down -v` and start again |
| `port is already allocated` (9870/8088/…) | Another lab stack is still running: `docker ps`, then `docker compose down` in that folder |
| Links in the web UIs point to `http://datanode1:9864` or `historyserver:19888` and do not open | Container hostnames do not resolve on your laptop. Replace the host with `localhost` for published ports, or add `127.0.0.1 namenode resourcemanager nodemanager historyserver` to `/etc/hosts` |
| NameNode UI *Browse the file system* → download fails | Same reason (it redirects to a DataNode hostname); use `hdfs dfs -get` instead |
| Build fails downloading the tarball | Retry; or build with the slower archive mirror: `docker build --build-arg APACHE_MIRROR=https://archive.apache.org/dist -t de-labs/hadoop:3.4.3 ../images/hadoop` |
| Containers exit with code 137 | Out of memory: raise Docker Desktop memory (≥ 4 GB, 6 GB recommended), stop other stacks |
