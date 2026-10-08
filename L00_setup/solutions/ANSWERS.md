# L00 — Answers

1. An **image** is a read-only, layered template (file system + metadata such as the default
   command). A **container** is a running (or stopped) instance of an image with its own writable
   layer, process tree, network namespace and resource limits. One image → many containers.
2. On macOS/Windows, containers run inside a Linux **VM** managed by Docker Desktop; `free` shows the
   VM's memory (the value set in Settings → Resources). On Linux, containers share the host kernel
   and `free` shows host memory unless a cgroup limit applies (`docker stats` shows the limit).
3. `down` removes containers and networks; `down -v` additionally removes the **named volumes**
   declared in the compose file (HDFS blocks, database files …) → the next `up` starts from scratch
   (e.g. Postgres re-runs the init scripts, the NameNode is formatted again).
4. Data is large and changes independently of software; bind mounts avoid bloating images, avoid
   rebuilding images when data changes, and let host tools and all labs share one copy.
5. Docker refuses (`no matching manifest for linux/arm64`) unless you pass `--platform linux/amd64`;
   then it runs under QEMU/Rosetta emulation — slower, and JVM-heavy software can be flaky. That is
   why the course builds its own multi-arch Hadoop image (`labs/images/hadoop`).
