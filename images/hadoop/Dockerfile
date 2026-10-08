# Multi-arch Apache Hadoop image for the course labs (L01-L04).
#
# Why not the official apache/hadoop image?  As of Sep 2026 every apache/hadoop
# tag (3.3.x, 3.4.x, 3.5.0) is published for linux/amd64 only.  On Apple
# Silicon it would run under emulation (slow, flaky JVMs).  This image is built
# locally from the official release tarball and works on arm64 and amd64.
#
# Build (done automatically by `docker compose up --build` in L01/L03/L04):
#   docker build -t de-labs/hadoop:3.4.3 labs/images/hadoop
FROM eclipse-temurin:11-jdk-noble

ARG HADOOP_VERSION=3.4.3
# dlcdn only carries current releases; archive.apache.org keeps everything (slower).
ARG APACHE_MIRROR=https://dlcdn.apache.org
ARG APACHE_ARCHIVE=https://archive.apache.org/dist
ARG TARGETARCH

ENV HADOOP_VERSION=${HADOOP_VERSION} \
    HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    PATH=/opt/hadoop/bin:/opt/hadoop/sbin:${PATH}

# python3 is needed on every NodeManager for Hadoop Streaming (L02);
# pyarrow is used by labs/datasets/parquet_to_csv.py (L02/L03).
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl python3 python3-pip procps netcat-openbsd less \
 && pip3 install --no-cache-dir --break-system-packages pyarrow==21.0.0 \
 && rm -rf /var/lib/apt/lists/* /root/.cache

# The aarch64 tarball ships native libs (libhadoop.so) built for ARM.
RUN set -eux; \
    if [ "${TARGETARCH:-$(dpkg --print-architecture)}" = "arm64" ]; then SUFFIX="-aarch64"; else SUFFIX=""; fi; \
    TGZ="hadoop-${HADOOP_VERSION}${SUFFIX}.tar.gz"; \
    curl -fSL --retry 3 -o /tmp/hadoop.tgz "${APACHE_MIRROR}/hadoop/common/hadoop-${HADOOP_VERSION}/${TGZ}" \
      || curl -fSL --retry 3 -o /tmp/hadoop.tgz "${APACHE_ARCHIVE}/hadoop/common/hadoop-${HADOOP_VERSION}/${TGZ}"; \
    mkdir -p /opt; tar -xzf /tmp/hadoop.tgz -C /opt; rm /tmp/hadoop.tgz; \
    mv /opt/hadoop-${HADOOP_VERSION} /opt/hadoop; \
    rm -rf /opt/hadoop/share/doc \
           /opt/hadoop/share/hadoop/tools/lib/bundle-*.jar \
           /opt/hadoop/share/hadoop/tools/lib/aliyun-* \
           /opt/hadoop/share/hadoop/tools/lib/azure-* \
           /opt/hadoop/share/hadoop/tools/lib/hadoop-azure* \
           /opt/hadoop/share/hadoop/tools/lib/hadoop-aliyun* \
           /opt/hadoop/share/hadoop/tools/lib/hadoop-aws* \
           /opt/hadoop/share/hadoop/tools/lib/hadoop-cos* \
           /opt/hadoop/share/hadoop/tools/lib/hadoop-huaweicloud* \
           /opt/hadoop/share/hadoop/tools/lib/cos_api-* \
           /opt/hadoop/share/hadoop/tools/lib/esdk-obs-*

# Default configuration: single-DataNode friendly (replication 1).
# L01 mounts its own hdfs-site.xml (replication 3, fast DataNode-death detection).
COPY conf/ /opt/hadoop/etc/hadoop/
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh \
 && mkdir -p /hadoop/dfs/name /hadoop/dfs/data /hadoop/yarn /hadoop/logs

ENV HADOOP_LOG_DIR=/hadoop/logs
WORKDIR /opt/hadoop
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["bash"]
