# Apache Sqoop 1.4.7 on top of the course Hadoop image (L04, Part 1).
#
# Sqoop was retired to the Apache Attic in June 2021: there is no official image,
# no release since 1.4.7 (2017), and the binary was built for Hadoop 2.6.
# It still runs on Hadoop 3.4 with three fixes:
#   * commons-lang 2.6   (Sqoop uses org.apache.commons.lang.*, removed from Hadoop 3)
#   * commons-cli 1.9 -> 1.5.0 (Sqoop reflects on Option.addValueForProcessing, which
#     was removed in commons-cli 1.6; 1.5.0 still has it AND the builder API Hadoop needs)
#   * org.json (saved jobs: `sqoop job --show/--exec` need org.json.JSONObject, not shipped)
#   * the PostgreSQL JDBC driver
# In LOCAL mode Sqoop forces a single mapper, so the L04 stack also runs a tiny YARN cluster.
# A JDK (not just a JRE) is required: Sqoop generates and compiles a Java class per table.
FROM de-labs/hadoop:3.4.3

ARG SQOOP_VERSION=1.4.7
ARG PG_JDBC_VERSION=42.7.8
ENV SQOOP_HOME=/opt/sqoop \
    HADOOP_COMMON_HOME=/opt/hadoop \
    HADOOP_MAPRED_HOME=/opt/hadoop \
    HBASE_HOME=/tmp/no-hbase HCAT_HOME=/tmp/no-hcat ACCUMULO_HOME=/tmp/no-accumulo ZOOKEEPER_HOME=/tmp/no-zk
ENV PATH=${SQOOP_HOME}/bin:${PATH}

RUN set -eux; \
    curl -fSL --retry 3 -o /tmp/sqoop.tgz \
      "https://archive.apache.org/dist/sqoop/${SQOOP_VERSION}/sqoop-${SQOOP_VERSION}.bin__hadoop-2.6.0.tar.gz"; \
    tar -xzf /tmp/sqoop.tgz -C /opt; rm /tmp/sqoop.tgz; \
    mv /opt/sqoop-${SQOOP_VERSION}.bin__hadoop-2.6.0 ${SQOOP_HOME}; \
    rm -rf ${SQOOP_HOME}/docs ${SQOOP_HOME}/src; \
    curl -fSL --retry 3 -o ${SQOOP_HOME}/lib/commons-lang-2.6.jar \
      https://repo1.maven.org/maven2/commons-lang/commons-lang/2.6/commons-lang-2.6.jar; \
    curl -fSL --retry 3 -o ${SQOOP_HOME}/lib/postgresql-${PG_JDBC_VERSION}.jar \
      "https://repo1.maven.org/maven2/org/postgresql/postgresql/${PG_JDBC_VERSION}/postgresql-${PG_JDBC_VERSION}.jar"; \
    curl -fSL --retry 3 -o ${SQOOP_HOME}/lib/json-20240303.jar \
      https://repo1.maven.org/maven2/org/json/json/20240303/json-20240303.jar; \
    for f in $(find /opt/hadoop/share -name 'commons-cli-*.jar'); do rm "$f"; \
      curl -fSL --retry 3 -o "$(dirname "$f")/commons-cli-1.5.0.jar" \
        https://repo1.maven.org/maven2/commons-cli/commons-cli/1.5.0/commons-cli-1.5.0.jar; done; \
    mkdir -p /tmp/no-hbase /tmp/no-hcat /tmp/no-accumulo /tmp/no-zk /work

WORKDIR /work
CMD ["bash"]
