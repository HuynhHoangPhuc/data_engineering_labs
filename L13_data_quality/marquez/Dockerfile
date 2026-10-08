# Marquez API (OpenLineage reference backend) as a NATIVE multi-arch image.
# Why: the official marquezproject/marquez images are published for linux/amd64 only (checked Oct 2026,
# latest 0.51.1, March 2025). The API is a plain Java 17 app, so we run the release jar from Maven Central
# (io.github.marquezproject:marquez-api:0.50.0 - the newest version on Maven Central) on an arm64/amd64 JRE.
FROM eclipse-temurin:17-jre
ARG MARQUEZ_VERSION=0.50.0
WORKDIR /usr/src/app
ADD --checksum=sha256:56bbac585c3eb9d3409ef2026383233af81eebf6633a20597789292b40924343 \
    https://repo1.maven.org/maven2/io/github/marquezproject/marquez-api/${MARQUEZ_VERSION}/marquez-api-${MARQUEZ_VERSION}.jar \
    /usr/src/app/marquez-api.jar
COPY marquez.yml /usr/src/app/marquez.yml
EXPOSE 5000 5001
ENTRYPOINT ["java", "-Duser.timezone=UTC", "-Dlog4j2.formatMsgNoLookups=true", "-jar", "/usr/src/app/marquez-api.jar", "server", "/usr/src/app/marquez.yml"]
