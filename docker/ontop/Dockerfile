# syntax=docker/dockerfile:1
#
# Ontop VKG, built from the fork that can carry the caller's identity and correlation ID down to
# the data source. See docker/ontop/README.md for what the fork changes and why stock Ontop
# cannot do this.
#
# Three stages: build Ontop from the fork, resolve the Spark Connect JDBC driver and its runtime
# closure, then put the two together on a JRE new enough for Spark.

ARG ONTOP_REPO=https://github.com/larsw/ontop.git
# A commit, not a branch: a branch name would keep resolving to whatever the Docker layer cache
# fetched first. Pass ONTOP_REF=<sha> (or a branch, which `git fetch` also accepts) to move it.
ARG ONTOP_REF=738ed7d61f8c0b1d3cf48cfcc327abe03a463d9f
ARG SPARK_VERSION=4.1.3

# ---------------------------------------------------------------- Ontop ------
# Java 11, which is what upstream's own image builds with. The result runs on any later JRE.
FROM maven:3-eclipse-temurin-11 AS ontop-builder
ARG ONTOP_REPO
ARG ONTOP_REF
WORKDIR /build
RUN apt-get update \
 && apt-get install -y --no-install-recommends git \
 && rm -rf /var/lib/apt/lists/*
# fetch rather than clone --branch, so ONTOP_REF may be a commit. GitHub serves a reachable SHA.
RUN git init -q . \
 && git remote add origin "${ONTOP_REPO}" \
 && git fetch -q --depth 1 origin "${ONTOP_REF}" \
 && git checkout -q FETCH_HEAD \
 && git rev-parse HEAD > /ontop-revision
# 'dir' rather than a zip, so the next stage can copy the tree straight across.
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -q clean package -Passet-cli -Dmaven.test.skip -Dassembly.cli.format=dir \
 && mv build/distribution/target/ontop-cli-*/ /ontop \
 && rm -f /ontop/ontop.bat /ontop/ontop-completion.sh

# ------------------------------------------------- Spark Connect JDBC --------
# The driver that speaks Spark Connect's gRPC protocol behind a java.sql.Driver. It is what makes
# this whole thing possible: its connection string takes arbitrary parameters and turns the ones it
# does not recognise into gRPC metadata -- which is exactly where the propagation plugin on the
# server reads x-user-token and x-correlation-id from.
FROM maven:3-eclipse-temurin-17 AS jdbc-driver
ARG SPARK_VERSION
WORKDIR /driver
RUN printf '%s\n' \
    '<project xmlns="http://maven.apache.org/POM/4.0.0">' \
    '  <modelVersion>4.0.0</modelVersion>' \
    '  <groupId>io.sparkconnect.poc</groupId>' \
    '  <artifactId>spark-connect-jdbc-bundle</artifactId>' \
    '  <version>0.1.0</version>' \
    '  <dependencies>' \
    '    <dependency>' \
    '      <groupId>org.apache.spark</groupId>' \
    "      <artifactId>spark-connect-client-jdbc_2.13</artifactId>" \
    "      <version>${SPARK_VERSION}</version>" \
    '    </dependency>' \
    '  </dependencies>' \
    '</project>' > pom.xml
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -q dependency:copy-dependencies -DoutputDirectory=/jdbc -DincludeScope=runtime

# ------------------------------------------------------------- runtime -------
# Java 17: Spark 4 refuses anything older, and Ontop's Spring Boot 2.7 is happy on it (21 is not
# a version Boot 2.7 claims to support).
FROM eclipse-temurin:17-jre-jammy

ENV PATH="/opt/ontop:${PATH}"
WORKDIR /opt/ontop

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl jq ca-certificates; \
    rm -rf /var/lib/apt/lists/*

COPY --from=ontop-builder /ontop/          /opt/ontop/
COPY --from=ontop-builder /ontop-revision  /opt/ontop/ONTOP_REVISION
# 'jdbc/' is on Ontop's classpath after 'lib/', so Ontop's own Guava and Jackson still win.
COPY --from=jdbc-driver   /jdbc/           /opt/ontop/jdbc/

COPY mapping.r2rml.ttl          /opt/ontop/input/mapping.r2rml.ttl
COPY db-metadata.json           /opt/ontop/input/db-metadata.json
COPY ontop.properties.template  /opt/ontop/input/ontop.properties.template
COPY entrypoint.sh              /opt/ontop/entrypoint-poc.sh
COPY extract-db-metadata.sh     /opt/ontop/extract-db-metadata.sh

RUN chmod +x /opt/ontop/entrypoint-poc.sh /opt/ontop/extract-db-metadata.sh

EXPOSE 8080
ENTRYPOINT ["/opt/ontop/entrypoint-poc.sh"]
