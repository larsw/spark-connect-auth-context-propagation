# syntax=docker/dockerfile:1
#
# One image for all three Spark roles (master, worker, connect driver). The worker needs the
# Iceberg classes to deserialise tasks, and the driver needs our plugin, so they share an image.
#
FROM apache/spark:4.1.3-scala2.13-java21-python3-ubuntu

USER root

ARG ICEBERG_VERSION=1.11.0
ARG OPENLINEAGE_VERSION=1.53.0
ARG MAVEN_CENTRAL=https://repo1.maven.org/maven2

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl ca-certificates; \
    rm -rf /var/lib/apt/lists/*

# Iceberg goes into $SPARK_HOME/jars, NOT --jars/--packages. This is load-bearing:
#   * --packages resolves over the network on every container start, and
#   * jars added that way land in a child classloader, which would split the static
#     PropagatedIdentityHolder into two instances -- one seen by the gRPC interceptor and a
#     different one seen by the Iceberg AuthManager. The token would silently "vanish".
RUN set -eux; \
    cd "$SPARK_HOME/jars"; \
    curl -fsSLO "${MAVEN_CENTRAL}/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/${ICEBERG_VERSION}/iceberg-spark-runtime-4.1_2.13-${ICEBERG_VERSION}.jar"; \
    curl -fsSLO "${MAVEN_CENTRAL}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"

# OpenLineage's Spark listener, which reports lineage to Marquez. Same reasoning as above for
# putting it in $SPARK_HOME/jars: spark.extraListeners is instantiated from the system
# classloader, so a jar supplied with --jars would not be visible to it.
RUN set -eux; \
    cd "$SPARK_HOME/jars"; \
    curl -fsSLO "${MAVEN_CENTRAL}/io/openlineage/openlineage-spark_2.13/${OPENLINEAGE_VERSION}/openlineage-spark_2.13-${OPENLINEAGE_VERSION}.jar"

# Our plugin jar (built by `make jar` before `docker compose build`).
COPY jars/ $SPARK_HOME/jars/

COPY spark-defaults.conf  $SPARK_HOME/conf/spark-defaults.conf
COPY log4j2.properties    $SPARK_HOME/conf/log4j2.properties
COPY entrypoint.sh        /opt/entrypoint-poc.sh

RUN chmod +x /opt/entrypoint-poc.sh

USER spark
ENTRYPOINT ["/opt/entrypoint-poc.sh"]
