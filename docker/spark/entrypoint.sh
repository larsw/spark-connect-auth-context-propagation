#!/usr/bin/env bash
#
# Role-dispatching entrypoint. SPARK_ROLE selects master | worker | connect.
#
# Note we exec spark-submit in the FOREGROUND rather than using sbin/start-connect-server.sh,
# which daemonises via spark-daemon.sh and would make the container exit immediately.
#
set -euo pipefail

: "${SPARK_HOME:=/opt/spark}"
: "${SPARK_ROLE:?SPARK_ROLE must be set to master, worker or connect}"

wait_for_tcp() {
  local host="$1" port="$2" tries="${3:-60}"
  echo "waiting for ${host}:${port} ..."
  for _ in $(seq 1 "$tries"); do
    if (exec 3<>"/dev/tcp/${host}/${port}") 2>/dev/null; then
      exec 3>&- 2>/dev/null || true
      echo "${host}:${port} is up"
      return 0
    fi
    sleep 1
  done
  echo "timed out waiting for ${host}:${port}" >&2
  return 1
}

# Fail loudly rather than starting a server whose interceptor class does not exist.
# (COPY in the Dockerfile is deliberately tolerant so the image can build before the
# Java module does; this is the check that keeps that from being silent.)
if [ "$SPARK_ROLE" = "connect" ]; then
  if ! ls "$SPARK_HOME"/jars/spark-connect-propagation-*.jar >/dev/null 2>&1; then
    echo "FATAL: the propagation plugin jar is missing from $SPARK_HOME/jars." >&2
    echo "       Run 'make jar' and rebuild the image ('make build')." >&2
    exit 1
  fi
fi

case "$SPARK_ROLE" in
  master)
    exec "$SPARK_HOME/bin/spark-class" org.apache.spark.deploy.master.Master \
      --host spark-master --port 7077 --webui-port 8080
    ;;

  worker)
    wait_for_tcp spark-master 7077
    exec "$SPARK_HOME/bin/spark-class" org.apache.spark.deploy.worker.Worker \
      "spark://spark-master:7077" --webui-port 8081
    ;;

  connect)
    wait_for_tcp spark-master 7077
    # The Connect server class lives in a jar already on the classpath; spark-submit still wants
    # a primary resource, so pass that same jar explicitly.
    CONNECT_JAR="$(ls "$SPARK_HOME"/jars/spark-connect_2.13-*.jar | head -1)"
    echo "using connect jar: $CONNECT_JAR"
    exec "$SPARK_HOME/bin/spark-submit" \
      --class org.apache.spark.sql.connect.service.SparkConnectServer \
      --name "Spark Connect server (propagation PoC)" \
      --conf spark.driver.host=spark-connect \
      --conf spark.driver.bindAddress=0.0.0.0 \
      "$CONNECT_JAR"
    ;;

  *)
    echo "unknown SPARK_ROLE: $SPARK_ROLE (expected master, worker or connect)" >&2
    exit 2
    ;;
esac
