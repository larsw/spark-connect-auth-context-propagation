#!/usr/bin/env bash
#
# Starts the Ontop SPARQL endpoint against Spark Connect.
#
# The endpoint holds no credential for the data. None. The only token in play is the caller's own,
# which arrives on the Authorization header of each SPARQL request and is put on the JDBC
# connection opened for that request -- see ontop.properties.template.
#
# That is possible because the table definitions are pinned in db-metadata.json rather than read
# from the database at start-up. Ontop needs a relation's columns before it can compile SPARQL
# into SQL, and it does that before any caller has shown up; given a metadata file it opens no
# connection at all. docker/ontop/extract-db-metadata.sh regenerates the file.
#
# jdbc.url is still configured, and deliberately carries no user token: if anything ever takes a
# connection outside a request, Spark Connect refuses it rather than serving it as somebody.
#
set -euo pipefail

: "${SPARK_REMOTE_HOST:=spark-connect}"
: "${SPARK_REMOTE_PORT:=15002}"
: "${CONNECT_SHARED_SECRET:=poc-shared-secret}"
: "${ONTOP_PORT:=8080}"

INPUT=/opt/ontop/input

wait_for() {
  local host="$1" port="$2" tries="${3:-90}"
  echo "waiting for ${host}:${port} ..."
  for _ in $(seq 1 "$tries"); do
    if (exec 3<>"/dev/tcp/${host}/${port}") 2>/dev/null; then
      exec 3>&- 2>/dev/null || true
      echo "${host}:${port} is up"
      return 0
    fi
    sleep 2
  done
  echo "timed out waiting for ${host}:${port}" >&2
  return 1
}

# Not needed to start -- nothing is queried here -- but it keeps the container from reporting
# healthy while the thing it fronts is still coming up.
wait_for "$SPARK_REMOTE_HOST" "$SPARK_REMOTE_PORT"

echo "==> rendering ${INPUT}/ontop.properties"
sed "s#@CONNECT_SHARED_SECRET@#${CONNECT_SHARED_SECRET}#g; \
     s#@SPARK_REMOTE_HOST@#${SPARK_REMOTE_HOST}#g; \
     s#@SPARK_REMOTE_PORT@#${SPARK_REMOTE_PORT}#g" \
  "${INPUT}/ontop.properties.template" > "${INPUT}/ontop.properties"

echo "==> starting the SPARQL endpoint on :${ONTOP_PORT} (Ontop $(cat /opt/ontop/ONTOP_REVISION))"
exec /opt/ontop/ontop endpoint \
  --mapping "${INPUT}/mapping.r2rml.ttl" \
  --properties "${INPUT}/ontop.properties" \
  --db-metadata "${INPUT}/db-metadata.json" \
  --port "${ONTOP_PORT}" \
  --cors-allowed-origins '*' \
  "$@"
