#!/usr/bin/env bash
#
# Regenerates docker/ontop/db-metadata.json: the table definitions Ontop needs to compile SPARQL
# into SQL.
#
# Pinning them in a file is what lets the endpoint hold no credential at all. The alternative is
# to let Ontop introspect at start-up, which means giving it a standing identity of its own -- and
# in this stack that identity would need real read rights, because Spark asks Polaris to vend
# storage credentials on every loadTable and Polaris authorises that as reading the data. There is
# no "schema only" to grant. See docker/ontop/README.md.
#
# So introspection happens here instead: deliberately, as a user who already has those rights, and
# the result is reviewed and committed. Run it after changing the mapped tables.
#
#   make ontop-metadata
#
set -euo pipefail

: "${KEYCLOAK_ISSUER:=http://keycloak:8080/realms/spark}"
: "${SPARK_REMOTE_HOST:=spark-connect}"
: "${SPARK_REMOTE_PORT:=15002}"
: "${CONNECT_SHARED_SECRET:=poc-shared-secret}"
: "${METADATA_USER:=alice}"
: "${METADATA_PASSWORD:=alice}"

token="$(curl -sS --fail-with-body \
  -d grant_type=password -d client_id=spark-cli \
  -d "username=${METADATA_USER}" -d "password=${METADATA_PASSWORD}" -d scope=openid \
  "${KEYCLOAK_ISSUER}/protocol/openid-connect/token" | jq -r .access_token)"

payload="$(printf '%s' "$token" | cut -d. -f2 | tr '_-' '/+')"
case $(( ${#payload} % 4 )) in
  2) payload="${payload}==" ;;
  3) payload="${payload}=" ;;
esac
subject="$(printf '%s' "$payload" | base64 -d | jq -r .sub)"

cat > /tmp/extract.properties <<EOF
jdbc.driver = org.apache.spark.sql.connect.client.jdbc.SparkConnectDriver
jdbc.url = jdbc:sc://${SPARK_REMOTE_HOST}:${SPARK_REMOTE_PORT}/;\
authorization=Bearer%20${CONNECT_SHARED_SECRET};\
user_id=${subject};x-user-token=${token};x-correlation-id=ontop-extract-metadata
EOF

# Logback writes to stdout, so the JSON goes to a file rather than down the pipe.
/opt/ontop/ontop extract-db-metadata -p /tmp/extract.properties -o /tmp/db-metadata.json >&2
cat /tmp/db-metadata.json
