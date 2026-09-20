#!/bin/sh
#
# Polaris bootstrap: catalog, namespaces, principals, roles and grants.
#
# Runs as the internal 'root' principal (Polaris is in 'mixed' auth mode), because
# alice and bob authenticate externally via Keycloak and cannot bootstrap themselves.
#
# Principals MUST be created here: Polaris's DefaultAuthenticator explicitly does not
# support federated principals that it does not manage. We create them by NAME only and
# configure Polaris with name-claim-path, so no numeric principal ID has to be round-
# tripped back into Keycloak claims.
#
# Idempotent: 409 Conflict is treated as success so `make bootstrap` can be re-run.
#
set -eu

POLARIS_URL="${POLARIS_URL:-http://polaris:8181}"
REALM="${POLARIS_REALM:-POLARIS}"
ROOT_ID="${POLARIS_CLIENT_ID:-root}"
ROOT_SECRET="${POLARIS_CLIENT_SECRET:-s3cr3t}"

CATALOG="poc_catalog"
BASE_LOCATION="s3://warehouse/poc"

# Keycloak names a client's service account user `service-account-<clientId>`,
# and that username is what the principal_name claim carries.
OM_PRINCIPAL="service-account-openmetadata"

apk add --no-cache jq >/dev/null 2>&1 || true

echo "==> obtaining root token from Polaris"
TOKEN="$(curl -sS --fail-with-body \
  --user "${ROOT_ID}:${ROOT_SECRET}" \
  -H "Polaris-Realm: ${REALM}" \
  -d grant_type=client_credentials \
  -d scope=PRINCIPAL_ROLE:ALL \
  "${POLARIS_URL}/api/catalog/v1/oauth/tokens" | jq -r .access_token)"

if [ -z "${TOKEN}" ] || [ "${TOKEN}" = "null" ]; then
  echo "FATAL: could not obtain a Polaris root token" >&2
  exit 1
fi

# call DESCRIPTION METHOD PATH [BODY]
call() {
  _desc="$1"; _method="$2"; _path="$3"; _body="${4:-}"
  if [ -n "${_body}" ]; then
    _code="$(curl -sS -o /tmp/polaris-resp -w '%{http_code}' -X "${_method}" \
      -H "Authorization: Bearer ${TOKEN}" \
      -H "Polaris-Realm: ${REALM}" \
      -H 'Content-Type: application/json' \
      -H 'Accept: application/json' \
      "${POLARIS_URL}${_path}" -d "${_body}")"
  else
    _code="$(curl -sS -o /tmp/polaris-resp -w '%{http_code}' -X "${_method}" \
      -H "Authorization: Bearer ${TOKEN}" \
      -H "Polaris-Realm: ${REALM}" \
      -H 'Accept: application/json' \
      "${POLARIS_URL}${_path}")"
  fi

  case "${_code}" in
    2*)  echo "    ok   (${_code}) ${_desc}" ;;
    409) echo "    skip (409 exists) ${_desc}" ;;
    *)   echo "    FAIL (${_code}) ${_desc}" >&2
         cat /tmp/polaris-resp >&2; echo >&2
         exit 1 ;;
  esac
}

echo "==> catalog"
call "create catalog ${CATALOG}" POST /api/management/v1/catalogs '{
  "catalog": {
    "name": "'"${CATALOG}"'",
    "type": "INTERNAL",
    "readOnly": false,
    "properties": { "default-base-location": "'"${BASE_LOCATION}"'" },
    "storageConfigInfo": {
      "storageType": "S3",
      "allowedLocations": [ "'"${BASE_LOCATION}"'" ],
      "endpoint": "http://minio:9000",
      "endpointInternal": "http://minio:9000",
      "pathStyleAccess": true,
      "region": "us-east-1"
    }
  }
}'

echo "==> principals (must pre-exist; resolved by name from the principal_name claim)"
call "principal alice" POST /api/management/v1/principals '{"principal":{"name":"alice"}}'
call "principal bob"   POST /api/management/v1/principals '{"principal":{"name":"bob"}}'
# OpenMetadata's crawler is a machine identity: it reaches Polaris with the OAuth2
# client credentials grant against Keycloak, so the name is Keycloak's service
# account username for the 'openmetadata' client, which is what ends up in the
# principal_name claim.
call "principal ${OM_PRINCIPAL}" POST /api/management/v1/principals '{"principal":{"name":"'"${OM_PRINCIPAL}"'"}}'

echo "==> principal roles (names must match the Keycloak realm roles exactly)"
call "principal-role data_engineer"  POST /api/management/v1/principal-roles '{"principalRole":{"name":"data_engineer"}}'
call "principal-role analyst"        POST /api/management/v1/principal-roles '{"principalRole":{"name":"analyst"}}'
call "principal-role metadata_reader" POST /api/management/v1/principal-roles '{"principalRole":{"name":"metadata_reader"}}'

call "alice -> data_engineer" PUT /api/management/v1/principals/alice/principal-roles '{"principalRole":{"name":"data_engineer"}}'
call "bob   -> analyst"       PUT /api/management/v1/principals/bob/principal-roles   '{"principalRole":{"name":"analyst"}}'
call "${OM_PRINCIPAL} -> metadata_reader" \
  PUT "/api/management/v1/principals/${OM_PRINCIPAL}/principal-roles" \
  '{"principalRole":{"name":"metadata_reader"}}'

echo "==> namespaces (created here so grants below have something to reference;"
echo "    alice still creates the TABLES herself through Spark Connect)"
call "namespace shared"     POST "/api/catalog/v1/${CATALOG}/namespaces" '{"namespace":["shared"]}'
call "namespace restricted" POST "/api/catalog/v1/${CATALOG}/namespaces" '{"namespace":["restricted"]}'

echo "==> catalog roles"
call "catalog-role engineer"      POST "/api/management/v1/catalogs/${CATALOG}/catalog-roles" '{"catalogRole":{"name":"engineer"}}'
call "catalog-role shared_reader" POST "/api/management/v1/catalogs/${CATALOG}/catalog-roles" '{"catalogRole":{"name":"shared_reader"}}'
call "catalog-role catalog_reader" POST "/api/management/v1/catalogs/${CATALOG}/catalog-roles" '{"catalogRole":{"name":"catalog_reader"}}'

echo "==> grants: alice (engineer) gets the whole catalog"
call "engineer: CATALOG_MANAGE_CONTENT" \
  PUT "/api/management/v1/catalogs/${CATALOG}/catalog-roles/engineer/grants" \
  '{"type":"catalog","privilege":"CATALOG_MANAGE_CONTENT"}'

echo "==> grants: bob (shared_reader) gets read on 'shared' ONLY -- nothing on 'restricted'"
for priv in NAMESPACE_LIST NAMESPACE_READ_PROPERTIES TABLE_LIST TABLE_READ_PROPERTIES TABLE_READ_DATA; do
  call "shared_reader: ${priv} on shared" \
    PUT "/api/management/v1/catalogs/${CATALOG}/catalog-roles/shared_reader/grants" \
    '{"type":"namespace","namespace":["shared"],"privilege":"'"${priv}"'"}'
done

echo "==> grants: the OpenMetadata crawler reads metadata in BOTH namespaces"
# On this branch the PDP decides, and its permission for this principal carries
# no WRITE scope. These grants exist so the bootstrap is still correct if
# polaris.authorization.type goes back to the built-in authorizer.
# TABLE_READ_DATA is in the set because PyIceberg asks for vended credentials on
# every load, and Polaris refuses the load outright if no delegation is allowed.
for ns in shared restricted; do
  for priv in NAMESPACE_LIST NAMESPACE_READ_PROPERTIES TABLE_LIST TABLE_READ_PROPERTIES TABLE_READ_DATA; do
    call "catalog_reader: ${priv} on ${ns}" \
      PUT "/api/management/v1/catalogs/${CATALOG}/catalog-roles/catalog_reader/grants" \
      '{"type":"namespace","namespace":["'"${ns}"'"],"privilege":"'"${priv}"'"}'
  done
done

echo "==> bind catalog roles to principal roles"
call "data_engineer -> engineer" \
  PUT "/api/management/v1/principal-roles/data_engineer/catalog-roles/${CATALOG}" \
  '{"catalogRole":{"name":"engineer"}}'
call "analyst -> shared_reader" \
  PUT "/api/management/v1/principal-roles/analyst/catalog-roles/${CATALOG}" \
  '{"catalogRole":{"name":"shared_reader"}}'
call "metadata_reader -> catalog_reader" \
  PUT "/api/management/v1/principal-roles/metadata_reader/catalog-roles/${CATALOG}" \
  '{"catalogRole":{"name":"catalog_reader"}}'

cat <<'SUMMARY'

==> bootstrap complete

    catalog     poc_catalog  ->  s3://warehouse/poc  (MinIO, path-style)
    namespaces  shared, restricted

    alice  realm role data_engineer -> catalog role engineer
           CATALOG_MANAGE_CONTENT on poc_catalog  (read + write, both namespaces)

    bob    realm role analyst -> catalog role shared_reader
           read-only on namespace 'shared'; NO grant on 'restricted'

    service-account-openmetadata
           realm role metadata_reader -> catalog role catalog_reader
           metadata read on BOTH namespaces, no write anywhere.
           Reaches Polaris with the OAuth2 client credentials grant (Keycloak
           client 'openmetadata'), not as a human user.

    Expected demo outcome:
      both read shared.events            -> OK
      alice reads restricted.salaries    -> OK
      bob   reads restricted.salaries    -> FORBIDDEN from Polaris
SUMMARY
