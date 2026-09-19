#!/bin/sh
#
# Writes the runtime configuration the console reads from window.APP_CONFIG, then starts nginx.
# Mirrors upstream's docker/generate-config.sh; the console prefers these values over anything
# baked in at build time.
set -eu

CONFIG_FILE=/usr/share/nginx/html/config.js

cat > "$CONFIG_FILE" <<CONFIG
// Generated at container start from the environment. Do not edit.
window.APP_CONFIG = {
  VITE_POLARIS_API_URL: '${VITE_POLARIS_API_URL:-http://polaris:8181}',
  VITE_POLARIS_REALM: '${VITE_POLARIS_REALM:-POLARIS}',
  VITE_POLARIS_PRINCIPAL_SCOPE: '${VITE_POLARIS_PRINCIPAL_SCOPE:-PRINCIPAL_ROLE:ALL}',
  VITE_OAUTH_TOKEN_URL: '${VITE_OAUTH_TOKEN_URL:-}',
  VITE_POLARIS_REALM_HEADER_NAME: '${VITE_POLARIS_REALM_HEADER_NAME:-Polaris-Realm}',
  VITE_OIDC_ISSUER_URL: '${VITE_OIDC_ISSUER_URL:-}',
  VITE_OIDC_CLIENT_ID: '${VITE_OIDC_CLIENT_ID:-}',
  VITE_OIDC_REDIRECT_URI: '${VITE_OIDC_REDIRECT_URI:-}',
  VITE_OIDC_SCOPE: '${VITE_OIDC_SCOPE:-openid profile email}'
};
CONFIG

echo "polaris-console runtime config:"
sed 's/^/    /' "$CONFIG_FILE"

exec nginx -g 'daemon off;'
