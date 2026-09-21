#!/bin/sh
#
# Writes the runtime configuration the console reads from window.APP_CONFIG, then lets nginx's own
# entrypoint carry on. Dropped into /docker-entrypoint.d, which the nginx image runs before
# starting the server.
#
# Every URL here is resolved by the BROWSER, not by this container: the console is static files
# and every call it makes comes from the user's machine. Hence localhost and published ports
# rather than compose service names.
set -eu

CONFIG_FILE=/usr/share/nginx/html/config.js

cat > "$CONFIG_FILE" <<CONFIG
// Generated at container start from the environment. Do not edit.
window.APP_CONFIG = {
  VITE_SPARQL_ENDPOINT: '${VITE_SPARQL_ENDPOINT:-http://localhost:8090/sparql}',
  VITE_OIDC_ISSUER_URL: '${VITE_OIDC_ISSUER_URL:-http://keycloak:8080/realms/spark}',
  VITE_OIDC_CLIENT_ID: '${VITE_OIDC_CLIENT_ID:-sparql-console}',
  VITE_OIDC_REDIRECT_URI: '${VITE_OIDC_REDIRECT_URI:-http://localhost:3002/auth/callback}',
  VITE_OIDC_SCOPE: '${VITE_OIDC_SCOPE:-openid profile email}',
  VITE_CORRELATION_HEADER: '${VITE_CORRELATION_HEADER:-x-correlation-id}'
};
CONFIG

# index.html loads this as a classic script and the bundle as a module, and modules are deferred,
# so everything below runs before the console starts. Quoted heredoc: none of it is shell.
cat >> "$CONFIG_FILE" <<'GUARD'

// Signing in uses PKCE S256, which hashes the verifier with crypto.subtle.digest. Browsers expose
// window.crypto.subtle ONLY in a secure context: HTTPS, or plain HTTP on localhost / 127.0.0.1 /
// ::1. That test is on the literal hostname, so reaching this page through a service-name alias
// that happens to resolve to loopback does not qualify -- crypto.subtle is undefined and Sign in
// dies with "Cannot read properties of undefined (reading 'digest')", which names neither the
// problem nor the fix. Say so here instead, while the page still works.
(function () {
  if (window.isSecureContext && window.crypto && window.crypto.subtle) {
    return;
  }
  var port = window.location.port ? ':' + window.location.port : '';
  var target = 'http://localhost' + port + window.location.pathname
    + window.location.search + window.location.hash;

  function explain() {
    document.title = 'Open this console on localhost';
    document.body.innerHTML = ''
      + '<div style="font:16px/1.6 system-ui,sans-serif;max-width:46rem;margin:12vh auto;'
      + 'padding:0 1.5rem;color:#1f2328">'
      + '<h1 style="font-size:1.5rem;margin:0 0 1rem">Open this console on '
      + '<code>localhost</code></h1>'
      + '<p style="margin:0 0 1rem">Signing in uses PKCE, which needs '
      + '<code>crypto.subtle</code>. Browsers only provide it in a <em>secure context</em>: '
      + 'HTTPS, or plain HTTP on <code>localhost</code>. The check looks at the hostname you '
      + 'typed, so <code>' + window.location.hostname + '</code> does not count even though it '
      + 'resolves to your own machine.</p>'
      + '<p style="margin:0"><a href="' + target + '" style="display:inline-block;'
      + 'background:#0969da;color:#fff;padding:.6rem 1.1rem;border-radius:6px;'
      + 'text-decoration:none;font-weight:600">Continue to ' + target + '</a></p>'
      + '</div>';
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', explain);
  } else {
    explain();
  }
})();
GUARD

echo "sparql-console runtime config:"
sed -n '1,/^};$/p' "$CONFIG_FILE" | sed 's/^/    /'
