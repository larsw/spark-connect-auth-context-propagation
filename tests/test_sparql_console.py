"""The SPARQL console's wiring.

Not a browser test — driving a real sign-in is what `make demo-sparql` and a human are for. These
check the things that break silently and are tedious to diagnose from a browser: a realm that does
not know the client (Keycloak answers "Client not found" with no hint as to which of five clients),
a redirect URI that is not registered, and a missing audience mapper, which produces a token the
console accepts happily and Spark Connect refuses much later.
"""
from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request

import pytest

CONSOLE = os.environ.get("SPARQL_CONSOLE_URL", "http://localhost:3002").rstrip("/")
ISSUER = os.environ.get("KEYCLOAK_ISSUER", "http://keycloak:8080/realms/spark")
CLIENT_ID = "sparql-console"

#: Where the browser is sent back to. localhost, deliberately: PKCE S256 needs crypto.subtle,
#: which browsers withhold outside a secure context. See sparql-console/entrypoint.sh.
REDIRECT_URI = "http://localhost:3002/auth/callback"


def get(url: str, timeout: int = 15) -> tuple[int, str]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return response.status, response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode("utf-8", errors="replace")


@pytest.fixture(scope="module", autouse=True)
def console_is_up():
    try:
        status, _ = get(f"{CONSOLE}/health")
    except Exception:
        pytest.skip(f"the SPARQL console at {CONSOLE} is not reachable")
    if status != 200:
        pytest.skip(f"the SPARQL console at {CONSOLE} answered {status}")


def test_the_runtime_config_is_rendered():
    """C1: the container writes /config.js at start, and index.html reads it before the bundle."""
    status, body = get(f"{CONSOLE}/config.js")
    assert status == 200
    assert "window.APP_CONFIG" in body
    assert "VITE_SPARQL_ENDPOINT" in body
    assert "VITE_OIDC_CLIENT_ID" in body


def test_the_config_holds_no_token():
    """C2: whatever is in there, it is not a credential for the data.

    The console signs the user in and forwards *their* token. A bearer token in the file the
    browser fetches on every load would mean everyone queries as whoever generated it.
    """
    _, body = get(f"{CONSOLE}/config.js")
    assert "Bearer " not in body
    assert "eyJ" not in body, "a JWT is being served to every visitor"


def test_client_side_routes_are_served():
    """C3: /auth/callback is a route, not a file. Without the SPA fallback the redirect 404s."""
    for path in ("/", "/about", "/auth/callback"):
        status, body = get(f"{CONSOLE}{path}")
        assert status == 200, path
        assert "<div id=\"root\">" in body, path


def test_the_realm_knows_this_client():
    """C4: Keycloak must have the client, with PKCE and this exact redirect URI.

    The realm is imported once at Keycloak start, so adding a client to spark-realm.json without
    recreating the container leaves the console facing a bare "Client not found".
    """
    status, body = get(
        f"{ISSUER}/protocol/openid-connect/auth"
        f"?client_id={CLIENT_ID}"
        f"&redirect_uri={urllib.parse.quote(REDIRECT_URI, safe='')}"
        "&response_type=code&scope=openid"
        "&code_challenge=E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
        "&code_challenge_method=S256"
    )
    assert status == 200, f"Keycloak refused the authorization request: {body[:400]}"
    assert "Sign in" in body or "kc-form" in body, body[:400]


def test_the_client_issues_tokens_spark_connect_will_accept():
    """C5: the console's tokens must carry aud=spark-connect.

    RFC 8693 filters audiences, it never adds one, so a token minted without the audience mapper
    is exchanged into something Polaris rejects — several hops from the realm file that caused it.

    Read from the *running* realm through the admin API rather than from spark-realm.json, because
    the file is only imported when Keycloak starts: the two disagree exactly when it matters.
    (admin/admin is this sandbox's throwaway bootstrap credential, in compose.yaml.)
    """
    base = ISSUER.rsplit("/realms/", 1)[0]
    realm = ISSUER.rsplit("/realms/", 1)[1]

    form = urllib.parse.urlencode({
        "grant_type": "password",
        "client_id": "admin-cli",
        "username": os.environ.get("KEYCLOAK_ADMIN", "admin"),
        "password": os.environ.get("KEYCLOAK_ADMIN_PASSWORD", "admin"),
    }).encode()
    try:
        with urllib.request.urlopen(
            urllib.request.Request(
                f"{base}/realms/master/protocol/openid-connect/token", data=form
            ),
            timeout=15,
        ) as response:
            admin_token = json.loads(response.read())["access_token"]
    except Exception as error:  # noqa: BLE001 - a skip is better than a confusing failure
        pytest.skip(f"no Keycloak admin access: {error}")

    def admin_get(path: str):
        request = urllib.request.Request(f"{base}/admin/realms/{realm}{path}")
        request.add_header("Authorization", f"Bearer {admin_token}")
        with urllib.request.urlopen(request, timeout=15) as response:
            return json.loads(response.read())

    clients = admin_get(f"/clients?clientId={CLIENT_ID}")
    assert clients, f"the running realm has no client {CLIENT_ID}"
    client = clients[0]

    assert client["publicClient"] is True, "the console must hold no secret"
    assert client["standardFlowEnabled"] is True
    assert client["attributes"]["pkce.code.challenge.method"] == "S256"
    assert REDIRECT_URI.replace("/auth/callback", "/*") in client["redirectUris"]

    mappers = admin_get(f"/clients/{client['id']}/protocol-mappers/models")
    audiences = [
        mapper["config"]["included.client.audience"]
        for mapper in mappers
        if mapper["protocolMapper"] == "oidc-audience-mapper"
    ]
    assert "spark-connect" in audiences, (
        "without this mapper the console's token is not addressed to Spark Connect, and every "
        "query is refused at the edge"
    )
