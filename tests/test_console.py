"""The Polaris console's browser login, driven headlessly.

The console is a public SPA using authorization code + PKCE. None of that is exercised by the
Spark path, and the realm configuration it depends on -- redirect URIs, PKCE, web origins and the
principal claim mappers -- is exactly the sort of thing that breaks silently. So drive the real
flow and check the token Polaris ends up seeing.
"""
from __future__ import annotations

import base64
import hashlib
import html
import http.cookiejar
import json
import os
import re
import secrets
import urllib.error
import urllib.parse
import urllib.request

import pytest

ISSUER = os.environ.get("KEYCLOAK_ISSUER", "http://keycloak:8080/realms/spark")
POLARIS = os.environ.get("POLARIS_URL", "http://polaris:8181")
REDIRECT_URI = "http://polaris-console:3000/auth/callback"
CLIENT_ID = "polaris-console"


def _b64(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def claims_of(jwt: str) -> dict:
    payload = jwt.split(".")[1]
    payload += "=" * (-len(payload) % 4)
    return json.loads(base64.urlsafe_b64decode(payload))


def console_login(user: str) -> str:
    """Complete the console's auth-code + PKCE flow and return the access token.

    Note everything stays on the same Keycloak hostname. Starting at one host and submitting the
    login form to another drops the session cookie and Keycloak answers 400 -- the same issuer
    consistency requirement that shapes the rest of this stack.
    """
    verifier = _b64(secrets.token_bytes(40))
    challenge = _b64(hashlib.sha256(verifier.encode()).digest())

    jar = http.cookiejar.CookieJar()

    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, *args, **kwargs):
            return None

    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar), NoRedirect)

    authorize = f"{ISSUER}/protocol/openid-connect/auth?" + urllib.parse.urlencode({
        "client_id": CLIENT_ID,
        "response_type": "code",
        "redirect_uri": REDIRECT_URI,
        "scope": "openid profile email",
        "state": secrets.token_hex(8),
        "code_challenge": challenge,
        "code_challenge_method": "S256",
    })
    page = opener.open(authorize, timeout=30).read().decode()
    action = html.unescape(re.search(r'<form[^>]*action="([^"]+)"', page).group(1))

    form = urllib.parse.urlencode(
        {"username": user, "password": user, "credentialId": ""}
    ).encode()
    try:
        opener.open(urllib.request.Request(action, data=form), timeout=30)
        raise AssertionError("expected a redirect carrying the authorization code")
    except urllib.error.HTTPError as error:
        location = error.headers.get("Location") or ""

    code = urllib.parse.parse_qs(urllib.parse.urlparse(location).query).get("code", [None])[0]
    assert code, f"no authorization code for {user}; Keycloak sent {location[:120]!r}"

    exchange = urllib.parse.urlencode({
        "grant_type": "authorization_code",
        "client_id": CLIENT_ID,
        "code": code,
        "redirect_uri": REDIRECT_URI,
        "code_verifier": verifier,
    }).encode()
    with urllib.request.urlopen(
        urllib.request.Request(f"{ISSUER}/protocol/openid-connect/token", data=exchange),
        timeout=30,
    ) as response:
        return json.load(response)["access_token"]


def polaris_status(token: str, path: str) -> int:
    request = urllib.request.Request(f"{POLARIS}{path}")
    request.add_header("Authorization", f"Bearer {token}")
    request.add_header("Accept", "application/json")
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status
    except urllib.error.HTTPError as error:
        return error.code


@pytest.fixture(scope="module")
def alice_console_token():
    return console_login("alice")


@pytest.fixture(scope="module")
def bob_console_token():
    return console_login("bob")


def test_console_login_yields_a_polaris_addressed_token(alice_console_token):
    """PKCE flow succeeds and the audience mapper puts Polaris on the token."""
    claims = claims_of(alice_console_token)
    audience = claims["aud"]
    audience = audience if isinstance(audience, list) else [audience]

    assert "polaris" in audience, f"console token is not addressed to Polaris: {audience}"
    assert claims["principal_name"] == "alice"
    assert claims["principal_roles"] == ["data_engineer"]


def test_console_token_maps_to_the_right_principal(bob_console_token):
    claims = claims_of(bob_console_token)
    assert claims["principal_name"] == "bob"
    assert claims["principal_roles"] == ["analyst"]


def test_console_shows_each_user_their_own_catalog(alice_console_token, bob_console_token):
    """The console is not an admin view: it renders whatever that user is allowed to see."""
    shared = "/api/catalog/v1/poc_catalog/namespaces/shared"
    restricted = "/api/catalog/v1/poc_catalog/namespaces/restricted"

    assert polaris_status(alice_console_token, shared) == 200
    assert polaris_status(alice_console_token, restricted) == 200

    assert polaris_status(bob_console_token, shared) == 200
    assert polaris_status(bob_console_token, restricted) == 403, (
        "bob must not see the restricted namespace in the console"
    )


def test_polaris_allows_the_console_origin():
    """Without CORS the console cannot call Polaris at all, and the failure is opaque."""
    request = urllib.request.Request(
        f"{POLARIS}/api/management/v1/catalogs", method="OPTIONS"
    )
    request.add_header("Origin", "http://polaris-console:3000")
    request.add_header("Access-Control-Request-Method", "GET")
    request.add_header("Access-Control-Request-Headers", "authorization")
    with urllib.request.urlopen(request, timeout=30) as response:
        allowed = response.headers.get("access-control-allow-origin")

    assert allowed == "http://polaris-console:3000", f"CORS not open to the console: {allowed!r}"
