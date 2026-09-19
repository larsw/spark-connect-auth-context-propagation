"""Token acquisition for the PySpark client.

Deliberately pluggable, because how a user proves who they are is orthogonal to the thing this
PoC is about -- threading that identity through Spark Connect. Two implementations:

* :class:`DeviceCodeTokenProvider` -- the real CLI experience (RFC 8628). Prints a URL, waits
  for the browser login, then caches and silently refreshes.
* :class:`PasswordGrantTokenProvider` -- used only by the test suite, so ``make test`` stays
  headless.

Both hand back a *callable* rather than a string, so an expiring token refreshes mid-session
instead of killing it.

Stdlib HTTP only: the client package's dependency list stays at pyspark-client alone.
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Protocol

# Refresh this many seconds before the server thinks the token expires.
_EXPIRY_MARGIN_SECONDS = 30


class TokenProvider(Protocol):
    """Anything that can supply a currently-valid access token."""

    def token(self) -> str:
        ...


def _post_form(url: str, form: dict[str, str], *, basic_auth: Optional[tuple[str, str]] = None) -> dict:
    data = urllib.parse.urlencode(form).encode()
    request = urllib.request.Request(url, data=data, method="POST")
    request.add_header("Content-Type", "application/x-www-form-urlencoded")
    request.add_header("Accept", "application/json")
    if basic_auth is not None:
        import base64

        raw = f"{basic_auth[0]}:{basic_auth[1]}".encode()
        request.add_header("Authorization", "Basic " + base64.b64encode(raw).decode())
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode())
    except urllib.error.HTTPError as error:
        body = error.read().decode(errors="replace")
        try:
            parsed = json.loads(body)
        except ValueError:
            raise RuntimeError(f"{url} returned HTTP {error.code}: {body[:300]}") from error
        raise OAuthError(parsed.get("error", "unknown"), parsed.get("error_description", ""), error.code) from error


class OAuthError(RuntimeError):
    def __init__(self, error: str, description: str, status: int = 0):
        super().__init__(f"{error}: {description}" if description else error)
        self.error = error
        self.description = description
        self.status = status


@dataclass
class Endpoints:
    """The OIDC endpoints of one realm."""

    issuer: str

    @property
    def token(self) -> str:
        return f"{self.issuer}/protocol/openid-connect/token"

    @property
    def device_authorization(self) -> str:
        return f"{self.issuer}/protocol/openid-connect/auth/device"


@dataclass
class _Tokens:
    access_token: str
    refresh_token: Optional[str]
    expires_at: float

    @classmethod
    def from_response(cls, payload: dict) -> "_Tokens":
        return cls(
            access_token=payload["access_token"],
            refresh_token=payload.get("refresh_token"),
            expires_at=time.time() + float(payload.get("expires_in", 60)),
        )

    def fresh(self) -> bool:
        return time.time() < self.expires_at - _EXPIRY_MARGIN_SECONDS


@dataclass
class PasswordGrantTokenProvider:
    """Direct access grant. Used by the automated tests, never by the interactive demo."""

    endpoints: Endpoints
    client_id: str
    username: str
    password: str
    _tokens: Optional[_Tokens] = field(default=None, repr=False)

    def token(self) -> str:
        if self._tokens is not None and self._tokens.fresh():
            return self._tokens.access_token
        if self._tokens is not None and self._tokens.refresh_token:
            try:
                self._tokens = _Tokens.from_response(
                    _post_form(
                        self.endpoints.token,
                        {
                            "grant_type": "refresh_token",
                            "client_id": self.client_id,
                            "refresh_token": self._tokens.refresh_token,
                        },
                    )
                )
                return self._tokens.access_token
            except (OAuthError, OSError):
                # Refresh token spent, or the network blinked. Either way a full grant is the
                # honest next move; if the IdP is really down, that fails loudly on its own.
                pass
        self._tokens = _Tokens.from_response(
            _post_form(
                self.endpoints.token,
                {
                    "grant_type": "password",
                    "client_id": self.client_id,
                    "username": self.username,
                    "password": self.password,
                },
            )
        )
        return self._tokens.access_token


@dataclass
class DeviceCodeTokenProvider:
    """OAuth 2.0 Device Authorization Grant (RFC 8628) -- the CLI login flow.

    Prints a URL, polls until the user finishes in a browser, then caches the refresh token so
    subsequent runs are silent. This is how ``gh auth login`` and ``az login --use-device-code``
    behave, and it is the right shape for a PySpark client with no redirect URI.
    """

    endpoints: Endpoints
    client_id: str
    label: str = "default"
    cache_dir: Path = field(default_factory=lambda: Path(
        os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache")) / "spark-connect-poc")
    _tokens: Optional[_Tokens] = field(default=None, repr=False)

    @property
    def _cache_file(self) -> Path:
        safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in f"{self.client_id}-{self.label}")
        return self.cache_dir / f"{safe}.json"

    def token(self) -> str:
        if self._tokens is None:
            self._tokens = self._load_cache()
        if self._tokens is not None and self._tokens.fresh():
            return self._tokens.access_token
        if self._tokens is not None and self._tokens.refresh_token:
            refreshed = self._try_refresh(self._tokens.refresh_token)
            if refreshed is not None:
                self._tokens = refreshed
                self._save_cache()
                return self._tokens.access_token
        self._tokens = self._device_flow()
        self._save_cache()
        return self._tokens.access_token

    def _try_refresh(self, refresh_token: str) -> Optional[_Tokens]:
        try:
            return _Tokens.from_response(
                _post_form(
                    self.endpoints.token,
                    {
                        "grant_type": "refresh_token",
                        "client_id": self.client_id,
                        "refresh_token": refresh_token,
                    },
                )
            )
        except (OAuthError, OSError):
            # Either the refresh token is spent -- Keycloak runs in dev mode here, so a restart
            # wipes every SSO session while the cache on disk still looks perfectly usable -- or
            # the network blinked. Both mean "go back to a device login", not "print a stack
            # trace". URLError, socket timeouts and ConnectionResetError are all OSError.
            return None

    def _device_flow(self) -> _Tokens:
        start = _post_form(
            self.endpoints.device_authorization,
            {"client_id": self.client_id, "scope": "openid profile"},
        )
        verification = start.get("verification_uri_complete") or start["verification_uri"]

        print()
        print("  To sign in, open this URL in a browser:")
        print()
        print(f"      {verification}")
        if not start.get("verification_uri_complete"):
            print()
            print(f"  and enter the code:  {start['user_code']}")
        print()
        print("  Waiting for you to finish ...", flush=True)

        interval = float(start.get("interval", 5))
        deadline = time.time() + float(start.get("expires_in", 600))

        while time.time() < deadline:
            time.sleep(interval)
            try:
                payload = _post_form(
                    self.endpoints.token,
                    {
                        "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                        "client_id": self.client_id,
                        "device_code": start["device_code"],
                    },
                )
            except OAuthError as error:
                if error.error == "authorization_pending":
                    continue
                if error.error == "slow_down":
                    interval += 5
                    continue
                raise
            print("  Signed in.\n")
            return _Tokens.from_response(payload)

        raise TimeoutError("device authorization expired before sign-in completed")

    def _load_cache(self) -> Optional[_Tokens]:
        try:
            raw = json.loads(self._cache_file.read_text())
            return _Tokens(raw["access_token"], raw.get("refresh_token"), float(raw["expires_at"]))
        except (OSError, ValueError, KeyError):
            return None

    def _save_cache(self) -> None:
        if self._tokens is None:
            return
        try:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
            self._cache_file.write_text(
                json.dumps(
                    {
                        "access_token": self._tokens.access_token,
                        "refresh_token": self._tokens.refresh_token,
                        "expires_at": self._tokens.expires_at,
                    }
                )
            )
            self._cache_file.chmod(0o600)
        except OSError:
            pass  # a cold login next time is a nuisance, not an error


@dataclass
class StaticTokenProvider:
    """A fixed token. Useful for negative tests: expired, wrong audience, malformed."""

    value: str

    def token(self) -> str:
        return self.value
