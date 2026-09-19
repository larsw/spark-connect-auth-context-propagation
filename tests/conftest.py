"""Fixtures for the propagation suite.

Runs either on the host (after ./install.sh has added the /etc/hosts aliases) or inside the
`client` compose service, which needs no host configuration at all. The endpoints come from the
environment so the same tests serve both.
"""
from __future__ import annotations

import os
import shutil
import subprocess

import pytest

from spark_connect_propagation import Endpoints, PasswordGrantTokenProvider, connect

REMOTE = os.environ.get("SPARK_REMOTE", "sc://spark-connect:15002")
ISSUER = os.environ.get("KEYCLOAK_ISSUER", "http://keycloak:8080/realms/spark")
CLIENT_ID = "spark-cli"


def token_provider(user: str) -> PasswordGrantTokenProvider:
    """Password grant, so the suite stays headless. The demo uses device flow instead."""
    return PasswordGrantTokenProvider(
        endpoints=Endpoints(ISSUER), client_id=CLIENT_ID, username=user, password=user
    )


def require_shared_secret() -> str:
    """Spark's channel-level pre-shared key, which is separate from the user's token.

    compose sets this for containers, so only host runs can hit a missing value -- where the
    server's reply ("No authentication token provided") points at the wrong credential entirely.
    """
    secret = os.environ.get("CONNECT_SHARED_SECRET")
    if not secret:
        # pytest.exit rather than a failure: this is a misconfigured run, not a broken system,
        # and one clear line beats the same traceback repeated for every test.
        pytest.exit(
            "CONNECT_SHARED_SECRET is not set, so the client cannot satisfy Spark Connect's "
            "pre-shared-key check and every RPC would fail as UNAUTHENTICATED. Run these "
            "through `make test`, which exports it, or set it to match compose.yaml.",
            returncode=2,
        )
    return secret


@pytest.fixture(scope="session")
def alice():
    require_shared_secret()
    session = connect(REMOTE, token_provider("alice"))
    yield session
    session.stop()


@pytest.fixture(scope="session")
def bob():
    require_shared_secret()
    session = connect(REMOTE, token_provider("bob"))
    yield session
    session.stop()


def compose_logs(service: str) -> str:
    """Logs of one compose service.

    Tries the docker CLI first (host runs), then falls back to the Docker Engine API over the
    mounted unix socket, so the assertion also runs inside the `client` container where no
    docker CLI exists. The response is a multiplexed stream, but a substring search over the raw
    bytes is all this needs.
    """
    if shutil.which("docker") is not None:
        try:
            result = subprocess.run(
                ["docker", "compose", "logs", "--no-log-prefix", service],
                capture_output=True, text=True, timeout=60,
                cwd=os.environ.get("COMPOSE_PROJECT_DIR", os.getcwd()),
            )
            if result.returncode == 0 and result.stdout:
                return result.stdout
        except Exception:
            pass

    socket_path = "/var/run/docker.sock"
    if not os.path.exists(socket_path):
        return ""
    try:
        import http.client
        import socket as _socket

        class _UnixConnection(http.client.HTTPConnection):
            def connect(self):
                self.sock = _socket.socket(_socket.AF_UNIX, _socket.SOCK_STREAM)
                self.sock.settimeout(30)
                self.sock.connect(socket_path)

        project = os.environ.get("COMPOSE_PROJECT_NAME", "spark-connect-propagation")
        connection = _UnixConnection("localhost")
        connection.request(
            "GET",
            f"/containers/{project}-{service}-1/logs?stdout=1&stderr=1&tail=4000",
        )
        response = connection.getresponse()
        if response.status != 200:
            return ""
        return response.read().decode("utf-8", errors="replace")
    except Exception:
        return ""


def docker_logs_available() -> bool:
    return shutil.which("docker") is not None or os.path.exists("/var/run/docker.sock")


requires_docker = pytest.mark.skipif(
    not docker_logs_available(),
    reason="log-tracing needs the docker CLI or a mounted /var/run/docker.sock",
)

def exchanged_token(user: str) -> str:
    """The downstream (audience=polaris) token the Connect server would obtain for this user.

    Performed here exactly as the server does it, so tests can inspect what Polaris actually
    receives and vends.
    """
    import json, urllib.parse, urllib.request, base64

    subject = token_provider(user).token()
    form = urllib.parse.urlencode({
        "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
        "subject_token": subject,
        "subject_token_type": "urn:ietf:params:oauth:token-type:access_token",
        "requested_token_type": "urn:ietf:params:oauth:token-type:access_token",
        "audience": "polaris",
    }).encode()
    request = urllib.request.Request(f"{ISSUER}/protocol/openid-connect/token", data=form)
    request.add_header("Content-Type", "application/x-www-form-urlencoded")
    secret = os.environ.get("EXCHANGE_CLIENT_SECRET", "spark-connect-secret")
    raw = base64.b64encode(f"spark-connect:{secret}".encode()).decode()
    request.add_header("Authorization", f"Basic {raw}")
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read())["access_token"]


def load_table_credentials(user: str, namespace: str, table: str) -> dict:
    """Ask Polaris to load a table with credential delegation, as that user would."""
    import json, urllib.request

    polaris = os.environ.get("POLARIS_URL", "http://polaris:8181")
    url = f"{polaris}/api/catalog/v1/poc_catalog/namespaces/{namespace}/tables/{table}"
    request = urllib.request.Request(url)
    request.add_header("Authorization", f"Bearer {exchanged_token(user)}")
    request.add_header("X-Iceberg-Access-Delegation", "vended-credentials")
    request.add_header("Accept", "application/json")
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read()).get("config", {})


AUDIT_FILE = "/audit/audit.jsonl"


def audit_offset() -> int:
    """Byte offset of the MinIO audit log, so a test can read only its own events."""
    try:
        return os.path.getsize(AUDIT_FILE)
    except OSError:
        return -1


def audit_events_since(offset: int) -> list:
    """MinIO audit events appended since `offset`."""
    import json

    if offset < 0:
        return []
    try:
        with open(AUDIT_FILE) as handle:
            handle.seek(offset)
            return [json.loads(line) for line in handle if line.strip()]
    except (OSError, ValueError):
        return []


audit_available = pytest.mark.skipif(
    not os.path.exists(AUDIT_FILE),
    reason="MinIO audit log not mounted; run inside the compose client service",
)
