"""The SPARQL endpoint: same identities, same catalog decisions, one more hop.

What these check is not that Ontop can talk to Spark -- it is that the *caller* reaches Polaris.
Ontop holds no credential for the data, so every row that comes back was authorised against the
token the HTTP client presented, and every refusal came from Polaris rather than from a rule
written into the mapping.

Skipped when the endpoint is not reachable, so the rest of the suite still runs without it.
"""
from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

import pytest

from conftest import (
    compose_logs,
    requires_docker,
    token_provider,
)

ONTOP = os.environ.get("ONTOP_URL", "http://localhost:8090").rstrip("/")

READY_TIMEOUT_SECONDS = int(os.environ.get("ONTOP_READY_TIMEOUT", "180"))

EVENTS_QUERY = """
PREFIX : <http://example.org/poc#>
SELECT ?event ?kind WHERE { ?event a :Event ; :kind ?kind }
"""

SALARIES_QUERY = """
PREFIX : <http://example.org/poc#>
SELECT ?name ?salary WHERE { ?p a :Person ; :name ?name ; :salary ?salary }
"""


# ------------------------------------------------------------------ plumbing --

def sparql(query: str, *, user: str | None = None, correlation_id: str | None = None,
           token: str | None = None) -> list[dict]:
    """POST a SPARQL query and return its bindings. Raises HTTPError on a refusal."""
    headers = {
        "Content-Type": "application/sparql-query",
        "Accept": "application/sparql-results+json",
    }
    if token is None and user is not None:
        token = token_provider(user).token()
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    if correlation_id is not None:
        headers["X-Correlation-ID"] = correlation_id

    request = urllib.request.Request(f"{ONTOP}/sparql", data=query.encode(), headers=headers)
    with urllib.request.urlopen(request, timeout=300) as response:
        return json.loads(response.read())["results"]["bindings"]


def endpoint_is_up() -> bool:
    try:
        with urllib.request.urlopen(f"{ONTOP}/", timeout=5) as response:
            return response.status == 200
    except Exception:
        return False


@pytest.fixture(scope="module", autouse=True)
def endpoint(alice):
    """Seed the tables, then wait for the endpoint that maps them.

    The endpoint does not need the tables to *start* -- its schemas are pinned in
    db-metadata.json, which is what lets it hold no credential of its own -- but it does need
    them to answer, and they do not exist until alice creates them through Spark Connect.
    """
    alice.sql("DROP TABLE IF EXISTS polaris.shared.events").collect()
    alice.sql("CREATE TABLE polaris.shared.events (id BIGINT, kind STRING) USING iceberg").collect()
    alice.sql(
        "INSERT INTO polaris.shared.events VALUES (1,'login'),(2,'logout'),(3,'purchase')"
    ).collect()

    alice.sql("DROP TABLE IF EXISTS polaris.restricted.salaries").collect()
    alice.sql(
        "CREATE TABLE polaris.restricted.salaries (person STRING, amount BIGINT) USING iceberg"
    ).collect()
    alice.sql("INSERT INTO polaris.restricted.salaries VALUES ('alice',100),('bob',90)").collect()

    deadline = time.monotonic() + READY_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if endpoint_is_up():
            return
        time.sleep(3)

    pytest.skip(f"the Ontop endpoint at {ONTOP} did not become ready within "
                f"{READY_TIMEOUT_SECONDS}s")


# ------------------------------------------------------------- authorisation --

def test_both_users_get_the_shared_triples():
    """O1: the mapping is the same for both, and so is the grant behind it."""
    assert len(sparql(EVENTS_QUERY, user="alice")) == 3
    assert len(sparql(EVENTS_QUERY, user="bob")) == 3


def test_alice_gets_the_restricted_triples_and_bob_does_not():
    """O2: the identity that reaches Polaris is the HTTP caller's, not the endpoint's.

    Note what is NOT happening: the mapping offers bob exactly the triples it offers alice, and
    Ontop applies no rule of its own. The refusal is Polaris's, made against bob's own token.
    """
    rows = sparql(SALARIES_QUERY, user="alice")
    assert {row["name"]["value"] for row in rows} == {"alice", "bob"}

    with pytest.raises(urllib.error.HTTPError) as caught:
        sparql(SALARIES_QUERY, user="bob")

    assert caught.value.code >= 400
    body = caught.value.read().decode(errors="replace")
    assert "bob" in body or "orbidden" in body or "not authorized" in body, body


def test_an_unauthenticated_request_is_refused():
    """O3: no token, no connection.

    The connection pool refuses to expand a URL whose credential placeholder resolves to nothing,
    rather than falling back to opening one without it. Otherwise an anonymous caller would reach
    Spark Connect as whoever the configuration file happened to name.
    """
    with pytest.raises(urllib.error.HTTPError) as caught:
        sparql(EVENTS_QUERY)

    assert caught.value.code >= 400


def test_a_bearer_token_from_the_wrong_issuer_is_refused():
    """O4: Ontop does not validate the token -- Spark Connect does, and that is enough."""
    with pytest.raises(urllib.error.HTTPError) as caught:
        sparql(EVENTS_QUERY, token="not.a.jwt")

    assert caught.value.code >= 400


@requires_docker
def test_the_endpoint_carries_no_credential_of_its_own():
    """O5: there is no identity the endpoint could fall back to.

    O3 shows an anonymous request being refused. This shows why that is structural rather than a
    policy: the connection pool logs the template it will expand for every request, and the only
    token in it is a placeholder filled from the caller's own Authorization header. If a JWT ever
    appears there, the per-caller authorisation the tests above assert is decoration.
    """
    logs = compose_logs("ontop")
    if "built from the query context" not in logs:
        pytest.skip("the endpoint has not logged its connection template")

    line = next(l for l in logs.splitlines() if "built from the query context" in l)

    assert "x-user-token=<bearer>" in line, line
    assert "user_id=<claim>" in line, line
    # A serialised JWT header is 'eyJ...'; the redaction above is what must be there instead.
    assert "x-user-token=ey" not in line, "a JWT is baked into the endpoint's configuration"


# --------------------------------------------------------------- correlation --

@requires_docker
def test_the_callers_correlation_id_reaches_spark_and_polaris():
    """O6: one UUID, grepped across the SPARQL endpoint's whole call chain.

    The client mints it, Ontop adopts it as its query id (ontop.queryIdHttpHeader), the connection
    pool puts it on the gRPC call, and the propagation plugin passes it to Polaris as X-Request-ID.
    """
    correlation_id = str(uuid.uuid4())
    assert len(sparql(EVENTS_QUERY, user="alice", correlation_id=correlation_id)) == 3

    # The lineage/log writes are not synchronous with the response.
    deadline = time.monotonic() + 60
    missing = []
    while time.monotonic() < deadline:
        missing = [s for s in ("spark-connect", "polaris")
                   if correlation_id not in compose_logs(s)]
        if not missing:
            return
        time.sleep(3)

    pytest.fail(f"correlation id {correlation_id} never reached: {', '.join(missing)}")
