"""The same two users and the same two tables -- asked in SPARQL instead of SQL.

Ontop compiles each SPARQL query into Spark SQL and runs it over Spark Connect **as the caller**,
carrying their token and their correlation ID. Nothing else changes: Polaris still decides, and it
still refuses bob the restricted namespace -- now through a SPARQL endpoint that holds no
credentials of its own.

Run it with `make demo-sparql` (password grant, headless). The interactive device-flow walkthrough
is `make demo`.
"""
from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "client", "src"))

from spark_connect_propagation import Endpoints, PasswordGrantTokenProvider, new_correlation_id

ONTOP = os.environ.get("ONTOP_URL", "http://localhost:8090").rstrip("/")
ISSUER = os.environ.get("KEYCLOAK_ISSUER", "http://keycloak:8080/realms/spark")

EVENTS = """
PREFIX : <http://example.org/poc#>
SELECT ?event ?kind WHERE { ?event a :Event ; :kind ?kind } ORDER BY ?event
"""

SALARIES = """
PREFIX : <http://example.org/poc#>
SELECT ?name ?salary WHERE { ?p a :Person ; :name ?name ; :salary ?salary } ORDER BY ?name
"""


def token(user: str) -> str:
    return PasswordGrantTokenProvider(
        endpoints=Endpoints(ISSUER), client_id="spark-cli", username=user, password=user
    ).token()


def ask(user: str, query: str, correlation_id: str) -> list[dict]:
    """POST one SPARQL query as `user`, and return its bindings.

    The two headers are the whole integration: the bearer token is the caller's own OIDC token,
    and the correlation ID is the one this script will quote if anything goes wrong.
    """
    request = urllib.request.Request(
        f"{ONTOP}/sparql",
        data=query.encode(),
        headers={
            "Content-Type": "application/sparql-query",
            "Accept": "application/sparql-results+json",
            "Authorization": f"Bearer {token(user)}",
            "X-Correlation-ID": correlation_id,
        },
    )
    with urllib.request.urlopen(request, timeout=180) as response:
        body = json.loads(response.read())
    return body["results"]["bindings"]


def show(rows: list[dict]) -> str:
    return ", ".join(
        "{" + " ".join(f"{k}={v['value']}" for k, v in sorted(row.items())) + "}" for row in rows
    ) or "(no rows)"


def main() -> int:
    cid = new_correlation_id()
    print(f"SPARQL endpoint: {ONTOP}/sparql")
    print(f"correlation id:  {cid}")
    print("   trace it afterwards with:  make cid CID=" + cid)

    print("\n[1] alice asks for the events  (shared: both users may read it)")
    print("    ", show(ask("alice", EVENTS, cid)))

    print("\n[2] bob asks for the same events")
    print("    ", show(ask("bob", EVENTS, cid)))

    print("\n[3] alice asks for the salaries  (restricted: only alice may read it)")
    print("    ", show(ask("alice", SALARIES, cid)))

    print("\n[4] bob asks for the salaries -- same endpoint, same mapping, same query")
    try:
        rows = ask("bob", SALARIES, cid)
    except urllib.error.HTTPError as error:
        detail = error.read().decode(errors="replace").strip().splitlines()
        print(f"     refused: HTTP {error.code}")
        print("     ", detail[0] if detail else "(no body)")
        print("\n     That refusal is Polaris's, not Ontop's. The mapping offers bob the same")
        print("     triples it offers alice; the catalog decided, against bob's own token.")
    else:
        print("     " + show(rows))
        print("\n     UNEXPECTED: bob should not have been able to read this.")
        return 1

    print("\nsparql demo OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
