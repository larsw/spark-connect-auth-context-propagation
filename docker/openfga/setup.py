"""Load the PoC's authorization policy into OpenFGA, and tell Polaris and Spark Connect where it is.

OpenFGA is the Policy Decision Point for Polaris and for the Spark Connect plugin, which both ask it
over the OpenID AuthZEN Authorization API. This script runs once per `docker compose up`, after
OpenFGA is healthy and before either caller starts. It is safe to run again: it reuses the store,
writes the model as a new version (OpenFGA always answers with the latest), and makes the stored
tuples match the ones below, deleting any it no longer wants.

    docker compose run --rm openfga-setup     # apply an edited policy to a running stack

Why a script rather than a .fga file: the table and namespace rules need a condition parameter of
type list<map<string>> (Polaris's `parents`). The OpenFGA API accepts that, but the DSL and the fga
CLI do not (FINDINGS §21), so the model is built here as the API's JSON.

How the AuthZEN request becomes an OpenFGA Check (pkg/server/authzen.go in OpenFGA):

    user      = subject.type ":" subject.id          user:bob
    relation  = action.name                          LOAD_TABLE
    object    = resource.type ":" resource.id        TABLE_LIKE:TABLE_LIKE
    context   = subject.properties as subject_<key>, resource.properties as resource_<key>,
                action.properties as action_<key>, and the request's context as it is

OpenFGA answers from relationship tuples, one per object, and has no wildcard object. Polaris's
objects are tables and namespaces created at runtime, and the plugin's are sessions, so neither
could have tuples written ahead of time. Both callers therefore send a fixed resource id: Polaris
with `resource-id-format: type` (the object is TABLE_LIKE:TABLE_LIKE for every table), the plugin
with `authzen.resource-id`. The instance, and the caller's roles, arrive as properties, and the
conditions below decide on them.

Writes, under /openfga (a volume shared with Polaris and Spark Connect):
    store-id             the store's id, which every AuthZEN path contains
    polaris.properties   Polaris's two AuthZEN endpoints, read through QUARKUS_CONFIG_LOCATIONS

Standard library only, so it runs in a stock python image.
"""
from __future__ import annotations

import json
import os
import pathlib
import sys
import time
import urllib.error
import urllib.request

OPENFGA = os.environ.get("OPENFGA_URL", "http://openfga:8080").rstrip("/")
STORE_NAME = os.environ.get("OPENFGA_STORE_NAME", "spark-connect-propagation")
OUT = pathlib.Path(os.environ.get("OPENFGA_OUT_DIR", "/openfga"))
HERE = pathlib.Path(__file__).resolve().parent

# ----------------------------------------------------------------------------- the policy --
# tag::conditions[]
# CEL, over the context OpenFGA builds from the AuthZEN request. Every parameter a condition names
# must be in the request, or OpenFGA answers HTTP 400 instead of a decision (FINDINGS §21).
CONDITIONS = {
    "data_engineer": (
        {"subject_roles": "list<string>"},
        '"data_engineer" in subject_roles',
    ),
    "analyst": (
        {"subject_roles": "list<string>"},
        '"analyst" in subject_roles',
    ),
    # The namespace is `shared`, or is nested inside it.
    "analyst_in_shared_namespace": (
        {"subject_roles": "list<string>", "resource_name": "string",
         "resource_parents": "list<map<string>>"},
        '"analyst" in subject_roles && (resource_name == "shared" || '
        'resource_parents.exists(p, p["type"] == "NAMESPACE" && p["name"] == "shared"))',
    ),
    # The table is in `shared`, or in a namespace nested inside it.
    # tag::table-condition[]
    "analyst_in_shared_table": (
        {"subject_roles": "list<string>", "resource_parents": "list<map<string>>"},
        '"analyst" in subject_roles && '
        'resource_parents.exists(p, p["type"] == "NAMESPACE" && p["name"] == "shared")',
    ),
    # end::table-condition[]
    # Polaris puts its realm in the subject's properties; the Spark Connect plugin never does.
    "from_polaris": (
        {"subject_realm": "string"},
        'subject_realm == "POLARIS"',
    ),
}
# end::conditions[]

ALL = "*"

POLARIS_OPERATIONS = [
    line.strip()
    for line in (HERE / "polaris-operations.txt").read_text().splitlines()
    if line.strip() and not line.startswith("#")
]

# The RPCs the plugin's interceptor reads. ReleaseSession and ReleaseExecute are never asked about,
# so a client can always free server state; they are relations anyway so that a question about
# them is answered rather than refused as malformed.
SPARK_CONNECT_RPCS = [
    "ExecutePlan", "AnalyzePlan", "Config", "AddArtifacts", "ArtifactStatus", "Interrupt",
    "ReattachExecute", "ReleaseExecute", "ReleaseSession", "FetchErrorDetails", "CloneSession",
]

# tag::policy[]
# For each resource type: its fixed object id, the operations it has, and who holds which.
# A holder is a relation on the object, filled by one tuple: `user:*` with a condition, or a named
# user with one. An operation is the union of the holders that list it.
POLICY = {
    # Polaris's realm-level operations. Both roles may list the catalogs; creating catalogs,
    # principals and principal roles is left to root.
    "ROOT": {
        "id": "ROOT",
        "operations": POLARIS_OPERATIONS,
        "holders": {
            "root": ("user:root", "from_polaris", ALL),
            "data_engineer": ("user:*", "data_engineer", ["LIST_CATALOGS"]),
            "analyst": ("user:*", "analyst", ["LIST_CATALOGS"]),
        },
    },
    # A data engineer may do anything to a catalog's contents. An analyst may look at the catalog
    # and list its namespaces, which is how the Polaris console and Spark find their way to `shared`.
    "CATALOG": {
        "id": "CATALOG",
        "operations": POLARIS_OPERATIONS,
        "holders": {
            "root": ("user:root", "from_polaris", ALL),
            "data_engineer": ("user:*", "data_engineer", ALL),
            "analyst": ("user:*", "analyst", [
                "GET_CATALOG", "LIST_NAMESPACES", "LIST_POLICY",
                "GET_APPLICABLE_POLICIES_ON_CATALOG",
            ]),
        },
    },
    "NAMESPACE": {
        "id": "NAMESPACE",
        "operations": POLARIS_OPERATIONS,
        "holders": {
            "root": ("user:root", "from_polaris", ALL),
            "data_engineer": ("user:*", "data_engineer", ALL),
            "analyst_in_shared": ("user:*", "analyst_in_shared_namespace", [
                "LOAD_NAMESPACE_METADATA", "NAMESPACE_EXISTS", "LIST_NAMESPACES", "LIST_TABLES",
                "LIST_VIEWS", "LIST_POLICY", "GET_APPLICABLE_POLICIES_ON_NAMESPACE",
            ]),
        },
    },
    # An analyst may read the tables in `shared`, and nothing else. LOAD_TABLE_WITH_WRITE_DELEGATION
    # is left out on purpose: on a delegated load Polaris asks for write delegation first and only
    # falls back to read when that is refused, so allowing it here would hand a reader an S3 key
    # that can write. tests/test_propagation.py checks the scope of the key bob actually gets.
    # tag::table[]
    "TABLE_LIKE": {
        "id": "TABLE_LIKE",
        "operations": POLARIS_OPERATIONS,
        "holders": {
            "root": ("user:root", "from_polaris", ALL),
            "data_engineer": ("user:*", "data_engineer", ALL),
            "analyst_in_shared": ("user:*", "analyst_in_shared_table", [
                "LOAD_TABLE", "LOAD_TABLE_WITH_READ_DELEGATION", "TABLE_EXISTS", "LOAD_VIEW",
                "VIEW_EXISTS", "REPORT_READ_METRICS", "GET_APPLICABLE_POLICIES_ON_TABLE",
            ]),
        },
    },
    # end::table[]
    # Polaris's administrative kinds: root only.
    "PRINCIPAL": {"id": "PRINCIPAL", "operations": POLARIS_OPERATIONS,
                  "holders": {"root": ("user:root", "from_polaris", ALL)}},
    "PRINCIPAL_ROLE": {"id": "PRINCIPAL_ROLE", "operations": POLARIS_OPERATIONS,
                       "holders": {"root": ("user:root", "from_polaris", ALL)}},
    "CATALOG_ROLE": {"id": "CATALOG_ROLE", "operations": POLARIS_OPERATIONS,
                     "holders": {"root": ("user:root", "from_polaris", ALL)}},
    "POLICY": {"id": "POLICY", "operations": POLARIS_OPERATIONS,
               "holders": {"root": ("user:root", "from_polaris", ALL)}},
    # Spark Connect, as the plugin asks about it. Data engineers may use every RPC. Analysts, and
    # the job's service account, which holds the analyst role, may run queries but may not upload
    # artifacts: AddArtifacts adds jars, Python files and archives that run in the server's JVM and
    # on the executors everyone shares. Anyone with neither role is refused before Spark does any
    # work.
    "spark_connect": {
        "id": "spark-connect",
        "operations": SPARK_CONNECT_RPCS,
        "holders": {
            "data_engineer": ("user:*", "data_engineer", ALL),
            "analyst": ("user:*", "analyst", [
                "ExecutePlan", "AnalyzePlan", "Config", "ReattachExecute", "Interrupt",
                "ArtifactStatus", "FetchErrorDetails", "CloneSession",
            ]),
        },
    },
}
# end::policy[]


# -------------------------------------------------------------------- building the model --

def param_type(spelled: str) -> dict:
    """`list<map<string>>` as the API's ConditionParamTypeRef."""
    if "<" in spelled:
        outer, inner = spelled.split("<", 1)
        return {"type_name": f"TYPE_NAME_{outer.upper()}", "generic_types": [param_type(inner[:-1])]}
    return {"type_name": f"TYPE_NAME_{spelled.upper()}"}


def directly_related(user: str, condition: str) -> dict:
    kind, _, ident = user.partition(":")
    ref = {"type": kind, "condition": condition}
    if ident == "*":
        ref["wildcard"] = {}
    return ref


def model() -> dict:
    users = sorted({h[0].split(":")[0] for spec in POLICY.values() for h in spec["holders"].values()})
    types = [{"type": t} for t in users]
    for kind, spec in POLICY.items():
        relations, metadata = {}, {}
        for holder, (user, condition, _) in spec["holders"].items():
            relations[holder] = {"this": {}}
            metadata[holder] = {"directly_related_user_types": [directly_related(user, condition)]}
        for operation in spec["operations"]:
            granted = [
                holder for holder, (_, _, ops) in spec["holders"].items()
                if ops == ALL or operation in ops
            ]
            if not granted:
                sys.exit(f"{kind}.{operation}: nobody holds it; give it a holder or drop it")
            if len(granted) == 1:
                relations[operation] = {"computedUserset": {"relation": granted[0]}}
            else:
                relations[operation] = {"union": {"child": [
                    {"computedUserset": {"relation": h}} for h in granted
                ]}}
        types.append({"type": kind, "relations": relations,
                      "metadata": {"relations": metadata}})
    conditions = {
        name: {"name": name, "expression": expression,
               "parameters": {p: param_type(t) for p, t in params.items()}}
        for name, (params, expression) in CONDITIONS.items()
    }
    return {"schema_version": "1.1", "type_definitions": types, "conditions": conditions}


# tag::tuples[]
def tuples() -> list[dict]:
    return [
        {"user": user, "relation": holder, "object": f"{kind}:{spec['id']}",
         "condition": {"name": condition}}
        for kind, spec in POLICY.items()
        for holder, (user, condition, _) in spec["holders"].items()
    ]
# end::tuples[]


# ------------------------------------------------------------------------ talking to it --

def call(method: str, path: str, body: dict | None = None) -> dict:
    request = urllib.request.Request(
        OPENFGA + path, method=method,
        data=None if body is None else json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return json.loads(response.read() or b"{}")
    except urllib.error.HTTPError as e:
        sys.exit(f"{method} {path}: HTTP {e.code} {e.read().decode(errors='replace')}")


def wait_for_openfga() -> None:
    for _ in range(60):
        try:
            with urllib.request.urlopen(OPENFGA + "/healthz", timeout=2):
                return
        except OSError:
            time.sleep(1)
    sys.exit(f"OpenFGA at {OPENFGA} did not become healthy")


def store_id() -> str:
    token = ""
    while True:
        page = call("GET", "/stores?page_size=100" + (f"&continuation_token={token}" if token else ""))
        for store in page.get("stores", []):
            if store["name"] == STORE_NAME:
                return store["id"]
        token = page.get("continuation_token", "")
        if not token:
            break
    return call("POST", "/stores", {"name": STORE_NAME})["id"]


def key(t: dict) -> tuple:
    return (t["user"], t["relation"], t["object"], (t.get("condition") or {}).get("name", ""))


def sync_tuples(store: str) -> tuple[int, int]:
    existing, token = [], ""
    while True:
        page = call("POST", f"/stores/{store}/read",
                    {"page_size": 100, **({"continuation_token": token} if token else {})})
        existing += [t["key"] for t in page.get("tuples", [])]
        token = page.get("continuation_token", "")
        if not token:
            break
    wanted = {key(t): t for t in tuples()}
    have = {key(t): t for t in existing}
    stale = [have[k] for k in have.keys() - wanted.keys()]
    missing = [wanted[k] for k in wanted.keys() - have.keys()]
    # Deletes first: a tuple whose condition changed is the same key to OpenFGA, so it has to go
    # before its replacement can be written.
    for i in range(0, len(stale), 100):
        chunk = [{"user": t["user"], "relation": t["relation"], "object": t["object"]}
                 for t in stale[i:i + 100]]
        call("POST", f"/stores/{store}/write", {"deletes": {"tuple_keys": chunk}})
    for i in range(0, len(missing), 100):
        call("POST", f"/stores/{store}/write", {"writes": {"tuple_keys": missing[i:i + 100]}})
    return len(missing), len(stale)


def main() -> None:
    if "--print-model" in sys.argv:
        json.dump(model(), sys.stdout, indent=2)
        return
    wait_for_openfga()
    store = store_id()
    model_id = call("POST", f"/stores/{store}/authorization-models", model())["authorization_model_id"]
    written, deleted = sync_tuples(store)

    base = f"{OPENFGA}/stores/{store}"
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "store-id").write_text(store + "\n")
    (OUT / "polaris.properties").write_text(
        "# Written by docker/openfga/setup.py. OpenFGA's AuthZEN endpoints are per store, and the\n"
        "# store's id is only known once it exists, so Polaris reads them from here.\n"
        f"polaris.authorization.authzen.access-evaluation-endpoint={base}/access/v1/evaluation\n"
        f"polaris.authorization.authzen.access-evaluations-endpoint={base}/access/v1/evaluations\n"
    )
    print(f"OpenFGA store {STORE_NAME} = {store}, model {model_id}, "
          f"{len(tuples())} tuples ({written} written, {deleted} deleted)")
    print(f"AuthZEN evaluation endpoint: {base}/access/v1/evaluation")


if __name__ == "__main__":
    main()
