"""Authorization decisions made by OpenFGA over AuthZEN, for Polaris and for Spark Connect.

Two parts. The first asks OpenFGA directly, with the requests Polaris and the plugin send, so a
policy mistake shows up as a policy failure rather than as a strange query error. The second goes
through Spark Connect and checks that the decisions are the ones the stack enforces, and that the
correlation ID reaches OpenFGA's log.
"""
from __future__ import annotations

import json
import os
import tempfile
import urllib.error
import urllib.request
import uuid

import pytest

from spark_connect_propagation import connect, correlation_id
from conftest import REMOTE, compose_logs, requires_docker, require_shared_secret, token_provider

OPENFGA = os.environ.get("OPENFGA_URL", "http://localhost:8093").rstrip("/")
STORE_NAME = os.environ.get("OPENFGA_STORE_NAME", "spark-connect-propagation")


def _get(path: str) -> dict:
    with urllib.request.urlopen(f"{OPENFGA}{path}", timeout=10) as response:
        return json.loads(response.read())


@pytest.fixture(scope="module")
def store() -> str:
    """The store docker/openfga/setup.py made. Its id is only known at runtime."""
    for candidate in _get("/stores?page_size=100").get("stores", []):
        if candidate["name"] == STORE_NAME:
            return candidate["id"]
    pytest.fail(f"no OpenFGA store named {STORE_NAME}; did openfga-setup run?")


def ask(store: str, body: dict, path: str = "evaluation") -> tuple[int, dict]:
    request = urllib.request.Request(
        f"{OPENFGA}/stores/{store}/access/v1/{path}",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


@pytest.fixture(scope="module")
def decide(store):
    """True or false, as OpenFGA decides. Any error fails the test, so a typo cannot pass as a no."""

    def decide(subject: dict, action: str, resource: dict) -> bool:
        status, body = ask(store, {"subject": subject, "action": {"name": action}, "resource": resource})
        assert status == 200, (status, body)
        return body["decision"]

    return decide


def polaris_user(name: str, *roles: str) -> dict:
    """The subject Polaris sends: the principal's name, its activated principal roles, its realm.

    Polaris leaves `roles` out when there are none, as it does for carol.
    """
    properties = {"realm": "POLARIS"}
    if roles:
        properties["roles"] = list(roles)
    return {"type": "user", "id": name, "properties": properties}


def plugin_user(name: str, *roles: str) -> dict:
    """The subject the Spark Connect plugin sends. It always sends `roles`, even an empty list."""
    return {"type": "user", "id": name, "properties": {"sub": f"sub-of-{name}", "roles": list(roles)}}


def table(namespace: str, name: str) -> dict:
    """A table as Polaris describes it with resource-id-format=type: the id is the type, and the
    table's name and its catalog and namespace are properties."""
    return {
        "type": "TABLE_LIKE",
        "id": "TABLE_LIKE",
        "properties": {
            "name": name,
            "parents": [
                {"type": "CATALOG", "name": "poc_catalog"},
                {"type": "NAMESPACE", "name": namespace},
            ],
        },
    }


def namespace(name: str) -> dict:
    return {
        "type": "NAMESPACE",
        "id": "NAMESPACE",
        "properties": {"name": name, "parents": [{"type": "CATALOG", "name": "poc_catalog"}]},
    }


ROOT = {"type": "ROOT", "id": "ROOT", "properties": {"name": "POLARIS"}}
SPARK = {"type": "spark_connect", "id": "spark-connect",
         "properties": {"session_id": "a-session", "user_id": "x"}}

ALICE = polaris_user("alice", "data_engineer")
BOB = polaris_user("bob", "analyst")


# ---------------------------------------------------------------------- the policies alone --


def test_the_store_answers_on_its_authzen_paths(store):
    """OpenFGA's AuthZEN endpoints are per store, so every caller's URL contains the store id."""
    status, body = ask(store, {"subject": BOB, "action": {"name": "LIST_CATALOGS"}, "resource": ROOT})
    assert status == 200 and body == {"decision": True}, (status, body)


def test_an_analyst_reads_shared_tables_and_nothing_else(decide):
    assert decide(BOB, "LOAD_TABLE_WITH_READ_DELEGATION", table("shared", "events"))
    assert decide(BOB, "LIST_TABLES", namespace("shared"))

    assert not decide(BOB, "LOAD_TABLE_WITH_READ_DELEGATION", table("restricted", "salaries"))
    assert not decide(BOB, "LOAD_NAMESPACE_METADATA", namespace("restricted"))
    assert not decide(BOB, "DROP_TABLE_WITH_PURGE", table("shared", "events"))


def test_an_analyst_is_never_granted_write_delegation(decide):
    """Polaris asks for write delegation first and falls back to read only when it is refused.

    So this refusal is what keeps bob's vended S3 key read-only. If the policy allowed it, bob would
    get s3:PutObject on a table he may only read, and every query would still succeed.
    """
    assert not decide(BOB, "LOAD_TABLE_WITH_WRITE_DELEGATION", table("shared", "events"))


def test_a_data_engineer_may_do_anything_to_catalog_content(decide):
    for action in ("LOAD_TABLE_WITH_WRITE_DELEGATION", "DROP_TABLE_WITH_PURGE", "UPDATE_TABLE"):
        assert decide(ALICE, action, table("restricted", "salaries")), action
    assert decide(ALICE, "CREATE_TABLE_DIRECT", namespace("restricted"))


def test_only_root_administers_the_realm(decide):
    root = polaris_user("root", "service_admin")
    assert decide(root, "CREATE_CATALOG", ROOT)
    assert not decide(ALICE, "CREATE_CATALOG", ROOT)
    assert not decide(ALICE, "CREATE_PRINCIPAL", ROOT)
    assert decide(root, "ASSIGN_PRINCIPAL_ROLE", {"type": "PRINCIPAL_ROLE", "id": "PRINCIPAL_ROLE"})


def test_root_gets_nothing_in_spark_connect(decide):
    """root's tuple names the user, so it exists only on Polaris's types and needs Polaris's realm.

    Without that, a Keycloak user who happened to be called root would be allowed anything.
    """
    assert not decide(plugin_user("root"), "AddArtifacts", SPARK)
    assert not decide(plugin_user("root"), "ExecutePlan", SPARK)


def test_analysts_query_spark_connect_but_do_not_upload_code(decide):
    analyst, engineer = plugin_user("bob", "analyst"), plugin_user("alice", "data_engineer")

    assert decide(analyst, "ExecutePlan", SPARK)
    assert decide(analyst, "AnalyzePlan", SPARK)
    assert decide(analyst, "CloneSession", SPARK)
    assert not decide(analyst, "AddArtifacts", SPARK)
    assert decide(engineer, "AddArtifacts", SPARK)
    assert not decide(plugin_user("carol"), "ExecutePlan", SPARK)


def test_a_missing_condition_parameter_is_an_error_not_a_no(store):
    """OpenFGA refuses to evaluate a condition whose parameter is absent (FINDINGS §21).

    Polaris leaves `roles` out for a principal with none, so for carol OpenFGA answers HTTP 400 to a
    single evaluation, and an error inside a decision of false to a batch, which Polaris uses when
    one authorization has several questions. Polaris denies on both. The plugin always sends `roles`, so carol gets a plain false
    there (test_analysts_query_spark_connect_but_do_not_upload_code).
    """
    carol = polaris_user("carol")
    status, body = ask(store, {"subject": carol, "action": {"name": "LIST_CATALOGS"}, "resource": ROOT})
    assert status == 400 and "missing context parameters" in body.get("message", ""), (status, body)

    status, body = ask(store, {
        "subject": carol,
        "resource": table("shared", "events"),
        "evaluations": [{"action": {"name": "LOAD_TABLE"}}, {"action": {"name": "TABLE_EXISTS"}}],
    }, path="evaluations")
    assert status == 200, (status, body)
    assert [e["decision"] for e in body["evaluations"]] == [False, False], body
    assert all("missing context parameters" in e["context"]["error"]["message"]
               for e in body["evaluations"]), body


def test_an_unknown_action_is_an_error_not_a_no(store):
    """An action is a relation, and OpenFGA rejects a question about a relation it does not have.

    Polaris treats the 400 as a deny, and the plugin as no answer (UNAVAILABLE). Every Polaris
    operation and every RPC the interceptor reads is a relation, so this is only for new ones.
    """
    status, body = ask(store, {"subject": ALICE, "action": {"name": "NOT_AN_OPERATION"},
                               "resource": table("shared", "events")})
    assert status == 400 and "not found" in body.get("message", ""), (status, body)


# ------------------------------------------------------------------ through Spark Connect --


def test_spark_connect_refuses_a_user_with_no_role_before_running_anything():
    """carol signs in, and her token is valid, but no policy lets her use Spark Connect."""
    require_shared_secret()
    carol = connect(REMOTE, token_provider("carol"))
    try:
        with pytest.raises(Exception) as caught:
            carol.sql("SELECT 1").collect()
        message = str(caught.value)
        assert "PERMISSION_DENIED" in message or "does not allow" in message, message
        assert "carol" in message, message
    finally:
        carol.client.close()


def test_an_analyst_may_not_add_artifacts_but_a_data_engineer_may(alice, bob):
    with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as handle:
        handle.write("artifact\n")
        path = handle.name
    try:
        with pytest.raises(Exception) as caught:
            bob.addArtifacts(path, file=True)
        message = str(caught.value)
        assert "AddArtifacts" in message and "bob" in message, message

        alice.addArtifacts(path, file=True)
    finally:
        os.unlink(path)


@requires_docker
def test_the_correlation_id_reaches_the_openfga_log(alice):
    """One ID for the request in Spark Connect, Polaris and the PDP that decided it.

    OpenFGA ignores X-Request-ID; the ID is found in the AuthZEN context it logs with each request.
    """
    marker = str(uuid.uuid4())
    with correlation_id(marker):
        alice.sql("SHOW NAMESPACES IN polaris").collect()

    assert marker in compose_logs("openfga"), "correlation ID missing from OpenFGA's log"
