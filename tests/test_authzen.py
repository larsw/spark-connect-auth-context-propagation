"""Authorization decisions made by Cerbos over AuthZEN, for Polaris and for Spark Connect.

Two parts. The first asks Cerbos directly, with the requests Polaris and the plugin send, so a
policy mistake shows up as a policy failure rather than as a strange query error. The second goes
through Spark Connect and checks that the decisions are the ones the stack enforces, and that the
correlation ID reaches Cerbos's decision log.
"""
from __future__ import annotations

import json
import os
import tempfile
import urllib.request
import uuid

import pytest

from spark_connect_propagation import connect, correlation_id
from conftest import REMOTE, compose_logs, requires_docker, require_shared_secret, token_provider

CERBOS = os.environ.get("CERBOS_URL", "http://localhost:3592")


def decide(subject: dict, action: str, resource: dict) -> bool:
    body = json.dumps({"subject": subject, "action": {"name": action}, "resource": resource})
    request = urllib.request.Request(
        f"{CERBOS}/access/v1/evaluation",
        data=body.encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.loads(response.read())["decision"]


def polaris_user(name: str, *roles: str) -> dict:
    """The subject Polaris sends: the principal's name, and its activated principal roles."""
    properties = {"realm": "POLARIS"}
    if roles:
        properties["roles"] = list(roles)
    return {"type": "user", "id": name, "properties": properties}


def table(namespace: str, name: str) -> dict:
    """A table as Polaris describes it: its name, and its catalog and namespace as parents."""
    return {
        "type": "TABLE_LIKE",
        "id": f"poc_catalog/{namespace}/{name}",
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
        "id": f"poc_catalog/{name}",
        "properties": {"name": name, "parents": [{"type": "CATALOG", "name": "poc_catalog"}]},
    }


SESSION = {"type": "spark_connect", "id": "a-session", "properties": {"user_id": "x"}}

ALICE = polaris_user("alice", "data_engineer")
BOB = polaris_user("bob", "analyst")


# ---------------------------------------------------------------------- the policies alone --


def test_cerbos_advertises_its_authzen_endpoints():
    with urllib.request.urlopen(f"{CERBOS}/.well-known/authzen-configuration", timeout=10) as r:
        metadata = json.loads(r.read())
    assert metadata["access_evaluation_endpoint"].endswith("/access/v1/evaluation")
    assert metadata["access_evaluations_endpoint"].endswith("/access/v1/evaluations")


def test_an_analyst_reads_shared_tables_and_nothing_else():
    assert decide(BOB, "LOAD_TABLE_WITH_READ_DELEGATION", table("shared", "events"))
    assert decide(BOB, "LIST_TABLES", namespace("shared"))

    assert not decide(BOB, "LOAD_TABLE_WITH_READ_DELEGATION", table("restricted", "salaries"))
    assert not decide(BOB, "LOAD_NAMESPACE_METADATA", namespace("restricted"))
    assert not decide(BOB, "DROP_TABLE_WITH_PURGE", table("shared", "events"))


def test_an_analyst_is_never_granted_write_delegation():
    """Polaris asks for write delegation first and falls back to read only when it is refused.

    So this refusal is what keeps bob's vended S3 key read-only. If the policy allowed it, bob would
    get s3:PutObject on a table he may only read, and every query would still succeed.
    """
    assert not decide(BOB, "LOAD_TABLE_WITH_WRITE_DELEGATION", table("shared", "events"))


def test_a_data_engineer_may_do_anything_to_catalog_content():
    for action in ("LOAD_TABLE_WITH_WRITE_DELEGATION", "DROP_TABLE_WITH_PURGE", "UPDATE_TABLE"):
        assert decide(ALICE, action, table("restricted", "salaries")), action
    assert decide(ALICE, "CREATE_TABLE_DIRECT", namespace("restricted"))


def test_a_principal_with_no_role_may_do_nothing():
    nobody = polaris_user("carol")
    assert not decide(nobody, "LIST_CATALOGS", {"type": "ROOT", "id": "POLARIS"})
    assert not decide(nobody, "LOAD_TABLE", table("shared", "events"))


def test_only_root_administers_the_realm():
    root = polaris_user("root", "service_admin")
    assert decide(root, "CREATE_CATALOG", {"type": "ROOT", "id": "POLARIS"})
    assert not decide(ALICE, "CREATE_CATALOG", {"type": "ROOT", "id": "POLARIS"})
    assert not decide(ALICE, "CREATE_PRINCIPAL", {"type": "ROOT", "id": "POLARIS"})


def test_root_is_allowed_by_name_only_for_requests_from_polaris():
    """root's principal policy matches on the name, so it is held to Polaris's own requests.

    Polaris sends a realm attribute and the Spark Connect plugin does not. Without the condition, a
    Keycloak user who happened to be called root would be allowed anything in Spark Connect.
    """
    from_plugin = {"type": "user", "id": "root", "properties": {"sub": "someone"}}
    assert not decide(from_plugin, "AddArtifacts", SESSION)
    assert not decide(
        {"type": "user", "id": "root"}, "LOAD_TABLE_WITH_WRITE_DELEGATION", table("restricted", "salaries")
    )
    assert decide(polaris_user("root", "service_admin"), "ASSIGN_PRINCIPAL_ROLE",
                  {"type": "PRINCIPAL_ROLE", "id": "analyst"})


def test_analysts_query_spark_connect_but_do_not_upload_code():
    analyst = {"type": "user", "id": "bob", "properties": {"roles": ["analyst"]}}
    engineer = {"type": "user", "id": "alice", "properties": {"roles": ["data_engineer"]}}

    assert decide(analyst, "ExecutePlan", SESSION)
    assert decide(analyst, "AnalyzePlan", SESSION)
    assert decide(analyst, "CloneSession", SESSION)
    assert not decide(analyst, "AddArtifacts", SESSION)
    assert decide(engineer, "AddArtifacts", SESSION)
    assert not decide({"type": "user", "id": "carol"}, "ExecutePlan", SESSION)


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
def test_the_correlation_id_reaches_the_cerbos_decision_log(alice):
    """One ID for the request in Spark Connect, Polaris and the PDP that decided it."""
    marker = str(uuid.uuid4())
    with correlation_id(marker):
        alice.sql("SHOW NAMESPACES IN polaris").collect()

    assert marker in compose_logs("cerbos"), "correlation ID missing from Cerbos's decision log"
