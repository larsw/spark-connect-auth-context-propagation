"""The claims this PoC makes, as executable assertions."""
from __future__ import annotations

import concurrent.futures
import os
import uuid

import pytest
from pyspark.errors.exceptions.connect import SparkConnectGrpcException

from spark_connect_propagation import (
    Endpoints,
    PropagatingChannelBuilder,
    StaticTokenProvider,
    connect,
    correlation_id,
)
from conftest import (
    REMOTE,
    audit_available,
    audit_events_since,
    audit_offset,
    compose_logs,
    requires_docker,
    token_provider,
)


# --------------------------------------------------------------- authorisation --

def test_both_users_read_the_shared_table(alice, bob):
    """D1: a grant both principals hold works for both."""
    assert alice.sql("SELECT count(*) c FROM polaris.shared.events").collect()[0].c == 3
    assert bob.sql("SELECT count(*) c FROM polaris.shared.events").collect()[0].c == 3


def test_alice_reads_restricted_but_bob_is_forbidden(alice, bob):
    """D2: the identity reaching Polaris is the *user's*, not the server's."""
    rows = alice.sql("SELECT * FROM polaris.restricted.salaries").collect()
    assert len(rows) == 2

    with pytest.raises(Exception) as caught:
        bob.sql("SELECT * FROM polaris.restricted.salaries").collect()

    message = str(caught.value)
    assert "bob" in message or "orbidden" in message or "not authorized" in message, message


def test_concurrent_users_do_not_leak_into_each_other(alice, bob):
    """D3: the thread bridge is keyed per session, so interleaving must not swap identities.

    A server that resolved one ambient identity would let bob read the restricted table here.
    """
    def alice_reads():
        return alice.sql("SELECT count(*) c FROM polaris.restricted.salaries").collect()[0].c

    def bob_tries():
        try:
            bob.sql("SELECT * FROM polaris.restricted.salaries").collect()
            return "ALLOWED"
        except Exception:
            return "DENIED"

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        futures = []
        for _ in range(3):
            futures.append(pool.submit(alice_reads))
            futures.append(pool.submit(bob_tries))
        results = [f.result() for f in futures]

    assert results[0::2] == [2, 2, 2], f"alice should always read 2 rows, got {results[0::2]}"
    assert results[1::2] == ["DENIED"] * 3, f"bob must always be denied, got {results[1::2]}"


# ------------------------------------------------------------------ rejection --

def test_missing_token_is_rejected():
    """D5: no credential means UNAUTHENTICATED at the edge, not a confusing failure later."""
    session = connect(REMOTE, StaticTokenProvider(""))
    with pytest.raises(SparkConnectGrpcException) as caught:
        session.sql("SELECT 1").collect()
    assert "UNAUTHENTICATED" in str(caught.value)


def test_malformed_token_is_rejected():
    session = connect(REMOTE, StaticTokenProvider("not-a-jwt"))
    with pytest.raises(SparkConnectGrpcException) as caught:
        session.sql("SELECT 1").collect()
    assert "UNAUTHENTICATED" in str(caught.value)


def test_rejection_message_carries_the_correlation_id():
    """D5: a user can quote one string and have it found in every service's logs."""
    session = connect(REMOTE, StaticTokenProvider("not-a-jwt"))
    with correlation_id() as cid:
        with pytest.raises(SparkConnectGrpcException) as caught:
            session.sql("SELECT 1").collect()
    assert cid in str(caught.value)


def test_downstream_audience_token_is_rejected():
    """A token minted for Polaris must not be accepted by Spark Connect.

    This is the audience check doing real work: the exchanged token is signed by the same issuer
    and belongs to the same user, and differs only in who it is addressed to.
    """
    from conftest import exchanged_token

    session = connect(REMOTE, StaticTokenProvider(exchanged_token("alice")))
    with pytest.raises(SparkConnectGrpcException) as caught:
        session.sql("SELECT 1").collect()
    assert "UNAUTHENTICATED" in str(caught.value)


# ------------------------------------------------------------------- spoofing --

def test_bob_cannot_claim_alices_session(alice):
    """D6: Spark Connect trusts the client's user_id/session_id; we must not.

    Without the binding in the interceptor, bob would attach to alice's live SessionHolder --
    her temp views, her cached frames and, here, her credentials.
    """
    alice.sql("SELECT 1").collect()  # make sure alice's session exists server-side
    alice_session_id = alice.client._session_id
    alice_user_id = alice.client._user_id

    builder = PropagatingChannelBuilder(
        url=REMOTE,
        token_provider=token_provider("bob"),
        shared_secret=os.environ.get("CONNECT_SHARED_SECRET"),
    )
    # Claim alice's coordinates outright.
    builder.set("session_id", alice_session_id)
    builder.set("user_id", alice_user_id)

    from pyspark.sql.connect.session import SparkSession

    impostor = SparkSession.builder.channelBuilder(builder).create()
    with pytest.raises(SparkConnectGrpcException) as caught:
        impostor.sql("SELECT 1").collect()
    assert "PERMISSION_DENIED" in str(caught.value), str(caught.value)


# -------------------------------------------------------------- observability --

@requires_docker
def test_one_correlation_id_appears_in_every_service(alice):
    """D4: the whole point -- grep one UUID, see the request in Spark and in Polaris."""
    marker = str(uuid.uuid4())
    with correlation_id(marker):
        # SHOW NAMESPACES always performs a REST call. A plain SELECT may not: Iceberg caches
        # table metadata, so a repeat query can be answered without contacting Polaris at all.
        alice.sql("SHOW NAMESPACES IN polaris").collect()
        alice.sql("SELECT count(*) FROM polaris.shared.events").collect()

    connect_logs = compose_logs("spark-connect")
    polaris_logs = compose_logs("polaris")

    assert marker in connect_logs, "correlation ID missing from the Spark Connect logs"
    assert marker in polaris_logs, "correlation ID never reached Polaris as X-Request-ID"


# ------------------------------------------------------- vended credentials --

def test_polaris_vends_distinct_temporary_credentials_per_user(alice, bob):
    """D7: the data plane is identity-aware, not just the catalog.

    Spark itself holds no S3 credentials at all, so whatever reaches the executor must have been
    vended for the authenticated user. This inspects exactly what Polaris hands back.
    """
    from conftest import load_table_credentials

    for_alice = load_table_credentials("alice", "shared", "events")
    for_bob = load_table_credentials("bob", "shared", "events")

    assert for_alice.get("s3.access-key-id"), f"no vended credentials for alice: {for_alice}"
    assert for_alice.get("s3.session-token"), "vended credentials are not temporary (no STS token)"

    assert for_alice["s3.access-key-id"] != "minio_root", "Polaris handed back its own root key"
    assert for_bob.get("s3.access-key-id") != "minio_root"

    assert for_alice["s3.session-token"] != for_bob.get("s3.session-token"), (
        "alice and bob received the same session token; credentials are not per-identity"
    )


@audit_available
def test_executor_presented_temporary_scoped_credentials_to_minio(alice):
    """D7, the last mile: what the executor actually sent to MinIO.

    The vended-credentials test above shows what Polaris hands out. This shows what the
    executor JVM genuinely presented on the wire, read from MinIO's own audit log.
    """
    offset = audit_offset()
    alice.sql("SELECT * FROM polaris.shared.events").collect()

    events = audit_events_since(offset)
    authenticated = [
        event.get("requestClaims") or {}
        for event in events
        if (event.get("requestClaims") or {}).get("accessKey")
    ]
    assert authenticated, f"no authenticated S3 requests captured ({len(events)} events seen)"

    access_keys = {claims["accessKey"] for claims in authenticated}
    assert "minio_root" not in access_keys, (
        f"the executor used MinIO's root credential: {access_keys}"
    )

    # An STS AssumeRole session derived from the Polaris server's own identity, not a static key.
    assert all(claims.get("parent") == "minio_root" for claims in authenticated), (
        "credentials were not STS-derived"
    )
    assert all("exp" in claims for claims in authenticated), "credentials were not temporary"
    assert any("sessionPolicy" in claims for claims in authenticated), (
        "credentials carried no session policy, so they were not sub-scoped to the table"
    )
