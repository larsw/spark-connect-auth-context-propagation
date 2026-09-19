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
    disposed,
    audit_available,
    audit_events_since,
    audit_offset,
    compose_logs,
    requires_docker,
    token_provider,
)


@pytest.fixture(scope="module", autouse=True)
def seed(alice):
    """alice seeds the tables through Spark Connect, exercising the write path for real.

    Module-scoped rather than in conftest: an autouse session fixture there would also fire for
    the unit tests, which are meant to run with no stack at all.
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
    with disposed(connect(REMOTE, StaticTokenProvider(""))) as session:
        with pytest.raises(SparkConnectGrpcException) as caught:
            session.sql("SELECT 1").collect()
    assert "UNAUTHENTICATED" in str(caught.value)


def test_malformed_token_is_rejected():
    with disposed(connect(REMOTE, StaticTokenProvider("not-a-jwt"))) as session:
        with pytest.raises(SparkConnectGrpcException) as caught:
            session.sql("SELECT 1").collect()
    assert "UNAUTHENTICATED" in str(caught.value)


def test_rejection_message_carries_the_correlation_id():
    """D5: a user can quote one string and have it found in every service's logs."""
    with disposed(connect(REMOTE, StaticTokenProvider("not-a-jwt"))) as session:
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

    with disposed(connect(REMOTE, StaticTokenProvider(exchanged_token("alice")))) as session:
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

    # release=False: this session carries alice's session id, so stop() would release HER live
    # server-side session as a side effect.
    with disposed(SparkSession.builder.channelBuilder(builder).create(), release=False) as impostor:
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


@requires_docker
def test_spark_adopts_the_operation_id_the_client_supplies():
    """D8: the client fills in ``ExecutePlanRequest.operation_id``, which stock PySpark leaves empty.

    That is what lets the interceptor file an identity under the exact operation instead of the
    session: Spark adopts the supplied id as its own, so it appears in ``ExecuteHolder``, in the
    Spark UI, and -- the part this design depends on -- in the operation job tag that reaches the
    ExecutionThread. Spark logs it as ``opId=...``, which is where this looks for it.
    """
    minted = []
    with disposed(connect(REMOTE, token_provider("alice"))) as session:
        supplied = session.client._execute_plan_request_with_metadata

        def capture(operation_id=None):
            request = supplied(operation_id)
            minted.append(request.operation_id)
            return request

        session.client._execute_plan_request_with_metadata = capture
        session.sql("SELECT count(*) FROM polaris.shared.events").collect()

    assert minted, "no ExecutePlan request was built"
    assert all(minted), f"the client left operation_id empty: {minted}"
    assert len(set(minted)) == len(minted), f"operation ids must be unique, got {minted}"

    connect_logs = compose_logs("spark-connect")
    for operation_id in minted:
        assert f"opId={operation_id}" in connect_logs, (
            f"Spark did not adopt the client's operation id {operation_id}"
        )


@requires_docker
def test_concurrent_operations_in_one_session_keep_their_own_correlation_id(alice):
    """D8, the payoff: correlation IDs stay attributed when one session runs several operations.

    Two queries at once in ONE Connect session under different correlation IDs. Keyed by session
    alone, the identity parked by one RPC is overwritten by its sibling's, and a query can reach
    Polaris stamped with the wrong ID; keyed by operation it cannot.

    Honest about what this proves: the interleaving needed to corrupt the session-keyed version is
    narrow -- each operation refreshes the session entry immediately before its own catalog call,
    so it usually wins the race -- and it could not be provoked here on demand. This asserts the
    property holds, not that the previous design reliably broke it. The keying itself is pinned
    down in ``PropagatedIdentityHolderTest.PerOperationKeying``.

    Polaris puts the ``X-Request-ID`` and the request path on one access-log line, so each ID can
    be checked against the table it was actually meant for.
    """
    rounds = 4
    markers = [(str(uuid.uuid4()), str(uuid.uuid4())) for _ in range(rounds)]

    def query(cid: str, sql: str) -> None:
        with correlation_id(cid):
            alice.sql(sql).collect()

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        for events_cid, salaries_cid in markers:
            futures = [
                pool.submit(query, events_cid, "SELECT * FROM polaris.shared.events"),
                pool.submit(query, salaries_cid, "SELECT * FROM polaris.restricted.salaries"),
            ]
            for future in futures:
                future.result()

    polaris_logs = compose_logs("polaris").splitlines()

    for events_cid, salaries_cid in markers:
        for cid, expected, forbidden in (
            (events_cid, "/tables/events", "/tables/salaries"),
            (salaries_cid, "/tables/salaries", "/tables/events"),
        ):
            lines = [line for line in polaris_logs if cid in line]
            assert lines, f"correlation ID {cid} never reached Polaris"
            assert any(expected in line for line in lines), (
                f"correlation ID {cid} never appeared on a {expected} request"
            )
            leaked = [line for line in lines if forbidden in line]
            assert not leaked, (
                f"correlation ID {cid} was stamped on a {forbidden} request -- a sibling "
                f"operation's identity overwrote it:\n" + "\n".join(leaked[:3])
            )


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
