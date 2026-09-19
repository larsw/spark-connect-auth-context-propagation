"""Client-side unit tests. No stack, no network, no docker -- these run anywhere.

Everything else in this suite needs the whole compose stack up; these cover the pieces that can
be checked in isolation, which is most of what is easy to get subtly wrong.
"""
from __future__ import annotations

import base64
import concurrent.futures
import json
import uuid

import pytest

from spark_connect_propagation import active_correlation_id, correlation_id, new_correlation_id
from spark_connect_propagation.channel import (
    DEFAULT_CORRELATION_HEADER,
    DEFAULT_TOKEN_HEADER,
    PropagationInterceptor,
    subject_of,
)
from spark_connect_propagation.context import current_correlation_id


def jwt_with(claims: dict) -> str:
    payload = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
    return f"header.{payload}.signature"


class FakeProvider:
    def __init__(self, value: str = "tok-1"):
        self.value = value
        self.calls = 0

    def token(self) -> str:
        self.calls += 1
        return self.value


class FakeCallDetails:
    """Mimics grpc.ClientCallDetails, which is read-only and has optional attributes."""

    def __init__(self, metadata=None):
        self.method = "/spark.connect.SparkConnectService/ExecutePlan"
        self.timeout = None
        self.metadata = metadata
        self.credentials = None
        self.wait_for_ready = None


def headers_sent(interceptor: PropagationInterceptor, details=None) -> dict:
    captured = {}

    def continuation(new_details, request):
        captured.update(dict(new_details.metadata))
        return "response"

    result = interceptor.intercept_unary_unary(
        continuation, details or FakeCallDetails(), "request"
    )
    assert result == "response"
    return captured


# ------------------------------------------------------------------ correlation --

def test_session_default_is_used_outside_a_block():
    assert current_correlation_id("session-default") == "session-default"
    assert active_correlation_id() is None


def test_block_overrides_the_session_default():
    with correlation_id("explicit") as cid:
        assert cid == "explicit"
        assert current_correlation_id("session-default") == "explicit"
    assert current_correlation_id("session-default") == "session-default"


def test_blocks_nest_and_unwind():
    with correlation_id("outer"):
        with correlation_id("inner"):
            assert current_correlation_id("d") == "inner"
        assert current_correlation_id("d") == "outer"
    assert current_correlation_id("d") == "d"


def test_generated_ids_are_uuid4():
    """Spark validates operation_id as a UUID4, so keeping this shape leaves that door open."""
    parsed = uuid.UUID(new_correlation_id())
    assert parsed.version == 4


def test_correlation_ids_do_not_leak_between_threads():
    """contextvars, not thread-locals: two users in one process must not share an ID."""
    def work(name: str) -> str:
        with correlation_id(name):
            return current_correlation_id("unused")

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(work, ["a", "b", "c", "d"]))

    assert results == ["a", "b", "c", "d"]


# -------------------------------------------------------------------- headers --

def test_interceptor_sends_token_and_correlation_id():
    provider = FakeProvider("jwt-value")
    interceptor = PropagationInterceptor(provider, session_correlation_id="session-cid")

    sent = headers_sent(interceptor)

    assert sent[DEFAULT_TOKEN_HEADER] == "jwt-value"
    assert sent[DEFAULT_CORRELATION_HEADER] == "session-cid"


def test_interceptor_uses_the_active_block():
    interceptor = PropagationInterceptor(FakeProvider(), session_correlation_id="session-cid")

    with correlation_id("per-operation"):
        sent = headers_sent(interceptor)

    assert sent[DEFAULT_CORRELATION_HEADER] == "per-operation"


def test_shared_secret_goes_on_authorization_not_the_token_header():
    """Spark's PreSharedKeyAuthenticationInterceptor owns Authorization exclusively."""
    interceptor = PropagationInterceptor(
        FakeProvider("jwt-value"), session_correlation_id="c", shared_secret="pre-shared"
    )

    sent = headers_sent(interceptor)

    assert sent["authorization"] == "Bearer pre-shared"
    assert sent[DEFAULT_TOKEN_HEADER] == "jwt-value"


def test_shared_secret_is_omitted_when_unset():
    interceptor = PropagationInterceptor(FakeProvider(), session_correlation_id="c")
    assert "authorization" not in headers_sent(interceptor)


def test_existing_metadata_is_preserved():
    interceptor = PropagationInterceptor(FakeProvider(), session_correlation_id="c")

    sent = headers_sent(interceptor, FakeCallDetails(metadata=[("user-agent", "pyspark")]))

    assert sent["user-agent"] == "pyspark"
    assert DEFAULT_TOKEN_HEADER in sent


def test_token_is_re_read_on_every_call():
    """A provider that refreshes must be able to keep a long-lived session alive."""
    provider = FakeProvider()
    interceptor = PropagationInterceptor(provider, session_correlation_id="c")

    headers_sent(interceptor)
    provider.value = "refreshed"
    sent = headers_sent(interceptor)

    assert provider.calls == 2
    assert sent[DEFAULT_TOKEN_HEADER] == "refreshed"


def test_unary_stream_is_intercepted_too():
    """ExecutePlan is unary-stream; covering only unary-unary leaves it unauthenticated."""
    interceptor = PropagationInterceptor(FakeProvider("jwt"), session_correlation_id="c")
    captured = {}

    def continuation(details, request):
        captured.update(dict(details.metadata))
        return iter([])

    list(interceptor.intercept_unary_stream(continuation, FakeCallDetails(), "req"))

    assert captured[DEFAULT_TOKEN_HEADER] == "jwt"


# -------------------------------------------------------------------- subject --

def test_subject_is_read_from_the_token():
    assert subject_of(jwt_with({"sub": "abc-123"})) == "abc-123"


@pytest.mark.parametrize("value", ["", "garbage", "a.b", "a.!!!.c"])
def test_subject_extraction_never_raises(value):
    assert subject_of(value) is None
