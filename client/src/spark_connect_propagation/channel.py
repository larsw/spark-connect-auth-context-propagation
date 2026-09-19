"""A Spark Connect channel that carries a user token and a correlation ID on every RPC.

Built entirely on public PySpark API -- ``DefaultChannelBuilder``, ``add_interceptor`` and
``SparkSession.builder.channelBuilder`` -- so nothing in the channel is a fork or a patch. (The
one private seam this package does use is in :mod:`.operation`, and is not on this path.)

Why headers rather than the request body: PySpark does expose
``SparkConnectClient.add_global_user_context_extension``, but Spark Connect stringifies the whole
request proto into the job description and callSite, so a JWT carried in the body would surface
in the Spark UI. Metadata does not.
"""

from __future__ import annotations

import base64
import collections
import json
from typing import Any, List, Optional, Sequence, Tuple

import grpc
from pyspark.sql.connect.client import DefaultChannelBuilder

from .auth import TokenProvider
from .context import current_correlation_id, new_correlation_id

DEFAULT_TOKEN_HEADER = "x-user-token"
DEFAULT_CORRELATION_HEADER = "x-correlation-id"


def subject_of(jwt: str) -> Optional[str]:
    """The ``sub`` claim of a JWT, without verifying it.

    The client has no business validating its own token -- the server does that. This is only
    used to tell Spark Connect which user_id the session belongs to, which the server then
    checks against the authenticated subject.
    """
    try:
        payload = jwt.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        return json.loads(base64.urlsafe_b64decode(payload)).get("sub")
    except Exception:
        return None


class _CallDetails(
    collections.namedtuple(
        "_CallDetails",
        ("method", "timeout", "metadata", "credentials", "wait_for_ready", "compression"),
    ),
    grpc.ClientCallDetails,
):
    """grpc.ClientCallDetails is read-only, so replacing metadata means rebuilding it."""


class PropagationInterceptor(
    grpc.UnaryUnaryClientInterceptor, grpc.UnaryStreamClientInterceptor
):
    """Stamps the auth and correlation headers onto every outgoing RPC.

    Both interceptor interfaces are required: Spark Connect uses unary-unary for AnalyzePlan,
    Config, Interrupt and ReleaseSession, and unary-stream for ExecutePlan and ReattachExecute.
    Implementing only one silently leaves half the traffic unauthenticated.

    The token is fetched per call rather than captured once, so a provider that refreshes keeps
    a long-lived Spark session alive instead of failing when the original token expires.
    """

    def __init__(
        self,
        token_provider: TokenProvider,
        session_correlation_id: str,
        shared_secret: Optional[str] = None,
        token_header: str = DEFAULT_TOKEN_HEADER,
        correlation_header: str = DEFAULT_CORRELATION_HEADER,
    ) -> None:
        self._token_provider = token_provider
        self._session_correlation_id = session_correlation_id
        self._shared_secret = shared_secret
        self._token_header = token_header
        self._correlation_header = correlation_header

    def _with_headers(self, details: grpc.ClientCallDetails) -> _CallDetails:
        metadata: List[Tuple[str, Any]] = list(details.metadata or [])
        metadata.append((self._token_header, self._token_provider.token()))
        metadata.append(
            (self._correlation_header, current_correlation_id(self._session_correlation_id))
        )
        if self._shared_secret:
            # Spark's own PreSharedKeyAuthenticationInterceptor owns this header and compares it
            # to a single shared secret. It answers "is this a trusted client"; the user token
            # above answers "which user is it".
            metadata.append(("authorization", f"Bearer {self._shared_secret}"))
        return _CallDetails(
            details.method,
            details.timeout,
            metadata,
            details.credentials,
            getattr(details, "wait_for_ready", None),
            getattr(details, "compression", None),
        )

    def intercept_unary_unary(self, continuation, client_call_details, request):
        return continuation(self._with_headers(client_call_details), request)

    def intercept_unary_stream(self, continuation, client_call_details, request):
        return continuation(self._with_headers(client_call_details), request)


class PropagatingChannelBuilder(DefaultChannelBuilder):
    """A ``DefaultChannelBuilder`` that installs :class:`PropagationInterceptor`."""

    def __init__(
        self,
        url: str,
        token_provider: TokenProvider,
        shared_secret: Optional[str] = None,
        session_correlation_id: Optional[str] = None,
        token_header: str = DEFAULT_TOKEN_HEADER,
        correlation_header: str = DEFAULT_CORRELATION_HEADER,
        channel_options: Optional[Sequence[Tuple[str, Any]]] = None,
    ) -> None:
        super().__init__(url, list(channel_options) if channel_options else None)

        self.session_correlation_id = session_correlation_id or new_correlation_id()
        self.token_provider = token_provider

        # Tell the server which user this session belongs to. The server rejects the request if
        # this does not match the authenticated subject, which is what stops one client from
        # attaching to another user's SessionHolder.
        subject = subject_of(token_provider.token())
        if subject:
            self.set(DefaultChannelBuilder.PARAM_USER_ID, subject)

        self.add_interceptor(
            PropagationInterceptor(
                token_provider=token_provider,
                session_correlation_id=self.session_correlation_id,
                shared_secret=shared_secret,
                token_header=token_header,
                correlation_header=correlation_header,
            )
        )

    @property
    def secure(self) -> bool:
        """Always plaintext; every credential is supplied as per-RPC metadata by the interceptor.

        Without this override, ``ChannelBuilder.secure`` becomes True merely because the
        environment happens to define ``SPARK_CONNECT_AUTHENTICATE_TOKEN`` -- which is exactly
        the variable name used for the shared secret -- silently changing how the channel is
        built and bypassing the interceptor path we rely on.
        """
        return False
