"""Convenience wiring from a token provider to a SparkSession."""

from __future__ import annotations

import os
from typing import Any, Optional, Sequence, Tuple

from pyspark.sql.connect.session import SparkSession

from .auth import TokenProvider
from .channel import PropagatingChannelBuilder
from .operation import install_operation_ids


def connect(
    remote: str,
    token_provider: TokenProvider,
    *,
    shared_secret: Optional[str] = None,
    session_correlation_id: Optional[str] = None,
    channel_options: Optional[Sequence[Tuple[str, Any]]] = None,
    per_operation_ids: bool = True,
) -> SparkSession:
    """Open a Spark Connect session that propagates identity and correlation on every RPC.

    ``shared_secret`` defaults to the ``CONNECT_SHARED_SECRET`` environment variable. It is
    Spark's own channel-level pre-shared key, and is unrelated to the user's token.

    ``per_operation_ids`` makes the client fill in ``ExecutePlanRequest.operation_id``, which
    PySpark otherwise leaves to the server, so the server can key propagated state per operation
    instead of per session. Turn it off to get a stock PySpark client on the wire; the server
    then falls back to keying by session. See :mod:`.operation`.

    Note each call creates a *new* session rather than reusing a cached one: two users in one
    process must not share a Connect session, which is the whole point of the isolation being
    tested here.
    """
    builder = PropagatingChannelBuilder(
        url=remote,
        token_provider=token_provider,
        shared_secret=shared_secret if shared_secret is not None
        else os.environ.get("CONNECT_SHARED_SECRET"),
        session_correlation_id=session_correlation_id,
        channel_options=channel_options,
    )
    session = SparkSession.builder.channelBuilder(builder).create()
    if per_operation_ids:
        install_operation_ids(session.client)
    return session
