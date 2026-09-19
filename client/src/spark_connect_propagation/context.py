"""Correlation-ID scoping for the client.

The default is session-scoped: one ID minted when the session is created and sent on every
RPC, so everything is always correlatable without ceremony. Opening a block narrows it.

The block matters because a single user action spans several RPCs -- ``spark.sql(...).show()``
issues an AnalyzePlan and an ExecutePlan -- and a fresh ID per RPC would make it impossible to
follow one action through the logs.
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Iterator, Optional

_CORRELATION_ID: ContextVar[Optional[str]] = ContextVar("spark_connect_correlation_id", default=None)


def new_correlation_id() -> str:
    """A fresh correlation ID.

    Deliberately a UUID4: Spark validates ``operation_id`` as a UUID4, so this stays
    compatible with binding the correlation ID to Spark's own operation id later.
    """
    return str(uuid.uuid4())


@contextmanager
def correlation_id(value: Optional[str] = None) -> Iterator[str]:
    """Scope a correlation ID to a block of work.

    Nests, and is contextvars-based, so it behaves correctly across threads and asyncio tasks.

        with correlation_id() as cid:
            spark.sql("SELECT * FROM polaris.shared.events").show()
            print("if that failed, quote", cid)
    """
    chosen = value or new_correlation_id()
    token = _CORRELATION_ID.set(chosen)
    try:
        yield chosen
    finally:
        _CORRELATION_ID.reset(token)


def current_correlation_id(session_default: str) -> str:
    """The ID to send on the next RPC: the innermost active block, else the session default."""
    return _CORRELATION_ID.get() or session_default


def active_correlation_id() -> Optional[str]:
    """The innermost active block's ID, or None when no block is open."""
    return _CORRELATION_ID.get()
