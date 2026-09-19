"""Client-minted operation ids, so the server can key what it learns per *operation*.

PySpark leaves ``ExecutePlanRequest.operation_id`` empty: all nine call sites in ``core.py``
invoke the private ``_execute_plan_request_with_metadata()`` with no argument, and the server
generates an id of its own instead. The gRPC interceptor therefore never sees one, and can only
file what it learns under ``(user_id, session_id)`` -- so two operations running concurrently in
*one* Connect session under *different* correlation IDs overwrite each other's.

Minting the id on this side closes that. The id travels in the request, Spark echoes it into the
operation job tag that reaches the ExecutionThread, and the server can then look up the exact
operation rather than the session.

Why not just use the correlation ID as the operation id -- the obvious move, since both are
UUID4s? Because one correlation ID is deliberately *not* one operation. ``spark.sql(x).collect()``
issues **two** ExecutePlan requests (the command, then the result relation), and a
``with correlation_id()`` block is meant to span several statements. Spark keys ``ExecuteHolder``
by ``(userId, sessionId, operationId)`` and refuses a repeat with
``INVALID_HANDLE.OPERATION_ALREADY_EXISTS``, or ``OPERATION_ABANDONED`` once the first has been
reaped -- verified against 4.1.3, where reusing one id failed on the very first statement. So the
operation id is fresh per request and the correlation ID keeps riding its own header; the server
logs the pair, which is what makes one grepable from the other.
"""

from __future__ import annotations

import uuid
from typing import Any, Optional

#: Name of the private PySpark seam. There is no public one -- see the module docstring.
SEAM = "_execute_plan_request_with_metadata"


def new_operation_id() -> str:
    """A fresh operation id.

    UUID4 because PySpark validates the value with ``uuid.UUID(operation_id, version=4)`` before
    putting it on the request, and rejects anything else with ``INVALID_OPERATION_UUID_ID``.
    """
    return str(uuid.uuid4())


def install_operation_ids(client: Any) -> bool:
    """Make ``client`` stamp a fresh operation id on every ExecutePlan request it builds.

    Patches the *instance*, not the class: a plain function in the instance ``__dict__`` shadows
    the class attribute for ordinary lookup, so only sessions this package opens are affected and
    nothing else in the process sees a mutated ``SparkConnectClient``.

    Returns False, without raising, when the seam is not where we expect it. A PySpark that
    renamed it costs per-operation keying -- the server falls back to keying by session, which is
    what this PoC did before -- and that is not worth refusing to open a session over.
    """
    original = getattr(client, SEAM, None)
    if not callable(original):
        return False

    def with_operation_id(operation_id: Optional[str] = None) -> Any:
        # `original` is already bound, so the id goes in as the first positional argument.
        return original(operation_id or new_operation_id())

    try:
        setattr(client, SEAM, with_operation_id)
    except AttributeError:
        return False
    return True
