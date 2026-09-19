"""Thread a user token and a correlation ID through Spark Connect."""

from .auth import (
    DeviceCodeTokenProvider,
    Endpoints,
    OAuthError,
    PasswordGrantTokenProvider,
    StaticTokenProvider,
    TokenProvider,
)
from .channel import PropagatingChannelBuilder, PropagationInterceptor
from .context import active_correlation_id, correlation_id, new_correlation_id
from .session import connect

__all__ = [
    "connect",
    "correlation_id",
    "active_correlation_id",
    "new_correlation_id",
    "PropagatingChannelBuilder",
    "PropagationInterceptor",
    "TokenProvider",
    "DeviceCodeTokenProvider",
    "PasswordGrantTokenProvider",
    "StaticTokenProvider",
    "Endpoints",
    "OAuthError",
]
