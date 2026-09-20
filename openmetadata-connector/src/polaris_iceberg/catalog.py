"""Builds the PyIceberg REST catalog.

This is the surviving half of the original connector's ``catalog/`` package. The
factory and the Glue, Hive and DynamoDB catalogs are gone: they existed to let
one service type cover every Iceberg deployment, and this connector talks to one
REST catalog. The REST branch is the only one PR #26365 left anybody wanting.
"""
from __future__ import annotations

import logging
from typing import Any, Dict

from pyiceberg.catalog import Catalog, load_rest

from polaris_iceberg.config import PolarisIcebergConfig

logger = logging.getLogger(__name__)

# Keys whose values must never reach a log line.
_SECRETS = {"credential", "token", "client_secret"}


def redact(properties: Dict[str, Any]) -> Dict[str, Any]:
    """Copy the property map with secrets masked, for logging."""
    safe: Dict[str, Any] = {}
    for key, value in properties.items():
        if key in _SECRETS:
            safe[key] = "***"
        elif key == "auth" and isinstance(value, dict):
            safe[key] = {
                "type": value.get("type"),
                **{
                    inner_key: {
                        k: ("***" if k in _SECRETS else v) for k, v in inner.items()
                    }
                    for inner_key, inner in value.items()
                    if isinstance(inner, dict)
                },
            }
        else:
            safe[key] = value
    return safe


def build_catalog(name: str, config: PolarisIcebergConfig) -> Catalog:
    """Return a PyIceberg REST catalog for the given configuration."""
    properties = config.pyiceberg_properties()
    logger.debug("Opening Iceberg REST catalog %s with %s", name, redact(properties))
    return load_rest(name, properties)
