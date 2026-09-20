"""Connection handling for the custom connector.

OpenMetadata resolves ``get_connection`` and ``test_connection`` for a custom
connector out of the module named by ``sourcePythonClass``
(``metadata/utils/importer.py``, ``import_connection_fn``), so both are
re-exported from ``polaris_iceberg.metadata`` -- that is the module the workflow
imports, and the lookup does not search this one.
"""
from __future__ import annotations

from typing import Optional

from pyiceberg.catalog import Catalog

from metadata.generated.schema.entity.automations.workflow import (
    Workflow as AutomationWorkflow,
)
from metadata.generated.schema.entity.services.connections.testConnectionResult import (
    TestConnectionResult,
)

# The public `test_connection_steps` first fetches a TestConnectionDefinition
# from the server, keyed on the service type, and raises when there is none.
# `customDatabase` has no such definition -- and the Iceberg one was deleted by
# the same PR that removed this connector -- so a custom connector has to call
# the runner underneath it and supply its own steps.
from metadata.ingestion.connections.test_connections import (
    TestConnectionStep,
    _test_connection_steps,
)
from metadata.ingestion.ometa.ometa_api import OpenMetadata

from polaris_iceberg.catalog import build_catalog
from polaris_iceberg.config import PolarisIcebergConfig

_CHECK_CONFIG = (
    "Check the connectionOptions on this service: `uri` must point at the Iceberg "
    "REST catalog (e.g. http://polaris:8181/api/catalog), `warehouse` must name a "
    "catalog that exists, and the credentials must be accepted by the token endpoint."
)


def get_connection(connection) -> Catalog:
    """Build the PyIceberg catalog for a CustomDatabaseConnection."""
    config = PolarisIcebergConfig.from_options(
        getattr(connection, "connectionOptions", None)
    )
    return build_catalog(name=config.warehouse or "iceberg", config=config)


def test_connection(
    metadata: OpenMetadata,
    catalog: Catalog,
    service_connection,  # noqa: ARG001 - part of the signature OpenMetadata calls
    automation_workflow: Optional[AutomationWorkflow] = None,
    timeout_seconds: Optional[int] = None,  # noqa: ARG001 - ditto
) -> TestConnectionResult:
    """Verify the catalog answers, during ingestion or as an Automation workflow.

    Kept to the original connector's two steps. ``GetTables`` stops at the first
    namespace on purpose: it is a reachability check, not an inventory, and a
    catalog with many namespaces should not pay for a full walk here.
    """

    def get_namespaces():
        list(catalog.list_namespaces())

    def get_tables():
        for namespace in catalog.list_namespaces():
            return list(catalog.list_tables(namespace))
        return []

    steps = [
        TestConnectionStep(
            name="GetNamespaces",
            description="List the namespaces in the catalog",
            function=get_namespaces,
            error_message=f"Could not list namespaces. {_CHECK_CONFIG}",
            mandatory=True,
        ),
        TestConnectionStep(
            name="GetTables",
            description="List the tables of the first namespace",
            function=get_tables,
            error_message=(
                "Could not list tables. The catalog is reachable, so this is most "
                "likely the principal lacking rights on the namespace."
            ),
            mandatory=True,
        ),
    ]

    return _test_connection_steps(
        metadata=metadata, steps=steps, automation_workflow=automation_workflow
    )
