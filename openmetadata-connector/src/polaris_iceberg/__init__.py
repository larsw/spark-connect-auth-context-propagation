"""An OpenMetadata custom connector for an Iceberg REST catalog.

OpenMetadata PR #26365 removed the built-in Iceberg service type. This package
revives its ingestion logic as a *custom* connector -- the migration path that
PR pointed existing Iceberg services at -- and aims it at Apache Polaris.

Configure a CustomDatabase service with::

    sourcePythonClass: polaris_iceberg.metadata.PolarisIcebergSource

and the catalog details in ``connectionOptions``. See ``polaris_iceberg.config``
for the keys.
"""

__version__ = "0.1.0"

__all__ = ["PolarisIcebergConfig", "PolarisIcebergSource", "build_catalog"]


def __getattr__(name: str):
    # Imported lazily: `metadata` pulls in the OpenMetadata ingestion framework,
    # which is not needed just to read __version__.
    if name in {"PolarisIcebergSource"}:
        from polaris_iceberg.metadata import PolarisIcebergSource

        return PolarisIcebergSource
    if name == "PolarisIcebergConfig":
        from polaris_iceberg.config import PolarisIcebergConfig

        return PolarisIcebergConfig
    if name == "build_catalog":
        from polaris_iceberg.catalog import build_catalog

        return build_catalog
    raise AttributeError(name)
