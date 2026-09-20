#  Copyright 2025 Collate
#  Licensed under the Collate Community License, Version 1.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  https://github.com/open-metadata/OpenMetadata/blob/main/ingestion/LICENSE
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""Iceberg source methods.

Derived from ``metadata/ingestion/source/database/iceberg/metadata.py`` as it
stood at 4b27ff3, the commit before OpenMetadata PR #26365 deleted it. The
topology -- database, schema, table -- is the original. What changed is where
the configuration comes from: the native service type's JSON Schema went with
the connector, so this reads a CustomDatabaseConnection's connectionOptions
instead. See ``polaris_iceberg.config``.
"""
import time
import traceback
from typing import Any, Iterable, Optional, Tuple

import pyiceberg
import pyiceberg.exceptions

from metadata.generated.schema.api.data.createDatabase import CreateDatabaseRequest
from metadata.generated.schema.api.data.createDatabaseSchema import (
    CreateDatabaseSchemaRequest,
)
from metadata.generated.schema.api.data.createStoredProcedure import (
    CreateStoredProcedureRequest,
)
from metadata.generated.schema.api.data.createTable import CreateTableRequest
from metadata.generated.schema.entity.data.database import Database
from metadata.generated.schema.entity.data.databaseSchema import DatabaseSchema
from metadata.generated.schema.entity.data.table import Table, TableType
from metadata.generated.schema.entity.services.connections.database.customDatabaseConnection import (
    CustomDatabaseConnection,
)
from metadata.generated.schema.entity.services.ingestionPipelines.status import (
    StackTraceError,
)
from metadata.generated.schema.metadataIngestion.databaseServiceMetadataPipeline import (
    DatabaseServiceMetadataPipeline,
)
from metadata.generated.schema.metadataIngestion.workflow import Source as WorkflowSource
from metadata.generated.schema.type.basic import EntityName, FullyQualifiedEntityName
from metadata.generated.schema.type.entityReferenceList import EntityReferenceList
from metadata.ingestion.api.models import Either
from metadata.ingestion.api.steps import InvalidSourceException
from metadata.ingestion.models.ometa_classification import OMetaTagAndClassification
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.source.database.database_service import DatabaseServiceSource
from metadata.utils import fqn
from metadata.utils.filters import filter_by_schema, filter_by_table
from metadata.utils.logger import ingestion_logger

from polaris_iceberg.config import PolarisIcebergConfig

# OpenMetadata imports `get_connection` and `test_connection` from the module
# that holds `sourcePythonClass`, so they are re-exported here on purpose.
from polaris_iceberg.connection import get_connection, test_connection  # noqa: F401
from polaris_iceberg.helper import get_owner_from_table, namespace_to_str
from polaris_iceberg.models import IcebergTable

logger = ingestion_logger()


class PolarisIcebergSource(DatabaseServiceSource):
    """
    Implements the necessary methods to extract Iceberg metadata.
    Relies on [PyIceberg](https://py.iceberg.apache.org/).
    """

    def __init__(self, config: WorkflowSource, metadata: OpenMetadata):
        super().__init__()
        self.config = config
        self.source_config: DatabaseServiceMetadataPipeline = (
            self.config.sourceConfig.config
        )
        self.metadata = metadata
        self.service_connection = self.config.serviceConnection.root.config

        # The native connector called `get_connection(self.service_connection)`,
        # which dispatches on the service type. A custom connector holds the
        # parsed configuration itself, so build both here and keep the config
        # around -- `databaseName` and `ownershipProperty` used to be fields on
        # the service connection and are now read off it.
        self.iceberg_config = PolarisIcebergConfig.from_options(
            self.service_connection.connectionOptions
        )
        self.iceberg = get_connection(self.service_connection)

        self.connection_obj = self.iceberg
        self.test_connection()

    @classmethod
    def create(
        cls, config_dict, metadata: OpenMetadata, pipeline_name: Optional[str] = None
    ):
        config: WorkflowSource = WorkflowSource.model_validate(config_dict)
        connection = config.serviceConnection.root.config
        if not isinstance(connection, CustomDatabaseConnection):
            raise InvalidSourceException(
                f"Expected CustomDatabaseConnection, but got {connection}"
            )
        return cls(config, metadata)

    def get_database_names(self) -> Iterable[str]:
        """
        Prepares the database name to be sent to stage.
        Filtering happens here.
        """
        yield self.iceberg_config.database_name

    def yield_database(
        self, database_name: str
    ) -> Iterable[Either[CreateDatabaseRequest]]:
        """
        From topology.
        Prepare a database request and pass it to the sink.
        """
        database_request = CreateDatabaseRequest(
            name=database_name,
            service=self.context.get().database_service,
        )
        yield Either(right=database_request)
        self.register_record_database_request(database_request=database_request)

    def get_database_schema_names(self) -> Iterable[str]:
        """
        Prepares the database schema name to be sent to stage.
        Filtering happens here.
        """
        for namespace in self.iceberg.list_namespaces():
            namespace_name = namespace_to_str(namespace)
            try:
                schema_fqn = fqn.build(
                    self.metadata,
                    entity_type=DatabaseSchema,
                    service_name=self.context.get().database_service,
                    database_name=self.context.get().database,
                    schema_name=namespace_name,
                )
                if filter_by_schema(
                    self.config.sourceConfig.config.schemaFilterPattern,
                    schema_fqn
                    if self.config.sourceConfig.config.useFqnForFiltering
                    else namespace_name,
                ):
                    self.status.filter(schema_fqn, "Schema Filtered Out")
                    continue
                yield namespace_name
            except Exception as exc:
                self.status.failed(
                    StackTraceError(
                        name=namespace_name,
                        error=f"Unexpected exception to get the namespace [{namespace_name}]: {exc}",
                        stackTrace=traceback.format_exc(),
                    )
                )

    def yield_database_schema(
        self, schema_name: str
    ) -> Iterable[Either[CreateDatabaseSchemaRequest]]:
        """
        From topology.
        Prepare a database schema request and pass it to the sink.
        """
        schema_request = CreateDatabaseSchemaRequest(
            name=EntityName(schema_name),
            database=FullyQualifiedEntityName(
                fqn.build(
                    metadata=self.metadata,
                    entity_type=Database,
                    service_name=self.context.get().database_service,
                    database_name=self.context.get().database,
                )
            ),
        )
        yield Either(right=schema_request)
        self.register_record_schema_request(schema_request=schema_request)

    def _load_iceberg_table(self, table_identifier):
        """Load one table, retrying only the errors that retrying can fix.

        The original rewrote `exc` in two log lines where the bound name was `e`,
        so every exhausted retry and every non-network error raised NameError
        inside the handler and surfaced as "Could not load iceberg table
        properly: name 'exc' is not defined" -- the real cause was never logged.
        """
        try:
            retryable: Tuple[type, ...] = (OSError,)
            try:
                # botocore is only present when an object-store extra is
                # installed; the connector must not require it.
                from botocore.exceptions import EndpointConnectionError

                retryable = (OSError, EndpointConnectionError)
            except ImportError:
                pass

            max_retries = 3
            retry_delay = 1

            for attempt in range(max_retries):
                try:
                    return self.iceberg.load_table(table_identifier)
                except retryable as err:
                    transient = "Couldn't resolve host name" in str(
                        err
                    ) or "NETWORK_CONNECTION" in str(err)
                    if not transient:
                        logger.warning(
                            f"Non-retryable error loading table {table_identifier}: {err}"
                        )
                        return None
                    if attempt == max_retries - 1:
                        logger.warning(
                            f"Giving up on table {table_identifier} after "
                            f"{max_retries} attempts: {err}"
                        )
                        return None
                    logger.warning(
                        f"Network error loading table {table_identifier}, retrying in "
                        f"{retry_delay}s (attempt {attempt + 1}/{max_retries}): {err}"
                    )
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
        except Exception as exc:
            logger.debug(traceback.format_exc())
            logger.warning(f"Could not load iceberg table properly {exc}")
        return None

    def get_tables_name_and_type(self) -> Optional[Iterable[Tuple[str, str]]]:
        """
        Prepares the table name to be sent to stage.
        Filtering happens here.
        """
        namespace = self.context.get().database_schema

        for table_identifier in self.iceberg.list_tables(namespace):
            try:
                table = self._load_iceberg_table(table_identifier)
                # The identifier is (*namespace, table), so the name is its last
                # element. The original dropped only the first element, which
                # folded the trailing namespace levels into the name of every
                # table in a nested namespace.
                table_name = table_identifier[-1]
                if not table:
                    logger.debug(
                        f"iceberg Table could not be fetched for table name = {table_name}"
                    )
                    continue
                table_fqn = fqn.build(
                    self.metadata,
                    entity_type=Table,
                    service_name=self.context.get().database_service,
                    database_name=self.context.get().database,
                    schema_name=self.context.get().database_schema,
                    table_name=table_name,
                )
                if filter_by_table(
                    self.config.sourceConfig.config.tableFilterPattern,
                    table_fqn
                    if self.config.sourceConfig.config.useFqnForFiltering
                    else table_name,
                ):
                    self.status.filter(table_fqn, "Table Filtered Out")
                    continue

                self.context.get().iceberg_table = table
                yield table_name, TableType.Regular
            except pyiceberg.exceptions.NoSuchPropertyException:
                logger.warning(
                    f"Table [{table_identifier}] does not have the 'table_type' property. Skipped."
                )
                continue
            except pyiceberg.exceptions.NoSuchIcebergTableError:
                logger.warning(
                    f"Table [{table_identifier}] is not an Iceberg Table. Skipped."
                )
                continue
            except pyiceberg.exceptions.NoSuchTableError:
                logger.warning(f"Table [{table_identifier}] not Found. Skipped.")
                continue
            except Exception as exc:
                table_name = ".".join(table_identifier)
                self.status.failed(
                    StackTraceError(
                        name=table_name,
                        error=f"Unexpected exception to get table [{table_name}]: {exc}",
                        stackTrace=traceback.format_exc(),
                    )
                )

    def get_owner_ref(self, table_name: str) -> Optional[EntityReferenceList]:
        owner = get_owner_from_table(
            self.context.get().iceberg_table, self.iceberg_config.ownership_property
        )
        try:
            if owner:
                return self.metadata.get_reference_by_email(owner)
        except Exception as err:
            logger.debug(traceback.format_exc())
            logger.warning(f"Could not fetch owner data due to {err}")
        return None

    def yield_table(
        self, table_name_and_type: Tuple[str, TableType]
    ) -> Iterable[Either[CreateTableRequest]]:
        """
        From topology.
        Prepare a table request and pass it to the sink.
        """
        table_name, table_type = table_name_and_type
        iceberg_table = self.context.get().iceberg_table
        try:
            owners = self.get_owner_ref(table_name)
            table = IcebergTable.from_pyiceberg(
                table_name, table_type, owners, iceberg_table
            )
            table_request = CreateTableRequest(
                name=EntityName(table.name),
                tableType=table.tableType,
                description=table.description,
                owners=table.owners,
                columns=table.columns,
                tablePartition=table.tablePartition,
                databaseSchema=FullyQualifiedEntityName(
                    fqn.build(
                        metadata=self.metadata,
                        entity_type=DatabaseSchema,
                        service_name=self.context.get().database_service,
                        database_name=self.context.get().database,
                        schema_name=self.context.get().database_schema,
                    )
                ),
            )
            yield Either(right=table_request)
            self.register_record(table_request=table_request)
        except Exception as exc:
            yield Either(
                left=StackTraceError(
                    name=table_name,
                    error=f"Unexpected exception to yield table [{table_name}]: {exc}",
                    stackTrace=traceback.format_exc(),
                )
            )

    def yield_tag(self, schema_name: str) -> Iterable[Either[OMetaTagAndClassification]]:
        """
        From topology. To be run for each schema
        """
        yield from []

    def get_stored_procedures(self) -> Iterable[Any]:
        """Not Implemented"""

    def yield_stored_procedure(
        self, stored_procedure: Any
    ) -> Iterable[Either[CreateStoredProcedureRequest]]:
        """Process the stored procedure information"""
        yield from []

    def close(self):
        """There is no connection to close."""


# The original class name, kept as an alias so a pipeline configured against
# either spelling of sourcePythonClass resolves.
IcebergSource = PolarisIcebergSource
