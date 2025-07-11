# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""
Utility module containing dataset naming helpers based on OpenLineage's dataset naming
specification.

Each class represents naming logic for a specific platform (e.g., BigQuery, AWS Glue),
and provides methods to construct valid dataset names and namespaces.
"""

from __future__ import annotations

from enum import Enum
from typing import Protocol

import attr


def _check_not_empty(instance: object, attribute: attr.Attribute[str], value: str) -> str:  # noqa: ARG001
    """Validator to check if a string is not None and not empty."""
    if value is None:
        msg = f"{attribute.name} cannot be None"
        raise ValueError(msg)
    if not value.strip():
        msg = f"{attribute.name} cannot be empty"
        raise ValueError(msg)
    return value


def _check_enum_not_none(
    _: object, attribute: attr.Attribute[PubSubResourceType], value: PubSubResourceType
) -> PubSubResourceType:
    """Validator to check if an enum value is not None."""
    if value is None:
        msg = f"{attribute.name} cannot be None"
        raise ValueError(msg)
    return PubSubResourceType(value)


class DatasetNaming(Protocol):
    """Interface representing dataset naming logic for a specific platform."""

    def get_namespace(self) -> str:
        """
        Returns the dataset namespace (e.g., URI or ARN identifying the data source).

        Returns:
            The namespace string
        """

    def get_name(self) -> str:
        """
        Returns the dataset name specific to the platform format.

        Returns:
            The dataset name string
        """


@attr.define
class Athena(DatasetNaming):
    """Naming implementation for Athena."""

    region_name: str = attr.field(validator=_check_not_empty)
    catalog: str = attr.field(validator=_check_not_empty)
    database: str = attr.field(validator=_check_not_empty)
    table: str = attr.field(validator=_check_not_empty)

    def get_namespace(self) -> str:
        return f"awsathena://athena.{self.region_name}.amazonaws.com"

    def get_name(self) -> str:
        return f"{self.catalog}.{self.database}.{self.table}"


@attr.define
class AWSGlue(DatasetNaming):
    """Naming implementation for AWS Glue."""

    region_name: str = attr.field(validator=_check_not_empty)
    account_id: str = attr.field(validator=_check_not_empty)
    database_name: str = attr.field(validator=_check_not_empty)
    table_name: str = attr.field(validator=_check_not_empty)

    def get_namespace(self) -> str:
        return f"arn:aws:glue:{self.region_name}:{self.account_id}"

    def get_name(self) -> str:
        return f"table/{self.database_name}/{self.table_name}"


@attr.define
class AzureCosmosDB(DatasetNaming):
    """Naming implementation for Azure Cosmos DB."""

    host: str = attr.field(validator=_check_not_empty)
    database: str = attr.field(validator=_check_not_empty)
    table: str = attr.field(validator=_check_not_empty)

    def get_namespace(self) -> str:
        return f"azurecosmos://{self.host}/dbs/{self.database}"

    def get_name(self) -> str:
        return f"colls/{self.table}"


@attr.define
class AzureDataExplorer(DatasetNaming):
    """Naming implementation for Azure Data Explorer."""

    host: str = attr.field(validator=_check_not_empty)
    database: str = attr.field(validator=_check_not_empty)
    table: str = attr.field(validator=_check_not_empty)

    def get_namespace(self) -> str:
        return f"azurekusto://{self.host}.kusto.windows.net"

    def get_name(self) -> str:
        return f"{self.database}/{self.table}"


@attr.define
class AzureSynapse(DatasetNaming):
    """Naming implementation for AzureSynapse."""

    host: str = attr.field(validator=_check_not_empty)
    port: str = attr.field(validator=_check_not_empty)
    schema: str = attr.field(validator=_check_not_empty)
    table: str = attr.field(validator=_check_not_empty)

    def get_namespace(self) -> str:
        return f"sqlserver://{self.host}:{self.port}"

    def get_name(self) -> str:
        return f"{self.schema}.{self.table}"


@attr.define
class BigQuery(DatasetNaming):
    """
    Naming implementation for BigQuery.

    Namespace is always "bigquery".
    """

    project_id: str = attr.field(validator=_check_not_empty)
    dataset_name: str = attr.field(validator=_check_not_empty)
    table_name: str = attr.field(validator=_check_not_empty)

    def get_namespace(self) -> str:
        return "bigquery"

    def get_name(self) -> str:
        return f"{self.project_id}.{self.dataset_name}.{self.table_name}"


@attr.define
class Cassandra(DatasetNaming):
    """Naming implementation for Cassandra."""

    host: str = attr.field(validator=_check_not_empty)
    port: str = attr.field(validator=_check_not_empty)
    keyspace: str = attr.field(validator=_check_not_empty)
    table: str = attr.field(validator=_check_not_empty)

    def get_namespace(self) -> str:
        return f"cassandra://{self.host}:{self.port}"

    def get_name(self) -> str:
        return f"{self.keyspace}.{self.table}"


@attr.define
class MySQL(DatasetNaming):
    """Naming implementation for MySQL."""

    host: str = attr.field(validator=_check_not_empty)
    port: str = attr.field(validator=_check_not_empty)
    database: str = attr.field(validator=_check_not_empty)
    table: str = attr.field(validator=_check_not_empty)

    def get_namespace(self) -> str:
        return f"mysql://{self.host}:{self.port}"

    def get_name(self) -> str:
        return f"{self.database}.{self.table}"


@attr.define
class CrateDB(DatasetNaming):
    """Naming implementation for CrateDB."""

    host: str = attr.field(validator=_check_not_empty)
    port: str = attr.field(validator=_check_not_empty)
    database: str = attr.field(validator=_check_not_empty)
    schema: str = attr.field(validator=_check_not_empty)
    table: str = attr.field(validator=_check_not_empty)

    def get_namespace(self) -> str:
        return f"crate://{self.host}:{self.port}"

    def get_name(self) -> str:
        return f"{self.database}.{self.schema}.{self.table}"


@attr.define
class DB2(DatasetNaming):
    """Naming implementation for DB2."""

    host: str = attr.field(validator=_check_not_empty)
    port: str = attr.field(validator=_check_not_empty)
    database: str = attr.field(validator=_check_not_empty)
    schema: str = attr.field(validator=_check_not_empty)
    table: str = attr.field(validator=_check_not_empty)

    def get_namespace(self) -> str:
        return f"db2://{self.host}:{self.port}"

    def get_name(self) -> str:
        return f"{self.database}.{self.schema}.{self.table}"


@attr.define
class OceanBase(DatasetNaming):
    """Naming implementation for OceanBase."""

    host: str = attr.field(validator=_check_not_empty)
    port: str = attr.field(validator=_check_not_empty)
    database: str = attr.field(validator=_check_not_empty)
    table: str = attr.field(validator=_check_not_empty)

    def get_namespace(self) -> str:
        return f"oceanbase://{self.host}:{self.port}"

    def get_name(self) -> str:
        return f"{self.database}.{self.table}"


@attr.define
class Oracle(DatasetNaming):
    """Naming implementation for Oracle."""

    host: str = attr.field(validator=_check_not_empty)
    port: str = attr.field(validator=_check_not_empty)
    service_name: str = attr.field(validator=_check_not_empty)
    schema: str = attr.field(validator=_check_not_empty)
    table: str = attr.field(validator=_check_not_empty)

    def get_namespace(self) -> str:
        return f"oracle://{self.host}:{self.port}"

    def get_name(self) -> str:
        return f"{self.service_name}.{self.schema}.{self.table}"


@attr.define
class Postgres(DatasetNaming):
    """Naming implementation for Postgres."""

    host: str = attr.field(validator=_check_not_empty)
    port: str = attr.field(validator=_check_not_empty)
    database: str = attr.field(validator=_check_not_empty)
    schema: str = attr.field(validator=_check_not_empty)
    table: str = attr.field(validator=_check_not_empty)

    def get_namespace(self) -> str:
        return f"postgres://{self.host}:{self.port}"

    def get_name(self) -> str:
        return f"{self.database}.{self.schema}.{self.table}"


@attr.define
class Teradata(DatasetNaming):
    """Naming implementation for Teradata."""

    host: str = attr.field(validator=_check_not_empty)
    port: str = attr.field(validator=_check_not_empty)
    database: str = attr.field(validator=_check_not_empty)
    table: str = attr.field(validator=_check_not_empty)

    def get_namespace(self) -> str:
        return f"teradata://{self.host}:{self.port}"

    def get_name(self) -> str:
        return f"{self.database}.{self.table}"


@attr.define
class Redshift(DatasetNaming):
    """Naming implementation for Redshift."""

    cluster_identifier: str = attr.field(validator=_check_not_empty)
    region: str = attr.field(validator=_check_not_empty)
    port: str = attr.field(validator=_check_not_empty)
    database: str = attr.field(validator=_check_not_empty)
    schema: str = attr.field(validator=_check_not_empty)
    table: str = attr.field(validator=_check_not_empty)

    def get_namespace(self) -> str:
        return f"redshift://{self.cluster_identifier}.{self.region}:{self.port}"

    def get_name(self) -> str:
        return f"{self.database}.{self.schema}.{self.table}"


@attr.define
class Snowflake(DatasetNaming):
    """Naming implementation for Snowflake."""

    organization_name: str = attr.field(validator=_check_not_empty)
    account_name: str = attr.field(validator=_check_not_empty)
    database: str = attr.field(validator=_check_not_empty)
    schema: str = attr.field(validator=_check_not_empty)
    table: str = attr.field(validator=_check_not_empty)

    def get_namespace(self) -> str:
        return f"snowflake://{self.organization_name}-{self.account_name}"

    def get_name(self) -> str:
        return f"{self.database}.{self.schema}.{self.table}"


@attr.define
class Trino(DatasetNaming):
    """Naming implementation for Trino."""

    host: str = attr.field(validator=_check_not_empty)
    port: str = attr.field(validator=_check_not_empty)
    catalog: str = attr.field(validator=_check_not_empty)
    schema: str = attr.field(validator=_check_not_empty)
    table: str = attr.field(validator=_check_not_empty)

    def get_namespace(self) -> str:
        return f"trino://{self.host}:{self.port}"

    def get_name(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table}"


@attr.define
class ABFSS(DatasetNaming):
    """Naming implementation for ABFSS."""

    container: str = attr.field(validator=_check_not_empty)
    service: str = attr.field(validator=_check_not_empty)
    path: str = attr.field(validator=_check_not_empty)

    def get_namespace(self) -> str:
        return f"abfss://{self.container}@{self.service}.dfs.core.windows.net"

    def get_name(self) -> str:
        return self.path


@attr.define
class DBFS(DatasetNaming):
    """Naming implementation for DBFS."""

    workspace: str = attr.field(validator=_check_not_empty)
    path: str = attr.field(validator=_check_not_empty)

    def get_namespace(self) -> str:
        return f"dbfs://{self.workspace}"

    def get_name(self) -> str:
        return self.path


@attr.define
class GCS(DatasetNaming):
    """Naming implementation for GCS."""

    bucket_name: str = attr.field(validator=_check_not_empty)
    object_key: str = attr.field(validator=_check_not_empty)

    def get_namespace(self) -> str:
        return f"gs://{self.bucket_name}"

    def get_name(self) -> str:
        return self.object_key


@attr.define
class HDFS(DatasetNaming):
    """Naming implementation for HDFS."""

    namenode_host: str = attr.field(validator=_check_not_empty)
    namenode_port: str = attr.field(validator=_check_not_empty)
    path: str = attr.field(validator=_check_not_empty)

    def get_namespace(self) -> str:
        return f"hdfs://{self.namenode_host}:{self.namenode_port}"

    def get_name(self) -> str:
        return self.path


@attr.define
class Hive(DatasetNaming):
    """Naming implementation for Hive."""

    host: str = attr.field(validator=_check_not_empty)
    port: str = attr.field(validator=_check_not_empty)
    database: str = attr.field(validator=_check_not_empty)
    table: str = attr.field(validator=_check_not_empty)

    def get_namespace(self) -> str:
        return f"hive://{self.host}:{self.port}"

    def get_name(self) -> str:
        return f"{self.database}.{self.table}"


@attr.define
class Kafka(DatasetNaming):
    """Naming implementation for Kafka."""

    bootstrap_server_host: str = attr.field(validator=_check_not_empty)
    port: str = attr.field(validator=_check_not_empty)
    topic: str = attr.field(validator=_check_not_empty)

    def get_namespace(self) -> str:
        return f"kafka://{self.bootstrap_server_host}:{self.port}"

    def get_name(self) -> str:
        return self.topic


@attr.define
class LocalFileSystem(DatasetNaming):
    """Naming implementation for Local File System."""

    path: str = attr.field(validator=_check_not_empty)

    def get_namespace(self) -> str:
        return "file"

    def get_name(self) -> str:
        return self.path


@attr.define
class RemoteFileSystem(DatasetNaming):
    """Naming implementation for Remote File System."""

    host: str = attr.field(validator=_check_not_empty)
    path: str = attr.field(validator=_check_not_empty)

    def get_namespace(self) -> str:
        return f"file://{self.host}"

    def get_name(self) -> str:
        return self.path


@attr.define
class S3(DatasetNaming):
    """Naming implementation for S3."""

    bucket_name: str = attr.field(validator=_check_not_empty)
    object_key: str = attr.field(validator=_check_not_empty)

    def get_namespace(self) -> str:
        return f"s3://{self.bucket_name}"

    def get_name(self) -> str:
        return self.object_key


@attr.define
class WASBS(DatasetNaming):
    """Naming implementation for WASBS."""

    container: str = attr.field(validator=_check_not_empty)
    service_name: str = attr.field(validator=_check_not_empty)
    object_key: str = attr.field(validator=_check_not_empty)

    def get_namespace(self) -> str:
        return f"wasbs://{self.container}@{self.service_name}.dfs.core.windows.net"

    def get_name(self) -> str:
        return self.object_key


class PubSubResourceType(Enum):
    """Enum representing supported Pub/Sub resource types."""

    TOPIC = "topic"
    SUBSCRIPTION = "subscription"


@attr.define
class PubSubNaming(DatasetNaming):
    """
    Naming implementation for Google Pub/Sub.

    Namespace is fixed as "pubsub". Name format depends on the resource type:
    - topic:{project_id}:{topic_id}
    - subscription:{project_id}:{subscription_id}
    """

    resource_type: PubSubResourceType = attr.field(validator=_check_enum_not_none)
    project_id: str = attr.field(validator=_check_not_empty)
    resource_id: str = attr.field(validator=_check_not_empty)

    def get_namespace(self) -> str:
        return "pubsub"

    def get_name(self) -> str:
        return f"{self.resource_type.value}:{self.project_id}:{self.resource_id}"
