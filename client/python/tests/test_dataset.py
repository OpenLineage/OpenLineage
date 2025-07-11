# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""Tests for the dataset naming utilities."""

import pytest
from openlineage.client.naming.dataset import (
    ABFSS,
    DB2,
    DBFS,
    GCS,
    HDFS,
    S3,
    WASBS,
    Athena,
    AWSGlue,
    AzureCosmosDB,
    AzureDataExplorer,
    AzureSynapse,
    BigQuery,
    Cassandra,
    CrateDB,
    Hive,
    Kafka,
    LocalFileSystem,
    MySQL,
    OceanBase,
    Oracle,
    Postgres,
    PubSubNaming,
    PubSubResourceType,
    Redshift,
    RemoteFileSystem,
    Snowflake,
    Teradata,
    Trino,
)


class TestAthena:
    def test_athena_naming(self):
        athena = Athena("us-east-1", "my-catalog", "my-database", "my-table")
        assert athena.get_namespace() == "awsathena://athena.us-east-1.amazonaws.com"
        assert athena.get_name() == "my-catalog.my-database.my-table"

    def test_athena_validation(self):
        with pytest.raises(ValueError, match="region_name cannot be None"):
            Athena(None, "catalog", "database", "table")
        with pytest.raises(ValueError, match="catalog cannot be empty"):
            Athena("us-east-1", "", "database", "table")


class TestAWSGlue:
    def test_aws_glue_naming(self):
        glue = AWSGlue("us-west-2", "123456789012", "my-database", "my-table")
        assert glue.get_namespace() == "arn:aws:glue:us-west-2:123456789012"
        assert glue.get_name() == "table/my-database/my-table"

    def test_aws_glue_validation(self):
        with pytest.raises(ValueError, match="region_name cannot be None"):
            AWSGlue(None, "123456789012", "database", "table")


class TestAzureCosmosDB:
    def test_azure_cosmos_db_naming(self):
        cosmos = AzureCosmosDB("my-host", "my-database", "my-table")
        assert cosmos.get_namespace() == "azurecosmos://my-host/dbs/my-database"
        assert cosmos.get_name() == "colls/my-table"


class TestAzureDataExplorer:
    def test_azure_data_explorer_naming(self):
        kusto = AzureDataExplorer("mycluster", "my-database", "my-table")
        assert kusto.get_namespace() == "azurekusto://mycluster.kusto.windows.net"
        assert kusto.get_name() == "my-database/my-table"


class TestAzureSynapse:
    def test_azure_synapse_naming(self):
        synapse = AzureSynapse("myserver", "1433", "my-schema", "my-table")
        assert synapse.get_namespace() == "sqlserver://myserver:1433"
        assert synapse.get_name() == "my-schema.my-table"


class TestBigQuery:
    def test_bigquery_naming(self):
        bq = BigQuery("my-project", "my-dataset", "my-table")
        assert bq.get_namespace() == "bigquery"
        assert bq.get_name() == "my-project.my-dataset.my-table"

    def test_bigquery_validation(self):
        with pytest.raises(ValueError, match="project_id cannot be None"):
            BigQuery(None, "dataset", "table")
        with pytest.raises(ValueError, match="dataset_name cannot be empty"):
            BigQuery("project", "   ", "table")


class TestCassandra:
    def test_cassandra_naming(self):
        cassandra = Cassandra("localhost", "9042", "my-keyspace", "my-table")
        assert cassandra.get_namespace() == "cassandra://localhost:9042"
        assert cassandra.get_name() == "my-keyspace.my-table"


class TestMySQL:
    def test_mysql_naming(self):
        mysql = MySQL("localhost", "3306", "my-database", "my-table")
        assert mysql.get_namespace() == "mysql://localhost:3306"
        assert mysql.get_name() == "my-database.my-table"


class TestCrateDB:
    def test_crate_db_naming(self):
        crate = CrateDB("localhost", "4200", "my-database", "my-schema", "my-table")
        assert crate.get_namespace() == "crate://localhost:4200"
        assert crate.get_name() == "my-database.my-schema.my-table"


class TestDB2:
    def test_db2_naming(self):
        db2 = DB2("localhost", "50000", "my-database", "my-schema", "my-table")
        assert db2.get_namespace() == "db2://localhost:50000"
        assert db2.get_name() == "my-database.my-schema.my-table"


class TestOceanBase:
    def test_ocean_base_naming(self):
        ocean = OceanBase("localhost", "2881", "my-database", "my-table")
        assert ocean.get_namespace() == "oceanbase://localhost:2881"
        assert ocean.get_name() == "my-database.my-table"


class TestOracle:
    def test_oracle_naming(self):
        oracle = Oracle("localhost", "1521", "xe", "my-schema", "my-table")
        assert oracle.get_namespace() == "oracle://localhost:1521"
        assert oracle.get_name() == "xe.my-schema.my-table"


class TestPostgres:
    def test_postgres_naming(self):
        postgres = Postgres("localhost", "5432", "my-database", "my-schema", "my-table")
        assert postgres.get_namespace() == "postgres://localhost:5432"
        assert postgres.get_name() == "my-database.my-schema.my-table"


class TestTeradata:
    def test_teradata_naming(self):
        teradata = Teradata("localhost", "1025", "my-database", "my-table")
        assert teradata.get_namespace() == "teradata://localhost:1025"
        assert teradata.get_name() == "my-database.my-table"


class TestRedshift:
    def test_redshift_naming(self):
        redshift = Redshift("my-cluster", "us-east-1", "5439", "my-database", "my-schema", "my-table")
        assert redshift.get_namespace() == "redshift://my-cluster.us-east-1:5439"
        assert redshift.get_name() == "my-database.my-schema.my-table"


class TestSnowflake:
    def test_snowflake_naming(self):
        snowflake = Snowflake("my-org", "my-account", "my-database", "my-schema", "my-table")
        assert snowflake.get_namespace() == "snowflake://my-org-my-account"
        assert snowflake.get_name() == "my-database.my-schema.my-table"


class TestTrino:
    def test_trino_naming(self):
        trino = Trino("localhost", "8080", "my-catalog", "my-schema", "my-table")
        assert trino.get_namespace() == "trino://localhost:8080"
        assert trino.get_name() == "my-catalog.my-schema.my-table"


class TestABFSS:
    def test_abfss_naming(self):
        abfss = ABFSS("my-container", "my-service", "path/to/file")
        assert abfss.get_namespace() == "abfss://my-container@my-service.dfs.core.windows.net"
        assert abfss.get_name() == "path/to/file"


class TestDBFS:
    def test_dbfs_naming(self):
        dbfs = DBFS("my-workspace", "path/to/file")
        assert dbfs.get_namespace() == "dbfs://my-workspace"
        assert dbfs.get_name() == "path/to/file"


class TestGCS:
    def test_gcs_naming(self):
        gcs = GCS("my-bucket", "path/to/object")
        assert gcs.get_namespace() == "gs://my-bucket"
        assert gcs.get_name() == "path/to/object"


class TestHDFS:
    def test_hdfs_naming(self):
        hdfs = HDFS("namenode", "9000", "/path/to/file")
        assert hdfs.get_namespace() == "hdfs://namenode:9000"
        assert hdfs.get_name() == "/path/to/file"


class TestHive:
    def test_hive_naming(self):
        hive = Hive("localhost", "8080", "my-database", "my-table")
        assert hive.get_namespace() == "hive://localhost:8080"
        assert hive.get_name() == "my-database.my-table"


class TestKafka:
    def test_kafka_naming(self):
        kafka = Kafka("localhost", "9092", "my-topic")
        assert kafka.get_namespace() == "kafka://localhost:9092"
        assert kafka.get_name() == "my-topic"


class TestLocalFileSystem:
    def test_local_file_system_naming(self):
        local_fs = LocalFileSystem("/path/to/file")
        assert local_fs.get_namespace() == "file"
        assert local_fs.get_name() == "/path/to/file"


class TestRemoteFileSystem:
    def test_remote_file_system_naming(self):
        remote_fs = RemoteFileSystem("remote-host", "/path/to/file")
        assert remote_fs.get_namespace() == "file://remote-host"
        assert remote_fs.get_name() == "/path/to/file"


class TestS3:
    def test_s3_naming(self):
        s3 = S3("my-bucket", "path/to/object")
        assert s3.get_namespace() == "s3://my-bucket"
        assert s3.get_name() == "path/to/object"


class TestWASBS:
    def test_wasbs_naming(self):
        wasbs = WASBS("my-container", "my-service", "path/to/object")
        assert wasbs.get_namespace() == "wasbs://my-container@my-service.dfs.core.windows.net"
        assert wasbs.get_name() == "path/to/object"


class TestPubSubNaming:
    def test_pubsub_topic_naming(self):
        pubsub = PubSubNaming(PubSubResourceType.TOPIC, "my-project", "my-topic")
        assert pubsub.get_namespace() == "pubsub"
        assert pubsub.get_name() == "topic:my-project:my-topic"

    def test_pubsub_subscription_naming(self):
        pubsub = PubSubNaming(PubSubResourceType.SUBSCRIPTION, "my-project", "my-subscription")
        assert pubsub.get_namespace() == "pubsub"
        assert pubsub.get_name() == "subscription:my-project:my-subscription"

    def test_pubsub_validation(self):
        with pytest.raises(ValueError, match="resource_type cannot be None"):
            PubSubNaming(None, "project", "resource")
        with pytest.raises(ValueError, match="project_id cannot be empty"):
            PubSubNaming(PubSubResourceType.TOPIC, "  ", "resource")


class TestValidationHelpers:
    def test_empty_string_validation(self):
        """Test that empty strings (including whitespace-only) are rejected."""
        with pytest.raises(ValueError, match="cannot be empty"):
            BigQuery("project", "", "table")
        with pytest.raises(ValueError, match="cannot be empty"):
            BigQuery("project", "   ", "table")
        with pytest.raises(ValueError, match="cannot be empty"):
            BigQuery("project", "\t\n ", "table")

    def test_none_validation(self):
        """Test that None values are rejected."""
        with pytest.raises(ValueError, match="cannot be None"):
            BigQuery(None, "dataset", "table")
