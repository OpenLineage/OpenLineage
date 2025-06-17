/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.dataset;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class NamingTest {

  @Test
  void testBigQueryNaming() {
    Naming.BigQuery naming =
        new Naming.BigQuery("my-project", "my_dataset", "my_table");
    assertEquals("bigquery", naming.getNamespace());
    assertEquals("my-project.my_dataset.my_table", naming.getName());
  }

  @Test
  void testGlueNaming() {
    Naming.Glue naming =
        new Naming.Glue("us-east-1", "123456789012", "my_db", "my_table");
    assertEquals("arn:aws:glue:us-east-1:123456789012", naming.getNamespace());
    assertEquals("table/my_db/my_table", naming.getName());
  }

  @Test
  void testPubSubNaming() {
    Naming.PubSubNaming naming =
        new Naming.PubSubNaming(
            Naming.PubSubNaming.PubSubResourceType.TOPIC, "my-project", "my-topic");
    assertEquals("pubsub", naming.getNamespace());
    assertEquals("topic:my-project:my-topic", naming.getName());
  }

  @Test
  void testAzureCosmosDbNaming() {
    Naming.AzureCosmosDB naming =
        new Naming.AzureCosmosDB("my-host", "my-db", "my-collection");
    assertEquals("azurecosmos://my-host/dbs/my-db", naming.getNamespace());
    assertEquals("colls/my-collection", naming.getName());
  }

  @Test
  void testAzureDataExplorerNaming() {
    Naming.AzureDataExplorer naming =
        new Naming.AzureDataExplorer("my-host", "my-db", "my-table");
    assertEquals("azurekusto://my-host.kusto.windows.net", naming.getNamespace());
    assertEquals("my-db/my-table", naming.getName());
  }

  @Test
  void testAzureSynapseNaming() {
    Naming.AzureSynapse naming =
        new Naming.AzureSynapse("my-host", "1433", "dbo", "my-table");
    assertEquals("sqlserver://my-host:1433", naming.getNamespace());
    assertEquals("dbo.my-table", naming.getName());
  }

  @Test
  void testCassandraNaming() {
    Naming.Cassandra naming =
        new Naming.Cassandra("localhost", "9042", "my_keyspace", "my_table");
    assertEquals("cassandra://localhost:9042", naming.getNamespace());
    assertEquals("my_keyspace.my_table", naming.getName());
  }

  @Test
  void testMySqlNaming() {
    Naming.MySQL naming = new Naming.MySQL("localhost", "3306", "my_db", "my_table");
    assertEquals("mysql://localhost:3306", naming.getNamespace());
    assertEquals("my_db.my_table", naming.getName());
  }

  @Test
  void testCrateDbNaming() {
    Naming.CrateDB naming =
        new Naming.CrateDB("localhost", "5432", "crate_db", "doc", "my_table");
    assertEquals("crate://localhost:5432", naming.getNamespace());
    assertEquals("crate_db.doc.my_table", naming.getName());
  }

  @Test
  void testDb2Naming() {
    Naming.DB2 naming =
        new Naming.DB2("localhost", "50000", "db2inst1", "myschema", "my_table");
    assertEquals("db2://localhost:50000", naming.getNamespace());
    assertEquals("db2inst1.myschema.my_table", naming.getName());
  }

  @Test
  void testOceanBaseNaming() {
    Naming.OceanBase naming =
        new Naming.OceanBase("localhost", "2881", "my_db", "my_table");
    assertEquals("oceanbase://localhost:2881", naming.getNamespace());
    assertEquals("my_db.my_table", naming.getName());
  }

  @Test
  void testOracleNaming() {
    Naming.Oracle naming =
        new Naming.Oracle("localhost", "1521", "ORCLCDB", "myschema", "my_table");
    assertEquals("oracle://localhost:1521", naming.getNamespace());
    assertEquals("ORCLCDB.myschema.my_table", naming.getName());
  }

  @Test
  void testPostgresNaming() {
    Naming.Postgres naming =
        new Naming.Postgres("localhost", "5432", "mydb", "my_schema", "my_table");
    assertEquals("postgres://localhost:5432", naming.getNamespace());
    assertEquals("mydb.my_schema.my_table", naming.getName());
  }

  @Test
  void testTeradataNaming() {
    Naming.Teradata naming =
        new Naming.Teradata("teradata-host", "1025", "my_db", "my_table");
    assertEquals("teradata://teradata-host:1025", naming.getNamespace());
    assertEquals("my_db.my_table", naming.getName());
  }

  @Test
  void testRedshiftNaming() {
    Naming.Redshift naming =
        new Naming.Redshift("cluster1", "us-east-1", "5439", "mydb", "my_schema", "my_table");
    assertEquals("redshift://cluster1.us-east-1:5439", naming.getNamespace());
    assertEquals("mydb.my_schema.my_table", naming.getName());
  }

  @Test
  void testSnowflakeNaming() {
    Naming.Snowflake naming =
        new Naming.Snowflake("org-name", "acct-name", "db", "schema", "table");
    assertEquals("snowflake://org-name-acct-name", naming.getNamespace());
    assertEquals("db.schema.table", naming.getName());
  }

  @Test
  void testTrinoNaming() {
    Naming.Trino naming =
        new Naming.Trino("localhost", "8080", "catalog", "schema", "table");
    assertEquals("trino://localhost:8080", naming.getNamespace());
    assertEquals("catalog.schema.table", naming.getName());
  }

  @Test
  void testAbfssNaming() {
    Naming.ABFSS naming = new Naming.ABFSS("container", "service", "path/to/data");
    assertEquals("abfss://container@service.dfs.core.windows.net", naming.getNamespace());
    assertEquals("path/to/data", naming.getName());
  }

  @Test
  void testDbfsNaming() {
    Naming.DBFS naming = new Naming.DBFS("workspace", "/mnt/data");
    assertEquals("dbfs://workspace", naming.getNamespace());
    assertEquals("/mnt/data", naming.getName());
  }

  @Test
  void testGcsNaming() {
    Naming.GCS naming = new Naming.GCS("my-bucket", "path/to/file");
    assertEquals("gs://my-bucket", naming.getNamespace());
    assertEquals("path/to/file", naming.getName());
  }

  @Test
  void testHdfsNaming() {
    Naming.HDFS naming = new Naming.HDFS("namenode", "9000", "/data");
    assertEquals("hdfs://namenode:9000", naming.getNamespace());
    assertEquals("/data", naming.getName());
  }

  @Test
  void testKafkaNaming() {
    Naming.Kafka naming = new Naming.Kafka("kafka-host", "9092", "my-topic");
    assertEquals("kafka://kafka-host:9092", naming.getNamespace());
    assertEquals("my-topic", naming.getName());
  }

  @Test
  void testLocalFsNaming() {
    Naming.LocalFileSystem naming = new Naming.LocalFileSystem("/local/path");
    assertEquals("file", naming.getNamespace());
    assertEquals("/local/path", naming.getName());
  }

  @Test
  void testRemoteFsNaming() {
    Naming.RemoteFileSystem naming =
        new Naming.RemoteFileSystem("remote-host", "/remote/path");
    assertEquals("file://remote-host", naming.getNamespace());
    assertEquals("/remote/path", naming.getName());
  }

  @Test
  void testS3Naming() {
    Naming.S3 naming = new Naming.S3("my-bucket", "my/object/key");
    assertEquals("s3://my-bucket", naming.getNamespace());
    assertEquals("my/object/key", naming.getName());
  }

  @Test
  void testWasbsNaming() {
    Naming.WASBS naming = new Naming.WASBS("container", "service", "object-key");
    assertEquals("wasbs://container@service.dfs.core.windows.net", naming.getNamespace());
    assertEquals("object-key", naming.getName());
  }
}
