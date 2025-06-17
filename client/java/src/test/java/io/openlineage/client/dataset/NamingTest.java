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
        Naming.BigQuery.builder()
            .projectId("my-project")
            .datasetName("my_dataset")
            .tableName("my_table")
            .build();
    assertEquals("bigquery", naming.getNamespace());
    assertEquals("my-project.my_dataset.my_table", naming.getName());
  }

  @Test
  void testGlueNaming() {
    Naming.Glue naming =
        Naming.Glue.builder()
            .region("us-east-1")
            .accountId("123456789012")
            .databaseName("my_db")
            .tableName("my_table")
            .build();
    assertEquals("arn:aws:glue:us-east-1:123456789012", naming.getNamespace());
    assertEquals("table/my_db/my_table", naming.getName());
  }

  @Test
  void testPubSubNaming() {
    Naming.PubSubNaming naming =
        Naming.PubSubNaming.builder()
            .resourceType(Naming.PubSubNaming.PubSubResourceType.TOPIC)
            .projectId("my-project")
            .resourceId("my-topic")
            .build();
    assertEquals("pubsub", naming.getNamespace());
    assertEquals("topic:my-project:my-topic", naming.getName());
  }

  @Test
  void testAzureCosmosDbNaming() {
    Naming.AzureCosmosDB naming =
        Naming.AzureCosmosDB.builder()
            .host("my-host")
            .database("my-db")
            .table("my-collection")
            .build();
    assertEquals("azurecosmos://my-host/dbs/my-db", naming.getNamespace());
    assertEquals("colls/my-collection", naming.getName());
  }

  @Test
  void testAzureDataExplorerNaming() {
    Naming.AzureDataExplorer naming =
        Naming.AzureDataExplorer.builder()
            .host("my-host")
            .database("my-db")
            .table("my-table")
            .build();
    assertEquals("azurekusto://my-host.kusto.windows.net", naming.getNamespace());
    assertEquals("my-db/my-table", naming.getName());
  }

  @Test
  void testAzureSynapseNaming() {
    Naming.AzureSynapse naming =
        Naming.AzureSynapse.builder()
            .host("my-host")
            .port("1433")
            .schema("dbo")
            .table("my-table")
            .build();
    assertEquals("sqlserver://my-host:1433", naming.getNamespace());
    assertEquals("dbo.my-table", naming.getName());
  }

  @Test
  void testCassandraNaming() {
    Naming.Cassandra naming =
        Naming.Cassandra.builder()
            .host("localhost")
            .port("9042")
            .keyspace("my_keyspace")
            .table("my_table")
            .build();
    assertEquals("cassandra://localhost:9042", naming.getNamespace());
    assertEquals("my_keyspace.my_table", naming.getName());
  }

  @Test
  void testMySqlNaming() {
    Naming.MySQL naming =
        Naming.MySQL.builder()
            .host("localhost")
            .port("3306")
            .database("my_db")
            .table("my_table")
            .build();
    assertEquals("mysql://localhost:3306", naming.getNamespace());
    assertEquals("my_db.my_table", naming.getName());
  }

  @Test
  void testCrateDbNaming() {
    Naming.CrateDB naming =
        Naming.CrateDB.builder()
            .host("localhost")
            .port("5432")
            .database("crate_db")
            .schema("doc")
            .table("my_table")
            .build();
    assertEquals("crate://localhost:5432", naming.getNamespace());
    assertEquals("crate_db.doc.my_table", naming.getName());
  }

  @Test
  void testDb2Naming() {
    Naming.DB2 naming =
        Naming.DB2
            .builder()
            .host("localhost")
            .port("50000")
            .database("db2inst1")
            .schema("myschema")
            .table("my_table")
            .build();
    assertEquals("db2://localhost:50000", naming.getNamespace());
    assertEquals("db2inst1.myschema.my_table", naming.getName());
  }

  @Test
  void testOceanBaseNaming() {
    Naming.OceanBase naming =
        Naming.OceanBase.builder()
            .host("localhost")
            .port("2881")
            .database("my_db")
            .table("my_table")
            .build();
    assertEquals("oceanbase://localhost:2881", naming.getNamespace());
    assertEquals("my_db.my_table", naming.getName());
  }

  @Test
  void testOracleNaming() {
    Naming.Oracle naming =
        Naming.Oracle.builder()
            .host("localhost")
            .port("1521")
            .serviceName("ORCLCDB")
            .schema("myschema")
            .table("my_table")
            .build();
    assertEquals("oracle://localhost:1521", naming.getNamespace());
    assertEquals("ORCLCDB.myschema.my_table", naming.getName());
  }

  @Test
  void testPostgresNaming() {
    Naming.Postgres naming =
        Naming.Postgres.builder()
            .host("localhost")
            .port("5432")
            .database("mydb")
            .schema("my_schema")
            .table("my_table")
            .build();
    assertEquals("postgres://localhost:5432", naming.getNamespace());
    assertEquals("mydb.my_schema.my_table", naming.getName());
  }

  @Test
  void testTeradataNaming() {
    Naming.Teradata naming =
        Naming.Teradata.builder()
            .host("teradata-host")
            .port("1025")
            .database("my_db")
            .table("my_table")
            .build();
    assertEquals("teradata://teradata-host:1025", naming.getNamespace());
    assertEquals("my_db.my_table", naming.getName());
  }

  @Test
  void testRedshiftNaming() {
    Naming.Redshift naming =
        Naming.Redshift.builder()
            .clusterIdentifier("cluster1")
            .region("us-east-1")
            .port("5439")
            .database("mydb")
            .schema("my_schema")
            .table("my_table")
            .build();
    assertEquals("redshift://cluster1.us-east-1:5439", naming.getNamespace());
    assertEquals("mydb.my_schema.my_table", naming.getName());
  }

  @Test
  void testSnowflakeNaming() {
    Naming.Snowflake naming =
        Naming.Snowflake.builder()
            .organizationName("org-name")
            .accountName("acct-name")
            .database("db")
            .schema("schema")
            .table("table")
            .build();
    assertEquals("snowflake://org-name-acct-name", naming.getNamespace());
    assertEquals("db.schema.table", naming.getName());
  }

  @Test
  void testTrinoNaming() {
    Naming.Trino naming =
        Naming.Trino.builder()
            .host("localhost")
            .port("8080")
            .catalog("catalog")
            .schema("schema")
            .table("table")
            .build();
    assertEquals("trino://localhost:8080", naming.getNamespace());
    assertEquals("catalog.schema.table", naming.getName());
  }

  @Test
  void testAbfssNaming() {
    Naming.ABFSS naming =
        Naming.ABFSS
            .builder()
            .container("container")
            .service("service")
            .path("path/to/data")
            .build();
    assertEquals("abfss://container@service.dfs.core.windows.net", naming.getNamespace());
    assertEquals("path/to/data", naming.getName());
  }

  @Test
  void testDbfsNaming() {
    Naming.DBFS naming = Naming.DBFS.builder().workspace("workspace").path("/mnt/data").build();
    assertEquals("dbfs://workspace", naming.getNamespace());
    assertEquals("/mnt/data", naming.getName());
  }

  @Test
  void testGcsNaming() {
    Naming.GCS naming =
        Naming.GCS.builder().bucketName("my-bucket").objectKey("path/to/file").build();
    assertEquals("gs://my-bucket", naming.getNamespace());
    assertEquals("path/to/file", naming.getName());
  }

  @Test
  void testHdfsNaming() {
    Naming.HDFS naming =
        Naming.HDFS.builder().namenodeHost("namenode").namenodePort("9000").path("/data").build();
    assertEquals("hdfs://namenode:9000", naming.getNamespace());
    assertEquals("/data", naming.getName());
  }

  @Test
  void testKafkaNaming() {
    Naming.Kafka naming =
        Naming.Kafka.builder()
            .bootstrapServerHost("kafka-host")
            .port("9092")
            .topic("my-topic")
            .build();
    assertEquals("kafka://kafka-host:9092", naming.getNamespace());
    assertEquals("my-topic", naming.getName());
  }

  @Test
  void testLocalFsNaming() {
    Naming.LocalFileSystem naming = Naming.LocalFileSystem.builder().path("/local/path").build();
    assertEquals("file", naming.getNamespace());
    assertEquals("/local/path", naming.getName());
  }

  @Test
  void testRemoteFsNaming() {
    Naming.RemoteFileSystem naming =
        Naming.RemoteFileSystem.builder().host("remote-host").path("/remote/path").build();
    assertEquals("file://remote-host", naming.getNamespace());
    assertEquals("/remote/path", naming.getName());
  }

  @Test
  void testS3Naming() {
    Naming.S3 naming =
        Naming.S3.builder().bucketName("my-bucket").objectKey("my/object/key").build();
    assertEquals("s3://my-bucket", naming.getNamespace());
    assertEquals("my/object/key", naming.getName());
  }

  @Test
  void testWasbsNaming() {
    Naming.WASBS naming =
        Naming.WASBS
            .builder()
            .containerName("container")
            .serviceName("service")
            .objectKey("object-key")
            .build();
    assertEquals("wasbs://container@service.dfs.core.windows.net", naming.getNamespace());
    assertEquals("object-key", naming.getName());
  }
}
