/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.dataset;

import lombok.Builder;

/**
 * Utility class containing dataset naming helpers based on OpenLineage's dataset naming
 * specification.
 *
 * <p>Each inner class represents naming logic for a specific platform (e.g., BigQuery, AWS Glue),
 * and provides methods to construct valid dataset names and namespaces.
 */
public final class Naming {

  private Naming() {}

  /** Naming implementation for Athena. */
  @Builder
  public static class Athena implements DatasetNaming {
    private final String regionName;
    private final String catalog;
    private final String database;
    private final String table;

    public Athena(String regionName, String catalog, String database, String table) {
      this.regionName = checkArgumentNotEmpty(regionName, "regionName");
      this.catalog = checkArgumentNotEmpty(catalog, "catalog");
      this.database = checkArgumentNotEmpty(database, "database");
      this.table = checkArgumentNotEmpty(table, "table");
    }

    @Override
    public String getNamespace() {
      return "awsathena://athena." + regionName + ".amazonaws.com";
    }

    @Override
    public String getName() {
      return catalog + "." + database + "." + table;
    }
  }

  /** Naming implementation for AWS Glue. */
  @Builder
  public static class AWSGlue implements DatasetNaming {
    private final String region;
    private final String accountId;
    private final String databaseName;
    private final String tableName;

    public AWSGlue(String region, String accountId, String databaseName, String tableName) {
      this.region = checkArgumentNotEmpty(region, "region");
      this.accountId = checkArgumentNotEmpty(accountId, "accountId");
      this.databaseName = checkArgumentNotEmpty(databaseName, "databaseName");
      this.tableName = checkArgumentNotEmpty(tableName, "tableName");
    }

    @Override
    public String getNamespace() {
      return "arn:aws:glue:" + region + ":" + accountId;
    }

    @Override
    public String getName() {
      return "table/" + databaseName + "/" + tableName;
    }
  }

  /** Naming implementation for Azure Cosmos DB. */
  @Builder
  public static class AzureCosmosDB implements DatasetNaming {
    private final String host;
    private final String database;
    private final String table;

    public AzureCosmosDB(String host, String database, String table) {
      this.host = checkArgumentNotEmpty(host, "host");
      this.database = checkArgumentNotEmpty(database, "database");
      this.table = checkArgumentNotEmpty(table, "table");
    }

    @Override
    public String getNamespace() {
      return "azurecosmos://" + host + "/dbs/" + database;
    }

    @Override
    public String getName() {
      return "colls/" + table;
    }
  }

  /** Naming implementation for Azure Data Explorer. */
  @Builder
  public static class AzureDataExplorer implements DatasetNaming {
    private final String host;
    private final String database;
    private final String table;

    public AzureDataExplorer(String host, String database, String table) {
      this.host = checkArgumentNotEmpty(host, "host");
      this.database = checkArgumentNotEmpty(database, "database");
      this.table = checkArgumentNotEmpty(table, "table");
    }

    @Override
    public String getNamespace() {
      return "azurekusto://" + host + ".kusto.windows.net";
    }

    @Override
    public String getName() {
      return database + "/" + table;
    }
  }

  /** Naming implementation for AzureSynapse. */
  @Builder
  public static class AzureSynapse implements DatasetNaming {
    private final String host;
    private final String port;
    private final String schema;
    private final String table;

    public AzureSynapse(String host, String port, String schema, String table) {
      this.host = checkArgumentNotEmpty(host, "host");
      this.port = checkArgumentNotEmpty(port, "port");
      this.schema = checkArgumentNotEmpty(schema, "schema");
      this.table = checkArgumentNotEmpty(table, "table");
    }

    @Override
    public String getNamespace() {
      return "sqlserver://" + host + ":" + port;
    }

    @Override
    public String getName() {
      return schema + "." + table;
    }
  }

  /**
   * Naming implementation for BigQuery.
   *
   * <p>Namespace is always {@code "bigquery"}.
   */
  @Builder
  public static class BigQuery implements DatasetNaming {
    private final String projectId;
    private final String datasetName;
    private final String tableName;

    /**
     * Constructs a BigQuery naming instance.
     *
     * @param projectId the GCP project ID (must be non-empty)
     * @param datasetName the BigQuery dataset name (must be non-empty)
     * @param tableName the BigQuery table name (must be non-empty)
     */
    public BigQuery(String projectId, String datasetName, String tableName) {
      this.projectId = checkArgumentNotEmpty(projectId, "projectId");
      this.datasetName = checkArgumentNotEmpty(datasetName, "datasetName");
      this.tableName = checkArgumentNotEmpty(tableName, "tableName");
    }

    @Override
    public String getNamespace() {
      return "bigquery";
    }

    @Override
    public String getName() {
      return String.format("%s.%s.%s", projectId, datasetName, tableName);
    }
  }

  /** Naming implementation for Cassandra. */
  @Builder
  public static class Cassandra implements DatasetNaming {
    private final String host;
    private final String port;
    private final String keyspace;
    private final String table;

    public Cassandra(String host, String port, String keyspace, String table) {
      this.host = checkArgumentNotEmpty(host, "host");
      this.port = checkArgumentNotEmpty(port, "port");
      this.keyspace = checkArgumentNotEmpty(keyspace, "keyspace");
      this.table = checkArgumentNotEmpty(table, "table");
    }

    @Override
    public String getNamespace() {
      return "cassandra://" + host + ":" + port;
    }

    @Override
    public String getName() {
      return keyspace + "." + table;
    }
  }

  /**
   * Naming implementation for AWS Glue.
   *
   * <p>Namespace follows: {@code arn:aws:glue:{region}:{accountId}}. Name follows: {@code
   * table/{databaseName}/{tableName}}.
   */
  @Builder
  public static class Glue implements DatasetNaming {
    private final String region;
    private final String accountId;
    private final String databaseName;
    private final String tableName;

    /**
     * Constructs a Glue naming instance.
     *
     * @param region AWS region (must be non-empty)
     * @param accountId AWS account ID (must be non-empty)
     * @param databaseName Glue database name (must be non-empty)
     * @param tableName Glue table name (must be non-empty)
     */
    public Glue(String region, String accountId, String databaseName, String tableName) {
      this.region = checkArgumentNotEmpty(region, "region");
      this.accountId = checkArgumentNotEmpty(accountId, "accountId");
      this.databaseName = checkArgumentNotEmpty(databaseName, "databaseName");
      this.tableName = checkArgumentNotEmpty(tableName, "tableName");
    }

    @Override
    public String getNamespace() {
      return String.format("arn:aws:glue:%s:%s", region, accountId);
    }

    @Override
    public String getName() {
      return String.format("table/%s/%s", databaseName, tableName);
    }
  }

  /** Naming implementation for MySQL. */
  @Builder
  public static class MySQL implements DatasetNaming {
    private final String host;
    private final String port;
    private final String database;
    private final String table;

    public MySQL(String host, String port, String database, String table) {
      this.host = checkArgumentNotEmpty(host, "host");
      this.port = checkArgumentNotEmpty(port, "port");
      this.database = checkArgumentNotEmpty(database, "database");
      this.table = checkArgumentNotEmpty(table, "table");
    }

    @Override
    public String getNamespace() {
      return "mysql://" + host + ":" + port;
    }

    @Override
    public String getName() {
      return database + "." + table;
    }
  }

  /** Naming implementation for CrateDB. */
  @Builder
  public static class CrateDB implements DatasetNaming {
    private final String host;
    private final String port;
    private final String database;
    private final String schema;
    private final String table;

    public CrateDB(String host, String port, String database, String schema, String table) {
      this.host = checkArgumentNotEmpty(host, "host");
      this.port = checkArgumentNotEmpty(port, "port");
      this.database = checkArgumentNotEmpty(database, "database");
      this.schema = checkArgumentNotEmpty(schema, "schema");
      this.table = checkArgumentNotEmpty(table, "table");
    }

    @Override
    public String getNamespace() {
      return "crate://" + host + ":" + port;
    }

    @Override
    public String getName() {
      return database + "." + schema + "." + table;
    }
  }

  /** Naming implementation for DB2. */
  @Builder
  public static class DB2 implements DatasetNaming {
    private final String host;
    private final String port;
    private final String database;
    private final String schema;
    private final String table;

    public DB2(String host, String port, String database, String schema, String table) {
      this.host = checkArgumentNotEmpty(host, "host");
      this.port = checkArgumentNotEmpty(port, "port");
      this.database = checkArgumentNotEmpty(database, "database");
      this.schema = checkArgumentNotEmpty(schema, "schema");
      this.table = checkArgumentNotEmpty(table, "table");
    }

    @Override
    public String getNamespace() {
      return "db2://" + host + ":" + port;
    }

    @Override
    public String getName() {
      return database + "." + schema + "." + table;
    }
  }

  /** Naming implementation for OceanBase. */
  @Builder
  public static class OceanBase implements DatasetNaming {
    private final String host;
    private final String port;
    private final String database;
    private final String table;

    public OceanBase(String host, String port, String database, String table) {
      this.host = checkArgumentNotEmpty(host, "host");
      this.port = checkArgumentNotEmpty(port, "port");
      this.database = checkArgumentNotEmpty(database, "database");
      this.table = checkArgumentNotEmpty(table, "table");
    }

    @Override
    public String getNamespace() {
      return "oceanbase://" + host + ":" + port;
    }

    @Override
    public String getName() {
      return database + "." + table;
    }
  }

  /** Naming implementation for Oracle. */
  @Builder
  public static class Oracle implements DatasetNaming {
    private final String host;
    private final String port;
    private final String serviceName;
    private final String schema;
    private final String table;

    public Oracle(String host, String port, String serviceName, String schema, String table) {
      this.host = checkArgumentNotEmpty(host, "host");
      this.port = checkArgumentNotEmpty(port, "port");
      this.serviceName = checkArgumentNotEmpty(serviceName, "serviceName");
      this.schema = checkArgumentNotEmpty(schema, "schema");
      this.table = checkArgumentNotEmpty(table, "table");
    }

    @Override
    public String getNamespace() {
      return "oracle://" + host + ":" + port;
    }

    @Override
    public String getName() {
      return serviceName + "." + schema + "." + table;
    }
  }

  /** Naming implementation for Postgres. */
  @Builder
  public static class Postgres implements DatasetNaming {
    private final String host;
    private final String port;
    private final String database;
    private final String schema;
    private final String table;

    public Postgres(String host, String port, String database, String schema, String table) {
      this.host = checkArgumentNotEmpty(host, "host");
      this.port = checkArgumentNotEmpty(port, "port");
      this.database = checkArgumentNotEmpty(database, "database");
      this.schema = checkArgumentNotEmpty(schema, "schema");
      this.table = checkArgumentNotEmpty(table, "table");
    }

    @Override
    public String getNamespace() {
      return "postgres://" + host + ":" + port;
    }

    @Override
    public String getName() {
      return database + "." + schema + "." + table;
    }
  }

  /** Naming implementation for Teradata. */
  @Builder
  public static class Teradata implements DatasetNaming {
    private final String host;
    private final String port;
    private final String database;
    private final String table;

    public Teradata(String host, String port, String database, String table) {
      this.host = checkArgumentNotEmpty(host, "host");
      this.port = checkArgumentNotEmpty(port, "port");
      this.database = checkArgumentNotEmpty(database, "database");
      this.table = checkArgumentNotEmpty(table, "table");
    }

    @Override
    public String getNamespace() {
      return "teradata://" + host + ":" + port;
    }

    @Override
    public String getName() {
      return database + "." + table;
    }
  }

  /** Naming implementation for Redshift. */
  @Builder
  public static class Redshift implements DatasetNaming {
    private final String clusterIdentifier;
    private final String region;
    private final String port;
    private final String database;
    private final String schema;
    private final String table;

    public Redshift(
        String clusterIdentifier,
        String region,
        String port,
        String database,
        String schema,
        String table) {
      this.clusterIdentifier = checkArgumentNotEmpty(clusterIdentifier, "clusterIdentifier");
      this.region = checkArgumentNotEmpty(region, "region");
      this.port = checkArgumentNotEmpty(port, "port");
      this.database = checkArgumentNotEmpty(database, "database");
      this.schema = checkArgumentNotEmpty(schema, "schema");
      this.table = checkArgumentNotEmpty(table, "table");
    }

    @Override
    public String getNamespace() {
      return "redshift://" + clusterIdentifier + "." + region + ":" + port;
    }

    @Override
    public String getName() {
      return database + "." + schema + "." + table;
    }
  }

  /** Naming implementation for Snowflake. */
  @Builder
  public static class Snowflake implements DatasetNaming {
    private final String organizationName;
    private final String accountName;
    private final String database;
    private final String schema;
    private final String table;

    public Snowflake(
        String organization_name,
        String accountName,
        String database,
        String schema,
        String table) {
      this.organizationName = checkArgumentNotEmpty(organization_name, "organizationName");
      this.accountName = checkArgumentNotEmpty(accountName, "accountName");
      this.database = checkArgumentNotEmpty(database, "database");
      this.schema = checkArgumentNotEmpty(schema, "schema");
      this.table = checkArgumentNotEmpty(table, "table");
    }

    @Override
    public String getNamespace() {
      return "snowflake://" + organizationName + "-" + accountName;
    }

    @Override
    public String getName() {
      return database + "." + schema + "." + table;
    }
  }

  /** Naming implementation for Trino. */
  @Builder
  public static class Trino implements DatasetNaming {
    private final String host;
    private final String port;
    private final String catalog;
    private final String schema;
    private final String table;

    public Trino(String host, String port, String catalog, String schema, String table) {
      this.host = checkArgumentNotEmpty(host, "host");
      this.port = checkArgumentNotEmpty(port, "port");
      this.catalog = checkArgumentNotEmpty(catalog, "catalog");
      this.schema = checkArgumentNotEmpty(schema, "schema");
      this.table = checkArgumentNotEmpty(table, "table");
    }

    @Override
    public String getNamespace() {
      return "trino://" + host + ":" + port;
    }

    @Override
    public String getName() {
      return catalog + "." + schema + "." + table;
    }
  }

  /** Naming implementation for ABFSS. */
  @Builder
  public static class ABFSS implements DatasetNaming {
    private final String container;
    private final String service;
    private final String path;

    public ABFSS(String container, String service, String path) {
      this.container = checkArgumentNotEmpty(container, "container");
      this.service = checkArgumentNotEmpty(service, "service");
      this.path = checkArgumentNotEmpty(path, "path");
    }

    @Override
    public String getNamespace() {
      return "abfss://" + container + "@" + service + ".dfs.core.windows.net";
    }

    @Override
    public String getName() {
      return path;
    }
  }

  /** Naming implementation for DBFS. */
  @Builder
  public static class DBFS implements DatasetNaming {
    private final String workspace;
    private final String path;

    public DBFS(String workspace, String path) {
      this.workspace = checkArgumentNotEmpty(workspace, "workspace");
      this.path = checkArgumentNotEmpty(path, "path");
    }

    @Override
    public String getNamespace() {
      return "dbfs://" + workspace;
    }

    @Override
    public String getName() {
      return path;
    }
  }

  /** Naming implementation for GCS. */
  @Builder
  public static class GCS implements DatasetNaming {
    private final String bucketName;
    private final String objectKey;

    public GCS(String bucketName, String objectKey) {
      this.bucketName = checkArgumentNotEmpty(bucketName, "bucketName");
      this.objectKey = checkArgumentNotEmpty(objectKey, "objectKey");
    }

    @Override
    public String getNamespace() {
      return "gs://" + bucketName;
    }

    @Override
    public String getName() {
      return objectKey;
    }
  }

  /** Naming implementation for HDFS. */
  @Builder
  public static class HDFS implements DatasetNaming {
    private final String namenodeHost;
    private final String namenodePort;
    private final String path;

    public HDFS(String namenodeHost, String namenodePort, String path) {
      this.namenodeHost = checkArgumentNotEmpty(namenodeHost, "namenodeHost");
      this.namenodePort = checkArgumentNotEmpty(namenodePort, "namenodePort");
      this.path = checkArgumentNotEmpty(path, "path");
    }

    @Override
    public String getNamespace() {
      return "hdfs://" + namenodeHost + ":" + namenodePort;
    }

    @Override
    public String getName() {
      return path;
    }
  }

  /** Naming implementation for Kafka. */
  @Builder
  public static class Kafka implements DatasetNaming {
    private final String bootstrapServerHost;
    private final String port;
    private final String topic;

    public Kafka(String bootstrapServerHost, String port, String topic) {
      this.bootstrapServerHost = checkArgumentNotEmpty(bootstrapServerHost, "bootstrapServerHost");
      this.port = checkArgumentNotEmpty(port, "port");
      this.topic = checkArgumentNotEmpty(topic, "topic");
    }

    @Override
    public String getNamespace() {
      return "kafka://" + bootstrapServerHost + ":" + port;
    }

    @Override
    public String getName() {
      return topic;
    }
  }

  /** Naming implementation for Local File System. */
  @Builder
  public static class LocalFileSystem implements DatasetNaming {
    private final String path;

    public LocalFileSystem(String path) {
      this.path = checkArgumentNotEmpty(path, "path");
    }

    @Override
    public String getNamespace() {
      return "file";
    }

    @Override
    public String getName() {
      return path;
    }
  }

  /** Naming implementation for Remote File System. */
  @Builder
  public static class RemoteFileSystem implements DatasetNaming {
    private final String host;
    private final String path;

    public RemoteFileSystem(String host, String path) {
      this.host = checkArgumentNotEmpty(host, "host");
      this.path = checkArgumentNotEmpty(path, "path");
    }

    @Override
    public String getNamespace() {
      return "file://" + host;
    }

    @Override
    public String getName() {
      return path;
    }
  }

  /** Naming implementation for S3. */
  @Builder
  public static class S3 implements DatasetNaming {
    private final String bucketName;
    private final String objectKey;

    public S3(String bucketName, String objectKey) {
      this.bucketName = checkArgumentNotEmpty(bucketName, "bucketName");
      this.objectKey = checkArgumentNotEmpty(objectKey, "objectKey");
    }

    @Override
    public String getNamespace() {
      return "s3://" + bucketName;
    }

    @Override
    public String getName() {
      return objectKey;
    }
  }

  /** Naming implementation for WASBS. */
  @Builder
  public static class WASBS implements DatasetNaming {
    private final String containerName;
    private final String serviceName;
    private final String objectKey;

    public WASBS(String containerName, String serviceName, String objectKey) {
      this.containerName = checkArgumentNotEmpty(containerName, "containerName");
      this.serviceName = checkArgumentNotEmpty(serviceName, "serviceName");
      this.objectKey = checkArgumentNotEmpty(objectKey, "objectKey");
    }

    @Override
    public String getNamespace() {
      return "wasbs://" + containerName + "@" + serviceName + ".dfs.core.windows.net";
    }

    @Override
    public String getName() {
      return objectKey;
    }
  }

  /**
   * Naming implementation for Google Pub/Sub.
   *
   * <p>Namespace is fixed as {@code "pubsub"}. Name format depends on the resource type:
   *
   * <ul>
   *   <li>{@code topic:{projectId}:{topicId}}
   *   <li>{@code subscription:{projectId}:{subscriptionId}}
   * </ul>
   */
  @Builder
  public static class PubSubNaming implements Naming.DatasetNaming {

    /** Enum representing supported Pub/Sub resource types. */
    public enum PubSubResourceType {
      TOPIC,
      SUBSCRIPTION
    }

    private final PubSubResourceType resourceType;
    private final String projectId;
    private final String resourceId;

    /**
     * Constructs a Pub/Sub naming instance.
     *
     * @param resourceType either {@code TOPIC} or {@code SUBSCRIPTION}
     * @param projectId GCP project ID (non-empty)
     * @param resourceId Topic ID or Subscription ID (non-empty)
     */
    public PubSubNaming(PubSubResourceType resourceType, String projectId, String resourceId) {
      this.resourceType = checkNotNull(resourceType, "resourceType");
      this.projectId = checkArgumentNotEmpty(projectId, "projectId");
      this.resourceId = checkArgumentNotEmpty(resourceId, "resourceId");
    }

    @Override
    public String getNamespace() {
      return "pubsub";
    }

    @Override
    public String getName() {
      String prefix = resourceType == PubSubResourceType.TOPIC ? "topic" : "subscription";
      return String.format("%s:%s:%s", prefix, projectId, resourceId);
    }
  }

  /** Interface representing dataset naming logic for a specific platform. */
  public interface DatasetNaming {

    /**
     * Returns the dataset namespace (e.g., URI or ARN identifying the data source).
     *
     * @return the namespace string
     */
    String getNamespace();

    /**
     * Returns the dataset name specific to the platform format.
     *
     * @return the dataset name string
     */
    String getName();
  }

  /**
   * Helper method to check if a string is not null and not empty.
   *
   * @param value the string to check
   * @param fieldName the name of the field for error messages
   * @return the string if valid
   * @throws IllegalArgumentException if null or empty
   */
  private static String checkArgumentNotEmpty(String value, String fieldName) {
    if (value == null) {
      throw new IllegalArgumentException(fieldName + " cannot be null");
    }
    if (value.trim().isEmpty()) {
      throw new IllegalArgumentException(fieldName + " cannot be empty");
    }
    return value;
  }

  private static <T> T checkNotNull(T value, String fieldName) {
    if (value == null) {
      throw new IllegalArgumentException(fieldName + " cannot be null");
    }
    return value;
  }
}
