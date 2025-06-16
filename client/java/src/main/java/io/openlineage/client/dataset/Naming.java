/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.dataset;

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
  public static class AthenaNaming implements DatasetNaming {
    private final String regionName;
    private final String catalog;
    private final String database;
    private final String table;

    public AthenaNaming(String regionName, String catalog, String database, String table) {
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
  public static class AWSGlueNaming implements DatasetNaming {
    private final String region;
    private final String accountId;
    private final String databaseName;
    private final String tableName;

    public AWSGlueNaming(String region, String accountId, String databaseName, String tableName) {
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
  public static class AzureCosmosDBNaming implements DatasetNaming {
    private final String host;
    private final String database;
    private final String table;

    public AzureCosmosDBNaming(String host, String database, String table) {
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
  public static class AzureDataExplorerNaming implements DatasetNaming {
    private final String host;
    private final String database;
    private final String table;

    public AzureDataExplorerNaming(String host, String database, String table) {
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

  public static class AzureSynapseNaming implements DatasetNaming {
    private final String host;
    private final String port;
    private final String schema;
    private final String table;

    public AzureSynapseNaming(String host, String port, String schema, String table) {
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
  public static class BigQueryNaming implements DatasetNaming {
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
    public BigQueryNaming(String projectId, String datasetName, String tableName) {
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
  public static class CassandraNaming implements DatasetNaming {
    private final String host;
    private final String port;
    private final String keyspace;
    private final String table;

    public CassandraNaming(String host, String port, String keyspace, String table) {
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
  public static class GlueNaming implements DatasetNaming {
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
    public GlueNaming(String region, String accountId, String databaseName, String tableName) {
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
  public static class MySQLNaming implements DatasetNaming {
    private final String host;
    private final String port;
    private final String database;
    private final String table;

    public MySQLNaming(String host, String port, String database, String table) {
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
  public static class CrateDBNaming implements DatasetNaming {
    private final String host;
    private final String port;
    private final String database;
    private final String schema;
    private final String table;

    public CrateDBNaming(String host, String port, String database, String schema, String table) {
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
  public static class DB2Naming implements DatasetNaming {
    private final String host;
    private final String port;
    private final String database;
    private final String schema;
    private final String table;

    public DB2Naming(String host, String port, String database, String schema, String table) {
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
  public static class OceanBaseNaming implements DatasetNaming {
    private final String host;
    private final String port;
    private final String database;
    private final String table;

    public OceanBaseNaming(String host, String port, String database, String table) {
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
  public static class OracleNaming implements DatasetNaming {
    private final String host;
    private final String port;
    private final String serviceName;
    private final String schema;
    private final String table;

    public OracleNaming(String host, String port, String serviceName, String schema, String table) {
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
  public static class PostgresNaming implements DatasetNaming {
    private final String host;
    private final String port;
    private final String database;
    private final String schema;
    private final String table;

    public PostgresNaming(String host, String port, String database, String schema, String table) {
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
  public static class TeradataNaming implements DatasetNaming {
    private final String host;
    private final String port;
    private final String database;
    private final String table;

    public TeradataNaming(String host, String port, String database, String table) {
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
  public static class RedshiftNaming implements DatasetNaming {
    private final String cluster_identifier;
    private final String region_name;
    private final String port;
    private final String database;
    private final String schema;
    private final String table;

    public RedshiftNaming(
        String cluster_identifier,
        String region_name,
        String port,
        String database,
        String schema,
        String table) {
      this.cluster_identifier = checkArgumentNotEmpty(cluster_identifier, "cluster_identifier");
      this.region_name = checkArgumentNotEmpty(region_name, "region_name");
      this.port = checkArgumentNotEmpty(port, "port");
      this.database = checkArgumentNotEmpty(database, "database");
      this.schema = checkArgumentNotEmpty(schema, "schema");
      this.table = checkArgumentNotEmpty(table, "table");
    }

    @Override
    public String getNamespace() {
      return "redshift://" + cluster_identifier + "." + region_name + ":" + port;
    }

    @Override
    public String getName() {
      return database + "." + schema + "." + table;
    }
  }

  /** Naming implementation for Snowflake. */
  public static class SnowflakeNaming implements DatasetNaming {
    private final String organization_name;
    private final String account_name;
    private final String database;
    private final String schema;
    private final String table;

    public SnowflakeNaming(
        String organization_name,
        String account_name,
        String database,
        String schema,
        String table) {
      this.organization_name = checkArgumentNotEmpty(organization_name, "organization_name");
      this.account_name = checkArgumentNotEmpty(account_name, "account_name");
      this.database = checkArgumentNotEmpty(database, "database");
      this.schema = checkArgumentNotEmpty(schema, "schema");
      this.table = checkArgumentNotEmpty(table, "table");
    }

    @Override
    public String getNamespace() {
      return "snowflake://" + organization_name + "-" + account_name;
    }

    @Override
    public String getName() {
      return database + "." + schema + "." + table;
    }
  }

  /** Naming implementation for Trino. */
  public static class TrinoNaming implements DatasetNaming {
    private final String host;
    private final String port;
    private final String catalog;
    private final String schema;
    private final String table;

    public TrinoNaming(String host, String port, String catalog, String schema, String table) {
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
  public static class ABFSSNaming implements DatasetNaming {
    private final String containerName;
    private final String serviceName;
    private final String path;

    public ABFSSNaming(String containerName, String serviceName, String path) {
      this.containerName = checkArgumentNotEmpty(containerName, "containeName");
      this.serviceName = checkArgumentNotEmpty(serviceName, "servicName");
      this.path = checkArgumentNotEmpty(path, "path");
    }

    @Override
    public String getNamespace() {
      return "abfss://" + containerName + "@" + serviceName + ".dfs.core.windows.net";
    }

    @Override
    public String getName() {
      return path;
    }
  }

  /** Naming implementation for DBFS. */
  public static class DBFSNaming implements DatasetNaming {
    private final String workspaceName;
    private final String path;

    public DBFSNaming(String workspaceName, String path) {
      this.workspaceName = checkArgumentNotEmpty(workspaceName, "workspaceName");
      this.path = checkArgumentNotEmpty(path, "path");
    }

    @Override
    public String getNamespace() {
      return "dbfs://" + workspaceName;
    }

    @Override
    public String getName() {
      return path;
    }
  }

  /** Naming implementation for GCS. */
  public static class GCSNaming implements DatasetNaming {
    private final String bucket_name;
    private final String object_key;

    public GCSNaming(String bucket_name, String object_key) {
      this.bucket_name = checkArgumentNotEmpty(bucket_name, "bucket_name");
      this.object_key = checkArgumentNotEmpty(object_key, "object_key");
    }

    @Override
    public String getNamespace() {
      return "gs://" + bucket_name;
    }

    @Override
    public String getName() {
      return object_key;
    }
  }

  /** Naming implementation for HDFS. */
  public static class HDFSNaming implements DatasetNaming {
    private final String namenodeHost;
    private final String namenodePort;
    private final String path;

    public HDFSNaming(String namenodeHost, String namenodePort, String path) {
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
  public static class KafkaNaming implements DatasetNaming {
    private final String bootstrapServerHost;
    private final String port;
    private final String topic;

    public KafkaNaming(String bootstrapServerHost, String port, String topic) {
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
  public static class LocalFileSystemNaming implements DatasetNaming {
    private final String path;

    public LocalFileSystemNaming(String path) {
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
  public static class RemoteFileSystemNaming implements DatasetNaming {
    private final String host;
    private final String path;

    public RemoteFileSystemNaming(String host, String path) {
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
  public static class S3Naming implements DatasetNaming {
    private final String bucketName;
    private final String objectKey;

    public S3Naming(String bucketName, String objectKey) {
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
  public static class WASBSNaming implements DatasetNaming {
    private final String container_name;
    private final String service_name;
    private final String object_key;

    public WASBSNaming(String container_name, String service_name, String object_key) {
      this.container_name = checkArgumentNotEmpty(container_name, "container_name");
      this.service_name = checkArgumentNotEmpty(service_name, "service_name");
      this.object_key = checkArgumentNotEmpty(object_key, "object_key");
    }

    @Override
    public String getNamespace() {
      return "wasbs://" + container_name + "@" + service_name + ".dfs.core.windows.net";
    }

    @Override
    public String getName() {
      return object_key;
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
