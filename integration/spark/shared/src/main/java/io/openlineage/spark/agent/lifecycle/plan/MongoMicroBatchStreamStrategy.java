/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.DatasetFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.connector.read.streaming.SparkDataStream;
import org.apache.spark.sql.execution.datasources.v2.StreamingDataSourceV2Relation;
import org.apache.spark.sql.types.StructType;

@Slf4j
public class MongoMicroBatchStreamStrategy extends StreamStrategy {
  // Constructor for Spark 3.x
  public MongoMicroBatchStreamStrategy(
      DatasetFactory<OpenLineage.InputDataset> inputDatasetDatasetFactory,
      StreamingDataSourceV2Relation relation) {
    super(inputDatasetDatasetFactory, relation.schema(), relation.stream(), Optional.empty());
  }

  // Constructor for Spark 4.0 (matches KafkaMicroBatchStreamStrategy signature)
  public MongoMicroBatchStreamStrategy(
      DatasetFactory<OpenLineage.InputDataset> inputDatasetDatasetFactory,
      StructType schema,
      SparkDataStream stream,
      Optional<Offset> offsetOption) {
    super(inputDatasetDatasetFactory, schema, stream, offsetOption);
  }

  @Override
  public List<OpenLineage.InputDataset> getInputDatasets() {
    Optional<Object> readConfig = tryReadField(stream, "readConfig");

    if (!readConfig.isPresent()) {
      log.warn("Failed to extract MongoDB lineage: readConfig field not found in stream object");
      return new ArrayList<>();
    }

    Optional<Map<String, String>> options = tryReadField(readConfig.get(), "options");
    if (!options.isPresent()) {
      log.warn("Failed to extract MongoDB lineage: options field not found in readConfig");
      return new ArrayList<>();
    }

    Map<String, String> option = options.get();

    // Try to get collection name (required)
    String collectionName =
        option.getOrDefault("spark.mongodb.collection", option.get("collection"));
    if (collectionName == null || collectionName.isEmpty()) {
      log.warn(
          "Failed to extract MongoDB lineage: collection name not specified in options (checked keys: spark.mongodb.collection, collection)");
      return new ArrayList<>();
    }

    // Try to get connection URI from various possible keys
    String connectionURL =
        option.getOrDefault(
            "spark.mongodb.connection.uri",
            option.getOrDefault(
                "connection.uri", option.getOrDefault("spark.mongodb.read.connection.uri", "")));

    if (connectionURL == null || connectionURL.isEmpty()) {
      log.warn(
          "Failed to extract MongoDB lineage: connection URI not found in options (checked keys: spark.mongodb.connection.uri, connection.uri, spark.mongodb.read.connection.uri)");
      return new ArrayList<>();
    }

    // Try to get database name from options first
    String databaseName = option.getOrDefault("spark.mongodb.database", option.get("database"));

    // Track if database came from URI (to avoid duplication in namespace)
    boolean databaseFromUri = false;

    // If database not in options, try to extract from connection URI
    // Format: mongodb://user:pass@host:port/database?params
    if (databaseName == null || databaseName.isEmpty()) {
      databaseName = extractDatabaseFromURI(connectionURL);
      databaseFromUri = true;
    }

    if (databaseName == null || databaseName.isEmpty()) {
      log.warn(
          "Failed to extract MongoDB lineage: database name not found in options or URI (URI: {})",
          connectionURL);
      return new ArrayList<>();
    }

    // Build namespace: strip database from URI if it was extracted from there
    String namespace;
    if (databaseFromUri) {
      // Database is already in connectionURL, use it as-is (strip query params for clean
      // namespace)
      int queryParamIndex = connectionURL.indexOf('?');
      if (queryParamIndex != -1) {
        // Keep base URI with database, remove query params
        namespace = connectionURL.substring(0, queryParamIndex);
      } else {
        namespace = connectionURL;
      }
    } else {
      // Database from options, append to connectionURL
      // First strip database from connectionURL if present, then append the options database
      String baseUri = stripDatabaseFromURI(connectionURL);
      namespace = baseUri + "/" + databaseName;
    }

    OpenLineage.InputDataset dataset =
        datasetFactory.getDataset(collectionName, namespace, schema);

    return Arrays.asList(dataset);
  }

  /**
   * Strips the database name from a MongoDB URI, keeping only the connection part.
   * mongodb://host:27017/mydb?params -> mongodb://host:27017
   */
  private String stripDatabaseFromURI(String uri) {
    try {
      int schemeEnd = uri.indexOf("://");
      if (schemeEnd == -1) {
        return uri; // No scheme, return as-is
      }

      // Find first "/" after scheme (database path start)
      int pathStart = uri.indexOf('/', schemeEnd + 3);
      if (pathStart == -1) {
        return uri; // No database in URI, return as-is
      }

      // Return everything before the database path
      return uri.substring(0, pathStart);

    } catch (Exception e) {
      log.error("Failed to strip database from MongoDB URI: {}", uri, e);
      return uri; // Return original on error
    }
  }

  private String extractDatabaseFromURI(String uri) {
    try {
      // Extract database from URI: mongodb://user:pass@host:port/database?params
      // Find the scheme separator "://"
      int schemeEnd = uri.indexOf("://");
      if (schemeEnd == -1) {
        return null; // No scheme found
      }

      // Find the first "/" after the scheme (database path start)
      int pathStart = uri.indexOf('/', schemeEnd + 3);
      if (pathStart == -1) {
        return null; // No database path in URI
      }

      // Extract everything after the "/"
      String afterSlash = uri.substring(pathStart + 1);

      // Handle trailing slash (empty database name)
      if (afterSlash.isEmpty()) {
        return null;
      }

      // Strip query parameters if present
      int questionMark = afterSlash.indexOf('?');
      String database =
          (questionMark != -1) ? afterSlash.substring(0, questionMark) : afterSlash;

      // Return null if database name is empty after stripping params
      return database.isEmpty() ? null : database;

    } catch (Exception e) {
      log.error("Failed to parse database name from MongoDB URI: {}", uri, e);
      return null;
    }
  }

  private <T> Optional<T> tryReadField(Object target, String fieldName) {
    try {
      T value = (T) FieldUtils.readField(target, fieldName, true);
      return Optional.ofNullable(value);
    } catch (IllegalArgumentException e) {
      log.error("Could not read the field '{}' because it does not exist", fieldName, e);
      return Optional.empty();
    } catch (IllegalAccessException e) {
      log.error("Could not read the field '{}'", fieldName, e);
      return Optional.empty();
    }
  }
}
