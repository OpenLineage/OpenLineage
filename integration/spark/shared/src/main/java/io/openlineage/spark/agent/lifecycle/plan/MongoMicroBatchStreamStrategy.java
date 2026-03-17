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
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.connector.read.streaming.SparkDataStream;
import org.apache.spark.sql.execution.datasources.v2.StreamingDataSourceV2Relation;
import org.apache.spark.sql.types.StructType;

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
      return new ArrayList<>();
    }

    Optional<Map<String, String>> options = tryReadField(readConfig.get(), "options");
    if (!options.isPresent()) {
      return new ArrayList<>();
    }

    Map<String, String> option = options.get();

    // Try to get collection name (required)
    String collectionName = option.getOrDefault("spark.mongodb.collection",
        option.get("collection"));
    if (collectionName == null || collectionName.isEmpty()) {
      return new ArrayList<>();  // Collection is required
    }

    // Try to get connection URI from various possible keys
    String connectionURL = option.getOrDefault("spark.mongodb.connection.uri",
        option.getOrDefault("connection.uri",
            option.getOrDefault("spark.mongodb.read.connection.uri", "")));

    if (connectionURL == null || connectionURL.isEmpty()) {
      return new ArrayList<>();  // Connection URL is required
    }

    // Try to get database name from options first
    String databaseName = option.getOrDefault("spark.mongodb.database",
        option.get("database"));

    // If database not in options, try to extract from connection URI
    // Format: mongodb://user:pass@host:port/database?params
    if (databaseName == null || databaseName.isEmpty()) {
      databaseName = extractDatabaseFromURI(connectionURL);
    }

    if (databaseName == null || databaseName.isEmpty()) {
      return new ArrayList<>();  // Database is required
    }

    OpenLineage.InputDataset dataset = datasetFactory.getDataset(
        collectionName, connectionURL + "/" + databaseName, schema);

    return Arrays.asList(dataset);
  }

  private String extractDatabaseFromURI(String uri) {
    try {
      // Extract database from URI: mongodb://user:pass@host:port/database?params
      int lastSlash = uri.lastIndexOf('/');
      if (lastSlash == -1 || lastSlash == uri.length() - 1) {
        return null;
      }
      String afterSlash = uri.substring(lastSlash + 1);
      int questionMark = afterSlash.indexOf('?');
      if (questionMark != -1) {
        return afterSlash.substring(0, questionMark);
      }
      return afterSlash;
    } catch (Exception e) {
      return null;
    }
  }

  private <T> Optional<T> tryReadField(Object target, String fieldName) {
    try {
      T value = (T) FieldUtils.readField(target, fieldName, true);
      return Optional.ofNullable(value);
    } catch (IllegalArgumentException e) {
      return Optional.empty();
    } catch (IllegalAccessException e) {
      return Optional.empty();
    }
  }
}
