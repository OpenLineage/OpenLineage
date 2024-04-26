/*
/* Copyright 2018-2024 contributors to the OpenLineage project
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
import org.apache.spark.sql.execution.datasources.v2.StreamingDataSourceV2Relation;

public class MongoMicroBatchStreamStrategy extends StreamStrategy {
  public MongoMicroBatchStreamStrategy(
      DatasetFactory<OpenLineage.InputDataset> inputDatasetDatasetFactory,
      StreamingDataSourceV2Relation relation) {
    super(inputDatasetDatasetFactory, relation);
  }

  @Override
  List<OpenLineage.InputDataset> getInputDatasets() {
    Optional<Object> readConfig = tryReadField(relation.stream(), "readConfig");

    if (!readConfig.isPresent()) {
      return new ArrayList<>();
    }

    Optional<Map<String, String>> options = tryReadField(readConfig.get(), "options");
    Optional<OpenLineage.InputDataset> dataset =
        options.map(
            option -> {
              String databaseName = option.get("spark.mongodb.database");
              String connectionURL = option.get("spark.mongodb.connection.uri");
              String collectionName = option.get("spark.mongodb.collection");

              return datasetFactory.getDataset(
                  collectionName, connectionURL + "/" + databaseName, relation.schema());
            });

    return dataset.map(Arrays::asList).orElseGet(ArrayList::new);
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
