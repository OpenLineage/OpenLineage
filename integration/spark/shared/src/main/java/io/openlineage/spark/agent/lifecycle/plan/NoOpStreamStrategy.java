/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.spark.api.DatasetFactory;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.connector.read.streaming.SparkDataStream;
import org.apache.spark.sql.types.StructType;

@Slf4j
public final class NoOpStreamStrategy extends StreamStrategy {

  public NoOpStreamStrategy(
      DatasetFactory<InputDataset> inputDatasetDatasetFactory,
      StructType schema,
      SparkDataStream stream,
      Optional<Offset> offsetOption) {
    super(inputDatasetDatasetFactory, schema, stream, offsetOption);
  }

  @Override
  public List<InputDataset> getInputDatasets() {
    log.debug(
        "The no-op stream strategy has been invoked, and thus an empty list will be returned");
    return Collections.emptyList();
  }
}
