/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.spark.api.DatasetFactory;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.connector.read.streaming.SparkDataStream;
import org.apache.spark.sql.types.StructType;

public abstract class StreamStrategy {
  protected final DatasetFactory<InputDataset> datasetFactory;
  protected final StructType schema;
  protected final SparkDataStream stream;
  protected final Optional<Offset> offsetOption;

  protected StreamStrategy(
      DatasetFactory<InputDataset> datasetFactory,
      StructType schema,
      SparkDataStream stream,
      Optional<Offset> offsetOption) {
    this.datasetFactory = datasetFactory;
    this.schema = schema;
    this.stream = stream;
    this.offsetOption = offsetOption;
  }

  public abstract List<InputDataset> getInputDatasets();
}
