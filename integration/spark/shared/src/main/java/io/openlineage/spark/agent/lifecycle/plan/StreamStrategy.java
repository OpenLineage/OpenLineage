/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.spark.api.DatasetFactory;
import java.util.List;
import org.apache.spark.sql.execution.datasources.v2.StreamingDataSourceV2Relation;

abstract class StreamStrategy {
  protected final DatasetFactory<InputDataset> datasetFactory;
  protected final StreamingDataSourceV2Relation relation;

  protected StreamStrategy(
      DatasetFactory<InputDataset> datasetFactory, StreamingDataSourceV2Relation relation) {
    this.datasetFactory = datasetFactory;
    this.relation = relation;
  }

  abstract List<InputDataset> getInputDatasets();
}
