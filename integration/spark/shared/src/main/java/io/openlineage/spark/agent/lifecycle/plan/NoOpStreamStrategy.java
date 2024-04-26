/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.spark.api.DatasetFactory;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.execution.datasources.v2.StreamingDataSourceV2Relation;

@Slf4j
final class NoOpStreamStrategy extends StreamStrategy {

  public NoOpStreamStrategy(
      DatasetFactory<InputDataset> inputDatasetDatasetFactory,
      StreamingDataSourceV2Relation relation) {
    super(inputDatasetDatasetFactory, relation);
  }

  @Override
  List<InputDataset> getInputDatasets() {
    log.debug(
        "The no-op stream strategy has been invoked, and thus an empty list will be returned");
    return Collections.emptyList();
  }
}
