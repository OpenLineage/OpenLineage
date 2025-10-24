/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.dataset.DatasetConfig;
import io.openlineage.client.dataset.partition.DatasetReducer;
import io.openlineage.client.dataset.partition.trimmer.DatasetNameTrimmer;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** Utility class for convenient one-line usage of DatasetReducer class methods. */
public class DatasetReducerUtils {

  public static List<InputDataset> inputs(OpenLineageContext context, List<InputDataset> inputs) {
    return new DatasetReducer(
            context.getOpenLineage(), context.getOpenLineageConfig().getDatasetConfig())
        .reduceInputs(inputs);
  }

  public static List<OutputDataset> outputs(
      OpenLineageContext context, List<OutputDataset> outputs) {
    return new DatasetReducer(
            context.getOpenLineage(), context.getOpenLineageConfig().getDatasetConfig())
        .reduceOutputs(outputs);
  }

  public static DatasetIdentifier trimDatasetIdentifier(
      OpenLineageContext context, DatasetIdentifier datasetIdentifier) {
    // apply dataset name trimming logic
    Collection<DatasetNameTrimmer> nameTrimmers =
        Optional.of(context.getOpenLineageConfig())
            .map(SparkOpenLineageConfig::getDatasetConfig)
            .map(DatasetConfig::getDatasetNameTrimmers)
            .orElse(Collections.emptyList());

    return datasetIdentifier.withTrimmedName(nameTrimmers);
  }

  public static String trimDatasetName(OpenLineageContext context, String datasetName) {
    // apply dataset name trimming logic
    Collection<DatasetNameTrimmer> nameTrimmers =
        Optional.of(context.getOpenLineageConfig())
            .map(SparkOpenLineageConfig::getDatasetConfig)
            .map(DatasetConfig::getDatasetNameTrimmers)
            .orElse(Collections.emptyList());

    return new DatasetIdentifier(datasetName, "").withTrimmedName(nameTrimmers).getName();
  }
}
