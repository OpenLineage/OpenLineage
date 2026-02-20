/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.client.utils.DatasetIdentifier;
import java.util.List;
import java.util.Optional;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.types.StructType;

public interface InputPartitionExtractor {
  boolean isDefinedAt(InputPartition inputPartition);

  List<DatasetIdentifier> extract(SparkContext sparkContext, InputPartition inputPartition);

  default Optional<StructType> extractSchema(
      SparkContext sparkContext, InputPartition inputPartition) {
    return Optional.empty();
  }
}
