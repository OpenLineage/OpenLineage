/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/** Traverses a MERGE source, which is not exposed through the command's children. */
final class MergeSourceDatasetTraversal {
  private MergeSourceDatasetTraversal() {}

  static List<InputDataset> collect(
      LogicalPlan source, Function<LogicalPlan, List<InputDataset>> delegate) {
    List<InputDataset> datasets = delegate.apply(source);
    if (!datasets.isEmpty()) {
      // A resolving alias or relation already represents this branch.
      return datasets;
    }
    List<InputDataset> inputs = new ArrayList<>();
    for (LogicalPlan child : ScalaConversionUtils.fromSeq(source.children())) {
      inputs.addAll(collect(child, delegate));
    }
    return inputs;
  }
}
