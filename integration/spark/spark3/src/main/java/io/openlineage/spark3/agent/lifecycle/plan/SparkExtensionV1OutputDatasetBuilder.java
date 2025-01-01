/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/** {@link LogicalPlan} visitor that matches nodes which implement `OutputLineageNode` interface. */
@Slf4j
public class SparkExtensionV1OutputDatasetBuilder
    extends AbstractQueryPlanOutputDatasetBuilder<LogicalPlan> {

  public SparkExtensionV1OutputDatasetBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan logicalPlan) {
    return context.getSparkExtensionVisitorWrapper().isDefinedAt(logicalPlan);
  }

  @Override
  protected List<OutputDataset> apply(SparkListenerEvent event, LogicalPlan x) {

    Pair<List<OutputDataset>, List<Object>> outputs =
        context.getSparkExtensionVisitorWrapper().getOutputs(x, event.getClass().getName());

    // extract datasets with delegate
    List<OutputDataset> outputDatasets =
        outputs.getRight().stream()
            .flatMap(d -> delegate(event, (LogicalPlan) d).stream())
            .collect(Collectors.toList());

    // extract datasets with identifier
    outputDatasets.addAll(outputs.getLeft());

    return outputDatasets;
  }

  protected List<OutputDataset> delegate(SparkListenerEvent event, LogicalPlan plan) {
    return new ArrayList<>(
        delegate(
                context.getOutputDatasetQueryPlanVisitors(),
                context.getOutputDatasetBuilders(),
                event)
            .applyOrElse(plan, (lp) -> Collections.emptyList()));
  }

  /**
   * For testing purpose
   *
   * @return
   */
  protected OpenLineageContext getContext() {
    return context;
  }
}
