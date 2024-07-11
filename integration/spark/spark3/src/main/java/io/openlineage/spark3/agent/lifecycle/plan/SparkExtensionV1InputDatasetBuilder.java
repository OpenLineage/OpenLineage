/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.spark.api.AbstractQueryPlanInputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/** {@link LogicalPlan} visitor that matches nodes which implement `InputLineageNode` interface. */
@Slf4j
public class SparkExtensionV1InputDatasetBuilder
    extends AbstractQueryPlanInputDatasetBuilder<LogicalPlan> {

  public SparkExtensionV1InputDatasetBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan logicalPlan) {
    return context.getSparkExtensionVisitorWrapper().isDefinedAt(logicalPlan);
  }

  @Override
  protected List<InputDataset> apply(SparkListenerEvent event, LogicalPlan x) {

    Pair<List<InputDataset>, List<Object>> inputs =
        context.getSparkExtensionVisitorWrapper().getInputs(x, event.getClass().getName());

    // extract datasets with delegate
    List<InputDataset> inputDatasets =
        inputs.getRight().stream()
            .flatMap(d -> delegate(event, (LogicalPlan) d).stream())
            .collect(Collectors.toList());

    inputDatasets.addAll(inputs.getLeft());

    return inputDatasets;
  }

  protected List<InputDataset> delegate(SparkListenerEvent event, LogicalPlan plan) {
    return new ArrayList<>(
        delegate(
                context.getInputDatasetQueryPlanVisitors(),
                context.getInputDatasetBuilders(),
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
