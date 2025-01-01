/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.spark.api.AbstractQueryPlanInputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

@Slf4j
public class MergeIntoCommandEdgeInputDatasetBuilder
    extends AbstractQueryPlanInputDatasetBuilder<LogicalPlan> {

  public MergeIntoCommandEdgeInputDatasetBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return x.getClass()
        .getCanonicalName()
        .endsWith("sql.transaction.tahoe.commands.MergeIntoCommandEdge");
  }

  @Override
  protected List<InputDataset> apply(SparkListenerEvent event, LogicalPlan x) {
    Object o1 = null;
    Object o2 = null;
    List<InputDataset> inputs = new ArrayList<>();

    try {
      o1 = MethodUtils.invokeExactMethod(x, "target", new Object[] {});
      o2 = MethodUtils.invokeExactMethod(x, "source", new Object[] {});
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      log.error("Cannot extract target from Databricks classes", e);
    }

    if (o1 != null && o1 instanceof LogicalPlan) {
      inputs.addAll(delegate((LogicalPlan) o1, event));
    }
    if (o2 != null && o2 instanceof LogicalPlan) {
      inputs.addAll(delegate((LogicalPlan) o2, event));
    }

    return inputs;
  }
}
