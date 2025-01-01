/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;

@Slf4j
public class MergeIntoCommandEdgeOutputDatasetBuilder
    extends AbstractQueryPlanOutputDatasetBuilder<LogicalPlan> {

  public MergeIntoCommandEdgeOutputDatasetBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return x.getClass()
        .getCanonicalName()
        .endsWith("sql.transaction.tahoe.commands.MergeIntoCommandEdge");
  }

  @Override
  protected List<OutputDataset> apply(SparkListenerEvent event, LogicalPlan x) {
    Object o = null;
    try {
      o = MethodUtils.invokeExactMethod(x, "target", new Object[] {});
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      log.error("Cannot extract target from Databricks classes", e);
    }

    if (o != null && o instanceof SubqueryAlias) {
      return delegate(((SubqueryAlias) o).child(), event);
    } else if (o != null && o instanceof LogicalPlan) {
      return delegate((LogicalPlan) o, event);
    }

    return Collections.emptyList();
  }
}
