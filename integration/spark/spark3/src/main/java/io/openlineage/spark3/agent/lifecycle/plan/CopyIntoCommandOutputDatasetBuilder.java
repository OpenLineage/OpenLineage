/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.CopyIntoCommandUtils;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;

/**
 * Extracts output datasets from the Databricks-specific {@code CopyIntoCommand} or {@code
 * CopyIntoCommandEdge}. Since these classes belong to the Databricks runtime and are not available
 * at compile time, reflection is used to access their {@code target()} method.
 */
@Slf4j
public class CopyIntoCommandOutputDatasetBuilder
    extends AbstractQueryPlanOutputDatasetBuilder<LogicalPlan> {

  public CopyIntoCommandOutputDatasetBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return CopyIntoCommandUtils.isCopyIntoCommand(x);
  }

  @Override
  protected List<OutputDataset> apply(SparkListenerEvent event, LogicalPlan x) {
    LogicalPlan target = extractTarget(x);
    if (target == null) {
      return Collections.emptyList();
    }

    if (target instanceof SubqueryAlias) {
      return delegate(((SubqueryAlias) target).child(), event);
    }
    return delegate(target, event);
  }

  @Override
  public Optional<String> jobNameSuffix(LogicalPlan plan) {
    LogicalPlan target = extractTarget(plan);
    if (target == null) {
      return Optional.empty();
    }
    if (target instanceof SubqueryAlias) {
      target = ((SubqueryAlias) target).child();
    }
    LogicalPlan finalTarget = target;
    return context.getOutputDatasetBuilders().stream()
        .filter(b -> b instanceof AbstractQueryPlanOutputDatasetBuilder)
        .map(b -> (AbstractQueryPlanOutputDatasetBuilder) b)
        .map(b -> b.jobNameSuffixFromLogicalPlan(finalTarget))
        .filter(Optional::isPresent)
        .map(o -> (String) o.get())
        .findFirst();
  }

  private LogicalPlan extractTarget(LogicalPlan x) {
    try {
      Object result = MethodUtils.invokeExactMethod(x, "target", new Object[] {});
      if (result instanceof LogicalPlan) {
        return (LogicalPlan) result;
      }
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      log.error("Cannot extract target from Databricks CopyIntoCommand", e);
    }
    return null;
  }
}
