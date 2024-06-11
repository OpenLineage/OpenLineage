/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark34.agent.lifecycle.plan.column;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.AbstractQueryPlanInputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.CreateTable;
import org.apache.spark.sql.catalyst.plans.logical.CreateTableAsSelect;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.ReplaceTable;
import org.apache.spark.sql.catalyst.plans.logical.ReplaceTableAsSelect;

/**
 * {@link LogicalPlan} visitor that matches an {@link CreateTableAsSelect} and extracts the output
 * {@link OpenLineage.Dataset} being written.
 */
@Slf4j
public class CreateReplaceInputDatasetBuilder
    extends AbstractQueryPlanInputDatasetBuilder<LogicalPlan> {

  public CreateReplaceInputDatasetBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAt(SparkListenerEvent event) {
    return true;
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    if (log.isDebugEnabled()) {
      log.debug(
          "Calling isDefinedAtLogicalPlan on {} with children {}", x.getClass(), x.children());
    }
    return ((x instanceof CreateTableAsSelect)
            || (x instanceof ReplaceTable)
            || (x instanceof ReplaceTableAsSelect)
            || (x instanceof CreateTable))
        && (x.children() == null || x.children().isEmpty());
  }

  @Override
  public List<OpenLineage.InputDataset> apply(SparkListenerEvent event, LogicalPlan x) {
    if (log.isDebugEnabled()) {
      log.debug("Calling apply on {}", x.getClass());
    }
    return extractChildren(x).stream()
        .flatMap(
            plan ->
                ScalaConversionUtils.fromSeq(
                        plan.collect(
                            delegate(
                                context.getInputDatasetQueryPlanVisitors(),
                                context.getInputDatasetBuilders(),
                                event)))
                    .stream()
                    .flatMap(a -> a.stream()))
        .collect(Collectors.toList());
  }

  protected List<LogicalPlan> extractChildren(LogicalPlan x) {
    try {
      Object query = MethodUtils.invokeMethod(x, "query");

      // query can be a single query or a list
      if (query instanceof List) {
        log.debug("Query is a list of size {}", ((List) query).size());
        // databricks addon to contain multiple plans as query
        return ((List<?>) query)
            .stream()
                .filter(o -> o instanceof LogicalPlan)
                .map(o -> (LogicalPlan) o)
                .collect(Collectors.toList());
      } else if (query instanceof LogicalPlan) {
        return Collections.singletonList((LogicalPlan) query);
      }
    } catch (Exception e) {
      // reflection didn't work
      log.warn("Failed to extract child query from {}", x);
    }
    return Collections.emptyList();
  }
}
