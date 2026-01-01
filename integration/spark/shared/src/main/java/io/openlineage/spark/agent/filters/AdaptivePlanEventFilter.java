/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.filters;

import static io.openlineage.spark.agent.filters.EventFilterUtils.isDeltaPlan;

import io.openlineage.spark.api.OpenLineageContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.execution.QueryExecution;

/** Removes events generated with Adaptive Plan Execution. Those events contain duplicate events. */
@Slf4j
public class AdaptivePlanEventFilter implements EventFilter {

  private final OpenLineageContext context;

  public AdaptivePlanEventFilter(OpenLineageContext context) {
    this.context = context;
  }

  /**
   * In case of Join queries spark plan may get optimized within Adaptive Query Execution engine,
   * which leads into multiple query plans and duplicated START/COMPLETE events.
   */
  @Override
  public boolean isDisabled(SparkListenerEvent event) {
    if (!isDeltaPlan()) {
      return false;
    }

    return context
        .getQueryExecution()
        .map(QueryExecution::executedPlan)
        .filter(sparkPlan -> sparkPlan.nodeName().contains("AdaptiveSparkPlan"))
        .isPresent();
  }
}
