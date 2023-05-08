/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.filters;

import io.openlineage.spark.api.OpenLineageContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;

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
   *
   * @return
   */
  public boolean isDisabled(SparkListenerEvent event) {
    return context
        .getQueryExecution()
        .filter(queryExecution -> queryExecution != null)
        .map(queryExecution -> queryExecution.executedPlan())
        .filter(sparkPlan -> sparkPlan != null)
        .filter(sparkPlan -> sparkPlan.nodeName().contains("AdaptiveSparkPlan"))
        .isPresent();
  }
}
