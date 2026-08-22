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

/**
 * Removes adaptive-plan duplicate events only for queries that actually write to Delta. Previously,
 * merely installing the Delta extension caused the only terminal event of non-Delta queries to be
 * removed; gating on any Delta reference in the plan was still insufficient, because a non-Delta
 * write that reads from Delta tables (e.g. Delta read into a plain Parquet
 * InsertIntoHadoopFsRelationCommand) lost its only terminal event too.
 */
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
    if (!isDeltaPlan() || !EventFilterUtils.isCurrentPlanDeltaWrite(context)) {
      return false;
    }

    return context
        .getQueryExecution()
        .map(QueryExecution::executedPlan)
        .filter(sparkPlan -> sparkPlan.nodeName().contains("AdaptiveSparkPlan"))
        .isPresent();
  }
}
