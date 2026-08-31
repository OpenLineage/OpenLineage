/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.filters;

import static io.openlineage.spark.agent.filters.EventFilterUtils.isDeltaPlan;

import io.openlineage.spark.agent.util.DatabricksUtils;
import io.openlineage.spark.api.OpenLineageContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;

/** Removes child SQL executions that duplicate their user-visible root execution. */
@Slf4j
public class AdaptivePlanEventFilter implements EventFilter {

  private final OpenLineageContext context;

  public AdaptivePlanEventFilter(OpenLineageContext context) {
    this.context = context;
  }

  /** Removes a correlated child while retaining the root execution that owns its lineage. */
  @Override
  public boolean isDisabled(SparkListenerEvent event) {
    if (!context.isCommandChildExecution()
        || (!isDeltaPlan(context) && !DatabricksUtils.isRunOnDatabricksPlatform(context))) {
      return false;
    }

    return context.getQueryExecution().isPresent();
  }
}
