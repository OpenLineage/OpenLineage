/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.filters;

import static io.openlineage.spark.agent.filters.EventFilterUtils.isDeltaPlan;

import io.openlineage.spark.agent.util.DatabricksUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.execution.QueryExecution;

/** Removes internal SQL executions that duplicate a user-visible Delta operation. */
@Slf4j
public class AdaptivePlanEventFilter implements EventFilter {

  private final OpenLineageContext context;

  public AdaptivePlanEventFilter(OpenLineageContext context) {
    this.context = context;
  }

  /** AQE also optimizes user queries, so an adaptive plan alone is not evidence of a duplicate. */
  @Override
  public boolean isDisabled(SparkListenerEvent event) {
    boolean deltaPlan = isDeltaPlan(context);
    Optional<Boolean> commandChildExecution = context.getCommandChildExecution();
    if (commandChildExecution.isPresent()) {
      return commandChildExecution.get()
          && (deltaPlan || DatabricksUtils.isRunOnDatabricksPlatform(context));
    }

    if (!deltaPlan) {
      return false;
    }

    // Spark before 3.4 exposes no root ID; keep the old behavior instead of guessing parentage.
    return context
        .getQueryExecution()
        .map(QueryExecution::executedPlan)
        .filter(sparkPlan -> sparkPlan.nodeName().contains("AdaptiveSparkPlan"))
        .isPresent();
  }
}
