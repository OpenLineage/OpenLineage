/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.filters;

import io.openlineage.spark.api.OpenLineageContext;
import java.util.Arrays;
import java.util.Optional;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

public class EventFilterUtils {

  /**
   * Method that verifies based on OpenLineageContext and SparkListenerEvent if OpenLineage event
   * has to be sent.
   *
   * @param context
   * @param event
   * @return
   */
  public static boolean isDisabled(OpenLineageContext context, SparkListenerEvent event) {
    return Arrays.asList(
            new DeltaEventFilter(context),
            new DatabricksEventFilter(context),
            new SparkNodesFilter(context),
            new CreateViewFilter(context),
            new AdaptivePlanEventFilter(context))
        .stream()
        .filter(filter -> filter.isDisabled(event.getClass().cast(event)))
        .findAny()
        .isPresent();
  }

  static Optional<LogicalPlan> getLogicalPlan(OpenLineageContext context) {
    return context
        .getQueryExecution()
        .filter(queryExecution -> queryExecution != null)
        .map(queryExecution -> queryExecution.optimizedPlan())
        .filter(plan -> plan != null);
  }

  /**
   * Verifies if `spark.sql.extensions` is set in Spark configuration and checks if it is a delta
   * extension.
   *
   * @return
   */
  static boolean isDeltaPlan() {
    return Optional.of(SparkSession.active())
        .map(SparkSession::sparkContext)
        .filter(context -> context != null)
        .map(SparkContext::conf)
        .map(conf -> conf.get("spark.sql.extensions", ""))
        .filter(extension -> extension.equals("io.delta.sql.DeltaSparkSessionExtension"))
        .isPresent();
  }
}
