/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.filters;

import static io.openlineage.spark.agent.filters.EventFilterUtils.getLogicalPlan;

import io.openlineage.spark.api.OpenLineageContext;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.execution.command.CreateViewCommand;

public class CreateViewFilter implements EventFilter {

  private final OpenLineageContext context;

  public CreateViewFilter(OpenLineageContext context) {
    this.context = context;
  }

  @Override
  public boolean isDisabled(SparkListenerEvent event) {
    return getLogicalPlan(context)
        .filter(plan -> plan instanceof CreateViewCommand)
        .filter(
            // CreateViewCommand is not filtered for lower spark versions as it breaks
            plan -> "3.3".compareTo(context.getSparkVersion()) < 0)
        // io.openlineage.spark.agent.lifecycle.SparkReadWriteIntegTest.testCacheReadFromFileWriteToParquet test
        .isPresent();
  }
}
