/*
/* Copyright 2018-2023 contributors to the OpenLineage project
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

  public boolean isDisabled(SparkListenerEvent event) {
    return getLogicalPlan(context)
        .filter(plan -> plan instanceof CreateViewCommand)
        .filter(
            plan ->
                context.getSparkVersion().compareTo("3.3")
                    >= 0) // CreateViewCommand is not filtered for lower spark versions as it breaks
        // io.openlineage.spark.agent.lifecycle.SparkReadWriteIntegTest.testCacheReadFromFileWriteToParquet test
        .isPresent();
  }
}
