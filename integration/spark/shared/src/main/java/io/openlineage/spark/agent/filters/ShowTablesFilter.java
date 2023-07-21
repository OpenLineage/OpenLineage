/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.filters;

import io.openlineage.spark.api.OpenLineageContext;
import org.apache.spark.scheduler.SparkListenerEvent;

public class ShowTablesFilter implements EventFilter {

  private final OpenLineageContext context;

  public ShowTablesFilter(OpenLineageContext context) {
    this.context = context;
  }

  public boolean isDisabled(SparkListenerEvent event) {
    return context
        .getQueryExecution()
        .map(queryExecution -> queryExecution.logical())
        .filter(
            logicalPlan ->
                logicalPlan
                    .getClass()
                    .getCanonicalName()
                    .equals("org.apache.spark.sql.catalyst.plans.logical.ShowTables"))
        .isPresent();
  }
}
