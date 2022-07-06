/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.utils;

import java.util.Optional;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SQLExecution;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;

public class LastQueryExecutionSparkEventListener extends SparkListener {

  private static Optional<QueryExecution> lastQueryExecution = Optional.empty();

  @Override
  public void onOtherEvent(SparkListenerEvent event) {
    if (event instanceof SparkListenerSQLExecutionStart) {
      lastQueryExecution =
          Optional.ofNullable(
              SQLExecution.getQueryExecution(
                  ((SparkListenerSQLExecutionStart) event).executionId()));
    }
  }

  public static Optional<LogicalPlan> getLastExecutedLogicalPlan() {
    return lastQueryExecution.map(qe -> qe.optimizedPlan());
  }
}
