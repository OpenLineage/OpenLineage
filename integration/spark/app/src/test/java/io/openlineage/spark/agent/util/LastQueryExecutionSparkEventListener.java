/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SQLExecution;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;

public class LastQueryExecutionSparkEventListener extends SparkListener {

  private static List<LogicalPlan> queryExecutions = new ArrayList<>();

  @Override
  public void onOtherEvent(SparkListenerEvent event) {
    if (event instanceof SparkListenerSQLExecutionStart) {
      QueryExecution queryExecution =
          SQLExecution.getQueryExecution(((SparkListenerSQLExecutionStart) event).executionId());

      if (queryExecution != null) {
        queryExecutions.add(queryExecution.optimizedPlan());
      }
    }
  }

  public static Optional<LogicalPlan> getLastExecutedLogicalPlan() {
    if (queryExecutions.isEmpty()) {
      return Optional.empty();
    } else {
      return Optional.of(queryExecutions.get(queryExecutions.size() - 1));
    }
  }

  public static List<LogicalPlan> getExecutedLogicalPlans() {
    return queryExecutions;
  }
}
