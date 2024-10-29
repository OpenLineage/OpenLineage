/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageContext;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageVisitor;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

@Slf4j
public class CustomCollectorsUtils {

  static void collectInputs(ColumnLevelLineageContext context, LogicalPlan plan) {
    getCollectors(context).forEach(collector -> collector.collectInputs(context, plan));
  }

  static void collectOutputs(ColumnLevelLineageContext context, LogicalPlan plan) {
    getCollectors(context).forEach(collector -> collector.collectOutputs(context, plan));
  }

  static void collectExpressionDependencies(ColumnLevelLineageContext context, LogicalPlan plan) {
    getCollectors(context)
        .forEach(collector -> collector.collectExpressionDependencies(context, plan));
  }

  /**
   * Gets the list of column-level lineage collectors from two sources.
   *
   * <p>The first source is the custom visitors provided by the user. These custom visitors are not
   * expected to be widely used but are included to support legacy implementations.
   *
   * <p>The second source is the OpenLineage context, which contains a dynamically built list of
   * visitors based on the Spark version and the libraries in use.
   */
  private static List<ColumnLevelLineageVisitor> getCollectors(ColumnLevelLineageContext context) {
    return context.getOlContext().getColumnLevelLineageVisitors();
  }
}
