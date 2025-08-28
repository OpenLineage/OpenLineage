/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * Traverses LogicalPlan and collects dependencies between the expressions and operations used
 * within the plan.
 */
@Slf4j
public class ExpressionDependencyCollector {
  private static final VisitorFactory visitorFactory = new VisitorFactory();

  static void collect(ColumnLevelLineageContext context, LogicalPlan plan) {
    plan.foreach(
        node -> {
          CustomCollectorsUtils.collectExpressionDependencies(context, node);
          collectFromNode(context.getBuilder(), node);
          return scala.runtime.BoxedUnit.UNIT;
        });
  }

  public static void collectFromNode(ColumnLevelLineageBuilder builder, LogicalPlan node) {
    visitorFactory.nodeVisitors().stream()
        .filter(collector -> collector.isDefinedAt(node))
        .forEach(collector -> collector.apply(node, builder));
  }
}
