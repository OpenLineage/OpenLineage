/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.node;

import static io.openlineage.spark3.agent.lifecycle.plan.column.ExpressionDependencyCollector.traverseExpression;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Window;

/**
 * Extracts expression dependencies from a Window node in {@link LogicalPlan}. Example query:
 *
 * <pre>{@code
 * SELECT id,
 *        amount,
 *        SUM(amount) OVER (PARTITION BY id ORDER BY ts) AS running_total
 * FROM payments;
 * }</pre>
 */
public class WindowNodeVisitor implements NodeVisitor {
  @Override
  public boolean isDefinedAt(LogicalPlan plan) {
    return plan instanceof Window;
  }

  @Override
  public void apply(LogicalPlan plan, ColumnLevelLineageBuilder builder) {
    ScalaConversionUtils.fromSeq(((Window) plan).windowExpressions())
        .forEach(
            e ->
                traverseExpression(
                    (Expression) e, e.exprId(), TransformationInfo.identity(), builder));
  }
}
