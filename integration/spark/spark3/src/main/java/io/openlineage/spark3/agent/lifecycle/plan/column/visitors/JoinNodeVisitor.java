/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors;

import static io.openlineage.client.utils.TransformationInfo.Subtypes.JOIN;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ExpressionDependencyCollector.traverseExpression;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.Join;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.Option;

/**
 * Extracts expression dependencies from a Join node in {@link LogicalPlan}. Example query:
 *
 * <pre>{@code
 * SELECT *
 * FROM orders o
 * JOIN customers c
 *   ON o.customer_id = c.id;
 * }</pre>
 */
public class JoinNodeVisitor implements ExpressionDependencyVisitor {
  @Override
  public boolean isDefinedAt(LogicalPlan plan) {
    return plan instanceof Join;
  }

  @Override
  public void apply(LogicalPlan plan, ColumnLevelLineageBuilder builder) {
    Option<Expression> condition = ((Join) plan).condition();
    if (condition.isDefined()) {
      ExprId exprId = NamedExpression.newExprId();
      builder.addDatasetDependency(exprId);
      traverseExpression(condition.get(), exprId, TransformationInfo.indirect(JOIN), builder);
    }
  }
}
