/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator;

import static io.openlineage.client.utils.TransformationInfo.Subtypes.JOIN;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark3.agent.lifecycle.plan.column.ExpressionTraverser;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.Join;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.Option;

/**
 * Extracts expression dependencies from a Join operator in {@link LogicalPlan}. Example query:
 *
 * <pre>{@code
 * SELECT *
 * FROM orders o
 * JOIN customers c
 *   ON o.customer_id = c.id;
 * }</pre>
 */
public class JoinVisitor implements OperatorVisitor {
  @Override
  public boolean isDefinedAt(LogicalPlan operator) {
    return operator instanceof Join;
  }

  @Override
  public void apply(LogicalPlan operator, ColumnLevelLineageBuilder builder) {
    Option<Expression> condition = ((Join) operator).condition();
    if (condition.isDefined()) {
      ExprId exprId = NamedExpression.newExprId();
      builder.addDatasetDependency(exprId);
      ExpressionTraverser.of(condition.get(), exprId, TransformationInfo.indirect(JOIN), builder)
          .traverse();
    }
  }
}
