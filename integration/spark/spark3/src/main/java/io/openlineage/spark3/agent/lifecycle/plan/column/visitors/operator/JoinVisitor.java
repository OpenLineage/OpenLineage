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
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.Join;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

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
    Join join = (Join) operator;
    if (join.condition().isDefined()) {
      ExprId exprId = NamedExpression.newExprId();
      String description = join.condition().get().sql();
      String outputExpressionString =
          String.format("%s JOIN ON %s", join.joinType().sql(), description);
      builder.addDatasetDependency(exprId, outputExpressionString, description);
      ExpressionTraverser.of(
              join.condition().get(),
              exprId,
              description,
              TransformationInfo.indirect(JOIN, description),
              builder)
          .traverse();
    }
  }
}
