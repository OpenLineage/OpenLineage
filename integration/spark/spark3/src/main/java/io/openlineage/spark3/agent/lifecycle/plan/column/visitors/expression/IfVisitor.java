/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.expression;

import static io.openlineage.client.utils.TransformationInfo.Subtypes.CONDITIONAL;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark3.agent.lifecycle.plan.column.ExpressionTraverser;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.If;

/**
 * Visitor that extracts lineage from Spark {@link If} expressions.
 *
 * <p>For an {@code IF(predicate, trueExpr, falseExpr)} expression this visitor propagates lineage
 * from the predicate and both result expressions. The predicate is treated as an <em>indirect</em>
 * dependency, while {@code trueExpr} and {@code falseExpr} are traversed as direct dependencies.
 */
public class IfVisitor implements ExpressionVisitor {
  @Override
  public boolean isDefinedAt(Expression expression) {
    return expression instanceof If;
  }

  @Override
  public void apply(Expression expression, ExpressionTraverser traverser) {
    If expr = (If) expression;
    traverser.copyFor(expr.predicate(), TransformationInfo.indirect(CONDITIONAL)).traverse();
    traverser.copyFor(expr.trueValue()).traverse();
    traverser.copyFor(expr.falseValue()).traverse();
  }
}
