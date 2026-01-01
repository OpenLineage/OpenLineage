/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.expression;

import static io.openlineage.client.utils.TransformationInfo.Subtypes.CONDITIONAL;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark3.agent.lifecycle.plan.column.ExpressionTraverser;
import org.apache.spark.sql.catalyst.expressions.Coalesce;
import org.apache.spark.sql.catalyst.expressions.Expression;

/**
 * Visitor that extracts lineage from Spark {@link Coalesce} expressions.
 *
 * <p>For {@code COALESCE(child1, child2, …)} this visitor propagates lineage from every child
 * expression. Each child is traversed twice:
 *
 * <ul>
 *   <li>once as an <em>indirect</em> dependency representing the null-check condition;
 *   <li>once as a direct dependency supplying the actual value.
 * </ul>
 */
public class CoalesceVisitor implements ExpressionVisitor {
  @Override
  public boolean isDefinedAt(Expression expression) {
    return expression instanceof Coalesce;
  }

  @Override
  public void apply(Expression expression, ExpressionTraverser traverser) {
    Coalesce expr = (Coalesce) expression;
    ScalaConversionUtils.fromSeq(expr.children())
        .forEach(
            e -> {
              traverser.copyFor(e, TransformationInfo.indirect(CONDITIONAL)).traverse();
              traverser.copyFor(e).traverse();
            });
  }
}
