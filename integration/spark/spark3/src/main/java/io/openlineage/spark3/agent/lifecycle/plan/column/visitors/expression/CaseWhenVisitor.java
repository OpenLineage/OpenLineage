/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.expression;

import static io.openlineage.client.utils.TransformationInfo.Subtypes.CONDITIONAL;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark3.agent.lifecycle.plan.column.ExpressionTraverser;
import java.util.List;
import org.apache.spark.sql.catalyst.expressions.CaseWhen;
import org.apache.spark.sql.catalyst.expressions.Expression;
import scala.Tuple2;

/**
 * Visitor that extracts lineage from Spark {@link CaseWhen} expressions.
 *
 * <p>For a {@code CASE WHEN … THEN … ELSE … END} expression this visitor propagates lineage from
 * every predicate (<em>when</em> clause) and every result expression (<em>then</em>,
 * <em>else</em>). Predicates are considered <em>indirect</em> dependencies because they do not
 * contribute data to the result column; result expressions are treated as direct dependencies.
 */
public class CaseWhenVisitor implements ExpressionVisitor {
  @Override
  public boolean isDefinedAt(Expression expression) {
    return expression instanceof CaseWhen;
  }

  @Override
  public void apply(Expression expression, ExpressionTraverser traverser) {
    CaseWhen expr = (CaseWhen) expression;
    List<Tuple2<Expression, Expression>> branches = ScalaConversionUtils.fromSeq(expr.branches());
    branches.stream()
        .map(e -> e._1)
        .forEach(e -> traverser.copyFor(e, TransformationInfo.indirect(CONDITIONAL)).traverse());
    branches.stream().map(e -> e._2).forEach(e -> traverser.copyFor(e).traverse());
    if (expr.elseValue().isDefined()) {
      traverser.copyFor(expr.elseValue().get()).traverse();
    }
  }
}
