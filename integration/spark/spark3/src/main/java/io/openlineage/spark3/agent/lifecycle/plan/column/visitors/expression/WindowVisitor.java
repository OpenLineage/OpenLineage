/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.expression;

import static io.openlineage.client.utils.TransformationInfo.Subtypes.WINDOW;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark3.agent.lifecycle.plan.column.ExpressionTraverser;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.RankLike;
import org.apache.spark.sql.catalyst.expressions.RowNumberLike;
import org.apache.spark.sql.catalyst.expressions.WindowExpression;

/**
 * Visitor that extracts lineage from Spark {@link WindowExpression}s.
 *
 * <p>For {@code windowFunction OVER (PARTITION BY … ORDER BY … FRAME …)} the visitor propagates
 * lineage as follows:
 *
 * <ul>
 *   <li>{@code windowFunction} is traversed with transformation {@link
 *       TransformationInfo.Subtypes#TRANSFORMATION} <strong>unless</strong> it is a {@link
 *       RankLike} or {@link RowNumberLike} function, whose results are considered independent of
 *       their potential arguments.
 *   <li>Every child of the {@code windowSpec} (partition keys, order keys, frame expressions) is
 *       traversed as an <em>indirect</em> dependency with kind {@link
 *       TransformationInfo.Subtypes#WINDOW}.
 * </ul>
 */
public class WindowVisitor implements ExpressionVisitor {
  @Override
  public boolean isDefinedAt(Expression expression) {
    return expression instanceof WindowExpression;
  }

  @Override
  public void apply(Expression expression, ExpressionTraverser traverser) {
    WindowExpression expr = (WindowExpression) expression;
    Expression windowFunction = expr.windowFunction();
    // in case of e.g. RANK() OVER (... ORDER BY X) we can get X as child of Rank
    // even though value of rank is only indirectly dependent of X
    // so in case of RankLike and RowNumberLike we omit possible children
    if (!(windowFunction instanceof RankLike || windowFunction instanceof RowNumberLike)) {
      traverser.copyFor(expr.windowFunction(), TransformationInfo.transformation()).traverse();
    }
    ScalaConversionUtils.fromSeq(expr.windowSpec().children())
        .forEach(child -> traverser.copyFor(child, TransformationInfo.indirect(WINDOW)).traverse());
  }
}
