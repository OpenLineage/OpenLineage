/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.expression;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark3.agent.lifecycle.plan.column.ExpressionTraverser;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression;
import scala.collection.Seq;

/**
 * Visitor that registers lineage for Spark {@link AggregateExpression}s.
 *
 * <p>For each aggregate expression two kinds of dependencies are recorded:
 *
 * <ul>
 *   <li>A dependency from the aggregate’s {@code resultId} (or {@code resultIds} on Databricks) to
 *       the current output column.
 *   <li>A recursive traversal of the underlying AggregateFunction.
 * </ul>
 */
@Slf4j
public class AggregateExpressionVisitor implements ExpressionVisitor {
  @Override
  public boolean isDefinedAt(Expression expression) {
    return expression instanceof AggregateExpression;
  }

  @Override
  public void apply(Expression expression, ExpressionTraverser traverser) {
    AggregateExpression expr = (AggregateExpression) expression;
    // in databricks `resultId` method is not present. Instead, there exists `resultIds`
    if (MethodUtils.getAccessibleMethod(AggregateExpression.class, "resultId") != null) {
      traverser.addDependency(expr.resultId(), TransformationInfo.aggregation());
    } else {
      try {
        Seq<ExprId> resultIds = (Seq<ExprId>) MethodUtils.invokeMethod(expr, "resultIds");
        ScalaConversionUtils.fromSeq(resultIds)
            .forEach(e -> traverser.addDependency(e, TransformationInfo.aggregation()));
      } catch (Exception e) {
        // do nothing
        log.warn("Failed extracting resultIds from AggregateExpression", e);
      }
    }
    traverser.copyFor(expr.aggregateFunction(), TransformationInfo.aggregation()).traverse();
  }
}
