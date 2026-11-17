/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark3.agent.lifecycle.plan.column.ExpressionTraverser;
import java.util.List;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.Expand;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.collection.Seq;

/**
 * Extracts expression dependencies from Expand operator in {@link LogicalPlan}. Spark plans ROLLUP,
 * CUBE, GROUPING SETS, UNPIVOT and aggregations with several DISTINCT columns with an Expand node,
 * which evaluates a list of projections for every input row and exposes the results as new
 * attributes. Each output attribute depends on the expression at the same position of every
 * projection. Example query 'SELECT a, sum(b) FROM t GROUP BY ROLLUP (a)'.
 */
public class ExpandVisitor implements OperatorVisitor {
  @Override
  public boolean isDefinedAt(LogicalPlan operator) {
    return operator instanceof Expand;
  }

  @Override
  public void apply(LogicalPlan operator, ColumnLevelLineageBuilder builder) {
    Expand node = (Expand) operator;
    List<Attribute> output = ScalaConversionUtils.fromSeq(node.output());
    List<? extends Seq<Expression>> projections = ScalaConversionUtils.fromSeq(node.projections());
    projections.forEach(
        projection -> {
          List<Expression> expressions = ScalaConversionUtils.fromSeq(projection);
          for (int i = 0; i < Math.min(expressions.size(), output.size()); i++) {
            Expression expression = expressions.get(i);
            Attribute attribute = output.get(i);
            // Attributes passed through unchanged keep their id, the traverser skips those.
            ExpressionTraverser.of(
                    expression,
                    attribute.exprId(),
                    ((Expression) attribute).sql(),
                    TransformationInfo.identity(expression.sql()),
                    builder)
                .traverse();
          }
        });
  }
}
