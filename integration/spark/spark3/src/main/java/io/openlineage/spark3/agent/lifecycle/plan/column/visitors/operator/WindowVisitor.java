/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark3.agent.lifecycle.plan.column.ExpressionTraverser;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Window;

/**
 * Extracts expression dependencies from a Window operator in {@link LogicalPlan}. Example query:
 *
 * <pre>{@code
 * SELECT id,
 *        amount,
 *        SUM(amount) OVER (PARTITION BY id ORDER BY ts) AS running_total
 * FROM payments;
 * }</pre>
 */
public class WindowVisitor implements OperatorVisitor {
  @Override
  public boolean isDefinedAt(LogicalPlan operator) {
    return operator instanceof Window;
  }

  @Override
  public void apply(LogicalPlan operator, ColumnLevelLineageBuilder builder) {
    ScalaConversionUtils.fromSeq(((Window) operator).windowExpressions())
        .forEach(
            e ->
                ExpressionTraverser.of(
                        (Expression) e, e.exprId(), TransformationInfo.identity(), builder)
                    .traverse());
  }
}
