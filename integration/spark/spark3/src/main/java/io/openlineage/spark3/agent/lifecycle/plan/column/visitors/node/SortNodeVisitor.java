/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.node;

import static io.openlineage.client.utils.TransformationInfo.Subtypes.SORT;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark3.agent.lifecycle.plan.column.ExpressionTraverser;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Sort;

/**
 * Extracts expression dependencies from a Sort node in {@link LogicalPlan}. Example query:
 *
 * <pre>{@code
 * SELECT name, salary
 * FROM employees
 * ORDER BY salary DESC;
 * }</pre>
 */
public class SortNodeVisitor implements NodeVisitor {
  @Override
  public boolean isDefinedAt(LogicalPlan plan) {
    return plan instanceof Sort;
  }

  @Override
  public void apply(LogicalPlan plan, ColumnLevelLineageBuilder builder) {
    Sort sort = (Sort) plan;
    ExprId exprId = NamedExpression.newExprId();
    builder.addDatasetDependency(exprId);
    ScalaConversionUtils.fromSeq(sort.order())
        .forEach(
            e ->
                ExpressionTraverser.of(e, exprId, TransformationInfo.indirect(SORT), builder)
                    .traverse());
  }
}
