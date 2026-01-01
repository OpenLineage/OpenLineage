/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator;

import static io.openlineage.client.utils.TransformationInfo.Subtypes.FILTER;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark3.agent.lifecycle.plan.column.ExpressionTraverser;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * Extracts expression dependencies from a Filter operator in {@link LogicalPlan}. Example query:
 *
 * <pre>{@code
 * SELECT *
 * FROM employees
 * WHERE salary > 50000;
 * }</pre>
 */
public class FilterVisitor implements OperatorVisitor {
  @Override
  public boolean isDefinedAt(LogicalPlan operator) {
    return operator instanceof Filter;
  }

  @Override
  public void apply(LogicalPlan operator, ColumnLevelLineageBuilder builder) {
    Filter filter = (Filter) operator;
    ExprId exprId = NamedExpression.newExprId();
    builder.addDatasetDependency(exprId);
    ExpressionTraverser.of(filter.condition(), exprId, TransformationInfo.indirect(FILTER), builder)
        .traverse();
  }
}
