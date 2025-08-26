/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors;

import static io.openlineage.client.utils.TransformationInfo.Subtypes.FILTER;
import static io.openlineage.spark3.agent.lifecycle.plan.column.ExpressionDependencyCollector.traverseExpression;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * Extracts expression dependencies from a Filter node in {@link LogicalPlan}. Example query:
 *
 * <pre>{@code
 * SELECT *
 * FROM employees
 * WHERE salary > 50000;
 * }</pre>
 */
public class FilterNodeVisitor implements ExpressionDependencyVisitor {
  @Override
  public boolean isDefinedAt(LogicalPlan plan) {
    return plan instanceof Filter;
  }

  @Override
  public void apply(LogicalPlan plan, ColumnLevelLineageBuilder builder) {
    Filter filter = (Filter) plan;
    ExprId exprId = NamedExpression.newExprId();
    builder.addDatasetDependency(exprId);
    traverseExpression(filter.condition(), exprId, TransformationInfo.indirect(FILTER), builder);
  }
}
