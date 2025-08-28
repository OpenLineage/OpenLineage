/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors;

import static io.openlineage.spark3.agent.lifecycle.plan.column.ExpressionDependencyCollector.traverseExpression;

import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;

/** Extracts expression dependencies from Project node in {@link LogicalPlan}. */
public class ProjectNodeVisitor implements NodeVisitor {
  @Override
  public boolean isDefinedAt(LogicalPlan plan) {
    return plan instanceof Project;
  }

  @Override
  public void apply(LogicalPlan plan, ColumnLevelLineageBuilder builder) {
    ScalaConversionUtils.fromSeq(((Project) plan).projectList())
        .forEach(
            expression ->
                traverseExpression((Expression) expression, expression.exprId(), builder));
  }
}
