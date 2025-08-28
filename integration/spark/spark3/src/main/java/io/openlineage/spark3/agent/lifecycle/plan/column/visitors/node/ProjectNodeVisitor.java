/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.node;

import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark3.agent.lifecycle.plan.column.ExpressionTraverser;
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
                ExpressionTraverser.of((Expression) expression, expression.exprId(), builder)
                    .traverse());
  }
}
