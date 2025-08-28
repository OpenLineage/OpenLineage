/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors;

import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import org.apache.spark.sql.catalyst.expressions.Expression;

/** Interface to visit LogicalPlan's {@link Expression}s to collect expression dependencies. */
public interface ExpressionVisitor {
  /**
   * Determines whether this visitor is applicable to the given expression.
   *
   * @param expression the expression to examine
   * @return {@code true} if this visitor knows how to process the expression
   */
  boolean isDefinedAt(Expression expression);

  /**
   * Processes the expression and records column-level lineage with the supplied builder.
   *
   * @param expression the expression to process
   * @param builder the lineage builder to which the visitor must add discovered dependencies
   */
  void apply(Expression expression, ColumnLevelLineageBuilder builder);
}
