/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.expression;

import io.openlineage.spark3.agent.lifecycle.plan.column.ExpressionTraverser;
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
   * Traverses through the expression tree and records column-level lineage.
   *
   * @param expression the expression to process
   * @param traverser the expression traverser used to go through the expression tree
   */
  void apply(Expression expression, ExpressionTraverser traverser);
}
