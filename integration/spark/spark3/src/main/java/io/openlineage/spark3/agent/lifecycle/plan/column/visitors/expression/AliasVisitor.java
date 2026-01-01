/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.expression;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark3.agent.lifecycle.plan.column.ExpressionTraverser;
import org.apache.spark.sql.catalyst.expressions.Alias;
import org.apache.spark.sql.catalyst.expressions.Expression;

/**
 * Visitor that propagates lineage through {@code Alias} expressions.
 *
 * <p>This visitor matches Spark {@link Alias} expressions and continues the traversal on the
 * aliased child expression, preserving the identity transformation.
 */
public class AliasVisitor implements ExpressionVisitor {
  @Override
  public boolean isDefinedAt(Expression expression) {
    return expression instanceof Alias;
  }

  @Override
  public void apply(Expression expression, ExpressionTraverser traverser) {
    traverser.copyFor(((Alias) expression).child(), TransformationInfo.identity()).traverse();
  }
}
