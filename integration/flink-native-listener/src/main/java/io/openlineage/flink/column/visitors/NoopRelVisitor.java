/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.column.visitors;

import io.openlineage.flink.column.expression.Expression;
import io.openlineage.flink.column.expression.ExpressionContainer;
import java.util.List;
import org.apache.calcite.rel.RelNode;

/**
 * Default RelNode visitor to label node as visited although
 * not detecting any field transformations.
 */
public class NoopRelVisitor implements CalciteNodeVisitor {

  @Override
  public boolean isDefinedAt(RelNode node) {
    return true;
  }

  @Override
  public List<Expression> loadExpressions(RelNode node,
      ExpressionContainer expressionContainer) {
    return List.of();
  }
}
