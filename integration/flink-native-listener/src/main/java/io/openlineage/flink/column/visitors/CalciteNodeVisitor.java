/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.column.visitors;

import io.openlineage.flink.column.expression.Expression;
import io.openlineage.flink.column.expression.ExpressionContainer;
import java.util.List;
import org.apache.calcite.rel.RelNode;

public interface CalciteNodeVisitor {

  boolean isDefinedAt(RelNode node);

  List<Expression> loadExpressions(RelNode node, ExpressionContainer expressionContainer);
}
