/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.column.visitors;

import io.openlineage.flink.column.expression.Expression;
import io.openlineage.flink.column.expression.ExpressionContainer;
import io.openlineage.flink.column.expression.TableFieldExpression;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;

public class TableScanVisitor implements CalciteNodeVisitor {

  @Override
  public boolean isDefinedAt(RelNode node) {
    return node instanceof TableScan;
  }

  @Override
  public List<Expression> loadExpressions(RelNode node, ExpressionContainer expressionContainer) {
    TableScan scan = (TableScan) node;
    List<String> tableName = scan.getTable().getQualifiedName();

    return scan.getRowType().getFieldList().stream()
        .map(
            f ->
                TableFieldExpression.builder()
                    .uuid(UUID.randomUUID())
                    .relNode(scan)
                    .outputRelNodeOrdinal(f.getIndex())
                    .tableName(tableName)
                    .name(f.getName())
                    .build())
        .collect(Collectors.toList());
  }
}
