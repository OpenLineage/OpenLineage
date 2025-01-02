/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.column.visitors;

import io.openlineage.flink.column.expression.Expression;
import io.openlineage.flink.column.expression.ExpressionContainer;
import io.openlineage.flink.column.expression.TransformationExpression;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.planner.plan.nodes.calcite.WatermarkAssigner;

/**
 * Column lineage wise, watermark assigner does not influence the lineage. However, we need to
 * support it so that we can traverse the RelNode tree.
 */
public class WatermarkAssignerVisitor implements CalciteNodeVisitor {

  @Override
  public boolean isDefinedAt(RelNode node) {
    return node instanceof WatermarkAssigner;
  }

  @Override
  public List<Expression> loadExpressions(RelNode node, ExpressionContainer expressionContainer) {
    RelNode input = ((WatermarkAssigner) node).getInput();
    return expressionContainer.getOutputsOf(input).stream()
        .map(
            e ->
                TransformationExpression.builder()
                    .relNode(node)
                    .uuid(UUID.randomUUID())
                    .inputIds(List.of(e.getUuid()))
                    .outputRelNodeOrdinal(e.getOutputRelNodeOrdinal())
                    .build())
        .collect(Collectors.toList());
  }
}
