/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.column.visitors;

import io.openlineage.flink.column.RexUtils;
import io.openlineage.flink.column.expression.Expression;
import io.openlineage.flink.column.expression.ExpressionContainer;
import io.openlineage.flink.column.expression.TransformationExpression;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;

public class ProjectVisitor implements CalciteNodeVisitor {

  @Override
  public boolean isDefinedAt(RelNode node) {
    return node instanceof Project;
  }

  @Override
  public List<Expression> loadExpressions(RelNode node, ExpressionContainer expressionContainer) {
    Project project = (Project)node;
    Map<Integer, Expression> ordinalToInputExpressions = expressionContainer
        .getOutputsOf(project.getInput())
        .stream()
        .collect(Collectors.toMap(Expression::getOutputRelNodeOrdinal, e -> e));

    return project
        .getProjects()
        .stream()
        .map(rexNode -> {
          List<UUID> inputIds = RexUtils
              .getRexDependencies(rexNode)
              .stream()
              .filter(ordinalToInputExpressions::containsKey)
              .map(ordinalToInputExpressions::get)
              .map(Expression::getUuid)
              .collect(Collectors.toList());

          return TransformationExpression
              .builder()
              .inputRelNode(project.getInput())
              .uuid(UUID.randomUUID())
              .inputIds(inputIds)
              .rexNode(rexNode)
              .outputRelNodeOrdinal(project.getProjects().indexOf(rexNode))
              .transformation(rexNode.toString())
              .build();
        })
        .collect(Collectors.toList());
  }

}
